package gateway

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/lzy98276/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

const maxPriceSourcePriority = 100000

func (a *AdminService) ListPriceSources() ([]storage.ModelPriceSource, error) {
	if a == nil || a.PriceSources == nil {
		return []storage.ModelPriceSource{}, nil
	}
	return a.PriceSources.List()
}

func (a *AdminService) CreatePriceSource(in CreatePriceSourceInput) (*storage.ModelPriceSource, error) {
	if a == nil || a.PriceSources == nil {
		return nil, errors.New("pricing sources not configured")
	}
	name, rawURL, err := normalizePriceSource(in.Name, in.URL)
	if err != nil {
		return nil, err
	}
	if _, err := a.PriceSources.FindByName(name); err == nil {
		return nil, errors.New("pricing source name already exists")
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	item := &storage.ModelPriceSource{
		Name: name, URL: rawURL, Enabled: enabled, Priority: clampPriceSourcePriority(in.Priority),
	}
	if err := a.PriceSources.Create(item); err != nil {
		return nil, err
	}
	return item, nil
}

func (a *AdminService) UpdatePriceSource(id uint, in UpdatePriceSourceInput) (*storage.ModelPriceSource, error) {
	if a == nil || a.PriceSources == nil {
		return nil, errors.New("pricing sources not configured")
	}
	item, err := a.PriceSources.FindByID(id)
	if err != nil {
		return nil, err
	}
	name, rawURL := item.Name, item.URL
	if in.Name != nil {
		name = *in.Name
	}
	if in.URL != nil {
		rawURL = *in.URL
	}
	name, rawURL, err = normalizePriceSource(name, rawURL)
	if err != nil {
		return nil, err
	}
	if name != item.Name {
		if existing, findErr := a.PriceSources.FindByName(name); findErr == nil && existing.ID != item.ID {
			return nil, errors.New("pricing source name already exists")
		} else if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return nil, findErr
		}
	}
	item.Name, item.URL = name, rawURL
	if in.Enabled != nil {
		item.Enabled = *in.Enabled
	}
	if in.Priority != nil {
		item.Priority = clampPriceSourcePriority(*in.Priority)
	}
	if err := a.PriceSources.Update(item); err != nil {
		return nil, err
	}
	return item, nil
}

func (a *AdminService) DeletePriceSource(id uint) error {
	if a == nil || a.PriceSources == nil {
		return errors.New("pricing sources not configured")
	}
	return a.PriceSources.Delete(id)
}

func normalizePriceSource(name, rawURL string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", errors.New("name is required")
	}
	if len(name) > 128 {
		return "", "", errors.New("name is too long")
	}
	rawURL = strings.TrimSpace(rawURL)
	u, err := url.Parse(rawURL)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", "", fmt.Errorf("url must be an absolute http(s) URL")
	}
	return name, u.String(), nil
}

func clampPriceSourcePriority(priority int) int {
	if priority > maxPriceSourcePriority {
		return maxPriceSourcePriority
	}
	if priority < -maxPriceSourcePriority {
		return -maxPriceSourcePriority
	}
	return priority
}
