package newapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/lzy98276/upstream-ops/backend/connector"
)

// AdminTarget identifies a New API administrator endpoint. APIKey must be a
// Root Token because channel administration is a root-only operation.
type AdminTarget struct {
	BaseURL string
	APIKey  string
}

// AdminChannel is the subset of New API's channel resource that upstream
// synchronization owns. New API uses type 60 for a New API-compatible channel.
type AdminChannel struct {
	ID                 int64          `json:"id"`
	Type               int            `json:"type"`
	Key                string         `json:"key"`
	Status             int            `json:"status"`
	Name               string         `json:"name"`
	Weight             uint           `json:"weight"`
	BaseURL            string         `json:"base_url"`
	Models             string         `json:"models"`
	Group              string         `json:"group"`
	ModelMapping       string         `json:"model_mapping"`
	Priority           int64          `json:"priority"`
	Remark             string         `json:"remark"`
	AutoBan            int            `json:"auto_ban"`
	Setting            string         `json:"setting"`
	OtherSettings      string         `json:"settings"`
	OpenAIOrganization string         `json:"openai_organization"`
	TestModel          string         `json:"test_model"`
	Other              string         `json:"other"`
	OtherInfo          string         `json:"other_info"`
	StatusCodeMapping  string         `json:"status_code_mapping"`
	Tag                string         `json:"tag"`
	ParamOverride      string         `json:"param_override"`
	HeaderOverride     string         `json:"header_override"`
	ChannelInfo        map[string]any `json:"channel_info"`
}

const (
	ChannelTypeNewAPI = 60
	ChannelStatusOn   = 1
	ChannelStatusOff  = 2
)

// AdminClient wraps the New API administrator channel API.
type AdminClient struct {
	client *Client
}

func NewAdminClient() *AdminClient { return &AdminClient{client: New()} }

func (a *AdminClient) Ping(ctx context.Context, target AdminTarget) error {
	_, err := a.ListChannels(ctx, target, 1, 1)
	return err
}

func (a *AdminClient) ListChannels(ctx context.Context, target AdminTarget, page, pageSize int) ([]AdminChannel, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	resp, err := a.request(ctx, target).
		SetQueryParams(map[string]string{
			"p":         strconv.Itoa(page),
			"page_size": strconv.Itoa(pageSize),
		}).
		Get(strings.TrimRight(target.BaseURL, "/") + "/api/channel/")
	if err != nil {
		return nil, err
	}
	data, err := decodeAdminResponse(resp.StatusCode(), resp.Body())
	if err != nil {
		return nil, fmt.Errorf("list New API channels: %w", err)
	}
	var wrapped struct {
		Items []AdminChannel `json:"items"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.Items != nil {
		return wrapped.Items, nil
	}
	var list []AdminChannel
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("decode New API channel list: %w", err)
	}
	return list, nil
}

func (a *AdminClient) ListAllChannels(ctx context.Context, target AdminTarget) ([]AdminChannel, error) {
	const pageSize = 100
	all := make([]AdminChannel, 0)
	for page := 1; page <= 100; page++ {
		items, err := a.ListChannels(ctx, target, page, pageSize)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
		if len(items) < pageSize {
			return all, nil
		}
	}
	return nil, errors.New("New API channel list exceeds 10000 items")
}

func (a *AdminClient) FindChannelByName(ctx context.Context, target AdminTarget, name string) (*AdminChannel, error) {
	items, err := a.ListAllChannels(ctx, target)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if strings.EqualFold(strings.TrimSpace(items[i].Name), strings.TrimSpace(name)) {
			return &items[i], nil
		}
	}
	return nil, nil
}

func (a *AdminClient) CreateChannel(ctx context.Context, target AdminTarget, channel AdminChannel) error {
	body, err := json.Marshal(map[string]any{
		"mode":    "single",
		"channel": newAPIChannelPayload(channel, false),
	})
	if err != nil {
		return err
	}
	resp, err := a.request(ctx, target).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Post(strings.TrimRight(target.BaseURL, "/") + "/api/channel/")
	if err != nil {
		return err
	}
	if _, err := decodeAdminResponse(resp.StatusCode(), resp.Body()); err != nil {
		return fmt.Errorf("create New API channel: %w", err)
	}
	return nil
}

func (a *AdminClient) UpdateChannel(ctx context.Context, target AdminTarget, channel AdminChannel) error {
	body, err := json.Marshal(newAPIChannelPayload(channel, true))
	if err != nil {
		return err
	}
	resp, err := a.request(ctx, target).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Put(strings.TrimRight(target.BaseURL, "/") + "/api/channel/")
	if err != nil {
		return err
	}
	if _, err := decodeAdminResponse(resp.StatusCode(), resp.Body()); err != nil {
		return fmt.Errorf("update New API channel: %w", err)
	}
	return nil
}

func (a *AdminClient) SetChannelStatus(ctx context.Context, target AdminTarget, id int64, status int) error {
	body, err := json.Marshal(map[string]int{"status": status})
	if err != nil {
		return err
	}
	resp, err := a.request(ctx, target).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Post(strings.TrimRight(target.BaseURL, "/") + "/api/channel/" + strconv.FormatInt(id, 10) + "/status")
	if err != nil {
		return err
	}
	if _, err := decodeAdminResponse(resp.StatusCode(), resp.Body()); err != nil {
		return fmt.Errorf("set New API channel status: %w", err)
	}
	return nil
}

func (a *AdminClient) DeleteChannel(ctx context.Context, target AdminTarget, id int64) error {
	resp, err := a.request(ctx, target).
		Delete(strings.TrimRight(target.BaseURL, "/") + "/api/channel/" + strconv.FormatInt(id, 10))
	if err != nil {
		return err
	}
	if _, err := decodeAdminResponse(resp.StatusCode(), resp.Body()); err != nil {
		return fmt.Errorf("delete New API channel: %w", err)
	}
	return nil
}

func (a *AdminClient) request(ctx context.Context, target AdminTarget) *resty.Request {
	return a.client.http.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("Authorization", "Bearer "+target.APIKey)
}

func newAPIChannelPayload(channel AdminChannel, includeID bool) map[string]any {
	payload := map[string]any{
		"type":                channel.Type,
		"key":                 channel.Key,
		"name":                channel.Name,
		"weight":              channel.Weight,
		"base_url":            channel.BaseURL,
		"models":              channel.Models,
		"group":               channel.Group,
		"model_mapping":       channel.ModelMapping,
		"priority":            channel.Priority,
		"remark":              channel.Remark,
		"auto_ban":            channel.AutoBan,
		"setting":             channel.Setting,
		"settings":            channel.OtherSettings,
		"openai_organization": channel.OpenAIOrganization,
		"test_model":          channel.TestModel,
		"other":               channel.Other,
		"other_info":          channel.OtherInfo,
		"status_code_mapping": channel.StatusCodeMapping,
		"tag":                 channel.Tag,
		"param_override":      channel.ParamOverride,
		"header_override":     channel.HeaderOverride,
		"channel_info":        channel.ChannelInfo,
	}
	if includeID {
		payload["id"] = channel.ID
	}
	return payload
}

func decodeAdminResponse(statusCode int, body []byte) (json.RawMessage, error) {
	if statusCode < 200 || statusCode >= 300 {
		return nil, connector.HTTPStatusError(statusCode, body)
	}
	var wrapped struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("decode New API administrator response: %w", err)
	}
	if !wrapped.Success {
		message := strings.TrimSpace(wrapped.Message)
		if message == "" {
			message = "New API rejected administrator request"
		}
		return nil, errors.New(message)
	}
	return wrapped.Data, nil
}
