package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/syncer"
)

func registerUpstreamSync(g *gin.RouterGroup, d *Deps) {
	if d.UpstreamSync == nil {
		return
	}
	gp := g.Group("/upstream-sync")
	gp.GET("/targets", func(c *gin.Context) { listUpstreamSyncTargets(c, d) })
	gp.POST("/targets", func(c *gin.Context) { createUpstreamSyncTarget(c, d) })
	gp.PUT("/targets/:id", func(c *gin.Context) { updateUpstreamSyncTarget(c, d) })
	gp.DELETE("/targets/:id", func(c *gin.Context) { deleteUpstreamSyncTarget(c, d) })
	gp.POST("/targets/:id/check", func(c *gin.Context) { checkUpstreamSyncTarget(c, d) })
	gp.GET("/targets/:id/newapi/auto-groups", func(c *gin.Context) { getNewAPIAutoGroups(c, d) })
	gp.PUT("/targets/:id/newapi/auto-groups", func(c *gin.Context) { updateNewAPIAutoGroups(c, d) })
	gp.GET("/targets/:id/newapi/groups", func(c *gin.Context) { listNewAPIGroups(c, d) })
	gp.POST("/targets/:id/newapi/groups", func(c *gin.Context) { saveNewAPIGroup(c, d) })
	gp.PUT("/targets/:id/newapi/groups/:name", func(c *gin.Context) { updateNewAPIGroup(c, d) })
	gp.DELETE("/targets/:id/newapi/groups/:name", func(c *gin.Context) { deleteNewAPIGroup(c, d) })
	gp.POST("/targets/:id/groups/sync", func(c *gin.Context) { syncUpstreamSyncTargetGroups(c, d) })
	gp.GET("/targets/:id/groups", func(c *gin.Context) { listUpstreamSyncTargetGroups(c, d) })
	gp.PUT("/targets/:id/groups/sort-order", func(c *gin.Context) { reorderUpstreamSyncTargetGroups(c, d) })
	gp.GET("/targets/:id/proxies", func(c *gin.Context) { listUpstreamSyncTargetProxies(c, d) })
	gp.GET("/source-models", func(c *gin.Context) { listUpstreamSyncSourceModels(c, d) })
	gp.GET("/sync-groups", func(c *gin.Context) { listUpstreamSyncGroups(c, d) })
	gp.POST("/sync-groups", func(c *gin.Context) { createUpstreamSyncGroup(c, d) })
	gp.PUT("/sync-groups/reorder", func(c *gin.Context) { reorderUpstreamSyncGroups(c, d) })
	gp.PUT("/sync-groups/:id", func(c *gin.Context) { updateUpstreamSyncGroup(c, d) })
	gp.DELETE("/sync-groups/:id", func(c *gin.Context) { deleteUpstreamSyncGroup(c, d) })
	gp.POST("/sync-groups/:id/apply", func(c *gin.Context) { applyUpstreamSyncGroup(c, d) })
	gp.POST("/sync-groups/:id/delete-managed", func(c *gin.Context) { deleteUpstreamSyncManaged(c, d) })
	gp.GET("/sync-groups/:id/logs", func(c *gin.Context) { listUpstreamSyncGroupLogs(c, d) })
}

func listUpstreamSyncTargets(c *gin.Context, d *Deps) {
	list, err := d.UpstreamSync.ListTargets()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func createUpstreamSyncTarget(c *gin.Context, d *Deps) {
	var in syncer.TargetInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.CreateTarget(c.Request.Context(), in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func updateUpstreamSyncTarget(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in syncer.TargetInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.UpdateTarget(c.Request.Context(), id, in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func deleteUpstreamSyncTarget(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.UpstreamSync.DeleteTarget(id); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func checkUpstreamSyncTarget(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.UpstreamSync.CheckTarget(c.Request.Context(), id); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func getNewAPIAutoGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.GetNewAPIAutoGroups(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func updateNewAPIAutoGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in struct {
		Groups []string `json:"groups"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.UpdateNewAPIAutoGroups(c.Request.Context(), id, in.Groups)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func listNewAPIGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.UpstreamSync.ListNewAPIGroups(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func saveNewAPIGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in syncer.NewAPIGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.SaveNewAPIGroup(c.Request.Context(), id, in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func updateNewAPIGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in syncer.NewAPIGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	in.Name = c.Param("name")
	item, err := d.UpstreamSync.SaveNewAPIGroup(c.Request.Context(), id, in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func deleteNewAPIGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.UpstreamSync.DeleteNewAPIGroup(c.Request.Context(), id, c.Param("name")); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func syncUpstreamSyncTargetGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.UpstreamSync.SyncTargetGroups(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusBadGateway, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func listUpstreamSyncTargetGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	includeMissing := c.Query("include_missing") == "1" || c.Query("include_missing") == "true"
	list, err := d.UpstreamSync.ListTargetGroups(id, includeMissing)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func reorderUpstreamSyncTargetGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in struct {
		OrderedIDs []int64 `json:"ordered_ids"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.UpstreamSync.ReorderTargetGroups(c.Request.Context(), id, in.OrderedIDs)
	if err != nil {
		if errors.Is(err, syncer.ErrInvalidTargetGroupOrder) {
			fail(c, http.StatusBadRequest, err)
			return
		}
		fail(c, http.StatusBadGateway, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func listUpstreamSyncTargetProxies(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.UpstreamSync.ListTargetProxies(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func listUpstreamSyncSourceModels(c *gin.Context, d *Deps) {
	channelID, err := strconv.ParseUint(c.Query("channel_id"), 10, 64)
	if err != nil || channelID == 0 {
		if err == nil {
			err = errors.New("channel_id is required")
		}
		fail(c, http.StatusBadRequest, err)
		return
	}
	in := syncer.SourceModelsInput{
		ChannelID:       uint(channelID),
		SourceGroupName: c.Query("source_group_name"),
		Platform:        c.Query("platform"),
	}
	if raw := c.Query("sync_account_id"); raw != "" {
		id, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			fail(c, http.StatusBadRequest, parseErr)
			return
		}
		in.SyncAccountID = uint(id)
	}
	if raw := c.Query("source_group_id"); raw != "" {
		id, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			fail(c, http.StatusBadRequest, parseErr)
			return
		}
		in.SourceGroupID = &id
	}
	list, err := d.UpstreamSync.ListSourceModels(c.Request.Context(), in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func listUpstreamSyncGroups(c *gin.Context, d *Deps) {
	list, err := d.UpstreamSync.ListSyncGroups()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func createUpstreamSyncGroup(c *gin.Context, d *Deps) {
	var in syncer.SyncGroupDTO
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.CreateSyncGroup(in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func reorderUpstreamSyncGroups(c *gin.Context, d *Deps) {
	var in struct {
		TargetID uint   `json:"target_id"`
		IDs      []uint `json:"ids"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.UpstreamSync.ReorderSyncGroups(in.TargetID, in.IDs)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func updateUpstreamSyncGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in syncer.SyncGroupDTO
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	item, err := d.UpstreamSync.UpdateSyncGroup(id, in)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": item})
}

func deleteUpstreamSyncGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.UpstreamSync.DeleteSyncGroup(id); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func applyUpstreamSyncGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	log, err := d.UpstreamSync.ApplySyncGroup(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": log})
}

func deleteUpstreamSyncManaged(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	log, err := d.UpstreamSync.DeleteManaged(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": log})
}

func listUpstreamSyncGroupLogs(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	page, pageSize, err := parsePageQuery(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, total, err := d.UpstreamSync.ListSyncGroupLogs(id, page, pageSize)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	pages := 1
	if total > 0 {
		pages = int((total + int64(pageSize) - 1) / int64(pageSize))
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"items":     list,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     pages,
	}})
}
