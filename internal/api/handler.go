package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"pkg.blksails.net/logs/internal/models"
	"pkg.blksails.net/logs/internal/storage"
)

type Handler struct {
	storage storage.Storage
}

func NewHandler(storage storage.Storage) *Handler {
	return &Handler{
		storage: storage,
	}
}

// RegisterRoutes 注册路由
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api/v1")
	{
		api.POST("/logs", h.handleLog)
		api.POST("/logs/batch", h.handleBatchLog)
	}
}

// handleLog 处理单条日志
func (h *Handler) handleLog(c *gin.Context) {
	var req models.LogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	timestamp := time.Now()
	if req.Timestamp != nil {
		timestamp = *req.Timestamp
	}

	// get ip from request
	ip := c.ClientIP()

	log := &models.LogEntry{
		Project:   req.Project,
		Table:     req.Table,
		Level:     req.Level,
		Message:   req.Message,
		Timestamp: timestamp,
		IP:        ip,
		Fields:    req.Fields,
		Tags:      req.Tags,
	}

	if err := h.storage.InsertLog(c.Request.Context(), req.Project, req.Table, log); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"id": log.ID})
}

// handleBatchLog 处理批量日志
func (h *Handler) handleBatchLog(c *gin.Context) {
	var req models.BatchLogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	logs := make([]*models.LogEntry, 0, len(req.Logs))
	for _, r := range req.Logs {
		timestamp := time.Now()
		if r.Timestamp != nil {
			timestamp = *r.Timestamp
		}

		log := &models.LogEntry{
			Project:   r.Project,
			Table:     r.Table,
			Level:     r.Level,
			Message:   r.Message,
			Timestamp: timestamp,
			Fields:    r.Fields,
			Tags:      r.Tags,
		}
		logs = append(logs, log)
	}

	if err := h.storage.BatchInsertLogs(c.Request.Context(), req.Logs[0].Project, req.Logs[0].Table, logs); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"count": len(logs)})
}
