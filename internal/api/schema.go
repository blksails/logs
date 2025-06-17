package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"pkg.blksails.net/logs/internal/models"
	"pkg.blksails.net/logs/internal/storage"
)

// SchemaHandler 处理 schema 相关的请求
type SchemaHandler struct {
	storage storage.Storage
}

// NewSchemaHandler 创建新的 SchemaHandler
func NewSchemaHandler(storage storage.Storage) *SchemaHandler {
	return &SchemaHandler{
		storage: storage,
	}
}

// RegisterRoutes 注册路由
func (h *SchemaHandler) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api/v1/schemas")
	{
		api.POST("", h.createSchema)
		api.GET("/:project/:table", h.getSchema)
	}
}

// createSchema 创建新的 schema
func (h *SchemaHandler) createSchema(c *gin.Context) {
	var schema models.Schema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 设置时间戳
	now := time.Now()
	schema.CreatedAt = now
	schema.UpdatedAt = now

	// 创建 schema
	if err := h.storage.CreateSchema(c.Request.Context(), &schema); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, schema)
}

// getSchema 获取 schema
func (h *SchemaHandler) getSchema(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")

	schema, err := h.storage.GetSchema(c.Request.Context(), project, table)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, schema)
}
