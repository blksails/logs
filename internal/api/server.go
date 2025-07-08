package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"pkg.blksails.net/logs/internal/models"
	"pkg.blksails.net/logs/internal/storage"
)

// Server 表示 API 服务器
type Server struct {
	storage storage.Storage
	router  *gin.Engine
	srv     *http.Server
}

// Config API 服务器配置
type Config struct {
	Host string
	Port int
}

// NewServer 创建新的 API 服务器
func NewServer(storage storage.Storage, cfg *Config) *Server {
	router := gin.Default()
	server := &Server{
		storage: storage,
		router:  router,
		srv: &http.Server{
			Addr:    fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
			Handler: router,
		},
	}

	server.setupRoutes()
	return server
}

// Start 启动服务器
func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
	// 配置 CORS
	s.router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Schema 相关路由
	s.router.POST("/api/v1/schemas", s.createSchema)
	s.router.PUT("/api/v1/schemas/:project/:table", s.updateSchema)
	s.router.DELETE("/api/v1/schemas/:project/:table", s.deleteSchema)
	s.router.GET("/api/v1/schemas/:project/:table", s.getSchema)
	s.router.GET("/api/v1/schemas", s.listSchemas)

	// 日志相关路由
	s.router.POST("/api/v1/logs/:project/:table", s.insertLog)
	s.router.POST("/api/v1/logs/:project/:table/batch", s.batchInsertLogs)
	s.router.POST("/api/v1/test", s.test)
}

// createSchema 创建 schema
func (s *Server) createSchema(c *gin.Context) {
	var schema models.Schema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 设置时间戳
	now := time.Now()
	schema.CreatedAt = now
	schema.UpdatedAt = now

	// 验证 schema
	if err := schema.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 创建 schema
	if err := s.storage.CreateSchema(c.Request.Context(), &schema); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, schema)
}

// updateSchema 更新 schema
func (s *Server) updateSchema(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")

	var schema models.Schema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 确保路径参数匹配
	if schema.Project != project || schema.Table != table {
		c.JSON(http.StatusBadRequest, gin.H{"error": "project and table in path must match body"})
		return
	}

	// 更新时间戳
	schema.UpdatedAt = time.Now()

	// 验证 schema
	if err := schema.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 更新 schema
	if err := s.storage.UpdateSchema(c.Request.Context(), &schema); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, schema)
}

// deleteSchema 删除 schema
func (s *Server) deleteSchema(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")

	if err := s.storage.DeleteSchema(c.Request.Context(), project, table); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

// getSchema 获取 schema
func (s *Server) getSchema(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")

	schema, err := s.storage.GetSchema(c.Request.Context(), project, table)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, schema)
}

// listSchemas 列出所有 schema
func (s *Server) listSchemas(c *gin.Context) {
	schemas, err := s.storage.ListSchemas(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, schemas)
}

// deserializeLogEntry 反序列化日志条目
func (s *Server) deserializeLogEntry(c *gin.Context, project, table string, rawData map[string]interface{}) (*models.LogEntry, error) {
	// 获取 schema
	schema, err := s.storage.GetSchema(c.Request.Context(), project, table)
	if err != nil {
		return nil, fmt.Errorf("schema not found: %v", err)
	}

	// 创建日志条目
	log := &models.LogEntry{
		Project:   project,
		Table:     table,
		Timestamp: time.Now(),
		IP:        c.ClientIP(),
		Fields:    make(map[string]interface{}),
	}

	// 处理基本字段
	if level, ok := rawData["level"].(string); ok {
		log.Level = level
		delete(rawData, "level")
	}
	if message, ok := rawData["message"].(string); ok {
		log.Message = message
		delete(rawData, "message")
	}
	if timestamp, ok := rawData["timestamp"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
			log.Timestamp = t
		}
		delete(rawData, "timestamp")
	}

	// 找到 Rest 字段（如果存在）
	var restField *models.Field
	for _, field := range schema.Fields {
		if field.Type == models.FieldTypeRest {
			restField = field
			break
		}
	}

	// 处理其他字段
	for name, value := range rawData {
		// 查找字段定义
		var fieldDef *models.Field
		for _, field := range schema.Fields {
			if field.Name == name {
				fieldDef = field
				break
			}
		}

		// 如果字段在 schema 中定义
		if fieldDef != nil {
			// 根据字段类型转换值
			convertedValue, err := convertFieldValue(value, fieldDef.Type)
			if err != nil {
				return nil, fmt.Errorf("invalid field value for %s: %v", name, err)
			}
			log.Fields[name] = convertedValue
		} else if restField != nil {
			// 如果字段未定义但有 Rest 字段，将值添加到 Rest 字段
			if restFields, ok := log.Fields[restField.Name].(map[string]interface{}); ok {
				restFields[name] = value
			} else {
				log.Fields[restField.Name] = map[string]interface{}{name: value}
			}
		}
	}

	// 验证日志数据
	if err := schema.ValidateLogEntry(log); err != nil {
		return nil, fmt.Errorf("invalid log data: %v", err)
	}

	return log, nil
}

// insertLog 插入单条日志
func (s *Server) insertLog(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")
	XJA4 := c.GetHeader("X-JA4")              // 获取 X-JA4 头
	XJA4String := c.GetHeader("X-JA4-String") // 获取 X-JA4-String 头
	fmt.Println("XJA4", XJA4)
	fmt.Println("XJA4String", XJA4String)
	// 存入 context
	c.Set("XJA4", XJA4)
	c.Set("XJA4String", XJA4String)

	// 解析请求数据
	var rawData map[string]interface{}
	if err := c.ShouldBindJSON(&rawData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	fmt.Println("rawData", rawData)

	// 反序列化日志条目
	log, err := s.deserializeLogEntry(c, project, table, rawData)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	fmt.Println("log数据", log)

	// 插入日志
	if err := s.storage.InsertLog(c.Request.Context(), project, table, log); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusCreated)
}

// insertLog 插入单条日志
func (s *Server) test(c *gin.Context) {

	log := &models.LogEntry{
		Project:   "myapp",
		Table:     "applogs",
		Level:     "INFO",
		Message:   "用户登录成功",
		Timestamp: time.Now(),
		IP:        "192.168.1.1",
	}
	err := s.storage.InsertLog(c.Request.Context(), "myapp", "applogs", log)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "success",
	})
}

// batchInsertLogs 批量插入日志
func (s *Server) batchInsertLogs(c *gin.Context) {
	project := c.Param("project")
	table := c.Param("table")

	// 解析请求数据
	var rawLogs []map[string]interface{}
	if err := c.ShouldBindJSON(&rawLogs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 处理每条日志
	logs := make([]*models.LogEntry, 0, len(rawLogs))
	for _, rawData := range rawLogs {
		// 反序列化日志条目
		log, err := s.deserializeLogEntry(c, project, table, rawData)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		logs = append(logs, log)
	}

	// 批量插入日志
	if err := s.storage.BatchInsertLogs(c.Request.Context(), project, table, logs); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusCreated)
}

// convertFieldValue 根据字段类型转换值
func convertFieldValue(value interface{}, fieldType models.FieldType) (interface{}, error) {
	switch fieldType {
	case models.FieldTypeString:
		switch v := value.(type) {
		case string:
			return v, nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case models.FieldTypeInt:
		switch v := value.(type) {
		case float64:
			return int64(v), nil
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", value)
		}
	case models.FieldTypeFloat:
		switch v := value.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}
	case models.FieldTypeBool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}
	case models.FieldTypeDateTime:
		switch v := value.(type) {
		case string:
			return time.Parse(time.RFC3339, v)
		case time.Time:
			return v, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to datetime", value)
		}
	case models.FieldTypeTime:
		switch v := value.(type) {
		case string:
			return time.Parse("15:04:05", v)
		default:
			return nil, fmt.Errorf("cannot convert %T to time", value)
		}
	case models.FieldTypeDuration:
		switch v := value.(type) {
		case string:
			// 尝试解析常见的持续时间格式
			if strings.HasSuffix(v, "ms") {
				ms, err := strconv.ParseInt(strings.TrimSuffix(v, "ms"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid duration format: %v", err)
				}
				return time.Duration(ms) * time.Millisecond, nil
			}
			if strings.HasSuffix(v, "s") {
				s, err := strconv.ParseInt(strings.TrimSuffix(v, "s"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid duration format: %v", err)
				}
				return time.Duration(s) * time.Second, nil
			}
			if strings.HasSuffix(v, "m") {
				m, err := strconv.ParseInt(strings.TrimSuffix(v, "m"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid duration format: %v", err)
				}
				return time.Duration(m) * time.Minute, nil
			}
			if strings.HasSuffix(v, "h") {
				h, err := strconv.ParseInt(strings.TrimSuffix(v, "h"), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid duration format: %v", err)
				}
				return time.Duration(h) * time.Hour, nil
			}
			// 尝试使用标准库解析
			return time.ParseDuration(v)
		case int:
			return time.Duration(v) * time.Second, nil
		case int64:
			return time.Duration(v) * time.Second, nil
		case float64:
			return time.Duration(v * float64(time.Second)), nil
		case time.Duration:
			return v, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to duration", value)
		}
	case models.FieldTypeJSON:
		// 将值转换为 JSON 字符串
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %v", err)
		}
		return string(jsonBytes), nil
	case models.FieldTypeRest:
		// 将值转换为 JSON 字符串
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %v", err)
		}
		return string(jsonBytes), nil
	default:
		return nil, fmt.Errorf("unsupported field type: %s", fieldType)
	}
}
