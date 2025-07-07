package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// LogEntry 日志条目
type LogEntry struct {
	ID        int                    `json:"id"`
	Project   string                 `json:"project"`
	Table     string                 `json:"table"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	IP        string                 `json:"ip"`
	Fields    map[string]interface{} `json:"fields"`
	Tags      map[string]string      `json:"tags"`
}

// LogRequest 表示接收日志的请求结构
type LogRequest struct {
	Project   string                 `json:"project" binding:"required"`
	Table     string                 `json:"table" binding:"required"`
	Level     string                 `json:"level" binding:"required"`
	Message   string                 `json:"message" binding:"required"`
	Timestamp *time.Time             `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields"`
	Tags      map[string]string      `json:"tags"`
}

// BatchLogRequest 表示批量接收日志的请求结构
type BatchLogRequest struct {
	Logs []LogRequest `json:"logs" binding:"required"`
}

// NewLogEntry 创建新的日志条目
func NewLogEntry(project, table string) *LogEntry {
	return &LogEntry{
		Project:   project,
		Table:     table,
		Timestamp: time.Now(),
		Fields:    make(map[string]interface{}),
	}
}

// SetField 设置字段值
func (l *LogEntry) SetField(name string, value interface{}) {
	if l.Fields == nil {
		l.Fields = make(map[string]interface{})
	}
	l.Fields[name] = value
}

// GetField 获取字段值
func (l *LogEntry) GetField(name string) (interface{}, bool) {
	if l.Fields == nil {
		return nil, false
	}
	value, ok := l.Fields[name]
	return value, ok
}

// HasField 检查字段是否存在
func (l *LogEntry) HasField(name string) bool {
	if l.Fields == nil {
		return false
	}
	_, ok := l.Fields[name]
	return ok
}

// ValidateFields 根据 schema 验证字段
func (l *LogEntry) ValidateFields(schema *Schema) error {
	if schema == nil {
		return nil
	}

	// 验证必填字段
	for _, field := range schema.Fields {
		if field.Required {
			if _, exists := l.Fields[field.Name]; !exists {
				return fmt.Errorf("required field missing: %s", field.Name)
			}
		}
	}

	// 验证字段类型
	for name, value := range l.Fields {
		// 查找字段定义
		var fieldDef *Field
		for i := range schema.Fields {
			if schema.Fields[i].Name == name {
				fieldDef = schema.Fields[i]
				break
			}
		}

		if fieldDef == nil {
			// 如果字段不在 schema 中定义，跳过验证
			continue
		}

		// 验证字段类型
		if err := validateFieldType(value, fieldDef.Type); err != nil {
			return fmt.Errorf("invalid field type for %s: %v", name, err)
		}
	}

	return nil
}

// validateFieldType 验证字段类型
func validateFieldType(value interface{}, fieldType FieldType) error {
	switch fieldType {
	case FieldTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case FieldTypeInt:
		switch v := value.(type) {
		case int, int32, int64, float64:
			// 允许数字类型
		default:
			return fmt.Errorf("expected number, got %T", v)
		}
	case FieldTypeFloat:
		switch v := value.(type) {
		case float32, float64:
			// 允许浮点数类型
		default:
			return fmt.Errorf("expected float, got %T", v)
		}
	case FieldTypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case FieldTypeDateTime:
		switch v := value.(type) {
		case time.Time:
			// 允许时间类型
		case string:
			// 尝试解析时间字符串
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				return fmt.Errorf("invalid datetime string: %v", err)
			}
		default:
			return fmt.Errorf("expected datetime, got %T", v)
		}
	case FieldTypeTime:
		_, ok := value.(string)
		if ok {
			// 可以进一步用正则或 time.Parse 校验格式
			return nil
		}
		_, ok = value.(time.Time)
		if ok {
			// 允许时间类型
			return nil
		}
		return fmt.Errorf("expected time, got %T", value)
	case FieldTypeDuration:
		_, ok := value.(int64)
		if ok {
			return nil
		}
		_, ok = value.(string)
		if ok {
			// 可以进一步用 time.ParseDuration 校验
			return nil
		}
		return fmt.Errorf("expected duration, got %T", value)
	case FieldTypeJSON:
		// 对于 JSON 类型，我们只验证它是否可以序列化为 JSON
		if _, err := json.Marshal(value); err != nil {
			return fmt.Errorf("invalid JSON value: %v", err)
		}
	default:
		return fmt.Errorf("unsupported field type: %s", fieldType)
	}

	return nil
}
