package models

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ErrSchemaNotFound is returned when a schema is not found
var ErrSchemaNotFound = fmt.Errorf("schema not found")

// FieldType 表示字段类型
type FieldType string

const (
	// 基本类型
	FieldTypeString   FieldType = "string"
	FieldTypeInt      FieldType = "int"
	FieldTypeFloat    FieldType = "float"
	FieldTypeBool     FieldType = "bool"
	FieldTypeDateTime FieldType = "datetime"
	FieldTypeTime     FieldType = "time"
	FieldTypeDuration FieldType = "duration"
	FieldTypeJSON     FieldType = "json"
	FieldTypeRest     FieldType = "rest" // 新增 Rest 类型

	// 复杂类型
	FieldTypeObject FieldType = "object"
	FieldTypeArray  FieldType = "array"
)

// Field 表示 schema 中的字段定义
type Field struct {
	Name        string      `yaml:"name" json:"name"`
	Type        FieldType   `yaml:"type" json:"type"`
	Required    bool        `yaml:"required" json:"required"`
	Indexed     bool        `yaml:"indexed" json:"indexed"`
	Description string      `yaml:"description,omitempty" json:"description,omitempty"`
	Default     interface{} `yaml:"default,omitempty" json:"default,omitempty"`
	Rest        bool        `yaml:"rest,omitempty" json:"rest,omitempty"` // 新增 Rest 标记

	// 用于复杂类型
	Fields    []*Field  `yaml:"fields,omitempty" json:"fields,omitempty"`       // 对象类型的子字段
	ItemType  FieldType `yaml:"item_type,omitempty" json:"item_type,omitempty"` // 数组元素类型
	MaxLength *int      `yaml:"max_length,omitempty" json:"max_length,omitempty"`
	MinLength *int      `yaml:"min_length,omitempty" json:"min_length,omitempty"`
	MaxValue  *float64  `yaml:"max_value,omitempty" json:"max_value,omitempty"`
	MinValue  *float64  `yaml:"min_value,omitempty" json:"min_value,omitempty"`
	Pattern   string    `yaml:"pattern,omitempty" json:"pattern,omitempty"`
}

// Schema 表示日志的 schema 定义
type Schema struct {
	Project     string    `yaml:"project" json:"project"`         // 项目名称
	Table       string    `yaml:"table" json:"table"`             // 表名
	Description string    `yaml:"description" json:"description"` // 描述
	Version     string    `yaml:"version" json:"version"`         // 版本号
	Fields      []*Field  `yaml:"fields" json:"fields"`           // 字段定义
	CreatedAt   time.Time `yaml:"created_at" json:"created_at"`   // 创建时间
	UpdatedAt   time.Time `yaml:"updated_at" json:"updated_at"`   // 更新时间
}

// SchemaRegistry 管理 schema 注册
type SchemaRegistry struct {
	schemas map[string]*Schema // key: project:table
}

// NewSchemaRegistry 创建新的 schema 注册表
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string]*Schema),
	}
}

// Register 注册新的 schema
func (r *SchemaRegistry) Register(schema *Schema) error {
	key := fmt.Sprintf("%s:%s", schema.Project, schema.Table)
	if _, exists := r.schemas[key]; exists {
		return fmt.Errorf("schema already exists: %s", key)
	}

	// 验证字段
	if err := r.validateSchema(schema); err != nil {
		return err
	}

	r.schemas[key] = schema
	return nil
}

// Get 获取 schema
func (r *SchemaRegistry) Get(project, table string) (*Schema, error) {
	key := fmt.Sprintf("%s:%s", project, table)
	schema, exists := r.schemas[key]
	if !exists {
		return nil, fmt.Errorf("schema not found: %s", key)
	}
	return schema, nil
}

// validateSchema 验证 schema
func (r *SchemaRegistry) validateSchema(schema *Schema) error {
	if schema.Project == "" {
		return fmt.Errorf("project name is required")
	}
	if schema.Table == "" {
		return fmt.Errorf("table name is required")
	}

	// 验证字段名称唯一性
	fieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		if field.Name == "" {
			return fmt.Errorf("field name is required")
		}
		if fieldNames[field.Name] {
			return fmt.Errorf("duplicate field name: %s", field.Name)
		}
		fieldNames[field.Name] = true

		// 验证字段类型
		switch field.Type {
		case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDateTime, FieldTypeJSON, FieldTypeTime, FieldTypeDuration:
			// 有效类型
		default:
			return fmt.Errorf("invalid field type: %s", field.Type)
		}
	}

	return nil
}

// GenerateTableSQL 生成创建表的 SQL 语句
func (s *Schema) GenerateTableSQL(dbType string) (string, error) {
	switch dbType {
	case "clickhouse":
		return s.generateClickHouseSQL()
	case "postgres":
		return s.generatePostgresSQL()
	default:
		return "", fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// generateClickHouseSQL 生成 ClickHouse 建表语句
func (s *Schema) generateClickHouseSQL() (string, error) {
	columns := []string{
		"id String",
		"project String",
		"table String",
		"level String",
		"message String",
		"timestamp DateTime",
	}

	// 添加自定义字段
	for _, field := range s.Fields {
		var columnType string
		switch field.Type {
		case FieldTypeString:
			columnType = "String"
		case FieldTypeInt:
			columnType = "Int64"
		case FieldTypeFloat:
			columnType = "Float64"
		case FieldTypeBool:
			columnType = "UInt8"
		case FieldTypeDateTime:
			columnType = "DateTime"
		case FieldTypeTime:
			columnType = "DateTime64(3)" // ClickHouse 没有 time 类型，用高精度 DateTime64 代替
		case FieldTypeDuration:
			columnType = "Int64" // duration 用 Int64 存储纳秒
		case FieldTypeJSON, FieldTypeRest:
			columnType = "String"
		default:
			return "", fmt.Errorf("unsupported field type: %s", field.Type)
		}
		columns = append(columns, fmt.Sprintf("%s %s", field.Name, columnType))
	}

	// 添加索引
	indexes := []string{
		"INDEX idx_project_table project, table",
		"INDEX idx_timestamp timestamp",
	}
	for _, field := range s.Fields {
		if field.Indexed {
			indexes = append(indexes, fmt.Sprintf("INDEX idx_%s %s", field.Name, field.Name))
		}
	}

	// 生成建表语句
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s (
		%s,
		%s
	) ENGINE = MergeTree()
	ORDER BY (project, table, timestamp)
	`, s.Project, s.Table, strings.Join(columns, ",\n\t\t"), strings.Join(indexes, ",\n\t\t"))

	return sql, nil
}

// generatePostgresSQL 生成 PostgreSQL 建表语句
func (s *Schema) generatePostgresSQL() (string, error) {
	columns := []string{
		"id VARCHAR(36) PRIMARY KEY",
		"project VARCHAR(255) NOT NULL",
		"table_name VARCHAR(255) NOT NULL",
		"level VARCHAR(50) NOT NULL",
		"message TEXT NOT NULL",
		"timestamp TIMESTAMP NOT NULL",
	}

	// 添加自定义字段
	for _, field := range s.Fields {
		var columnType string
		switch field.Type {
		case FieldTypeString:
			columnType = "TEXT"
		case FieldTypeInt:
			columnType = "BIGINT"
		case FieldTypeFloat:
			columnType = "DOUBLE PRECISION"
		case FieldTypeBool:
			columnType = "BOOLEAN"
		case FieldTypeDateTime:
			columnType = "TIMESTAMP WITH TIME ZONE"
		case FieldTypeTime:
			columnType = "TIME"
		case FieldTypeDuration:
			columnType = "BIGINT" // duration 用 BIGINT 存储纳秒
		case FieldTypeJSON, FieldTypeRest:
			columnType = "JSONB"
		default:
			columnType = "TEXT"
		}
		columns = append(columns, fmt.Sprintf("%s %s", field.Name, columnType))
	}

	// 生成建表语句
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s.%s (
		%s
	);
	`, s.Project, s.Table, strings.Join(columns, ",\n\t\t"))

	// 添加索引
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_project_table ON %s.%s (project, table_name);", s.Table, s.Project, s.Table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_timestamp ON %s.%s (timestamp);", s.Table, s.Project, s.Table),
	}
	for _, field := range s.Fields {
		if field.Indexed {
			indexes = append(indexes, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s.%s (%s);",
				s.Table, field.Name, s.Project, s.Table, field.Name))
		}
	}

	return sql + "\n" + strings.Join(indexes, "\n"), nil
}

// GetTableName 获取表名
func (s *Schema) GetTableName() string {
	return fmt.Sprintf("%s_%s", s.Project, s.Table)
}

// YAMLSchema 定义 YAML 格式的 schema 配置
type YAMLSchema struct {
	Project     string      `yaml:"project"`
	Table       string      `yaml:"table"`
	Description string      `yaml:"description,omitempty"`
	Fields      []YAMLField `yaml:"fields"`
}

// YAMLField 定义 YAML 格式的字段配置
type YAMLField struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Description string `yaml:"description,omitempty"`
	Required    bool   `yaml:"required"`
	Indexed     bool   `yaml:"indexed"`
}

// FromYAML 从 YAML 数据创建 Schema
func SchemaFromYAML(data []byte) (*Schema, error) {
	var yamlSchema YAMLSchema
	if err := yaml.Unmarshal(data, &yamlSchema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	schema := &Schema{
		Project:     yamlSchema.Project,
		Table:       yamlSchema.Table,
		Description: yamlSchema.Description,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	for _, yamlField := range yamlSchema.Fields {
		fieldType := FieldType(yamlField.Type)
		switch fieldType {
		case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool,
			FieldTypeDateTime, FieldTypeJSON, FieldTypeTime, FieldTypeDuration:
			// 有效类型
		default:
			return nil, fmt.Errorf("invalid field type for field %s: %s", yamlField.Name, yamlField.Type)
		}

		field := &Field{
			Name:        yamlField.Name,
			Type:        fieldType,
			Description: yamlField.Description,
			Required:    yamlField.Required,
			Indexed:     yamlField.Indexed,
		}
		schema.Fields = append(schema.Fields, field)
	}

	return schema, nil
}

// ToYAML 将 Schema 转换为 YAML 格式
func (s *Schema) ToYAML() ([]byte, error) {
	yamlSchema := YAMLSchema{
		Project:     s.Project,
		Table:       s.Table,
		Description: s.Description,
	}

	for _, field := range s.Fields {
		yamlField := YAMLField{
			Name:        field.Name,
			Type:        string(field.Type),
			Description: field.Description,
			Required:    field.Required,
			Indexed:     field.Indexed,
		}
		yamlSchema.Fields = append(yamlSchema.Fields, yamlField)
	}

	return yaml.Marshal(yamlSchema)
}

// LoadSchemaFromFile 从 YAML 文件加载 Schema
func LoadSchemaFromFile(filename string) (*Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取 schema 文件失败: %w", err)
	}

	var schema Schema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("解析 schema 文件失败: %w", err)
	}

	return &schema, nil
}

// SaveSchemaToFile 将 Schema 保存为 YAML 文件
func (s *Schema) SaveToFile(filename string) error {
	s.UpdatedAt = time.Now()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = s.UpdatedAt
	}

	data, err := yaml.Marshal(s)
	if err != nil {
		return fmt.Errorf("序列化 schema 失败: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("保存 schema 文件失败: %w", err)
	}

	return nil
}

// ValidateLogEntry 验证日志条目是否符合 schema 定义
func (s *Schema) ValidateLogEntry(entry *LogEntry) error {
	if entry.Project != s.Project || entry.Table != s.Table {
		return fmt.Errorf("project 或 table 不匹配")
	}

	// 验证基本字段
	if entry.Level == "" {
		return fmt.Errorf("level 字段不能为空")
	}
	if entry.Message == "" {
		return fmt.Errorf("message 字段不能为空")
	}
	if entry.Timestamp.IsZero() {
		return fmt.Errorf("timestamp 字段不能为空")
	}

	// 找到 Rest 字段（如果存在）
	var restField *Field
	for _, field := range s.Fields {
		if field.Type == FieldTypeRest {
			restField = field
			break
		}
	}

	// 验证必填字段
	for _, field := range s.Fields {
		if field.Type == FieldTypeRest {
			continue // 跳过 Rest 字段的必填验证
		}

		// 跳过基本字段的验证
		if strings.ToLower(field.Name) == "level" || strings.ToLower(field.Name) == "message" || strings.ToLower(field.Name) == "timestamp" {
			continue
		}

		value, exists := entry.Fields[strings.ToLower(field.Name)]
		if field.Required && !exists {
			return fmt.Errorf("缺少必填字段: %s", field.Name)
		}
		if !exists {
			continue
		}

		// 验证字段类型
		if err := s.validateFieldValue(field.Type, value); err != nil {
			return fmt.Errorf("字段 %s 类型错误: %w", field.Name, err)
		}
	}

	// 如果有 Rest 字段，收集所有未定义的字段
	if restField != nil {
		restFields := make(map[string]interface{})
		for name, value := range entry.Fields {
			// 检查字段是否已在 schema 中定义
			isDefined := false
			for _, field := range s.Fields {
				if field.Name == name {
					isDefined = true
					break
				}
			}
			if !isDefined {
				restFields[name] = value
			}
		}
		// 将未定义的字段放入 Rest 字段
		if len(restFields) > 0 {
			entry.Fields[restField.Name] = restFields
		}
	}

	return nil
}

// validateFieldValue 验证字段值是否符合指定类型
func (s *Schema) validateFieldValue(fieldType FieldType, value interface{}) error {
	if value == nil {
		return nil
	}

	switch fieldType {
	case FieldTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("期望 string 类型")
		}
	case FieldTypeInt:
		switch v := value.(type) {
		case int, int32, int64, float64:
			// 数字类型都可以接受
		default:
			return fmt.Errorf("期望 int 类型，实际为 %T", v)
		}
	case FieldTypeFloat:
		switch v := value.(type) {
		case float32, float64:
			// float 类型可以接受
		default:
			return fmt.Errorf("期望 float 类型，实际为 %T", v)
		}
	case FieldTypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("期望 bool 类型")
		}
	case FieldTypeDateTime:
		switch v := value.(type) {
		case string:
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				return fmt.Errorf("无效的日期时间格式，期望 RFC3339 格式")
			}
		case time.Time:
			// time.Time 类型可以接受
		default:
			return fmt.Errorf("期望 datetime 类型")
		}
	case FieldTypeTime:
		if str, ok := value.(string); ok {
			if _, err := time.Parse("15:04:05", str); err != nil {
				return fmt.Errorf("无效的时间格式，期望 HH:MM:SS 格式")
			}
		} else {
			return fmt.Errorf("期望 time 类型")
		}
	case FieldTypeDuration:
		switch v := value.(type) {
		case string:
			if _, err := time.ParseDuration(v); err != nil {
				return fmt.Errorf("无效的持续时间格式")
			}
		case int, int64:
			// 假设是秒数
		case float64:
			// 假设是秒数
		case time.Duration:
			// time.Duration 类型可以接受
		default:
			return fmt.Errorf("期望 duration 类型")
		}
	case FieldTypeJSON, FieldTypeRest:
		// JSON 和 Rest 类型可以是任何值
	default:
		return fmt.Errorf("未知的字段类型: %s", fieldType)
	}

	return nil
}

// Validate 验证 schema 是否有效
func (s *Schema) Validate() error {
	if s.Project == "" {
		return fmt.Errorf("project name is required")
	}
	if s.Table == "" {
		return fmt.Errorf("table name is required")
	}
	if len(s.Fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}

	// 验证字段
	fieldNames := make(map[string]bool)
	for _, field := range s.Fields {
		if err := validateField(field, fieldNames); err != nil {
			return err
		}
	}

	return nil
}

// validateField 验证字段定义是否有效
func validateField(field *Field, fieldNames map[string]bool) error {
	if field.Name == "" {
		return fmt.Errorf("field name is required")
	}
	if fieldNames[field.Name] {
		return fmt.Errorf("duplicate field name: %s", field.Name)
	}
	fieldNames[field.Name] = true

	switch field.Type {
	case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDateTime,
		FieldTypeTime, FieldTypeDuration, FieldTypeJSON, FieldTypeRest:
		// 基本类型不需要额外验证
	case FieldTypeObject:
		if len(field.Fields) == 0 {
			return fmt.Errorf("object field %s must have sub-fields", field.Name)
		}
		// 验证子字段
		subFieldNames := make(map[string]bool)
		for _, subField := range field.Fields {
			if err := validateField(subField, subFieldNames); err != nil {
				return fmt.Errorf("in field %s: %w", field.Name, err)
			}
		}
	case FieldTypeArray:
		if field.ItemType == "" {
			return fmt.Errorf("array field %s must specify item_type", field.Name)
		}
		switch field.ItemType {
		case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDateTime,
			FieldTypeObject, FieldTypeJSON, FieldTypeRest:
			// 有效的数组元素类型
		default:
			return fmt.Errorf("invalid array item type for field %s: %s", field.Name, field.ItemType)
		}
	default:
		return fmt.Errorf("invalid field type for field %s: %s", field.Name, field.Type)
	}

	return nil
}
