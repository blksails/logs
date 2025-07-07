package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"pkg.blksails.net/logs/internal/models"

	_ "github.com/lib/pq"
)

// PostgresStorage PostgreSQL 存储实现
type PostgresStorage struct {
	db     *sql.DB
	config Config
	schema string
	logger *zap.Logger
}

// NewPostgresStorage 创建 PostgreSQL 存储实例
func NewPostgresStorage(config Config) *PostgresStorage {
	logger := config.Logger
	if logger == nil {
		logger = zap.L()
	}
	return &PostgresStorage{
		config: config,
		logger: logger,
	}
}

// Initialize 初始化 PostgreSQL 连接和表结构
func (s *PostgresStorage) Initialize(ctx context.Context) error {
	// 构建连接字符串
	schema := s.config.Postgres.Schema
	if schema == "" {
		schema = "logs"
	}
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable search_path=%s",
		s.config.Postgres.Host,
		s.config.Postgres.Port,
		s.config.Postgres.Username,
		s.config.Postgres.Password,
		s.config.Postgres.Database,
		schema,
	)

	// 连接数据库
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	s.db = db
	s.schema = schema

	// 创建 logs schema
	if err := s.createLogsSchema(ctx); err != nil {
		return err
	}

	// 设置默认 search_path
	if err := s.setSearchPath(ctx); err != nil {
		return err
	}

	// 创建 schema 表
	if err := s.createSchemaTable(ctx); err != nil {
		return err
	}

	return nil
}

// createLogsSchema 创建 logs schema
func (s *PostgresStorage) createLogsSchema(ctx context.Context) error {
	query := `CREATE SCHEMA IF NOT EXISTS ` + quote(s.schema)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建 logs schema 失败: %w", err)
	}
	return nil
}

// setSearchPath 设置默认 search_path
func (s *PostgresStorage) setSearchPath(ctx context.Context) error {
	query := `SET search_path TO ` + quote(s.schema)
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("设置 search_path 失败: %w", err)
	}
	return nil
}

// createSchemaTable 创建 schema 表
func (s *PostgresStorage) createSchemaTable(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS schemas (
		project VARCHAR(255),
		table_name VARCHAR(255),
		description TEXT,
		fields JSONB,
		created_at TIMESTAMP WITH TIME ZONE,
		updated_at TIMESTAMP WITH TIME ZONE,
		PRIMARY KEY (project, table_name)
	)`

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建 schema 表失败: %w", err)
	}

	return nil
}

// CreateSchema 创建或更新 schema
func (s *PostgresStorage) CreateSchema(ctx context.Context, schema *models.Schema) error {
	// 将字段转换为 JSON
	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil {
		return fmt.Errorf("序列化字段失败: %w", err)
	}

	// 创建日志表
	if err := s.createLogTable(ctx, schema); err != nil {
		return err
	}

	// 保存 schema
	query := `
	INSERT INTO schemas (project, table_name, description, fields, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (project, table_name) DO UPDATE
	SET description = EXCLUDED.description,
		fields = EXCLUDED.fields,
		updated_at = EXCLUDED.updated_at`

	_, err = s.db.ExecContext(ctx, query,
		schema.Project,
		schema.Table,
		schema.Description,
		fieldsJSON,
		schema.CreatedAt,
		schema.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("保存 schema 失败: %w", err)
	}

	return nil
}

// GetSchema 获取指定的 schema
func (s *PostgresStorage) GetSchema(ctx context.Context, project, table string) (*models.Schema, error) {
	query := `
	SELECT description, fields, created_at, updated_at
	FROM schemas
	WHERE project = $1 AND table_name = $2`

	var (
		description string
		fieldsJSON  []byte
		createdAt   time.Time
		updatedAt   time.Time
	)

	err := s.db.QueryRowContext(ctx, query, project, table).Scan(
		&description,
		&fieldsJSON,
		&createdAt,
		&updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schema not found")
	}
	if err != nil {
		return nil, fmt.Errorf("查询 schema 失败: %w", err)
	}

	var fields []models.Field
	if err := json.Unmarshal(fieldsJSON, &fields); err != nil {
		return nil, fmt.Errorf("解析字段失败: %w", err)
	}

	// Convert []Field to []*Field
	fieldPtrs := make([]*models.Field, len(fields))
	for i := range fields {
		fieldPtrs[i] = &fields[i]
	}

	return &models.Schema{
		Project:     project,
		Table:       table,
		Description: description,
		Fields:      fieldPtrs,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

// createLogTable 创建日志表
func (s *PostgresStorage) createLogTable(ctx context.Context, schema *models.Schema) error {
	// 构建表名
	tableName := fmt.Sprintf("%s.%s_%s", quote(s.schema), schema.Project, schema.Table)

	// 构建基础字段定义
	columns := []string{
		"id SERIAL PRIMARY KEY",
		"project VARCHAR(255)",
		"table_name VARCHAR(255)",
		"timestamp TIMESTAMP WITH TIME ZONE",
	}

	// 默认字段列表
	defaultFields := map[string]string{
		"level":   "VARCHAR(50)",
		"message": "TEXT",
		"ip":      "VARCHAR(45)",
	}

	// 检查schema中是否已定义默认字段，如果没有则添加
	schemaFieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		schemaFieldNames[field.Name] = true
	}

	// 添加未在schema中定义的默认字段
	for fieldName, fieldType := range defaultFields {
		if !schemaFieldNames[fieldName] {
			columns = append(columns, fmt.Sprintf("%s %s", fieldName, fieldType))
		}
	}

	// 添加自定义字段
	for _, field := range schema.Fields {
		colType := s.getPostgresType(field.Type)
		colDef := fmt.Sprintf("%s %s", field.Name, colType)
		columns = append(columns, colDef)
	}

	// 创建表
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		%s
	)`, tableName, strings.Join(columns, ",\n"))

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建日志表失败: %w", err)
	}

	pureTableName := fmt.Sprintf("%s_%s", schema.Project, schema.Table)

	// 为索引字段创建索引
	for _, field := range schema.Fields {
		if field.Indexed {
			indexName := fmt.Sprintf("idx_%s_%s", pureTableName, field.Name)
			indexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
				indexName, pureTableName, field.Name)
			if _, err := s.db.ExecContext(ctx, indexQuery); err != nil {
				return fmt.Errorf("创建索引失败: %w", err)
			}
		}
	}

	return nil
}

// getPostgresType 获取 PostgreSQL 字段类型
func (s *PostgresStorage) getPostgresType(fieldType models.FieldType) string {
	switch fieldType {
	case models.FieldTypeString:
		return "TEXT"
	case models.FieldTypeInt:
		return "BIGINT"
	case models.FieldTypeFloat:
		return "DOUBLE PRECISION"
	case models.FieldTypeBool:
		return "BOOLEAN"
	case models.FieldTypeDateTime:
		return "TIMESTAMP WITH TIME ZONE"
	case models.FieldTypeTime:
		return "TIME"
	case models.FieldTypeDuration:
		return "INTERVAL"
	case models.FieldTypeJSON, models.FieldTypeRest:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// InsertLog 插入单条日志
func (s *PostgresStorage) InsertLog(ctx context.Context, project, table string, log *models.LogEntry) error {
	return s.BatchInsertLogs(ctx, project, table, []*models.LogEntry{log})
}

// Close 关闭数据库连接
func (s *PostgresStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Ping 测试数据库连接
func (s *PostgresStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// UpdateSchema 更新 schema
func (s *PostgresStorage) UpdateSchema(ctx context.Context, schema *models.Schema) error {
	return s.CreateSchema(ctx, schema)
}

// ListSchemas 列出所有 schemas
func (s *PostgresStorage) ListSchemas(ctx context.Context) ([]*models.Schema, error) {
	query := `SELECT project, table_name, description, fields, created_at, updated_at FROM schemas`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询 schemas 失败: %w", err)
	}
	defer rows.Close()

	var schemas []*models.Schema
	for rows.Next() {
		var schema models.Schema
		var fieldsJSON []byte
		err := rows.Scan(
			&schema.Project,
			&schema.Table,
			&schema.Description,
			&fieldsJSON,
			&schema.CreatedAt,
			&schema.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		var fields []*models.Field
		if err := json.Unmarshal(fieldsJSON, &fields); err != nil {
			return nil, fmt.Errorf("解析字段失败: %w", err)
		}
		schema.Fields = fields
		schemas = append(schemas, &schema)
	}

	return schemas, nil
}

// BatchInsertLogs 批量插入日志
func (s *PostgresStorage) BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// 获取 schema
	schema, err := s.GetSchema(ctx, project, table)
	if err != nil {
		return fmt.Errorf("获取 schema 失败: %w", err)
	}

	// 找到 Rest 字段（如果存在）
	var restField *models.Field
	for _, field := range schema.Fields {
		if field.Type == models.FieldTypeRest {
			restField = field
			break
		}
	}

	// 使用事务批量插入
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	// 构建表名
	tableName := fmt.Sprintf("%s.%s_%s", quote(s.schema), project, table)

	// 准备字段列表
	var columns []string
	// 添加基础字段
	columns = append(columns, "project", "table_name", "timestamp")

	// 默认字段列表
	defaultFieldNames := []string{"level", "message", "ip"}

	// 检查schema中是否已定义默认字段
	schemaFieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		schemaFieldNames[field.Name] = true
	}

	// 添加未在schema中定义的默认字段
	for _, fieldName := range defaultFieldNames {
		if !schemaFieldNames[fieldName] {
			columns = append(columns, fieldName)
		}
	}

	// 添加自定义字段
	for _, field := range schema.Fields {
		if field.Type != models.FieldTypeRest {
			columns = append(columns, field.Name)
		}
	}

	// 如果有 Rest 字段，添加到列名列表
	if restField != nil {
		columns = append(columns, restField.Name)
	}

	// 批量插入
	for _, log := range logs {
		// 验证日志数据
		if err := schema.ValidateLogEntry(log); err != nil {
			return fmt.Errorf("日志数据验证失败: %w", err)
		}

		// 构建插入语句
		values := make([]interface{}, 0, len(columns))
		placeholders := make([]string, 0, len(columns))
		paramCount := 1

		// 处理所有字段
		for _, col := range columns {
			var value interface{}

			// 根据字段名获取对应的值
			switch col {
			case "project":
				value = log.Project
			case "table_name":
				value = log.Table
			case "timestamp":
				value = log.Timestamp
			case "level":
				value = log.Level
			case "message":
				value = log.Message
			case "ip":
				value = log.IP
			default:
				// 处理自定义字段
				if restField != nil && col == restField.Name {
					// 处理 Rest 字段
					if restValue, ok := log.Fields[restField.Name]; ok {
						// 将 Rest 字段转换为 JSON 字符串
						jsonBytes, err := json.Marshal(restValue)
						if err != nil {
							return fmt.Errorf("序列化 Rest 字段失败: %w", err)
						}
						value = string(jsonBytes)
					} else {
						value = "{}"
					}
				} else if fieldValue, ok := log.Fields[col]; ok {
					// 如果是 map 类型，转换为 JSON 字符串
					if m, ok := fieldValue.(map[string]interface{}); ok {
						jsonBytes, err := json.Marshal(m)
						if err != nil {
							return fmt.Errorf("序列化字段 %s 失败: %w", col, err)
						}
						value = string(jsonBytes)
					} else {
						value = fieldValue
					}
				} else {
					value = nil
				}
			}

			values = append(values, value)
			placeholders = append(placeholders, fmt.Sprintf("$%d", paramCount))
			paramCount++
		}

		query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
		RETURNING id`,
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
		)

		s.logger.Info("insert log", zap.String("query", query), zap.Any("values", values))

		var id int64
		if err := tx.QueryRowContext(ctx, query, values...).Scan(&id); err != nil {
			return fmt.Errorf("插入日志失败: %w", err)
		}

		// 更新日志ID
		log.ID = int(id)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// DeleteSchema 删除 schema
func (s *PostgresStorage) DeleteSchema(ctx context.Context, project, table string) error {
	// 开启事务
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	// 删除 schema 元数据
	query := `
	DELETE FROM schemas
	WHERE project = $1 AND table_name = $2`

	result, err := tx.ExecContext(ctx, query, project, table)
	if err != nil {
		return fmt.Errorf("删除 schema 失败: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("获取影响行数失败: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("schema not found: %s_%s", project, table)
	}

	// 删除日志表
	tableName := fmt.Sprintf("%s.%s_%s", quote(s.schema), project, table)
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := tx.ExecContext(ctx, dropQuery); err != nil {
		return fmt.Errorf("删除日志表失败: %w", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

var _ Storage = (*PostgresStorage)(nil)

func quote(s string) string {
	return strconv.Quote(s)
}
