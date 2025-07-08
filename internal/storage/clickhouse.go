package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"pkg.blksails.net/logs/internal/models"
)

// ClickHouseStorage ClickHouse 存储实现
type ClickHouseStorage struct {
	db     *sql.DB
	config Config
}

// NewClickHouseStorage 创建 ClickHouse 存储实例
func NewClickHouseStorage(config Config) *ClickHouseStorage {
	return &ClickHouseStorage{
		config: config,
	}
}

// Initialize 初始化 ClickHouse 连接和表结构
func (s *ClickHouseStorage) Initialize(ctx context.Context) error {
	// 构建连接字符串
	connStr := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s&read_timeout=20s",
		s.config.ClickHouse.Username,
		s.config.ClickHouse.Password,
		s.config.ClickHouse.Host,
		s.config.ClickHouse.Port,
		s.config.ClickHouse.Database,
	)

	// 连接数据库
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	s.db = db

	// 创建 schema 表
	if err := s.createSchemaTable(ctx); err != nil {
		return err
	}

	return nil
}

// createSchemaTable 创建 schema 表
func (s *ClickHouseStorage) createSchemaTable(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS schemas (
		project String,
		table_name String,
		description String,
		fields String,
		created_at DateTime64(3),
		updated_at DateTime64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (project, table_name)`

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建 schema 表失败: %w", err)
	}

	return nil
}

// CreateSchema 创建或更新 schema
func (s *ClickHouseStorage) CreateSchema(ctx context.Context, schema *models.Schema) error {
	// 将字段转换为 JSON
	fieldsJSON, err := json.Marshal(schema.Fields)
	if err != nil {
		return fmt.Errorf("序列化字段失败: %w", err)
	}

	// 将字节数组转换为字符串
	fieldsJSONString := string(fieldsJSON)

	// 创建日志表
	if err := s.createLogTable(ctx, schema); err != nil {
		return err
	}

	// 保存 schema
	query := `
	INSERT INTO schemas (project, table_name, description, fields, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, query,
		schema.Project,
		schema.Table,
		schema.Description,
		fieldsJSONString,
		schema.CreatedAt,
		schema.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("保存 schema 失败: %w", err)
	}

	return nil
}

// GetSchema 获取指定的 schema
func (s *ClickHouseStorage) GetSchema(ctx context.Context, project, table string) (*models.Schema, error) {
	query := `
	SELECT description, fields, created_at, updated_at
	FROM schemas
	WHERE project = ? AND table_name = ?
	ORDER BY updated_at DESC
	LIMIT 1`

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

	fmt.Println("fieldsJSON string:", string(fieldsJSON)) // 会显示为真实的 JSON 字符串

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
func (s *ClickHouseStorage) createLogTable(ctx context.Context, schema *models.Schema) error {
	// 构建表名
	tableName := fmt.Sprintf("logs_%s_%s", schema.Project, schema.Table)

	// 构建字段定义
	columns := []string{
		"id String",
		"project String",
		"table_name String",
		"timestamp DateTime64(3)",
	}

	// 添加自定义字段
	for _, field := range schema.Fields {
		colType := s.getClickHouseType(field.Type)
		colDef := fmt.Sprintf("%s %s", field.Name, colType)
		columns = append(columns, colDef)
	}

	// 创建表
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		%s
	) ENGINE = MergeTree()
	ORDER BY (timestamp, id)
	PARTITION BY toYYYYMM(timestamp)`,
		tableName,
		strings.Join(columns, ",\n"),
	)

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("创建日志表失败: %w", err)
	}

	// 为索引字段创建物化视图
	for _, field := range schema.Fields {
		if field.Indexed {
			viewName := fmt.Sprintf("%s_%s_mv", tableName, field.Name)
			viewQuery := fmt.Sprintf(`
			CREATE MATERIALIZED VIEW IF NOT EXISTS %s
			ENGINE = MergeTree()
			ORDER BY (%s, timestamp)
			PARTITION BY toYYYYMM(timestamp)
			AS SELECT *
			FROM %s`,
				viewName, field.Name, tableName,
			)
			if _, err := s.db.ExecContext(ctx, viewQuery); err != nil {
				return fmt.Errorf("创建物化视图失败: %w", err)
			}
		}
	}

	return nil
}

// getClickHouseType 获取 ClickHouse 字段类型
func (s *ClickHouseStorage) getClickHouseType(fieldType models.FieldType) string {
	switch fieldType {
	case models.FieldTypeString:
		return "String"
	case models.FieldTypeInt:
		return "Int64"
	case models.FieldTypeFloat:
		return "Float64"
	case models.FieldTypeBool:
		return "UInt8"
	case models.FieldTypeDateTime:
		return "DateTime64(3)"
	case models.FieldTypeTime:
		return "String"
	case models.FieldTypeDuration:
		return "Int64" // 存储为纳秒
	case models.FieldTypeJSON:
		return "String"
	default:
		return "String"
	}
}

// Store 存储单条日志
func (s *ClickHouseStorage) Store(ctx context.Context, log *models.LogEntry) error {
	// 获取 schema
	schema, err := s.GetSchema(ctx, log.Project, log.Table)
	if err != nil {
		return fmt.Errorf("获取 schema 失败: %w", err)
	}

	// 验证日志数据
	if err := schema.ValidateLogEntry(log); err != nil {
		return fmt.Errorf("日志数据验证失败: %w", err)
	}

	// 构建表名
	tableName := fmt.Sprintf("logs_%s_%s", log.Project, log.Table)

	// 构建插入语句
	columns := []string{"id", "project", "table_name", "timestamp"}
	values := []interface{}{log.ID, log.Project, log.Table, log.Timestamp}
	placeholders := []string{"?", "?", "?", "?"}

	for _, field := range schema.Fields {
		if value, ok := log.Fields[field.Name]; ok {
			columns = append(columns, field.Name)
			values = append(values, value)
			placeholders = append(placeholders, "?")
		}
	}

	query := fmt.Sprintf(`
	INSERT INTO %s (%s)
	VALUES (%s)`,
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	if _, err := s.db.ExecContext(ctx, query, values...); err != nil {
		return fmt.Errorf("插入日志失败: %w", err)
	}

	return nil
}

// BatchStore 批量存储日志
func (s *ClickHouseStorage) BatchStore(ctx context.Context, logs []*models.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// 使用事务批量插入
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	for _, log := range logs {
		if err := s.Store(ctx, log); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// Close 关闭数据库连接
func (s *ClickHouseStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// BatchInsertLogs 批量插入日志
func (s *ClickHouseStorage) BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}

	// 打印日志的 JSON 格式（调试用）
	logsJSON, err := json.MarshalIndent(logs, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling logs:", err)
		return err
	}
	fmt.Println("logs:", string(logsJSON))

	// 获取 schema
	schema, err := s.GetSchema(ctx, project, table)
	if err != nil {
		return fmt.Errorf("获取 schema 失败: %w", err)
	}

	// 使用事务批量插入
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	// 构建表名
	tableName := fmt.Sprintf("logs_%s_%s", project, table)

	// 准备字段列表
	var columns []string
	for _, field := range schema.Fields {
		columns = append(columns, field.Name)
	}

	// 批量插入
	for _, log := range logs {
		// 验证日志数据
		if err := schema.ValidateLogEntry(log); err != nil {
			return fmt.Errorf("日志数据验证失败: %w", err)
		}

		values := make([]interface{}, 0, len(columns))
		placeholders := make([]string, 0, len(columns))
		for _, col := range columns {
			if value, ok := log.Fields[col]; ok {
				values = append(values, value)
				placeholders = append(placeholders, "?")
			}
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))

		if _, err := tx.ExecContext(ctx, query, values...); err != nil {
			return fmt.Errorf("插入日志失败: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

//// BatchInsertLogs 批量插入日志
//func (s *ClickHouseStorage) BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error {
//	if len(logs) == 0 {
//		return nil
//	}
//
//	// 打印日志的 JSON 格式（调试用）
//	logsJSON, err := json.MarshalIndent(logs, "", "  ")
//	if err != nil {
//		fmt.Println("Error marshalling logs:", err)
//		return err
//	}
//	fmt.Println("logs:", string(logsJSON))
//
//	// 使用事务批量插入
//	tx, err := s.db.BeginTx(ctx, nil)
//	if err != nil {
//		return fmt.Errorf("开始事务失败: %w", err)
//	}
//	defer tx.Rollback()
//
//	// 构建表名
//	tableName := fmt.Sprintf("logs_%s_%s", project, table)
//
//	// 准备插入的字段列表（即 logs 中的 key）
//	var columns []string
//	var allValues []interface{}
//	var allPlaceholders []string
//
//	for _, log := range logs {
//		// 从 log 的根级别提取 'level' 和 'message' 字段
//		values := make([]interface{}, 0)
//		placeholders := make([]string, 0)
//
//		// 根级字段 level 和 message
//		columns = append(columns, "level", "message")
//		values = append(values, log.Level, log.Message)
//		placeholders = append(placeholders, "?", "?")
//
//		// 从 log.Fields 中提取出字段名（key）和值（value）
//		for key, value := range log.Fields {
//			columns = append(columns, key) // 将 key 作为列名
//			values = append(values, value) // 将 value 作为值
//			placeholders = append(placeholders, "?")
//		}
//
//		// 准备一个占位符，并将其添加到查询中
//		allPlaceholders = append(allPlaceholders, "("+strings.Join(placeholders, ", ")+")")
//		allValues = append(allValues, values...)
//	}
//
//	// 批量插入查询
//	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
//		tableName,
//		strings.Join(columns, ", "),
//		strings.Join(allPlaceholders, ", "),
//	)
//
//	// 打印最终的 SQL 查询（调试用）
//	logrus.Info("Executing Query: ", query)
//	fmt.Println("SQL Query: ", query)
//
//	// 执行批量插入
//	if _, err := tx.ExecContext(ctx, query, allValues...); err != nil {
//		return fmt.Errorf("插入日志失败: %w", err)
//	}
//
//	// 提交事务
//	if err := tx.Commit(); err != nil {
//		return fmt.Errorf("提交事务失败: %w", err)
//	}
//
//	return nil
//}

func printQuery(query string, values []interface{}) string {
	return fmt.Sprintf("Executing query: %s with values: %v", query, values)
}

// CountLogs 统计日志数量
func (s *ClickHouseStorage) CountLogs(ctx context.Context, project, table string, query map[string]interface{}) (int64, error) {
	// 构建表名
	tableName := fmt.Sprintf("logs_%s_%s", project, table)

	// 构建查询条件
	conditions := make([]string, 0, len(query))
	values := make([]interface{}, 0, len(query))
	paramCount := 1

	for key, value := range query {
		conditions = append(conditions, fmt.Sprintf("%s = ?", key))
		values = append(values, value)
		paramCount++
	}

	// 构建 SQL 语句
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if len(conditions) > 0 {
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}

	// 执行查询
	var count int64
	err := s.db.QueryRowContext(ctx, sql, values...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("统计日志失败: %w", err)
	}

	return count, nil
}

// DeleteSchema 删除 schema
func (s *ClickHouseStorage) DeleteSchema(ctx context.Context, project, table string) error {
	// 开启事务
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}
	defer tx.Rollback()

	// 删除 schema 元数据
	query := `DELETE FROM schemas WHERE project = ? AND table_name = ?`
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
	tableName := fmt.Sprintf("logs_%s_%s", project, table)
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

// InsertLog 插入单条日志
func (s *ClickHouseStorage) InsertLog(ctx context.Context, project, table string, log *models.LogEntry) error {
	return s.BatchInsertLogs(ctx, project, table, []*models.LogEntry{log})
}

// ListSchemas 列出所有 schemas
func (s *ClickHouseStorage) ListSchemas(ctx context.Context) ([]*models.Schema, error) {
	query := `
	SELECT project, table_name, description, fields, created_at, updated_at
	FROM schemas
	GROUP BY project, table_name, description, fields, created_at, updated_at`

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

// Ping 测试数据库连接
func (s *ClickHouseStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// QueryLogs 查询日志
func (s *ClickHouseStorage) QueryLogs(ctx context.Context, project, table string, query map[string]interface{}, limit, offset int) ([]map[string]interface{}, error) {
	// 构建表名
	tableName := fmt.Sprintf("logs_%s_%s", project, table)

	// 构建查询条件
	conditions := make([]string, 0, len(query))
	values := make([]interface{}, 0, len(query))
	paramCount := 1

	for key, value := range query {
		conditions = append(conditions, fmt.Sprintf("%s = ?", key))
		values = append(values, value)
		paramCount++
	}

	// 构建 SQL 语句
	sql := fmt.Sprintf("SELECT * FROM %s", tableName)
	if len(conditions) > 0 {
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}
	sql += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	// 执行查询
	rows, err := s.db.QueryContext(ctx, sql, values...)
	if err != nil {
		return nil, fmt.Errorf("查询日志失败: %w", err)
	}
	defer rows.Close()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取列名失败: %w", err)
	}

	// 准备结果
	var result []map[string]interface{}
	for rows.Next() {
		// 创建值容器
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		// 构建行数据
		row := make(map[string]interface{})
		for i, col := range columns {
			if values[i] != nil {
				row[col] = values[i]
			}
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历结果失败: %w", err)
	}

	return result, nil
}

// UpdateSchema 更新 schema
func (s *ClickHouseStorage) UpdateSchema(ctx context.Context, schema *models.Schema) error {
	return s.CreateSchema(ctx, schema)
}

var _ Storage = (*ClickHouseStorage)(nil)
