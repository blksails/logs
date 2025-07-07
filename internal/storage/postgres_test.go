package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"pkg.blksails.net/logs/internal/models"

	_ "github.com/lib/pq"
)

// 测试配置 - 可以通过环境变量覆盖
var testConfig = Config{
	Type: "postgres",
	Postgres: PostgresConfig{
		Host:     getEnvOrDefault("POSTGRES_HOST", "localhost"),
		Port:     getEnvOrDefaultInt("POSTGRES_PORT", 5432),
		Database: getEnvOrDefault("POSTGRES_DATABASE", "logs_test"),
		Username: getEnvOrDefault("POSTGRES_USERNAME", "postgres"),
		Password: getEnvOrDefault("POSTGRES_PASSWORD", "postgres"),
		Schema:   "logs_test",
	},
	Logger: zap.NewNop(),
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func createTestStorage(t *testing.T) *PostgresStorage {
	storage := NewPostgresStorage(testConfig)

	// 跳过测试如果无法连接到数据库
	if err := storage.Initialize(context.Background()); err != nil {
		t.Skipf("Skipping test: cannot connect to PostgreSQL: %v", err)
	}

	return storage
}

func createTestSchema() *models.Schema {
	return &models.Schema{
		Project:     "test_project",
		Table:       "test_table",
		Description: "Test schema for unit tests",
		Fields: []*models.Field{
			{
				Name:     "user_id",
				Type:     models.FieldTypeString,
				Required: true,
				Indexed:  true,
			},
			{
				Name:     "action",
				Type:     models.FieldTypeString,
				Required: true,
			},
			{
				Name:     "count",
				Type:     models.FieldTypeInt,
				Required: false,
			},
			{
				Name:     "score",
				Type:     models.FieldTypeFloat,
				Required: false,
			},
			{
				Name:     "is_active",
				Type:     models.FieldTypeBool,
				Required: false,
			},
			{
				Name:     "metadata",
				Type:     models.FieldTypeJSON,
				Required: false,
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func createTestLogEntry() *models.LogEntry {
	return &models.LogEntry{
		Project:   "test_project",
		Table:     "test_table",
		Level:     "INFO",
		Message:   "Test log message",
		Timestamp: time.Now(),
		IP:        "192.168.1.100",
		Fields: map[string]interface{}{
			"user_id":   "user123",
			"action":    "login",
			"count":     42,
			"score":     95.5,
			"is_active": true,
			"metadata":  map[string]interface{}{"browser": "chrome", "version": "91.0"},
		},
	}
}

func TestPostgresStorage_NewPostgresStorage(t *testing.T) {
	storage := NewPostgresStorage(testConfig)
	assert.NotNil(t, storage)
	assert.Equal(t, testConfig, storage.config)
	assert.NotNil(t, storage.logger)
}

func TestPostgresStorage_Initialize(t *testing.T) {
	storage := NewPostgresStorage(testConfig)

	// 测试初始化
	err := storage.Initialize(context.Background())
	if err != nil {
		t.Skipf("Skipping test: cannot connect to PostgreSQL: %v", err)
	}

	// 测试连接
	err = storage.Ping(context.Background())
	assert.NoError(t, err)

	// 清理
	defer storage.Close()
}

func TestPostgresStorage_CreateSchema(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 测试创建schema
	err := storage.CreateSchema(context.Background(), schema)
	assert.NoError(t, err)

	// 验证schema是否创建成功
	retrievedSchema, err := storage.GetSchema(context.Background(), schema.Project, schema.Table)
	assert.NoError(t, err)
	assert.Equal(t, schema.Project, retrievedSchema.Project)
	assert.Equal(t, schema.Table, retrievedSchema.Table)
	assert.Equal(t, schema.Description, retrievedSchema.Description)
	assert.Len(t, retrievedSchema.Fields, len(schema.Fields))
}

func TestPostgresStorage_GetSchema(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 先创建schema
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 测试获取存在的schema
	retrievedSchema, err := storage.GetSchema(context.Background(), schema.Project, schema.Table)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedSchema)
	assert.Equal(t, schema.Project, retrievedSchema.Project)
	assert.Equal(t, schema.Table, retrievedSchema.Table)

	// 测试获取不存在的schema
	_, err = storage.GetSchema(context.Background(), "nonexistent", "table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema not found")
}

func TestPostgresStorage_InsertLog(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 先创建schema
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	logEntry := createTestLogEntry()

	// 测试插入日志
	err = storage.InsertLog(context.Background(), schema.Project, schema.Table, logEntry)
	assert.NoError(t, err)
	assert.NotZero(t, logEntry.ID, "Log ID should be set after insertion")

	// 验证日志是否插入成功
	tableName := fmt.Sprintf(`"%s"."%s_%s"`, storage.schema, schema.Project, schema.Table)
	query := fmt.Sprintf("SELECT project, table_name, level, message, ip FROM %s WHERE id = $1", tableName)

	var project, table, level, message, ip string
	row := storage.db.QueryRow(query, logEntry.ID)
	err = row.Scan(&project, &table, &level, &message, &ip)
	assert.NoError(t, err)
	assert.Equal(t, logEntry.Project, project)
	assert.Equal(t, logEntry.Table, table)
	assert.Equal(t, logEntry.Level, level)
	assert.Equal(t, logEntry.Message, message)
	assert.Equal(t, logEntry.IP, ip)
}

func TestPostgresStorage_BatchInsertLogs(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 先创建schema
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 创建多个日志条目
	logs := []*models.LogEntry{
		{
			Project:   schema.Project,
			Table:     schema.Table,
			Level:     "INFO",
			Message:   "First log message",
			Timestamp: time.Now(),
			IP:        "192.168.1.100",
			Fields: map[string]interface{}{
				"user_id": "user1",
				"action":  "login",
				"count":   1,
			},
		},
		{
			Project:   schema.Project,
			Table:     schema.Table,
			Level:     "WARN",
			Message:   "Second log message",
			Timestamp: time.Now(),
			IP:        "192.168.1.101",
			Fields: map[string]interface{}{
				"user_id": "user2",
				"action":  "logout",
				"count":   2,
			},
		},
		{
			Project:   schema.Project,
			Table:     schema.Table,
			Level:     "ERROR",
			Message:   "Third log message",
			Timestamp: time.Now(),
			IP:        "10.0.0.1",
			Fields: map[string]interface{}{
				"user_id": "user3",
				"action":  "error",
				"count":   3,
			},
		},
	}

	// 测试批量插入
	err = storage.BatchInsertLogs(context.Background(), schema.Project, schema.Table, logs)
	assert.NoError(t, err)

	// 验证所有日志ID都被设置
	for i, log := range logs {
		assert.NotZero(t, log.ID, "Log %d ID should be set after insertion", i)
	}

	// 验证插入的日志数量
	tableName := fmt.Sprintf(`"%s"."%s_%s"`, storage.schema, schema.Project, schema.Table)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

	var count int
	row := storage.db.QueryRow(countQuery)
	err = row.Scan(&count)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, count, len(logs))
}

func TestPostgresStorage_InsertLogWithIPField(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 先创建schema
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 测试不同IP格式的日志
	testCases := []struct {
		name string
		ip   string
	}{
		{"IPv4", "192.168.1.100"},
		{"IPv6", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"},
		{"Localhost IPv4", "127.0.0.1"},
		{"Localhost IPv6", "::1"},
		{"Empty IP", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logEntry := &models.LogEntry{
				Project:   schema.Project,
				Table:     schema.Table,
				Level:     "INFO",
				Message:   fmt.Sprintf("Test log with %s", tc.name),
				Timestamp: time.Now(),
				IP:        tc.ip,
				Fields: map[string]interface{}{
					"user_id": "user123",
					"action":  "test",
				},
			}

			err := storage.InsertLog(context.Background(), schema.Project, schema.Table, logEntry)
			assert.NoError(t, err)
			assert.NotZero(t, logEntry.ID)

			// 验证IP字段是否正确存储
			tableName := fmt.Sprintf(`"%s"."%s_%s"`, storage.schema, schema.Project, schema.Table)
			query := fmt.Sprintf("SELECT ip FROM %s WHERE id = $1", tableName)

			var storedIP string
			row := storage.db.QueryRow(query, logEntry.ID)
			err = row.Scan(&storedIP)
			assert.NoError(t, err)
			assert.Equal(t, tc.ip, storedIP)
		})
	}
}

func TestPostgresStorage_EmptyBatchInsert(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()

	// 先创建schema
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 测试空批次插入
	err = storage.BatchInsertLogs(context.Background(), schema.Project, schema.Table, []*models.LogEntry{})
	assert.NoError(t, err)
}

func TestPostgresStorage_Ping(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()

	err := storage.Ping(context.Background())
	assert.NoError(t, err)
}

func TestPostgresStorage_Close(t *testing.T) {
	storage := createTestStorage(t)

	err := storage.Close()
	assert.NoError(t, err)

	// 关闭后应该无法ping
	err = storage.Ping(context.Background())
	assert.Error(t, err)
}

func TestPostgresStorage_SchemaValidation(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 测试插入不符合schema的日志
	invalidLog := &models.LogEntry{
		Project:   schema.Project,
		Table:     schema.Table,
		Level:     "INFO",
		Message:   "Test log",
		Timestamp: time.Now(),
		IP:        "192.168.1.100",
		Fields: map[string]interface{}{
			// 缺少必填字段 user_id
			"action": "test",
		},
	}

	err = storage.InsertLog(context.Background(), schema.Project, schema.Table, invalidLog)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required field missing")
}

// 辅助函数：检查表是否存在
func tableExists(db *sql.DB, schema, tableName string) bool {
	query := `
	SELECT EXISTS (
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = $2
	)`

	var exists bool
	err := db.QueryRow(query, schema, tableName).Scan(&exists)
	return err == nil && exists
}

func TestPostgresStorage_TableCreation(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 验证表是否创建
	tableName := fmt.Sprintf("%s_%s", schema.Project, schema.Table)
	exists := tableExists(storage.db, storage.schema, tableName)
	assert.True(t, exists, "Table should be created")

	// 验证表结构是否包含IP字段
	query := `
	SELECT column_name, data_type 
	FROM information_schema.columns 
	WHERE table_schema = $1 AND table_name = $2 AND column_name = 'ip'`

	var columnName, dataType string
	err = storage.db.QueryRow(query, storage.schema, tableName).Scan(&columnName, &dataType)
	assert.NoError(t, err)
	assert.Equal(t, "ip", columnName)
	assert.Contains(t, dataType, "character")
}

func TestPostgresStorage_DefaultFields(t *testing.T) {
	storage := createTestStorage(t)
	defer storage.Close()
	defer cleanupTestData(t, storage)

	schema := createTestSchema()
	err := storage.CreateSchema(context.Background(), schema)
	require.NoError(t, err)

	// 验证表结构包含所有默认字段
	tableName := fmt.Sprintf("%s_%s", schema.Project, schema.Table)
	query := `
	SELECT column_name 
	FROM information_schema.columns 
	WHERE table_schema = $1 AND table_name = $2 
	ORDER BY column_name`

	rows, err := storage.db.Query(query, storage.schema, tableName)
	require.NoError(t, err)
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		require.NoError(t, err)
		columns = append(columns, columnName)
	}

	// 检查必需的默认字段
	expectedColumns := []string{"id", "project", "table_name", "timestamp", "level", "message", "ip"}
	for _, expected := range expectedColumns {
		found := false
		for _, col := range columns {
			if col == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "Column %s should exist", expected)
	}
}

// 清理测试数据
func cleanupTestData(t *testing.T, storage *PostgresStorage) {
	if storage.db == nil {
		return
	}

	// 删除测试表
	_, _ = storage.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."test_project_test_table"`, storage.schema))

	// 删除测试schema记录
	_, _ = storage.db.Exec(`DELETE FROM schemas WHERE project = 'test_project' AND table_name = 'test_table'`)
}
