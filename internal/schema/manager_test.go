package schema

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pkg.blksails.net/logs/internal/models"
)

type mockStorage struct {
	schemas map[string]*models.Schema
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		schemas: make(map[string]*models.Schema),
	}
}

func (s *mockStorage) Initialize(ctx context.Context) error { return nil }
func (s *mockStorage) BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error {
	return nil
}

func (s *mockStorage) DeleteSchema(ctx context.Context, project, table string) error { return nil }
func (s *mockStorage) InsertLog(ctx context.Context, project, table string, log *models.LogEntry) error {
	return nil
}
func (s *mockStorage) ListSchemas(ctx context.Context) ([]*models.Schema, error) { return nil, nil }
func (s *mockStorage) Close() error                                              { return nil }
func (s *mockStorage) Ping(ctx context.Context) error                            { return nil }

func (s *mockStorage) UpdateSchema(ctx context.Context, schema *models.Schema) error {
	key := schema.Project + ":" + schema.Table
	s.schemas[key] = schema
	return nil
}

func (s *mockStorage) CreateSchema(ctx context.Context, schema *models.Schema) error {
	key := schema.Project + ":" + schema.Table
	s.schemas[key] = schema
	return nil
}

func (s *mockStorage) GetSchema(ctx context.Context, project, table string) (*models.Schema, error) {
	key := project + ":" + table
	if schema, ok := s.schemas[key]; ok {
		return schema, nil
	}
	return nil, models.ErrSchemaNotFound
}

func TestManager(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "schema_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储实例
	storage := newMockStorage()

	// 创建管理器
	manager, err := NewManager(storage, tempDir)
	require.NoError(t, err)

	// 创建测试 schema
	schema := &models.Schema{
		Project:     "test",
		Table:       "logs",
		Description: "Test logs",
		Fields: []*models.Field{
			{
				Name:        "level",
				Type:        models.FieldTypeString,
				Description: "Log level",
				Required:    true,
				Indexed:     true,
			},
			{
				Name:        "message",
				Type:        models.FieldTypeString,
				Description: "Log message",
				Required:    true,
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 保存 schema 文件
	schemaFile := filepath.Join(tempDir, "test_logs.yaml")
	err = schema.SaveToFile(schemaFile)
	require.NoError(t, err)

	// 启动管理器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = manager.Start()
	require.NoError(t, err)

	// 等待 schema 加载
	time.Sleep(time.Second)

	// 验证 schema 是否被加载
	loadedSchema, err := storage.GetSchema(ctx, "test", "logs")
	require.NoError(t, err)
	assert.Equal(t, schema.Project, loadedSchema.Project)
	assert.Equal(t, schema.Table, loadedSchema.Table)
	assert.Equal(t, schema.Description, loadedSchema.Description)
	assert.Len(t, loadedSchema.Fields, len(schema.Fields))

	// 修改 schema 文件
	schema.Description = "Updated test logs"
	err = schema.SaveToFile(schemaFile)
	require.NoError(t, err)

	// 等待 schema 重新加载
	time.Sleep(time.Second)

	// 验证 schema 是否被更新
	updatedSchema, err := storage.GetSchema(ctx, "test", "logs")
	require.NoError(t, err)
	assert.Equal(t, "Updated test logs", updatedSchema.Description)

	// 删除 schema 文件
	err = os.Remove(schemaFile)
	require.NoError(t, err)

	// 等待 schema 被删除
	time.Sleep(time.Second)

	// 验证 schema 是否被删除
	_, err = storage.GetSchema(ctx, "test", "logs")
	assert.Equal(t, models.ErrSchemaNotFound, err)
}

func TestManagerInvalidSchema(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "schema_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储实例
	storage := newMockStorage()

	// 创建管理器
	manager, err := NewManager(storage, tempDir)
	require.NoError(t, err)

	// 创建无效的 schema 文件
	schemaFile := filepath.Join(tempDir, "invalid.yaml")
	err = os.WriteFile(schemaFile, []byte("invalid yaml"), 0644)
	require.NoError(t, err)

	// 启动管理器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = manager.Start()
	require.NoError(t, err)

	// 等待 schema 加载
	time.Sleep(time.Second)

	// 验证无效的 schema 是否被忽略
	_, err = storage.GetSchema(ctx, "invalid", "logs")
	assert.Equal(t, models.ErrSchemaNotFound, err)
}

func TestManagerDuplicateSchema(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "schema_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储实例
	storage := newMockStorage()

	// 创建管理器
	manager, err := NewManager(storage, tempDir)
	require.NoError(t, err)

	// 创建测试 schema
	schema := &models.Schema{
		Project:     "test",
		Table:       "logs",
		Description: "Test logs",
		Fields: []*models.Field{
			{
				Name:        "level",
				Type:        models.FieldTypeString,
				Description: "Log level",
				Required:    true,
				Indexed:     true,
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 保存两个相同的 schema 文件
	schemaFile1 := filepath.Join(tempDir, "test_logs_1.yaml")
	err = schema.SaveToFile(schemaFile1)
	require.NoError(t, err)

	schema.Description = "Duplicate test logs"
	schemaFile2 := filepath.Join(tempDir, "test_logs_2.yaml")
	err = schema.SaveToFile(schemaFile2)
	require.NoError(t, err)

	// 启动管理器
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = manager.Start()
	require.NoError(t, err)

	// 等待 schema 加载
	time.Sleep(time.Second)

	// 验证只有一个 schema 被加载
	loadedSchema, err := storage.GetSchema(ctx, "test", "logs")
	require.NoError(t, err)
	assert.Equal(t, "Duplicate test logs", loadedSchema.Description)
}
