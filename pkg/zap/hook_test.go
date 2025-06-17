package zap

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"pkg.blksails.net/logs/internal/models"
)

type mockStorage struct {
	lastLog *models.LogEntry
	called  bool
}

func (m *mockStorage) Initialize(ctx context.Context) error { return nil }
func (m *mockStorage) BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error {
	return nil
}
func (m *mockStorage) DeleteSchema(ctx context.Context, project, table string) error { return nil }
func (m *mockStorage) InsertLog(ctx context.Context, project, table string, log *models.LogEntry) error {
	return nil
}
func (m *mockStorage) ListSchemas(ctx context.Context) ([]*models.Schema, error)     { return nil, nil }
func (m *mockStorage) Ping(ctx context.Context) error                                { return nil }
func (m *mockStorage) UpdateSchema(ctx context.Context, schema *models.Schema) error { return nil }
func (m *mockStorage) Close() error                                                  { return nil }
func (m *mockStorage) CreateSchema(ctx context.Context, schema *models.Schema) error { return nil }
func (m *mockStorage) GetSchema(ctx context.Context, project, table string) (*models.Schema, error) {
	return nil, nil
}

func TestStorageHook_Write_FieldTypes(t *testing.T) {
	mock := &mockStorage{}
	hook := NewStorageHook(StorageHookConfig{
		Storage:  mock,
		Project:  "test_project",
		Table:    "test_table",
		MinLevel: zapcore.InfoLevel,
	})
	tm := time.Now()
	dur := time.Second * 5

	fields := []zapcore.Field{
		{Key: "str", Type: zapcore.StringType, String: "hello"},
		{Key: "int", Type: zapcore.Int64Type, Integer: 42},
		{Key: "float", Type: zapcore.Float64Type, Integer: int64(math.Float64bits(3.14))},
		{Key: "bool", Type: zapcore.BoolType, Integer: 1},
		{Key: "time", Type: zapcore.StringType, String: tm.Format(time.RFC3339)},
		{Key: "duration", Type: zapcore.Int64Type, Integer: int64(dur)},
	}

	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Message: "test message",
		Time:    tm,
	}

	err := hook.Write(entry, fields)
	assert.NoError(t, err)
	assert.True(t, mock.called)
	log := mock.lastLog
	assert.Equal(t, "test_project", log.Project)
	assert.Equal(t, "test_table", log.Table)
	assert.Equal(t, "test message", log.Message)
	assert.Equal(t, "hello", log.Fields["str"])
	assert.Equal(t, int64(42), log.Fields["int"])
	assert.InDelta(t, 3.14, log.Fields["float"].(float64), 0.0001)
	assert.Equal(t, true, log.Fields["bool"])
	assert.Equal(t, tm.Format(time.RFC3339), log.Fields["time"])
	assert.Equal(t, int64(dur), log.Fields["duration"])
}
