package zap

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
	"pkg.blksails.net/logs/internal/models"
	"pkg.blksails.net/logs/internal/storage"
)

// StorageHook 实现 zap 的 Core 接口
type StorageHook struct {
	storage  storage.Storage
	project  string
	table    string
	fields   []zapcore.Field
	minLevel zapcore.Level
}

// StorageHookConfig 配置
type StorageHookConfig struct {
	Storage  storage.Storage
	Project  string
	Table    string
	MinLevel zapcore.Level
}

// NewStorageHook 创建新的存储 hook
func NewStorageHook(config StorageHookConfig) *StorageHook {
	return &StorageHook{
		storage:  config.Storage,
		project:  config.Project,
		table:    config.Table,
		minLevel: config.MinLevel,
		fields:   make([]zapcore.Field, 0),
	}
}

// Enabled 实现 zapcore.Core 接口
func (h *StorageHook) Enabled(level zapcore.Level) bool {
	return level >= h.minLevel
}

// With 实现 zapcore.Core 接口
func (h *StorageHook) With(fields []zapcore.Field) zapcore.Core {
	clone := *h
	clone.fields = append(clone.fields, fields...)
	return &clone
}

// Check 实现 zapcore.Core 接口
func (h *StorageHook) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(ent.Level) {
		return ce.AddCore(ent, h)
	}
	return ce
}

// Write 实现 zapcore.Core 接口
func (h *StorageHook) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	// 创建日志条目
	log := &models.LogEntry{
		Project:   h.project,
		Table:     h.table,
		Timestamp: ent.Time,
		Fields:    make(map[string]interface{}),
	}

	// 设置基本字段
	log.Fields["level"] = ent.Level.String()
	log.Fields["message"] = ent.Message
	if ent.Caller.Defined {
		log.Fields["module"] = ent.Caller.TrimmedPath()
		log.Fields["function"] = ent.Caller.Function
		log.Fields["line"] = ent.Caller.Line
	}
	if ent.Stack != "" {
		log.Fields["stack_trace"] = ent.Stack
	}

	// 添加自定义字段
	allFields := append(h.fields, fields...)
	for _, field := range allFields {
		switch field.Type {
		case zapcore.StringType:
			log.Fields[field.Key] = field.String
		case zapcore.BoolType:
			log.Fields[field.Key] = field.Integer == 1
		case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type,
			zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
			log.Fields[field.Key] = field.Integer
		case zapcore.Float32Type, zapcore.Float64Type:
			log.Fields[field.Key] = math.Float64frombits(uint64(field.Integer))
		case zapcore.DurationType:
			log.Fields[field.Key] = time.Duration(field.Integer).String()
		case zapcore.TimeType:
			log.Fields[field.Key] = time.Unix(0, field.Integer).Format(time.RFC3339Nano)
		case zapcore.ErrorType:
			log.Fields[field.Key] = field.Interface.(error).Error()
		case zapcore.ReflectType:
			log.Fields[field.Key] = field.Interface
		}
	}

	// 存储日志
	if err := h.storage.InsertLog(context.Background(), h.project, h.table, log); err != nil {
		return fmt.Errorf("存储日志失败: %w", err)
	}

	return nil
}

// Sync 实现 zapcore.Core 接口
func (h *StorageHook) Sync() error {
	return nil
}

// Hook 实现 Zap 日志钩子
type Hook struct {
	storage  storage.Storage
	project  string
	table    string
	buffer   []*models.LogEntry
	bufSize  int
	interval time.Duration
	mu       sync.Mutex
	done     chan struct{}
}

// Config Hook 配置
type Config struct {
	Project     string
	Table       string
	BufferSize  int
	FlushPeriod time.Duration
}

// NewHook 创建新的 Zap 日志钩子
func NewHook(storage storage.Storage, cfg *Config) (*Hook, error) {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}
	if cfg.FlushPeriod <= 0 {
		cfg.FlushPeriod = 5 * time.Second
	}

	hook := &Hook{
		storage:  storage,
		project:  cfg.Project,
		table:    cfg.Table,
		buffer:   make([]*models.LogEntry, 0, cfg.BufferSize),
		bufSize:  cfg.BufferSize,
		interval: cfg.FlushPeriod,
		done:     make(chan struct{}),
	}

	// 启动定期刷新
	go hook.periodicFlush()

	return hook, nil
}

// Write 实现 zapcore.WriteSyncer 接口
func (h *Hook) Write(p []byte) (n int, err error) {
	// 这里不实际写入数据，因为我们使用 Core 接口
	return len(p), nil
}

// Sync 实现 zapcore.WriteSyncer 接口
func (h *Hook) Sync() error {
	return h.Flush()
}

// Close 关闭钩子
func (h *Hook) Close() error {
	close(h.done)
	return h.Flush()
}

// WriteLog 写入日志
func (h *Hook) WriteLog(entry zapcore.Entry, fields []zapcore.Field) error {
	// 构建日志数据
	log := &models.LogEntry{
		Project:   h.project,
		Table:     h.table,
		Timestamp: entry.Time,
		Fields:    make(map[string]interface{}),
	}

	// 添加基本字段
	log.Fields["level"] = entry.Level.String()
	log.Fields["message"] = entry.Message
	log.Fields["caller"] = entry.Caller.String()

	if entry.Stack != "" {
		log.Fields["stack_trace"] = entry.Stack
	}

	// 添加自定义字段
	for _, field := range fields {
		switch field.Type {
		case zapcore.StringType:
			log.Fields[field.Key] = field.String
		case zapcore.BoolType:
			log.Fields[field.Key] = field.Integer == 1
		case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
			log.Fields[field.Key] = field.Integer
		case zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type:
			log.Fields[field.Key] = uint64(field.Integer)
		case zapcore.Float64Type:
			log.Fields[field.Key] = float64(field.Integer)
		case zapcore.Float32Type:
			log.Fields[field.Key] = float32(field.Integer)
		case zapcore.DurationType:
			log.Fields[field.Key] = time.Duration(field.Integer)
		case zapcore.TimeType:
			log.Fields[field.Key] = time.Unix(0, field.Integer)
		case zapcore.ErrorType:
			if field.Interface != nil {
				log.Fields[field.Key] = field.Interface.(error).Error()
			}
		case zapcore.ReflectType:
			log.Fields[field.Key] = field.Interface
		default:
			log.Fields[field.Key] = fmt.Sprintf("%v", field.Interface)
		}
	}

	// 添加到缓冲区
	h.mu.Lock()
	h.buffer = append(h.buffer, log)
	shouldFlush := len(h.buffer) >= h.bufSize
	h.mu.Unlock()

	// 如果缓冲区已满，立即刷新
	if shouldFlush {
		return h.Flush()
	}

	return nil
}

// Flush 刷新缓冲区
func (h *Hook) Flush() error {
	h.mu.Lock()
	if len(h.buffer) == 0 {
		h.mu.Unlock()
		return nil
	}
	logs := make([]*models.LogEntry, len(h.buffer))
	copy(logs, h.buffer)
	h.buffer = h.buffer[:0]
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return h.storage.BatchInsertLogs(ctx, h.project, h.table, logs)
}

// periodicFlush 定期刷新缓冲区
func (h *Hook) periodicFlush() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := h.Flush(); err != nil {
				fmt.Printf("Failed to flush logs: %v\n", err)
			}
		case <-h.done:
			return
		}
	}
}

// Core 创建 zapcore.Core
type Core struct {
	zapcore.LevelEnabler
	hook *Hook
	enc  zapcore.Encoder
}

// NewCore 创建新的 Core
func NewCore(hook *Hook, enc zapcore.Encoder, enab zapcore.LevelEnabler) *Core {
	return &Core{
		LevelEnabler: enab,
		hook:         hook,
		enc:          enc,
	}
}

// With 添加字段
func (c *Core) With(fields []zapcore.Field) zapcore.Core {
	clone := c.clone()
	for _, field := range fields {
		field.AddTo(clone.enc)
	}
	return clone
}

// Check 检查是否应该记录
func (c *Core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

// Write 写入日志
func (c *Core) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	return c.hook.WriteLog(ent, fields)
}

// Sync 同步缓冲区
func (c *Core) Sync() error {
	return c.hook.Sync()
}

// clone 克隆 Core
func (c *Core) clone() *Core {
	return &Core{
		LevelEnabler: c.LevelEnabler,
		hook:         c.hook,
		enc:          c.enc.Clone(),
	}
}
