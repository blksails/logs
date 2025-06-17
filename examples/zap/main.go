package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"pkg.blksails.net/logs/internal/storage"
	loghook "pkg.blksails.net/logs/pkg/zap"
)

func main() {
	// 创建存储实例
	config := storage.Config{
		Type: "postgres",
		Postgres: storage.PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "logs",
			Username: "postgres",
			Password: "postgres",
		},
	}

	// 初始化存储
	store := storage.NewPostgresStorage(config)
	if err := store.Initialize(context.Background()); err != nil {
		log.Fatalf("初始化存储失败: %v", err)
	}
	defer store.Close()

	// 创建 zap logger
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 创建 hook
	hook := loghook.NewStorageHook(loghook.StorageHookConfig{
		Storage:  store,
		Project:  "app",
		Table:    "logs",
		MinLevel: zapcore.InfoLevel,
	})

	// 创建 core
	core := zapcore.NewTee(
		hook,
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.AddSync(log.Writer()),
			zapcore.InfoLevel,
		),
	)

	// 创建 logger
	logger := zap.New(core, zap.AddCaller())
	defer logger.Sync()

	// 记录一些示例日志
	logger.Info("用户登录",
		zap.String("user_id", "123456"),
		zap.String("ip", "192.168.1.1"),
		zap.String("user_agent", "Mozilla/5.0"),
	)

	logger.Info("API 请求",
		zap.String("request_id", "req-123"),
		zap.String("method", "GET"),
		zap.String("path", "/api/v1/users"),
		zap.Int("status_code", 200),
		zap.Duration("duration", 50*time.Millisecond),
	)

	logger.Error("数据库连接失败",
		zap.String("db", "users"),
		zap.Error(ErrDatabaseConnection),
		zap.Duration("retry_after", 5*time.Second),
	)

	// 使用 with 字段
	reqLogger := logger.With(
		zap.String("request_id", "req-456"),
		zap.String("user_id", "789012"),
	)

	reqLogger.Info("处理请求",
		zap.String("action", "create_user"),
		zap.Duration("duration", 100*time.Millisecond),
	)

	reqLogger.Warn("配额即将用完",
		zap.Int("quota_remaining", 100),
		zap.Int("quota_limit", 1000),
	)

	fmt.Println("Logs have been written to both stdout and storage backend")
}

// ErrDatabaseConnection 示例错误
var ErrDatabaseConnection = errors.New("连接数据库失败: 连接超时")
