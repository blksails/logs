package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"pkg.blksails.net/logs/internal/api"
	"pkg.blksails.net/logs/internal/storage"
	zaphook "pkg.blksails.net/logs/pkg/zap"
)

func main() {
	// 加载配置
	cfg, err := loadConfig("configs/config.yaml")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 创建存储实例
	store, err := initializeStorage(cfg)
	if err != nil {
		fmt.Printf("Failed to create storage: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// 创建 API 服务器
	server := api.NewServer(store, &api.Config{
		Host: cfg.Server.Host,
		Port: cfg.Server.Port,
	})

	// 创建 Zap 日志钩子
	hook, err := zaphook.NewHook(store, &zaphook.Config{
		Project:     "myapp",
		Table:       "app_logs",
		BufferSize:  100,
		FlushPeriod: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("Failed to create hook: %v\n", err)
		os.Exit(1)
	}
	defer hook.Close()

	// 创建 Zap logger
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stack_trace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zaphook.NewCore(
		hook,
		zapcore.NewJSONEncoder(encoderConfig),
		zap.NewAtomicLevelAt(zap.InfoLevel),
	)

	logger := zap.New(core, zap.AddCaller())
	defer logger.Sync()

	// 替换全局 logger
	zap.ReplaceGlobals(logger)

	// 创建 HTTP 服务器
	router := gin.Default()

	// 添加中间件记录请求日志
	router.Use(func(c *gin.Context) {
		start := time.Now()

		// 处理请求
		c.Next()

		// 记录日志
		duration := time.Since(start)
		logger.Info("HTTP Request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", duration),
			zap.String("ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
			zap.String("referer", c.Request.Referer()),
		)
	})

	// 添加示例路由
	router.GET("/hello", func(c *gin.Context) {
		logger.Info("Handling hello request",
			zap.String("user_id", "123"),
			zap.String("request_id", "req-456"),
		)

		// 模拟一些业务逻辑
		time.Sleep(100 * time.Millisecond)

		if c.Query("error") == "true" {
			err := fmt.Errorf("something went wrong")
			logger.Error("Failed to process request",
				zap.Error(err),
				zap.String("error_code", "INTERNAL_ERROR"),
				zap.Any("error_details", map[string]interface{}{
					"query_params": c.Request.URL.Query(),
				}),
			)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		logger.Info("Request processed successfully",
			zap.Any("response", map[string]interface{}{
				"message": "Hello, World!",
			}),
		)

		c.JSON(http.StatusOK, gin.H{
			"message": "Hello, World!",
		})
	})

	// 启动 HTTP 服务器
	go func() {
		if err := router.Run(fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)); err != nil {
			logger.Fatal("Failed to start HTTP server", zap.Error(err))
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// 优雅关闭
	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Stop(ctx); err != nil {
		logger.Error("Server forced to shutdown", zap.Error(err))
	}

	logger.Info("Server exited")
}

// initializeStorage 初始化存储实例
func initializeStorage(cfg *Config) (storage.Storage, error) {
	config := storage.Config{
		Type: "postgres",
		Postgres: storage.PostgresConfig{
			Host:     cfg.Storage.Postgres.Host,
			Port:     cfg.Storage.Postgres.Port,
			Database: cfg.Storage.Postgres.Database,
			Username: cfg.Storage.Postgres.User,
			Password: cfg.Storage.Postgres.Password,
		},
	}

	store := storage.NewPostgresStorage(config)
	if err := store.Initialize(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return store, nil
}

// Config 配置结构
type Config struct {
	Server struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"server"`

	Schema struct {
		Dir   string `yaml:"dir"`
		Watch bool   `yaml:"watch"`
	} `yaml:"schema"`

	Storage struct {
		Postgres struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			Database string `yaml:"database"`
			SSLMode  string `yaml:"sslmode"`
		} `yaml:"postgres"`
	} `yaml:"storage"`

	Log struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
		Output string `yaml:"output"`
	} `yaml:"log"`
}

// loadConfig 加载配置文件
func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}
