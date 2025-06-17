package storage

import (
	"context"

	"go.uber.org/zap"
	"pkg.blksails.net/logs/internal/models"
)

// Storage 定义存储接口
type Storage interface {
	// 初始化
	Initialize(ctx context.Context) error

	// Schema 相关操作
	CreateSchema(ctx context.Context, schema *models.Schema) error
	UpdateSchema(ctx context.Context, schema *models.Schema) error
	DeleteSchema(ctx context.Context, project, table string) error
	GetSchema(ctx context.Context, project, table string) (*models.Schema, error)
	ListSchemas(ctx context.Context) ([]*models.Schema, error)

	// 日志相关操作
	InsertLog(ctx context.Context, project, table string, log *models.LogEntry) error
	BatchInsertLogs(ctx context.Context, project, table string, logs []*models.LogEntry) error

	// 连接管理
	Close() error
	Ping(ctx context.Context) error
}

// Config 存储配置
type Config struct {
	Type       string           `yaml:"type"`
	Postgres   PostgresConfig   `yaml:"postgres,omitempty"`
	MySQL      MySQLConfig      `yaml:"mysql,omitempty"`
	SQLite     SQLiteConfig     `yaml:"sqlite,omitempty"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse,omitempty"`
	Logger     *zap.Logger      `yaml:"logger,omitempty"`
}

// PostgresConfig PostgreSQL 配置
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
}

// MySQLConfig MySQL 配置
type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// SQLiteConfig SQLite 配置
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// ClickHouseConfig ClickHouse 配置
type ClickHouseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}
