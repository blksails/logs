package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/viper"
	"pkg.blksails.net/logs/internal/api"
	"pkg.blksails.net/logs/internal/schema"
	"pkg.blksails.net/logs/internal/storage"
)

var (
	configFile  string
	schemasDir  string
	storageType string
)

func init() {
	flag.StringVar(&configFile, "config", "configs/config.yaml", "配置文件路径")
	flag.StringVar(&schemasDir, "schemas", "configs/schemas", "Schema 配置目录")
	flag.StringVar(&storageType, "storage", "postgres", "存储后端类型 (postgres, mysql, sqlite, clickhouse)")
}

func main() {
	flag.Parse()

	// 加载配置文件
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("读取配置文件失败: %v", err)
	}

	// 确保配置目录存在
	if err := os.MkdirAll(filepath.Dir(configFile), 0755); err != nil {
		log.Fatalf("创建配置目录失败: %v", err)
	}

	// 确保 schema 目录存在
	if err := os.MkdirAll(schemasDir, 0755); err != nil {
		log.Fatalf("创建 schema 目录失败: %v", err)
	}

	// 初始化存储后端
	store, err := initializeStorage(storageType)
	if err != nil {
		log.Fatalf("初始化存储后端失败: %v", err)
	}
	defer store.Close()

	// 初始化 schema 管理器
	schemaManager, err := schema.NewManager(store, schemasDir)
	if err != nil {
		log.Fatalf("初始化 schema 管理器失败: %v", err)
	}
	defer schemaManager.Stop()

	// 启动 schema 管理器
	if err := schemaManager.Start(); err != nil {
		log.Fatalf("启动 schema 管理器失败: %v", err)
	}

	// 初始化 API 服务器
	server := api.NewServer(store, &api.Config{
		Host: viper.GetString("server.host"),
		Port: viper.GetInt("server.port"),
	})

	// 启动服务器
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("服务器停止: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 优雅关闭
	fmt.Println("\n正在关闭服务...")
	if err := server.Stop(context.Background()); err != nil {
		log.Printf("服务器关闭出错: %v", err)
	}
}

func initializeStorage(storageType string) (storage.Storage, error) {
	ctx := context.Background()

	config := storage.Config{
		Type: storageType,
		Postgres: storage.PostgresConfig{
			Host:     viper.GetString("storage.postgres.host"),
			Port:     viper.GetInt("storage.postgres.port"),
			Database: viper.GetString("storage.postgres.database"),
			Username: viper.GetString("storage.postgres.user"),
			Password: viper.GetString("storage.postgres.password"),
		},
		MySQL: storage.MySQLConfig{
			Host:     viper.GetString("storage.mysql.host"),
			Port:     viper.GetInt("storage.mysql.port"),
			Database: viper.GetString("storage.mysql.database"),
			Username: viper.GetString("storage.mysql.user"),
			Password: viper.GetString("storage.mysql.password"),
		},
		SQLite: storage.SQLiteConfig{
			Path: viper.GetString("storage.sqlite.path"),
		},
		ClickHouse: storage.ClickHouseConfig{
			Host:     viper.GetString("storage.clickhouse.host"),
			Port:     viper.GetInt("storage.clickhouse.port"),
			Database: viper.GetString("storage.clickhouse.database"),
			Username: viper.GetString("storage.clickhouse.user"),
			Password: viper.GetString("storage.clickhouse.password"),
		},
	}

	var store storage.Storage
	log.Println(storageType)
	log.Printf("%+v", config)
	switch storageType {
	case "postgres":
		store = storage.NewPostgresStorage(config)
	case "mysql":
		store = storage.NewMySQLStorage(config)
	case "sqlite":
		store = storage.NewSQLiteStorage(config)
	case "clickhouse":
		store = storage.NewClickHouseStorage(config)
	default:
		return nil, fmt.Errorf("不支持的存储后端类型: %s", storageType)
	}

	if err := store.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("初始化存储后端失败: %w", err)
	}

	return store, nil
}
