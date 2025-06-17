package schema

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
	"pkg.blksails.net/logs/internal/models"
	"pkg.blksails.net/logs/internal/storage"
)

// Manager 管理 schema 的加载和更新
type Manager struct {
	storage    storage.Storage
	schemasDir string
	watcher    *fsnotify.Watcher
	schemas    map[string]*models.Schema // key: project:table
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewManager 创建新的 schema 管理器
func NewManager(storage storage.Storage, schemasDir string) (*Manager, error) {
	// 确保目录存在
	if err := os.MkdirAll(schemasDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schemas directory: %w", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		storage:    storage,
		schemasDir: schemasDir,
		watcher:    watcher,
		schemas:    make(map[string]*models.Schema),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start 启动 schema 管理器
func (m *Manager) Start() error {
	// 加载现有的 schema 文件
	if err := m.loadSchemas(); err != nil {
		return err
	}

	// 监控目录变化
	if err := m.watcher.Add(m.schemasDir); err != nil {
		return fmt.Errorf("failed to watch directory: %w", err)
	}

	go m.watchChanges()

	return nil
}

// Stop 停止 schema 管理器
func (m *Manager) Stop() error {
	m.cancel()
	return m.watcher.Close()
}

// GetSchemasDir 获取 schema 目录路径
func (m *Manager) GetSchemasDir() string {
	return m.schemasDir
}

// loadSchemas 加载所有 schema 文件
func (m *Manager) loadSchemas() error {
	files, err := os.ReadDir(m.schemasDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".yaml" {
			continue
		}

		if err := m.loadSchema(filepath.Join(m.schemasDir, file.Name())); err != nil {
			// 记录错误但继续处理其他文件
			fmt.Printf("Failed to load schema %s: %v\n", file.Name(), err)
		}
	}

	return nil
}

// loadSchema 加载单个 schema 文件
func (m *Manager) loadSchema(filename string) error {
	// 读取文件
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	// 解析 YAML
	schema := &models.Schema{}
	if err := yaml.Unmarshal(data, schema); err != nil {
		return fmt.Errorf("解析 YAML 失败: %w", err)
	}

	// 更新时间戳
	now := time.Now()
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = now
	}
	schema.UpdatedAt = now

	// 保存到存储
	if err := m.storage.CreateSchema(m.ctx, schema); err != nil {
		return err
	}

	// 更新内存缓存
	m.mu.Lock()
	m.schemas[schema.Project+":"+schema.Table] = schema
	m.mu.Unlock()

	return nil
}

// watchChanges 监控文件变化
func (m *Manager) watchChanges() {
	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}

			if filepath.Ext(event.Name) != ".yaml" {
				continue
			}

			switch {
			case event.Op&(fsnotify.Create|fsnotify.Write) != 0:
				if err := m.loadSchema(event.Name); err != nil {
					fmt.Printf("Failed to load schema %s: %v\n", event.Name, err)
				}
			case event.Op&fsnotify.Remove != 0:
				// 从内存缓存中删除
				m.mu.Lock()
				for key, schema := range m.schemas {
					if filepath.Join(m.schemasDir, schema.Project+"_"+schema.Table+".yaml") == event.Name {
						delete(m.schemas, key)
						break
					}
				}
				m.mu.Unlock()
			}

		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			fmt.Printf("Watcher error: %v\n", err)

		case <-m.ctx.Done():
			return
		}
	}
}

// GetSchema 获取指定的 schema
func (m *Manager) GetSchema(project, table string) (*models.Schema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	schema, ok := m.schemas[project+":"+table]
	if !ok {
		return nil, fmt.Errorf("schema not found: %s:%s", project, table)
	}
	return schema, nil
}

// ListSchemas 列出所有 schema
func (m *Manager) ListSchemas() []*models.Schema {
	m.mu.RLock()
	defer m.mu.RUnlock()

	schemas := make([]*models.Schema, 0, len(m.schemas))
	for _, schema := range m.schemas {
		schemas = append(schemas, schema)
	}
	return schemas
}
