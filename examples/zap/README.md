# Zap Logger Integration Example

这个示例展示了如何将 zap logger 与我们的日志存储系统集成。

## 功能特性

1. 支持多种日志级别（Debug、Info、Warn、Error）
2. 支持多种字段类型：
   - 字符串（String）
   - 整数（Int）
   - 浮点数（Float）
   - 布尔值（Bool）
   - 时间（Time）
   - 时间间隔（Duration）
   - JSON/结构化数据
   - 错误（Error）

3. 支持字段验证和索引
4. 同时输出到控制台和存储后端

## 运行示例

1. 首先确保你有一个可用的存储后端。示例中使用 ClickHouse，你可以通过 Docker 快速启动一个：

```bash
docker run -d --name clickhouse-server \
    -p 8123:8123 -p 9000:9000 \
    --ulimit nofile=262144:262144 \
    clickhouse/clickhouse-server
```

2. 修改配置（如果需要）：

```go
store := storage.NewClickHouseStorage(storage.Config{
    Type: "clickhouse",
    ClickHouse: storage.ClickHouseConfig{
        Host:     "localhost",  // 修改为你的 ClickHouse 主机
        Port:     9000,        // 修改为你的 ClickHouse 端口
        Database: "logs",      // 修改为你的数据库名
        Username: "default",   // 修改为你的用户名
        Password: "",          // 修改为你的密码
    },
})
```

3. 运行示例：

```bash
go run examples/zap/main.go
```

## 日志示例

示例程序会生成以下类型的日志：

1. 基本信息日志：
```go
logger.Info("User login",
    zap.Int("user_id", 123),
    zap.String("action", "login"),
    zap.Duration("duration", time.Millisecond*100),
)
```

2. 带结构化数据的日志：
```go
logger.Info("Page view",
    zap.Int("user_id", 456),
    zap.String("action", "view"),
    zap.Any("metadata", metadata),
)
```

3. 错误日志：
```go
logger.Error("Failed login attempt",
    zap.Int("user_id", 789),
    zap.String("action", "login"),
    zap.Error(fmt.Errorf("invalid credentials")),
)
```

4. 带时间的日志：
```go
logger.Info("Scheduled task",
    zap.Time("next_run", time.Now().Add(time.Hour*24)),
    zap.Duration("interval", time.Hour*24),
)
```

## Schema 定义

示例中定义了以下字段：

```go
Fields: []models.Field{
    {
        Name:        "user_id",
        Type:        models.FieldTypeInt,
        Description: "User ID",
        Required:    true,
        Indexed:     true,
    },
    {
        Name:        "action",
        Type:        models.FieldTypeString,
        Description: "User action",
        Required:    true,
        Indexed:     true,
    },
    {
        Name:        "duration",
        Type:        models.FieldTypeDuration,
        Description: "Action duration",
        Required:    false,
        Indexed:     false,
    },
    {
        Name:        "metadata",
        Type:        models.FieldTypeJSON,
        Description: "Additional metadata",
        Required:    false,
        Indexed:     false,
    },
}
```

## 查看日志

1. 在 ClickHouse 中查看日志：

```sql
SELECT *
FROM logs.example_app_logs
ORDER BY timestamp DESC
LIMIT 10;
```

2. 按字段查询：

```sql
SELECT *
FROM logs.example_app_logs
WHERE user_id = 123
  AND action = 'login'
ORDER BY timestamp DESC;
```

3. 查询高延迟日志：

```sql
SELECT *
FROM logs.example_app_logs
WHERE duration > 1000000000  -- 1秒（纳秒）
ORDER BY duration DESC;
``` 