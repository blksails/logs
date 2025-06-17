-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS logs;
-- 切换到 logs 数据库
USE logs;
-- 创建 schema_meta 表
CREATE TABLE IF NOT EXISTS schema_meta (
    project String,
    table_name String,
    description String,
    version String,
    schema_def String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (project, table_name) SETTINGS index_granularity = 8192;
-- 创建 schema_meta 的物化视图，用于按更新时间排序
CREATE MATERIALIZED VIEW IF NOT EXISTS schema_meta_by_updated_at ENGINE = MergeTree()
ORDER BY updated_at POPULATE AS
SELECT *
FROM schema_meta;