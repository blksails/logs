-- 创建 schema_meta 表
CREATE TABLE IF NOT EXISTS schema_meta (
    project VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    description TEXT,
    version VARCHAR(50),
    schema_def JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (project, table_name)
);
-- 创建索引
CREATE INDEX IF NOT EXISTS idx_schema_meta_project ON schema_meta (project);
CREATE INDEX IF NOT EXISTS idx_schema_meta_table_name ON schema_meta (table_name);
CREATE INDEX IF NOT EXISTS idx_schema_meta_updated_at ON schema_meta (updated_at);
-- 创建函数：自动更新 updated_at 字段
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ language 'plpgsql';
-- 创建触发器
CREATE TRIGGER update_schema_meta_updated_at BEFORE
UPDATE ON schema_meta FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();