package models

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaYAML(t *testing.T) {
	// 创建测试 schema
	schema := &Schema{
		Project:     "test",
		Table:       "logs",
		Description: "Test logs",
		Fields: []*Field{
			{
				Name:        "user_id",
				Type:        FieldTypeInt,
				Description: "User ID",
				Required:    true,
				Indexed:     true,
			},
			{
				Name:        "action",
				Type:        FieldTypeString,
				Description: "User action",
				Required:    true,
				Indexed:     true,
			},
			{
				Name:        "duration",
				Type:        FieldTypeDuration,
				Description: "Action duration",
				Required:    false,
				Indexed:     false,
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 测试转换为 YAML
	yamlData, err := schema.ToYAML()
	require.NoError(t, err)
	assert.Contains(t, string(yamlData), "project: test")
	assert.Contains(t, string(yamlData), "table: logs")
	assert.Contains(t, string(yamlData), "name: user_id")
	assert.Contains(t, string(yamlData), "type: int")

	// 测试从 YAML 加载
	newSchema, err := SchemaFromYAML(yamlData)
	require.NoError(t, err)
	assert.Equal(t, schema.Project, newSchema.Project)
	assert.Equal(t, schema.Table, newSchema.Table)
	assert.Equal(t, schema.Description, newSchema.Description)
	assert.Equal(t, len(schema.Fields), len(newSchema.Fields))
	assert.Equal(t, schema.Fields[0].Name, newSchema.Fields[0].Name)
	assert.Equal(t, schema.Fields[0].Type, newSchema.Fields[0].Type)
	assert.Equal(t, schema.Fields[0].Required, newSchema.Fields[0].Required)
	assert.Equal(t, schema.Fields[0].Indexed, newSchema.Fields[0].Indexed)
}

func TestSchemaYAMLFile(t *testing.T) {
	// 创建临时文件
	tmpfile, err := os.CreateTemp("", "schema-*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// 创建测试 schema
	schema := &Schema{
		Project:     "test",
		Table:       "logs",
		Description: "Test logs",
		Fields: []*Field{
			{
				Name:        "user_id",
				Type:        FieldTypeInt,
				Description: "User ID",
				Required:    true,
				Indexed:     true,
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 测试保存到文件
	err = schema.SaveToFile(tmpfile.Name())
	require.NoError(t, err)

	// 测试从文件加载
	newSchema, err := LoadSchemaFromFile(tmpfile.Name())
	require.NoError(t, err)
	assert.Equal(t, schema.Project, newSchema.Project)
	assert.Equal(t, schema.Table, newSchema.Table)
	assert.Equal(t, len(schema.Fields), len(newSchema.Fields))
}

func TestSchemaYAMLValidation(t *testing.T) {
	// 测试无效的字段类型
	yamlData := []byte(`
project: test
table: logs
fields:
  - name: invalid_field
    type: invalid_type
    required: true
    indexed: true
`)

	_, err := SchemaFromYAML(yamlData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid field type")

	// 测试必需字段缺失
	yamlData = []byte(`
table: logs
fields:
  - name: user_id
    type: int
`)

	_, err = SchemaFromYAML(yamlData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "project name is required")
}

func TestSchemaYAMLAllTypes(t *testing.T) {
	yamlData := []byte(`
project: test
table: logs
description: Test all field types
fields:
  - name: string_field
    type: string
    required: true
  - name: int_field
    type: int
    required: true
  - name: float_field
    type: float
    required: true
  - name: bool_field
    type: bool
    required: true
  - name: datetime_field
    type: datetime
    required: true
  - name: time_field
    type: time
    required: true
  - name: duration_field
    type: duration
    required: true
  - name: json_field
    type: json
    required: true
`)

	schema, err := SchemaFromYAML(yamlData)
	require.NoError(t, err)
	assert.Equal(t, 8, len(schema.Fields))
	assert.Equal(t, FieldTypeString, schema.Fields[0].Type)
	assert.Equal(t, FieldTypeInt, schema.Fields[1].Type)
	assert.Equal(t, FieldTypeFloat, schema.Fields[2].Type)
	assert.Equal(t, FieldTypeBool, schema.Fields[3].Type)
	assert.Equal(t, FieldTypeDateTime, schema.Fields[4].Type)
	assert.Equal(t, FieldTypeTime, schema.Fields[5].Type)
	assert.Equal(t, FieldTypeDuration, schema.Fields[6].Type)
	assert.Equal(t, FieldTypeJSON, schema.Fields[7].Type)
}
