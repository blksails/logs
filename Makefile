.PHONY: all build test clean run

# 变量定义
BINARY_NAME=logs
GO=go
GOFLAGS=-v
LDFLAGS=-ldflags "-s -w"

all: build

# 构建
build:
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/server

build-linux:
	GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY_NAME)-linux-amd64 ./cmd/server

# 运行测试
test:
	$(GO) test $(GOFLAGS) ./...

# 运行测试并生成覆盖率报告
test-coverage:
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

# 运行程序
run:
	$(GO) run examples/main.go

# 安装依赖
deps:
	$(GO) mod download
	$(GO) mod tidy

# 生成代码
generate:
	$(GO) generate ./...

# 格式化代码
fmt:
	$(GO) fmt ./...

# 运行 linter
lint:
	golangci-lint run

# 清理构建产物
clean:
	rm -f bin/$(BINARY_NAME)
	rm -f bin/$(BINARY_NAME)-linux
	rm -f coverage.out
	rm -f coverage.html

# 创建数据库表
migrate:
	psql -U postgres -d logs -f scripts/schema.sql

deploy: build-linux
	@scripts/deploy.sh

# 帮助信息
help:
	@echo "可用的 make 命令："
	@echo "  make build         - 构建程序"
	@echo "  make test         - 运行测试"
	@echo "  make test-coverage - 运行测试并生成覆盖率报告"
	@echo "  make run          - 运行程序"
	@echo "  make deps         - 安装依赖"
	@echo "  make generate     - 生成代码"
	@echo "  make fmt          - 格式化代码"
	@echo "  make lint         - 运行 linter"
	@echo "  make clean        - 清理构建产物"
	@echo "  make migrate      - 创建数据库表" 