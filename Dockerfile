# 构建阶段
FROM golang:1.21-alpine AS builder

# 设置工作目录
WORKDIR /app

# 安装构建依赖
RUN apk add --no-cache git make

# 复制源代码
COPY . .

# 下载依赖
RUN go mod download

# 构建应用
RUN make build

# 运行阶段
FROM alpine:latest

# 安装运行时依赖
RUN apk add --no-cache ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 创建非 root 用户
RUN adduser -D -g '' appuser

# 创建必要的目录
RUN mkdir -p /app/configs /app/schemas /app/data
RUN chown -R appuser:appuser /app

# 切换到非 root 用户
USER appuser

# 设置工作目录
WORKDIR /app

# 复制构建产物和配置文件
COPY --from=builder /app/bin/logs /app/
COPY --from=builder /app/configs/config.yaml /app/configs/
COPY --from=builder /app/examples/app_logs.yaml /app/schemas/

# 暴露端口
EXPOSE 8080

# 启动应用
CMD ["./logs"] 