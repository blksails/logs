#!/bin/bash

# 你的Go程序名
APP_NAME="myapp"

# 查找旧进程PID
PID=$(pgrep -f $APP_NAME)

if [ -z "$PID" ]; then
  echo "No running process found for $APP_NAME"
else
  echo "Killing process $APP_NAME with PID: $PID"
  kill $PID
  # 等待几秒确保进程退出
  sleep 2
fi

# 启动程序（后台运行）
echo "Starting $APP_NAME ..."
nohup ./$APP_NAME > app.log 2>&1 &

echo "$APP_NAME restarted successfully."
