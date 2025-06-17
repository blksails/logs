#!/bin/bash
PROJECT_DIR="~/logs"
SHORT_NAME=logs
APPNAME=$SHORT_NAME-linux-amd64

SERVER=ad5
DEST=$PROJECT_DIR

# if $1 is set used it as the server name
if [ -n "$1" ]; then
    SERVER=$1
fi

ssh $SERVER "mkdir -p $DEST"
scp -C bin/$APPNAME $SERVER:$DEST
scp -r -C bin/configs  $SERVER:$DEST/configs
# ssh $SERVER "sudo dnf config-manager --add-repo=https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo"
# ssh $SERVER "sudo dnf -y install dnf-plugin-releasever-adapter --repo alinux3-plus"
# ssh $SERVER "sudo dnf -y install docker-ce --nobest"

