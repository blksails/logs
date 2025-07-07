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
scp -r ./configs  $SERVER:$DEST/configs
scp scripts/logs.service $SERVER:$DEST

# Install and enable the service
ssh $SERVER "sudo cp $DEST/logs.service /etc/systemd/system/ && \
    sudo systemctl daemon-reload && \
    sudo systemctl enable logs.service && \
    sudo systemctl restart logs.service"

