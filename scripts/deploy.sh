#!/bin/bash
PROJECT_DIR="~/logs"
SHORT_NAME=logs
SERVICE_NAME=logs
APPNAME=$SHORT_NAME-linux-amd64

SERVER=ad5
DEST=$PROJECT_DIR

if [ -n "$1" ]; then
    SERVER=$1
fi

ssh $SERVER "cd $DEST;mv $APPNAME $APPNAME.bak"
scp -C bin/$APPNAME $SERVER:$DEST

ssh $SERVER "systemctl restart $SERVICE_NAME"
