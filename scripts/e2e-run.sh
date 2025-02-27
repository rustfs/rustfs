#!/bin/bash -ex
BIN=$1
VOLUME=$2

chmod +x $BIN
sudo mkdir -p $VOLUME

export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
export RUST_BACKTRACE=full

sudo nohup $BIN $VOLUME > /tmp/rustfs.log 2>&1 &
