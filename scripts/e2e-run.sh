#!/bin/bash -ex
BIN=$1
VOLUME=$2

chmod +x $BIN
mkdir -p $VOLUME

export RUST_LOG="rustfs=debug,ecstore=debug,s3s=debug,iam=debug"
export RUST_BACKTRACE=full
$BIN $VOLUME > /tmp/rustfs.log 2>&1 &

sleep 10

export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000
export RUST_LOG="s3s_e2e=debug,s3s_test=info,s3s=debug"
export RUST_BACKTRACE=full
s3s-e2e

killall $BIN
