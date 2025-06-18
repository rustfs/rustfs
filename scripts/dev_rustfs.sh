#!/bin/bash

# ps -ef | grep rustfs | awk '{print $2}'| xargs kill -9

# 本地 rustfs.zip 路径
ZIP_FILE="./rustfs.zip"
# 解压目标
UNZIP_TARGET="./"


SERVER_LIST=(
    "root@node1" # node1
    "root@node2" # node2
    "root@node3" # node3
    "root@node4" # node4
    # "root@node5" # node5
    # "root@node6" # node6
    # "root@node7" # node7
    # "root@node8" # node8
)

REMOTE_TMP="~/rustfs"

# 部署 rustfs 到所有服务器
deploy() {
    echo "解压 $ZIP_FILE ..."
    unzip -o "$ZIP_FILE" -d "$UNZIP_TARGET"
    if [ $? -ne 0 ]; then
        echo "解压失败，退出"
        exit 1
    fi

    LOCAL_RUSTFS="${UNZIP_TARGET}rustfs"
    if [ ! -f "$LOCAL_RUSTFS" ]; then
        echo "未找到解压后的 rustfs 文件，退出"
        exit 1
    fi

    for SERVER in "${SERVER_LIST[@]}"; do
        echo "上传 $LOCAL_RUSTFS 到 $SERVER:$REMOTE_TMP"
        scp "$LOCAL_RUSTFS" "${SERVER}:${REMOTE_TMP}"
        if [ $? -ne 0 ]; then
            echo "❌ 上传到 $SERVER 失败，跳过"
            continue
        fi

        echo "在 $SERVER 上操作 systemctl 和文件替换"
        ssh "$SERVER" bash <<EOF
set -e
echo "停止 rustfs 服务"
sudo systemctl stop rustfs || true
echo "覆盖 /usr/local/bin/rustfs"
sudo cp ~/rustfs /usr/local/bin/rustfs
sudo chmod +x /usr/local/bin/rustfs
echo "启动 rustfs 服务"
sudo systemctl start rustfs
echo "检测 rustfs 服务状态"
sudo systemctl status rustfs --no-pager --lines=10
EOF

        if [ $? -eq 0 ]; then
            echo "✅ $SERVER 部署并重启 rustfs 成功"
        else
            echo "❌ $SERVER 部署或重启 rustfs 失败"
        fi
    done
}

# 清空 /data/rustfs0~3 目录下所有文件（包括隐藏文件）
clear_data_dirs() {
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "清空 $SERVER:/data/rustfs0~3 下所有文件"
        ssh "$SERVER" bash <<EOF
for i in {0..3}; do
    DIR="/data/rustfs$i"
    echo "处理 $DIR"
    if [ -d "$DIR" ]; then
        echo "清空 $DIR"
        sudo rm -rf "$DIR"/* "$DIR"/.[!.]* "$DIR"/..?* 2>/dev/null || true
        echo "已清空 $DIR"
    else
        echo "$DIR 不存在，跳过"
    fi
done
EOF
    done
}

# 控制 rustfs 服务
stop_rustfs() {
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "停止 $SERVER rustfs 服务"
        ssh "$SERVER" "sudo systemctl stop rustfs"
    done
}

start_rustfs() {
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "启动 $SERVER rustfs 服务"
        ssh "$SERVER" "sudo systemctl start rustfs"
    done
}

restart_rustfs() {
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "重启 $SERVER rustfs 服务"
        ssh "$SERVER" "sudo systemctl restart rustfs"
    done
}

# 向所有服务器追加公钥到 ~/.ssh/authorized_keys
add_ssh_key() {
    if [ -z "$2" ]; then
        echo "用法: $0 addkey <pubkey_file>"
        exit 1
    fi
    PUBKEY_FILE="$2"
    if [ ! -f "$PUBKEY_FILE" ]; then
        echo "指定的公钥文件不存在: $PUBKEY_FILE"
        exit 1
    fi
    PUBKEY_CONTENT=$(cat "$PUBKEY_FILE")
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "追加公钥到 $SERVER:~/.ssh/authorized_keys"
        ssh "$SERVER" "mkdir -p ~/.ssh && chmod 700 ~/.ssh && echo '$PUBKEY_CONTENT' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
        if [ $? -eq 0 ]; then
            echo "✅ $SERVER 公钥追加成功"
        else
            echo "❌ $SERVER 公钥追加失败"
        fi
    done
}

monitor_logs() {
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "监控 $SERVER:/var/logs/rustfs/rustfs.log ..."
        ssh "$SERVER" "tail -F /var/logs/rustfs/rustfs.log" |
            sed "s/^/[$SERVER] /" &
    done
    wait
}

set_env_file() {
    if [ -z "$2" ]; then
        echo "用法: $0 setenv <env_file>"
        exit 1
    fi
    ENV_FILE="$2"
    if [ ! -f "$ENV_FILE" ]; then
        echo "指定的环境变量文件不存在: $ENV_FILE"
        exit 1
    fi
    for SERVER in "${SERVER_LIST[@]}"; do
        echo "上传 $ENV_FILE 到 $SERVER:~/rustfs.env"
        scp "$ENV_FILE" "${SERVER}:~/rustfs.env"
        if [ $? -ne 0 ]; then
            echo "❌ 上传到 $SERVER 失败，跳过"
            continue
        fi
        echo "覆盖 $SERVER:/etc/default/rustfs"
        ssh "$SERVER" "sudo mv ~/rustfs.env /etc/default/rustfs"
        if [ $? -eq 0 ]; then
            echo "✅ $SERVER /etc/default/rustfs 覆盖成功"
        else
            echo "❌ $SERVER /etc/default/rustfs 覆盖失败"
        fi
    done
}

# 主命令分发
case "$1" in
    deploy)
        deploy
        ;;
    clear)
        clear_data_dirs
        ;;
    stop)
        stop_rustfs
        ;;
    start)
        start_rustfs
        ;;
    restart)
        restart_rustfs
        ;;
    addkey)
        add_ssh_key "$@"
        ;;
    monitor_logs)
        monitor_logs
        ;;
    setenv)
        set_env_file "$@"
        ;;
    *)
        echo "用法: $0 {deploy|clear|stop|start|restart|addkey <pubkey_file>|monitor_logs|setenv <env_file>}"
        ;;
esac