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