#!/bin/bash

# RustFS 删除标记消失问题复现脚本
# 使用两个 RustFS 实例测试主动-主动复制环境中的删除标记问题

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
RUSTFS1_PORT=9000
RUSTFS1_CONSOLE_PORT=9001
RUSTFS2_PORT=9002
RUSTFS2_CONSOLE_PORT=9003
RUSTFS1_DATA_DIR="/tmp/rustfs1"
RUSTFS2_DATA_DIR="/tmp/rustfs2"
TEST_BUCKET="testb"
TEST_FILE="/tmp/test.txt"
TEST_OBJECT="test3.txt"

# 错误处理函数
error_exit() {
    echo -e "${RED}错误: $1${NC}" >&2
    cleanup
    exit 1
}

# 信息输出
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# 成功输出
success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# 警告输出
warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 清理函数
cleanup() {
    info "清理进程和临时文件..."

    # 停止 RustFS 进程 - 优先使用保存的PID
    if [ -f /tmp/rustfs1.pid ]; then
        RUSTFS1_PID=$(cat /tmp/rustfs1.pid)
        if ps -p $RUSTFS1_PID > /dev/null; then
            sudo kill -TERM $RUSTFS1_PID 2>/dev/null || true
            sleep 2
            sudo kill -KILL $RUSTFS1_PID 2>/dev/null || true
        fi
        rm -f /tmp/rustfs1.pid
    fi

    if [ -f /tmp/rustfs2.pid ]; then
        RUSTFS2_PID=$(cat /tmp/rustfs2.pid)
        if ps -p $RUSTFS2_PID > /dev/null; then
            sudo kill -TERM $RUSTFS2_PID 2>/dev/null || true
            sleep 2
            sudo kill -KILL $RUSTFS2_PID 2>/dev/null || true
        fi
        rm -f /tmp/rustfs2.pid
    fi

    # 备用清理方法 - 如果PID文件不存在
    if pgrep -f "rustfs.*${RUSTFS1_DATA_DIR}" > /dev/null; then
        sudo pkill -f "rustfs.*${RUSTFS1_DATA_DIR}" || true
    fi
    if pgrep -f "rustfs.*${RUSTFS2_DATA_DIR}" > /dev/null; then
        sudo pkill -f "rustfs.*${RUSTFS2_DATA_DIR}" || true
    fi

    # 删除数据目录
    sudo rm -rf "${RUSTFS1_DATA_DIR}" "${RUSTFS2_DATA_DIR}" || true

    # 删除临时文件
    rm -f /tmp/replication-config.json
    rm -f /tmp/rustfs1.log /tmp/rustfs2.log

    # 删除 mc 配置
    ~/mc alias remove rustfs1 2>/dev/null || true
    ~/mc alias remove rustfs2 2>/dev/null || true

    info "清理完成"
}

# 检查依赖
check_dependencies() {
    info "检查依赖项..."

    # 检查 mc 客户端
    if ! command -v ~/mc &> /dev/null; then
        error_exit "mc 客户端未安装。请先运行: wget https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && mv mc ~/"
    fi

    # 检查 RustFS 二进制文件
    if [ ! -f "./target/debug/rustfs" ]; then
        error_exit "RustFS 二进制文件不存在。请先运行: cargo build"
    fi

    success "依赖项检查通过"
}

# 准备环境
prepare_environment() {
    info "准备测试环境..."

    # 创建数据目录
    mkdir -p "${RUSTFS1_DATA_DIR}" "${RUSTFS2_DATA_DIR}" ||
        error_exit "无法创建数据目录"

    # 创建测试文件
    echo "test content for replication - $(date)" > "${TEST_FILE}" ||
        error_exit "无法创建测试文件"

    success "环境准备完成"
}

# 启动 RustFS 实例
start_rustfs_instances() {
    info "启动 RustFS 实例..."

    # 启动 rustfs1
    cd "$(pwd)"
    export RUST_LOG=trace
    export RUSTFS_OBS_ENDPOINT="http://localhost:4318"
    export RUSTFS_ACCESS_KEY="admin"
    export RUSTFS_SECRET_KEY="TOPSECRET"
    export RUSTFS_SERVER_URL="http://rustfs1.example.com"

    sudo -E ./target/debug/rustfs "${RUSTFS1_DATA_DIR}" \
        --address "0.0.0.0:${RUSTFS1_PORT}" \
        --console-enable \
        --console-address "0.0.0.0:${RUSTFS1_CONSOLE_PORT}" \
        > /tmp/rustfs1.log 2>&1 &

    RUSTFS1_PID=$!

    # 启动 rustfs2
    export RUSTFS_SERVER_URL="http://rustfs2.example.com"

    sudo -E ./target/debug/rustfs "${RUSTFS2_DATA_DIR}" \
        --address "0.0.0.0:${RUSTFS2_PORT}" \
        --console-enable \
        --console-address "0.0.0.0:${RUSTFS2_CONSOLE_PORT}" \
        > /tmp/rustfs2.log 2>&1 &

    RUSTFS2_PID=$!

    # 保存PID到文件用于清理
    echo $RUSTFS1_PID > /tmp/rustfs1.pid
    echo $RUSTFS2_PID > /tmp/rustfs2.pid

    info "等待服务启动..."
    sleep 15  # 增加等待时间

    # 验证服务启动
    if ! curl -s "http://localhost:${RUSTFS1_PORT}/minio/health/live" > /dev/null; then
        warn "rustfs1 健康检查失败，检查日志..."
        tail -10 /tmp/rustfs1.log
        error_exit "rustfs1 启动失败"
    fi

    if ! curl -s "http://localhost:${RUSTFS2_PORT}/minio/health/live" > /dev/null; then
        warn "rustfs2 健康检查失败，检查日志..."
        tail -10 /tmp/rustfs2.log
        error_exit "rustfs2 启动失败"
    fi

    success "两个 RustFS 实例启动成功"
}

# 配置 mc 客户端
configure_mc() {
    info "配置 mc 客户端..."

    ~/mc alias set rustfs1 "http://localhost:${RUSTFS1_PORT}" admin TOPSECRET ||
        error_exit "无法配置 rustfs1 alias"

    ~/mc alias set rustfs2 "http://localhost:${RUSTFS2_PORT}" admin TOPSECRET ||
        error_exit "无法配置 rustfs2 alias"

    # 测试连接
    ~/mc ls rustfs1 > /dev/null || error_exit "无法连接到 rustfs1"
    ~/mc ls rustfs2 > /dev/null || error_exit "无法连接到 rustfs2"

    success "mc 客户端配置完成"
}

# 创建桶和启用版本控制
setup_buckets() {
    info "创建桶并启用版本控制..."

    # 先删除可能存在的桶
    warn "清理可能存在的桶..."
    ~/mc rb rustfs1/"${TEST_BUCKET}" --force 2>/dev/null || true
    ~/mc rb rustfs2/"${TEST_BUCKET}" --force 2>/dev/null || true

    # 创建桶
    ~/mc mb rustfs1/"${TEST_BUCKET}" || error_exit "无法创建 rustfs1 桶"
    ~/mc mb rustfs2/"${TEST_BUCKET}" || error_exit "无法创建 rustfs2 桶"

    # 启用版本控制
    ~/mc version enable rustfs1/"${TEST_BUCKET}" || error_exit "无法启用 rustfs1 版本控制"
    ~/mc version enable rustfs2/"${TEST_BUCKET}" || error_exit "无法启用 rustfs2 版本控制"

    # 验证版本控制状态
    ~/mc version info rustfs1/"${TEST_BUCKET}" > /dev/null || warn "rustfs1 版本控制验证失败"
    ~/mc version info rustfs2/"${TEST_BUCKET}" > /dev/null || warn "rustfs2 版本控制验证失败"

    success "桶创建和版本控制配置完成"
}

# 配置复制规则
setup_replication() {
    info "配置复制规则..."

    # 创建复制配置文件
    cat > /tmp/replication-config.json << 'EOF'
{
    "ReplicationConfiguration": {
        "Role": "arn:rustfs:replication:us-east-1:609b95ca-cb66-4587-9dd9-1066daedf21c:testb",
        "Rules": [{
            "ID": "replication-rule-1766126142060",
            "Priority": 1,
            "Status": "Enabled",
            "SourceSelectionCriteria": {
                "SseKmsEncryptedObjects": {"Status": "Enabled"}
            },
            "ExistingObjectReplication": {"Status": "Enabled"},
            "Destination": {
                "Bucket": "arn:rustfs:replication:us-east-1:609b95ca-cb66-4587-9dd9-1066daedf21c:testb",
                "StorageClass": "STANDARD"
            },
            "DeleteMarkerReplication": {"Status": "Enabled"}
        }]
    }
}
EOF

    # 应用复制配置
    ~/mc admin bucket replicate create rustfs1/"${TEST_BUCKET}" --config /tmp/replication-config.json ||
        error_exit "无法配置 rustfs1 复制规则"

    ~/mc admin bucket replicate create rustfs2/"${TEST_BUCKET}" --config /tmp/replication-config.json ||
        error_exit "无法配置 rustfs2 复制规则"

    success "复制规则配置完成"
}

# 执行复现流程
reproduce_issue() {
    info "执行复现流程..."

    # 步骤 1: 上传测试对象
    info "从 rustfs1 上传测试对象..."
    ~/mc cp "${TEST_FILE}" "rustfs1/${TEST_BUCKET}/${TEST_OBJECT}" ||
        error_exit "无法上传测试对象"

    info "验证对象存在..."
    echo "=== rustfs1 对象列表 ==="
    ~/mc ls rustfs1/"${TEST_BUCKET}" --versions
    echo -e "\n=== rustfs2 对象列表 ==="
    ~/mc ls rustfs2/"${TEST_BUCKET}" --versions

    # 等待复制完成
    warn "等待复制完成 (10秒)..."
    sleep 10

    # 步骤 2: 从 rustfs2 删除对象
    info "从 rustfs2 删除对象..."
    ~/mc rm "rustfs2/${TEST_BUCKET}/${TEST_OBJECT}" ||
        error_exit "无法删除对象"

    # 等待删除标记复制
    warn "等待删除标记复制 (5秒)..."
    sleep 5

    # 步骤 3: 观察问题现象
    info "检查删除标记状态..."
    echo -e "\n${RED}=== 预期结果: ===${NC}"
    echo "rustfs1 和 rustfs2 都应该显示删除标记"
    echo -e "\n${BLUE}=== 实际结果: ===${NC}"

    echo -e "\n${YELLOW}rustfs2 对象列表 (删除源):${NC}"
    ~/mc ls rustfs2/"${TEST_BUCKET}" --versions

    echo -e "\n${YELLOW}rustfs1 对象列表 (复制目标):${NC}"
    ~/mc ls rustfs1/"${TEST_BUCKET}" --versions

    # 分析结果
    echo -e "\n${BLUE}=== 问题分析: ===${NC}"
    RUSTFS1_DELETE_MARKER=$(~/mc ls rustfs1/"${TEST_BUCKET}" --versions | grep "DEL" | wc -l)
    RUSTFS2_DELETE_MARKER=$(~/mc ls rustfs2/"${TEST_BUCKET}" --versions | grep "DEL" | wc -l)

    if [ "${RUSTFS2_DELETE_MARKER}" -gt 0 ] && [ "${RUSTFS1_DELETE_MARKER}" -eq 0 ]; then
        echo -e "${RED}✓ 问题复现成功!${NC}"
        echo "- rustfs2 显示删除标记 (${RUSTFS2_DELETE_MARKER} 个)"
        echo "- rustfs1 删除标记消失 (${RUSTFS1_DELETE_MARKER} 个)"
        echo "这证实了删除标记在主动-主动复制环境中消失的问题"
        return 0
    elif [ "${RUSTFS1_DELETE_MARKER}" -gt 0 ] && [ "${RUSTFS2_DELETE_MARKER}" -gt 0 ]; then
        echo -e "${GREEN}✓ 删除标记正常工作${NC}"
        echo "- rustfs1 显示删除标记 (${RUSTFS1_DELETE_MARKER} 个)"
        echo "- rustfs2 显示删除标记 (${RUSTFS2_DELETE_MARKER} 个)"
        return 1
    else
        echo -e "${YELLOW}⚠ 意外结果${NC}"
        echo "- rustfs1 删除标记: ${RUSTFS1_DELETE_MARKER} 个"
        echo "- rustfs2 删除标记: ${RUSTFS2_DELETE_MARKER} 个"
        return 1
    fi
}

# 显示日志
show_logs() {
    echo -e "\n${BLUE}=== RustFS 日志 ===${NC}"
    echo "rustfs1 日志 (最后20行):"
    tail -20 /tmp/rustfs1.log
    echo -e "\nrustfs2 日志 (最后20行):"
    tail -20 /tmp/rustfs2.log
}

# 手动清理函数
manual_cleanup() {
    echo -e "${YELLOW}执行手动清理...${NC}"
    cleanup
    echo -e "${GREEN}清理完成，可以安全退出${NC}"
}

# 主函数
main() {
    # 检查是否是清理模式
    if [ "$1" = "clean" ]; then
        manual_cleanup
        exit 0
    fi

    echo -e "${BLUE}"
    cat << 'EOF'
==================================================
RustFS 删除标记消失问题复现脚本
==================================================

此脚本将:
1. 启动两个 RustFS 实例 (端口 9000, 9002)
2. 配置主动-主动复制
3. 演示删除标记消失问题
4. 分析结果

使用方法:
  ./reproduce_delete_marker_issue.sh    # 运行测试
  ./reproduce_delete_marker_issue.sh clean  # 手动清理

EOF
    echo -e "${NC}"

    # 设置错误时清理
    trap cleanup EXIT

    check_dependencies
    prepare_environment
    start_rustfs_instances
    configure_mc
    setup_buckets
    setup_replication

    # 尝试复现问题
    if reproduce_issue; then
        echo -e "\n${RED}问题已成功复现!${NC}"
        read -p "是否查看日志? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            show_logs
        fi
    else
        echo -e "\n${GREEN}问题未复现或已修复${NC}"
    fi

    echo -e "\n${BLUE}脚本执行完成。按任意键退出...${NC}"
    read -n 1 -s
}

# 运行主函数
main "$@"