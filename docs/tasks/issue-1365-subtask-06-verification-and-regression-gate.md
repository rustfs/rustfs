# Subtask 06 - 最终验证与回归闸门

## 目标

- 对前 5 个子任务进行总体验证。
- 将“禁止引入新的 bug 及错误”落实为最终合入闸门。

## 执行项

1. 运行子任务级 targeted tests。
2. 运行格式化与编译检查。
3. 运行仓库总闸门。
4. 做最小故障注入回归确认。

## 最终质量闸门

以下任一项失败，视为不允许完成:

1. 新增或放大 panic、unwrap、expect 风险路径。
2. healthy 节点出现明显误判离线。
3. 读元数据或控制面出现新的稳定性回归。
4. 写路径 cleanup/undo 语义受到影响。
5. `make pre-commit` 未通过。

## 必做验证

### 1. 格式与编译

```bash
cargo fmt --all
cargo fmt --all --check
cargo check -p rustfs-config -p rustfs-protos -p rustfs-ecstore -p rustfs
```

### 2. 针对性测试

```bash
cargo test -p rustfs-ecstore remote_disk -- --nocapture
cargo test -p rustfs-ecstore peer_s3_client -- --nocapture
cargo test -p rustfs-ecstore disk_store -- --nocapture
cargo test -p rustfs-ecstore set_disk -- --nocapture
cargo test -p rustfs admin_usecase -- --nocapture
```

### 3. 仓库总闸门

```bash
make pre-commit
```

### 4. 最小故障注入矩阵

至少确认以下场景:

1. `kill -9` 单节点
2. `systemctl stop` 单节点
3. `iptables DROP` 单节点
4. 经 nginx 入口访问
5. 直连健康 RustFS 节点访问

### 5. 本地 Docker 最终验证

当所有子任务代码完成后，至少执行一轮本地 Docker 模式验证，作为最终交付前的本地验收。

建议执行:

```bash
docker compose -f docker-compose-simple.yml up -d
curl http://127.0.0.1:9000/health
curl http://127.0.0.1:9000/health/ready
DEPLOY_MODE=docker ./scripts/s3-tests/run.sh
docker compose -f docker-compose-simple.yml down -v
```

如果后续补齐多节点 Docker Compose 场景，则在该阶段追加:

1. 单节点停机/断开
2. 首次请求耗时验证
3. 第二批请求耗时验证
4. 控制面 fast-fail 与恢复验证

## 验收标准

1. 所有命令通过。
2. 故障注入场景下，无新的 panic 或稳定性异常。
3. 首次故障请求耗时、第二批请求耗时、控制面耗时均明显优于未改造前。
4. 本地 Docker 模式验证通过。
5. 没有发现新的 correctness 回归。

## 提交要求

- 如验证阶段发现问题，必须先修复再重新验证。
- 最终提交只包含验证阶段确实需要的修复，不混入新范围改动。
