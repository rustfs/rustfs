# RustFS Scanner

RustFS Scanner 是后台维护扫描循环，负责用量统计、生命周期过期和分层转移准入、桶复制修复准入、scanner 发起的 heal/bitrot 检查，以及命名空间告警。

面向运维人员的运行时控制项、状态字段和调参流程，请参考
[Scanner Runtime Controls](../../docs/operations/scanner-runtime-controls.md)。

英文文档请参考 [README.md](README.md)。

## 开发

### 构建

```bash
cargo build --package rustfs-scanner
```

### 测试

```bash
cargo test --package rustfs-scanner
```

## 许可证

Apache License 2.0
