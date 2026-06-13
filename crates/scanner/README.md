# RustFS Scanner

RustFS Scanner is the background maintenance scan loop. It handles usage
accounting, lifecycle expiry and transition admission, bucket replication repair
admission, scanner-driven heal/bitrot checks, and namespace alerts.

For operator-facing runtime controls, status fields, and tuning workflows, see
[Scanner Runtime Controls](../../docs/operations/scanner-runtime-controls.md).
For repeatable scanner-pressure validation, see
[Scanner Benchmark Runbook](../../docs/operations/scanner-benchmark-runbook.md).

Chinese documentation is available in [README.zh-CN.md](README.zh-CN.md).

## Development

### Build

```bash
cargo build --package rustfs-scanner
```

### Test

```bash
cargo test --package rustfs-scanner
```

## License

Apache License 2.0
