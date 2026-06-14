# Recovery-Monitor Log Baseline

This document provides a small end-to-end baseline for a request-scoped
failure that triggers a `recovery-monitor` child span.

## Covered Scenario

- Parent request span carries `request_id`
- A network-like remote-disk operation error marks the disk suspect
- The request path spawns a `recovery-monitor` child task
- The child task keeps the parent request context in the span chain

## Validated By

- `cargo test -p rustfs-ecstore remote_disk_network_error_starts_recovery_monitor_with_request_context --lib`
- `cargo test -p rustfs-obs test_json_log_layer_promotes_parent_request_id_for_recovery_monitor_child_span --lib`

## Representative NDJSON Sample

The exact timestamp, filename, line number, and thread identifiers vary by
build and runtime. The structural fields below are the baseline to preserve.

```json
{"timestamp":"2026-06-14T12:00:00+08:00","level":"WARN","message":"Remote disk marked suspect","event":"remote_disk_health","component":"ecstore","subsystem":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","reason":"operation_network_error","runtime_state":"Suspect","state":"marked_suspect","target":"rustfs_ecstore::rpc::remote_disk","filename":"crates/ecstore/src/rpc/remote_disk.rs","line_number":688,"span":{"request_id":"req-remote-disk-e2e","name":"request-span"},"spans":[{"request_id":"req-remote-disk-e2e","name":"request-span"}],"threadName":"main","threadId":"ThreadId(1)","request_id":"req-remote-disk-e2e"}
{"timestamp":"2026-06-14T12:00:00+08:00","level":"DEBUG","message":"Remote disk recovery monitor started","event":"remote_disk_health","component":"ecstore","subsystem":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","state":"recovery_monitor_started","target":"rustfs_ecstore::rpc::remote_disk","filename":"crates/ecstore/src/rpc/remote_disk.rs","line_number":413,"span":{"component":"ecstore","subsystem":"remote_disk","kind":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","name":"recovery-monitor"},"spans":[{"request_id":"req-remote-disk-e2e","name":"request-span"},{"component":"ecstore","subsystem":"remote_disk","kind":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","name":"recovery-monitor"}],"threadName":"main","threadId":"ThreadId(1)","request_id":"req-remote-disk-e2e"}
{"timestamp":"2026-06-14T12:00:00+08:00","level":"DEBUG","message":"Remote disk recovery monitor cancelled","event":"remote_disk_health","component":"ecstore","subsystem":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","state":"recovery_monitor_cancelled","target":"rustfs_ecstore::rpc::remote_disk","filename":"crates/ecstore/src/rpc/remote_disk.rs","line_number":425,"span":{"component":"ecstore","subsystem":"remote_disk","kind":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","name":"recovery-monitor"},"spans":[{"request_id":"req-remote-disk-e2e","name":"request-span"},{"component":"ecstore","subsystem":"remote_disk","kind":"remote_disk","endpoint":"http://127.0.0.1:59997/data","addr":"http://127.0.0.1:59997","name":"recovery-monitor"}],"threadName":"main","threadId":"ThreadId(1)","request_id":"req-remote-disk-e2e"}
```

## What To Check In Production

- The top-level `request_id` must still equal the parent request's ID.
- The current `span.name` should be `recovery-monitor`.
- The `spans` chain should contain both:
  - the parent request span with `request_id`
  - the child `recovery-monitor` span with `kind`
- `marked_suspect` should appear before `recovery_monitor_started` for this flow.
