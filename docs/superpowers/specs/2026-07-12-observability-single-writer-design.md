# Logging Safety Design

## Problem

RustFS file logging and the packaged systemd service can open the same `rustfs.log` inode. The internal appender renames and later removes that inode during rotation while systemd continues writing through its original stdout file descriptor. Non-production logging also forces stdout mirroring even when the operator explicitly sets `RUSTFS_OBS_LOG_STDOUT_ENABLED=false`. The combination creates duplicated logs and an unbounded deleted file that bypasses retention limits.

## Goals

- Make the packaged systemd service send stdout and stderr to journald.
- Preserve explicit file logging as a supported sink.
- Give an explicitly configured stdout-mirror value precedence over environment defaults.
- Reject an exact same-inode stdout/file configuration before logging workers or cleanup start.
- Apply identical resolution and validation to local and OTLP file logging.
- Prevent ECStore data-path spans from serializing collections, metadata, or byte payloads at any level.
- Add deployment and end-to-end guardrails that verify the combined fix.

## Non-goals

- Replacing rename-based rolling or cleaner retention.
- Using `copytruncate`.
- Changing Docker or Helm persistent-log defaults.
- Adding probabilistic sampling, a new trace transport, or generalized error deduplication.

## Design

`log_stdout_enabled` remains optional when read from the environment. Unset means the runtime chooses the existing default: enabled outside production and disabled in production. Explicit `true` and `false` always win. A shared resolver is used by local and OTLP initialization.

On Unix, file logging validates sink ownership after the active file has been opened but before the tracing subscriber and cleanup task are registered. If stdout is a regular file and its device/inode pair equals the active rolling file's device/inode pair, initialization returns a typed observability error with an actionable message. Pipes, sockets, terminals, and different regular files remain valid. Non-Unix behavior is unchanged.

The packaged systemd unit explicitly selects journald for stdout and stderr, leaving the internal rolling appender as the sole owner of its active file.

ECStore per-object, per-disk, per-RPC, and per-batch success spans use TRACE with `skip_all` at the Disk facade, LocalDisk, and RemoteDisk layers. These automatic spans retain only the method/span identity and never serialize arguments. Existing explicit RemoteDisk trace events may retain selected scalar operation fields. INFO remains reserved for low-frequency lifecycle and state changes, while WARN and ERROR retain actionable failures.

Static guardrails pin both invariants: official deployment files must not redirect stdout to the active rolling file, and ECStore data-path instrumentation must declare an explicit non-INFO level and skip heavy payloads.

## Verification

- Table tests pin unset/true/false behavior in production and development.
- Unix tests pin same-inode detection and allow distinct sinks.
- Local and OTLP file paths use the shared helpers.
- A static guard prevents the packaged unit from appending stdout to `rustfs.log`.
- Existing rolling, compression, and cleaner tests remain unchanged and pass.
- Static guardrails prove every scoped ECStore data-path span is TRACE-only with `skip_all` and reject DEBUG RemoteDisk success events.
- The four-node concurrency-64 Warp workload is the final acceptance test for deleted descriptors, log growth, throughput, `unexpected EOF`, and `SlowDown`.
