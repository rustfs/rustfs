# Profiling And NUMA Capability Inventory

This inventory covers `G-013` for `rustfs/backlog#667`. It records the current
profiling, memory sampling, allocator, and NUMA baseline before optional runtime
sidecars are designed.

## Platform Support Matrix

| Capability | Current support | Current owner | Baseline recommendation |
|---|---|---|---|
| CPU pprof dump | Linux and macOS builds use `pprof`; other targets return an unsupported-platform error. | `rustfs/src/profiling.rs` | Keep CPU profiling opt-in through existing env flags and cancellation token. |
| Continuous CPU profiling | Linux and macOS builds can hold a continuous `ProfilerGuard` when enabled. | `rustfs/src/profiling.rs` | Preserve single-guard ownership and avoid starting multiple continuous guards. |
| Periodic CPU profiling | Linux and macOS builds can spawn a periodic sampling loop. | `rustfs/src/profiling.rs` | Keep the loop cancellation-driven and non-fatal. |
| Jemalloc memory pprof | Only `linux` + `gnu` + `x86_64` exposes jemalloc pprof dumping. Other supported builds return an unsupported-target error. | `rustfs/src/profiling.rs` | Treat memory pprof as optional and target-gated. |
| Periodic memory pprof | Only runs where jemalloc profiling control is available and active. | `rustfs/src/profiling.rs` | Keep inactive jemalloc as a skipped dump, not a startup failure. |
| Process/system memory sampling | Uses `rustfs_io_metrics::snapshot_process_resource_and_system` plus `sysinfo` total memory. | `rustfs/src/memory_observability.rs` | Keep sampling portable and metric-gated. |
| cgroup memory sampling | Reads Linux cgroup v2 or v1 memory files when present. Missing files produce no cgroup split. | `rustfs/src/memory_observability.rs` | Keep cgroup data opportunistic and absent-safe. |
| Allocator reclaim | Uses jemalloc backend on `linux` + `gnu` + `x86_64`; otherwise mimalloc variants. | `rustfs/src/allocator_reclaim.rs` | Keep backend detection read-only and preserve effective-force behavior. |
| eBPF | No runtime eBPF sidecar is currently wired into startup. | N/A | Treat eBPF as future optional Linux-only inventory, never as a required baseline. |
| NUMA | No NUMA placement or topology controller is currently wired into startup. | N/A | Treat NUMA as future optional capability with no-op fallback. |

## Cross-Platform Baseline

The current safe baseline is:

- Profiling is opt-in through env flags and must not make startup fatal.
- Startup and shutdown call profiling through `startup_profiling` lifecycle
  hooks; `profiling.rs` remains the CPU/memory profiling implementation and
  admin dump API owner.
- Unsupported profiling targets return structured unsupported errors or skip
  startup tasks.
- Memory observability records process/system metrics and adds cgroup split
  only when cgroup files exist.
- Allocator reclaim observes active HTTP, delete-tail, scanner, heal, erasure,
  and GET-buffer activity before reclaiming.
- Runtime thread sizing remains owned by the Tokio runtime builder and sysinfo
  core detection, not NUMA topology.

## Optional Sidecar Invariants

Future sidecars for profiling, eBPF, or NUMA must preserve these invariants:

- Sidecars must be disabled by default or target-gated until explicitly enabled.
- Unsupported targets must degrade to no-op status, not panic or fail startup.
- Sidecars must use the runtime cancellation token or an equivalent explicit
  shutdown handle.
- Sidecars must not mutate Tokio worker counts after runtime creation.
- Profiling output directory fallback must stay local to profiling and must not
  affect object storage paths.
- NUMA fallback must preserve current runtime thread defaults, storage set
  placement, and request admission behavior.

## First Implementation Candidates

`API-013`:

- Define a read-only capability contract for profiling, cgroup memory, eBPF,
  allocator backend, and NUMA availability.
- Keep the contract in a low-dependency crate and report unsupported states
  explicitly.

`R-016`:

- Wire storage runtime startup to consume capability snapshots read-only.
- Do not start sidecars or mutate runtime worker ownership in the same PR.

`X-012`:

- Define the `ops.profiler.v1` extension schema for profiling capability
  reporting, backend status, redaction requirements, and provenance.
- Keep the schema capability-only; it must not request profiler execution,
  start sidecars, or change profile export behavior.
- Keep unsupported targets, disabled sidecars, and unknown future backends
  representable as no-op capability states.

`X-013`:

- Add the extension capability snapshot contract for disabled, unsupported, and
  enabled profiler backends.
- Verify optional profiler sidecar and Wasm runtimes stay disabled by default
  and cannot declare a startup fatal boundary.

`R-017`:

- If a runtime service sidecar is added later, start it from the startup service
  boundary with explicit shutdown ownership.
- Preserve current service order, KMS/audit/notification fatal boundaries, and
  scanner/heal startup semantics.
