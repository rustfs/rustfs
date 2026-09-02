# Container resource detection

**Use this when:** RustFS runs under a cgroup CPU or memory limit (Kubernetes, Docker) and you need to know which limit it detected, how to override it, or which log line and metrics expose it.
**Source of truth:** `rustfs/src/cgroup_resources.rs` (detection, `ContainerResources`, `container_resources()`), `rustfs/src/memory_observability.rs` (metrics), `rustfs/src/server/runtime.rs` (Tokio thread sizing consumer), `rustfs/src/startup_entrypoint.rs` (startup log call).

RustFS resolves the CPU core count and the memory limit once at startup (cached in a `OnceLock`) and uses them for Tokio worker/blocking-thread sizing, the memory budget, and memory metrics. Precedence is override env var, then cgroup limit, then host value from `sysinfo`.

## Detection rules

| Resource | Order | Source | Rule |
| --- | --- | --- | --- |
| CPU | 1 | cgroup v2 `/sys/fs/cgroup/cpu.max` | `"<quota> <period>"` (or bare `"<quota>"` with period 100000) gives `ceil(quota / period)`; `"max"` or a zero quota means no limit. |
| CPU | 2 | cgroup v1 `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` with `cpu.cfs_period_us` | `ceil(quota / period)`; a zero, unparsable, or `u64::MAX` quota means no limit. |
| CPU | 3 | host | `sysinfo` CPU count, minimum 1. |
| Memory | 1 | cgroup v2 `/sys/fs/cgroup/memory.max` | bytes; `"max"` means no limit. |
| Memory | 2 | cgroup v1 `/sys/fs/cgroup/memory/memory.limit_in_bytes` | bytes; values `>= 1 << 62` mean no limit. |
| Memory | 3 | host | `sysinfo` total memory. |

cgroup reads are compiled only for Linux; other platforms always take the host branch. `cgroup_detected` is true when at least one of the two cgroup reads returned a limit.

## Environment variables

Names are the constants `ENV_DISABLE_CGROUP_DETECTION`, `ENV_OVERRIDE_CPU_CORES`, and `ENV_OVERRIDE_MEMORY_BYTES` in `rustfs/src/cgroup_resources.rs`.

| Variable | Accepted values | Effect |
| --- | --- | --- |
| `RUSTFS_DISABLE_CGROUP_DETECTION` | `1` or `true` (case-insensitive) | Skip cgroup reads; host values apply unless overridden. |
| `RUSTFS_OVERRIDE_CPU_CORES` | integer `> 0` | Replaces the CPU core count regardless of cgroup or host. |
| `RUSTFS_OVERRIDE_MEMORY_BYTES` | integer `> 0`, bytes | Replaces the memory limit regardless of cgroup or host. |

Non-positive or unparsable override values are ignored. Changes take effect on process restart.

## Startup log

`log_container_resources` emits exactly one of these lines with `cpu_cores` and `memory_bytes` fields (the INFO variants also carry `memory_mib`):

| Message | Level | Condition |
| --- | --- | --- |
| `container resources (overridden by environment variables)` | INFO | an override env var was applied; also carries `cgroup_detected` |
| `container resources (detected from cgroup)` | INFO | no override, at least one cgroup limit read |
| `container resources (using host values)` | DEBUG | neither override nor cgroup limit |

## Metrics

Gauges emitted from `rustfs/src/memory_observability.rs`.

| Metric | Meaning |
| --- | --- |
| `rustfs_memory_effective_total_bytes{basis}` | Effective memory total. `basis` is `cgroup` when a cgroup limit was detected, else `host`; an override does not change the basis label. |
| `rustfs_container_cpu_cores` | Effective CPU cores. |
| `rustfs_container_memory_bytes` | Effective memory limit in bytes. |
| `rustfs_container_cgroup_detected` | `1` when a cgroup limit was read, else `0`. |
| `rustfs_container_overridden` | `1` when an override env var was applied, else `0`. |
| `rustfs_memory_total_bytes`, `rustfs_memory_usage_percent` | Computed against the effective total (`record_memory_usage` in `crates/io-metrics/src/lib.rs`). |

## Troubleshooting

When effective values look like the host rather than the container:

1. Confirm detection is not disabled: `env | grep RUSTFS_DISABLE_CGROUP_DETECTION`.
2. Find the startup line: `grep "container resources" <rustfs log>`. The `(using host values)` variant is DEBUG, so raise the log level if no variant appears.
3. Inspect the cgroup filesystem inside the container:

```bash
stat -fc %T /sys/fs/cgroup/                                                        # cgroup2fs = v2, tmpfs = v1
cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max                                # v2
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us /sys/fs/cgroup/memory/memory.limit_in_bytes # v1
```

4. If the runtime does not expose limits to the container, pin them with `RUSTFS_OVERRIDE_CPU_CORES` / `RUSTFS_OVERRIDE_MEMORY_BYTES` and confirm `rustfs_container_overridden` reads `1`.
