# Admin peer probe timeout

RustFS admin server information and storage information aggregate read-only
state from remote peers. A peer may answer the RPC while its local disk
diagnostic is still recovering after a restart or outage, so these probes use a
bounded per-peer round budget.

## Configuration

| Environment variable | Default | Accepted range | Behavior |
| --- | ---: | ---: | --- |
| `RUSTFS_ADMIN_PEER_PROBE_TIMEOUT_SECS` | `10` seconds | `1..=60` seconds | Total budget for one peer probe round; `server_info` may reconnect once, while `storage_info` remains a single attempt. |

`0` and invalid values fall back to the default. Values above `60` are clamped
to `60`. The timeout is read by the node aggregating the admin response; it is
not a wire or mixed-version protocol setting.

Any retry shares the same per-peer deadline. A fast transport failure can still
trigger the existing reconnect retry, but a slow first attempt consumes the
remaining budget and cannot add another full timeout. Configure this value
with margin below any external health-check deadline (for example, a
keepalived script timeout); the default preserves the previous two-attempt
worst-case budget and may need to be lowered for a tighter watchdog.

This setting does not change `RUSTFS_INTERNODE_RPC_TIMEOUT_SECS` or the drive
health policy. A disk probe timeout can still update drive health according to
`RUSTFS_DRIVE_TIMEOUT_HEALTH_ACTION`.
