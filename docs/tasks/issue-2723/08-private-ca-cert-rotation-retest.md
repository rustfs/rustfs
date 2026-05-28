# Task 08: Private CA + Certificate Rotation Retest

## Goal

Validate that site-replication admin peer calls remain healthy in a private CA environment, including after TLS certificate rotation.

## Scope

- management-plane site replication calls (`/rustfs/admin/v3/site-replication/*`)
- private CA trust path (`RUSTFS_TLS_PATH`)
- outbound TLS generation refresh behavior after rotation

## Preconditions

1. Two RustFS nodes are running with HTTPS enabled.
2. Both nodes use certificates signed by a private CA.
3. `RUSTFS_TLS_PATH` is mounted in both containers and readable by the runtime user.
4. Baseline replication topology contains exactly 2 sites (no ghost duplicate).

## Baseline Commands

```bash
# node-1
curl -sk https://127.0.0.1:9001/rustfs/console/health

# node-2
curl -sk https://127.0.0.1:9001/rustfs/console/health
```

Expected:

1. both console health endpoints return `ready=true`
2. no `Volume not found` startup crash in container logs

## Step A: Verify TLS Inspect UX Compatibility

```bash
docker exec -it rustfs rustfs tls inspect --path /opt/tls
docker exec -it rustfs rustfs tls inspect --tls-path /opt/tls
```

Expected:

1. both commands execute TLS inspection logic
2. no fallback into server bootstrap path
3. no fatal `Volume not found` for these utility invocations

## Step B: Private-CA Site Replication Baseline

1. Trigger a site replication action from UI (for example: start sync / resync).
2. Capture status from both nodes:

```bash
curl -sk "https://<node-1-admin>/rustfs/admin/v3/site-replication/status?peer-state=true&metrics=true"
curl -sk "https://<node-2-admin>/rustfs/admin/v3/site-replication/status?peer-state=true&metrics=true"
```

Expected:

1. no `tls handshake` error in RustFS logs for peer admin requests
2. peer state is not stuck at `Unknown`
3. sync/resync action returns success (or partial success with actionable non-TLS reason)

## Step C: Rotate TLS Material (Private CA Scenario)

Perform a controlled cert rotation on both nodes:

1. replace cert/key and CA bundle under mounted `RUSTFS_TLS_PATH`
2. keep endpoint names stable (SAN/CN remains compatible with configured endpoints)
3. restart nodes or wait for runtime reload path, depending on deployment policy

Suggested evidence:

```bash
# optional diagnostics endpoint if enabled in deployment tooling
curl -sk https://<node-admin>/rustfs/admin/v3/tls/debug
```

## Step D: Post-Rotation Replication Recheck

Repeat Step B immediately after rotation.

Expected:

1. site replication peer admin calls still succeed
2. no persistent stale TLS trust behavior after rotation
3. no requirement to manually wipe replication state

## Step E: Restart Stability

Restart both nodes one more time and recheck:

```bash
curl -sk "https://<node-1-admin>/rustfs/admin/v3/site-replication/info"
curl -sk "https://<node-2-admin>/rustfs/admin/v3/site-replication/info"
```

Expected:

1. still exactly 2 sites
2. no duplicate identity resurrection (`http`/`https` drift ghost)
3. peer status remains stable

## Pass Criteria

All of the following must be true:

1. TLS utility command works with both `--path` and `--tls-path`
2. private CA management-plane replication succeeds before rotation
3. private CA management-plane replication succeeds after rotation
4. no duplicate-site regression after restart
5. no fatal CLI diagnostic regression (`Volume not found`) for `tls inspect`

## Failure Capture Template

If failed, capture and attach:

1. RustFS version/tag/commit
2. exact command and full stderr/stdout
3. container logs around failure window
4. `site-replication/status` and `site-replication/info` outputs from both nodes
5. whether failure reproduces with both `--path` and `--tls-path`
