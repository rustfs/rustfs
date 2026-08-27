# RustFS Heal Test

Node-outage heal test driven by
[`scripts/test/rustfs_heal_test.sh`](rustfs_heal_test.sh), based on the
Obsidian note "RustFS Heal 测试步骤". Uses the same 3-node test environment as
the pool expansion test (`vm000 vm001 vm002`).

All status checks talk to the RustFS admin API directly (SigV4-signed,
`jq` assertions), no `rc` required.

## What it does

1. Downloads the `.deb` package on all nodes (release tag or a direct URL such
   as the nightly/R2 package).
2. Installs it, writes the 3x4 config
   (`http://rustfs-node{1...3}:9000/data/rustfs{1...4}/mnmd`), starts all
   three nodes simultaneously, verifies the cluster is up.
3. Writes data with `warp` while monitoring disk usage on the surviving nodes
   (`df -B1G | grep /data/rustfs`):
   - when both surviving nodes reach `STOP_NODE_AT_GB` (default 15 GiB), stop
     the outage node (`vm002`, `OUTAGE_NODE_INDEX=2`);
   - keep writing until both surviving nodes reach `WARP_STOP_AT_GB`
     (default 40 GiB), then stop warp.
4. Restarts the outage node.
5. Starts cluster heal: `POST /rustfs/admin/v3/heal/` with body
   `{"recursive":true}` (retried, returns a `clientToken`).
6. Monitors the heal task via `POST /rustfs/admin/v3/heal/?clientToken=<token>`
   until the server verdict is a terminal success (`finished`/`completed`) with
   `objects_failed == 0`.
7. Result analysis: heal stats (scanned/healed/failed), an **S3 read-back
   verification** of the written objects (list the test bucket and GET a
   sample — every read must succeed), per-node disk usage (observability),
   pass/fail verdict.

Success is the server's own scan/repair verdict (heal finished, 0 failed)
**plus** an end-to-end data read-back; per-node disk usage is logged as
observability, not a pass gate (EC distributes different shards per node, so a
fixed per-node GB target is not a meaningful invariant).

## Self-hosted runner prerequisites

- Register the admin host (e.g. `heal`) as a runner with the
  `smoke-testing` label.
- Install `jq`, `openssl`, `curl` and `warp` on the runner. `rc` is **not**
  required.
- The runner user must be able to SSH to `vm000/vm001/vm002` without a
  password prompt; nodes need passwordless `sudo` for the SSH user and
  resolvable `rustfs-node*` hostnames.
- Admin API credentials need the `admin:server-info`, `admin:heal` and
  `admin:rebalance` actions.

## Configuration

Same repository secrets/variables as the pool expansion workflow:

| Kind   | Name                  | Purpose                                        |
| ------ | --------------------- | ---------------------------------------------- |
| Secret | `RUSTFS_ACCESS_KEY`   | RustFS access key (default `rustfs@test`)      |
| Secret | `RUSTFS_SECRET_KEY`   | RustFS secret key (default `rustfs@test`)      |
| Var    | `RUSTFS_API_ENDPOINT` | Admin API endpoint, e.g. `http://127.0.0.1:9000` (`RUSTFS_RC_ENDPOINT` fallback) |
| Var    | `RUSTFS_NODES`        | `vm000 vm001 vm002`                            |
| Var    | `RUSTFS_SSH_USER`     | `azureuser`                                    |
| Var    | `RUSTFS_NIGHTLY_PACKAGE_URL` | Default nightly deb URL (defaults to the R2 `latest` alias) |

## Workflow inputs

| Input            | Default | Meaning                                   |
| ---------------- | ------- | ----------------------------------------- |
| `package_url`    | nightly | Direct `.deb` URL; empty = latest nightly |
| `stop_node_gb`   | `15`    | Stop outage node at N GiB on survivors    |
| `warp_stop_gb`   | `40`    | Stop warp at N GiB on survivors           |
| `cleanup_before` | `true`  | Reset nodes before the test               |
| `cleanup_after`  | `true`  | Reset nodes after the test                |

> ⚠️ `--reset` purges the `rustfs` package and deletes the data directories on
> all nodes. Only run against a dedicated test environment.

## Manual usage

```bash
./scripts/test/rustfs_heal_test.sh --all -y \
  --package-url https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb \
  --endpoint http://127.0.0.1:9000

./scripts/test/rustfs_heal_test.sh --steps 5,6,7
./scripts/test/rustfs_heal_test.sh --reset -y
```

## Known issues

- Nightly builds gate pool/rebalance activation on a live fleet capability
  proof (rustfs/backlog#2031); the script retries heal/rebalance starts and
  prints a hint when the signature appears.
- The cluster-level `GET /rustfs/admin/v3/background-heal/status` aggregator
  returns 501 in the single-pool 3x4 topology (no notification system), so the
  script monitors the started heal task via its `clientToken` instead.
- The heal task may report `progress: null` while running; the script logs
  this as evidence (rustfs/backlog#2035) rather than coercing it to zero, and
  reads the canonical camelCase progress fields
  (`objectsScanned`/`objectsHealed`/`objectsFailed`/`progressPercentage`) with
  a snake_case fallback.
- The server-side per-task heal timeout defaults to 5 minutes; the script
  writes `RUSTFS_HEAL_TASK_TIMEOUT_SECS=21600` (6h) into the node config so a
  multi-tens-of-GiB heal can finish. The background scanner is disabled
  (`RUSTFS_HEAL_AUTO_HEAL_ENABLE=false`) so the explicit heal is the only
  repair mechanism and the outage effect stays observable.
