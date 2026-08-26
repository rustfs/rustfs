# RustFS Pool Expansion / Decommission Test

End-to-end test for **storage pool expansion**, **data rebalancing** and
**pool decommission**, driven by
[`scripts/test/rustfs_pool_expand.sh`](rustfs_pool_expand.sh).

All pool / rebalance / decommission checks talk to the RustFS **admin API
directly** with SigV4-signed HTTP requests and assert on the JSON responses
(`jq`), so the results are exact and independent of any client CLI output
formatting. **`rc` is not required.**

## What it does

1. Downloads the RustFS `.deb` package on all nodes (a release tag, or a
   direct URL such as a nightly/R2 package).
2. Installs it (`dpkg -i`).
3. Starts the first pool and verifies it via
   `GET /rustfs/admin/v3/pools/list`.
4. Writes data with `warp` and monitors storage usage
   (`GET /rustfs/admin/v3/storageinfo`) until the threshold.
5. Expands to a second pool (nodes started in parallel).
6. Starts rebalance (`POST /rustfs/admin/v3/rebalance/start`) and waits for
   **all** pools to report `Completed`.
7. (3-pool mode) Expands to a third pool, rebalances again.
8. (optional) Decommissions pool 0
   (`POST /rustfs/admin/v3/pools/decommission`), with automatic clear + retry
   on failure, and waits for `decommissionInfo.complete == true`.

When an assertion fails, the script prints a per-pool summary **and the full
JSON response** (pool state, progress, failure counters, `waitingReason`,
`unresolvedEntries`, last rebalance error), so the GitHub Actions log shows
exactly where the test stopped. Credentials never appear in the logs.

## Self-hosted runner prerequisites

- Register the admin host (e.g. `heal`) as a runner with the
  `smoke-testing` label.
- Install `jq`, `openssl`, `curl` and `warp` (only needed for `--with-warp`)
  on the runner. `rc` is **not** required.
- The runner user must be able to SSH to all nodes without a password prompt
  (`~/.ssh/config` with keys).
- The nodes need passwordless `sudo` for the SSH user, resolvable
  `rustfs-node*` hostnames in `/etc/hosts`, and writable data directories.
- The admin API credentials must have the `admin:server-info`,
  `admin:decommission` and `admin:rebalance` actions.

## Configuration

Set these in the repository (secrets/variables):

| Kind   | Name                  | Purpose                                        |
| ------ | --------------------- | ---------------------------------------------- |
| Secret | `RUSTFS_ACCESS_KEY`   | RustFS access key                              |
| Secret | `RUSTFS_SECRET_KEY`   | RustFS secret key                              |
| Var    | `RUSTFS_API_ENDPOINT` | Admin API endpoint, e.g. `http://10.0.0.7:9000` (`RUSTFS_RC_ENDPOINT` is used as a fallback) |
| Var    | `RUSTFS_NODES`        | Space-separated node names, e.g. `vm000 vm001 vm002` |
| Var    | `RUSTFS_SSH_USER`     | SSH user for the nodes, e.g. `azureuser`       |

## Workflow inputs

| Input               | Default       | Meaning                                  |
| ------------------- | ------------- | ---------------------------------------- |
| `rustfs_version`    | `1.0.0-rc.3`  | GitHub release tag to test               |
| `package_url`       | *(empty)*     | Direct `.deb` URL (e.g. nightly/R2); overrides `rustfs_version` |
| `pools`             | `3`           | Expand to 2 or 3 pools                   |
| `storage_threshold` | `50`          | Stop warp writes at N% usage             |
| `warp_duration`     | `10m`         | warp write duration                      |
| `run_decommission`  | `true`        | Run decommission (3-pool mode only)      |
| `cleanup_before`    | `true`        | Reset nodes before the test              |
| `cleanup_after`     | `true`        | Reset nodes after the test               |

> ⚠️ `cleanup_before` / `cleanup_after` run the script's `--reset` mode, which
> **stops the services and deletes the data directories and config** on all
> nodes. Only use this workflow against a dedicated test environment.

## Manual usage

```bash
# Full workflow with a release tag
./scripts/test/rustfs_pool_expand.sh --all --with-warp -y \
  --version 1.0.0-rc.3 --endpoint http://10.0.0.7:9000

# Use a direct .deb URL (e.g. nightly package on R2)
./scripts/test/rustfs_pool_expand.sh --all --with-warp -y \
  --package-url https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb \
  --endpoint http://10.0.0.7:9000

# Preflight / reset / single step
./scripts/test/rustfs_pool_expand.sh --preflight --version 1.0.0-rc.3
./scripts/test/rustfs_pool_expand.sh --reset -y
./scripts/test/rustfs_pool_expand.sh --step 9 --finalize-decommission -y
```

## Known issues and caveats

- RustFS `1.0.0-rc.3` fails decommission with a large `warp`-written bucket
  ("metacache listing quorum failed / timeout"). If decommission repeatedly
  fails, reduce the written data (lower `storage_threshold`) or remove the test
  bucket, then re-run step 9. The script detects the failure, clears metadata
  and retries `DECOMMISSION_RETRIES` times before giving up.
- Multi-pool nodes must start **simultaneously** (the script does this) or the
  first node dies with `not first disk`.
- The admin API is SigV4-signed (`host`, `x-amz-content-sha256:
  UNSIGNED-PAYLOAD`, `x-amz-date`), matching the signer the RustFS server
  itself trusts. If the cluster requires a non-default region, set
  `SIGV4_REGION` in the script.
