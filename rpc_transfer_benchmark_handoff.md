# RPC Transfer Benchmark Handoff

## Goal

Continue the internode RPC upload benchmark bisect on another machine and identify which optimization batch introduces the upload regression.

The work is based on branch `rpc-transfer-optimization`.

Current HEAD:

```bash
git rev-parse --short HEAD
# ba8d3521
```

This branch already contains:

- batch 1: remove internode HTTP preflight requests
- batch 2: switch hot metadata RPC payloads to binary with fallback
- batch 3: prefer disk id over endpoint string in hot RPC paths
- batch 4: split `/rustfs/rpc/*` out of admin router
- batch 5: internode transfer metrics
- merge from `origin/main`

## Current conclusion

The upload regression is **not** in batch 1-3.

Stage-1 benchmark already finished:

- `base = 73364e7f`
- `v13 = a1e4e2fe`

Measured upload times:

| variant | size | run1 | run2 | run3 | avg |
| --- | --- | --- | --- | --- | --- |
| `base` | `256MB` | `5.339s` | `3.576s` | `4.434s` | `4.450s` |
| `base` | `512MB` | `6.337s` | `7.934s` | `7.147s` | `7.139s` |
| `v13` | `256MB` | `3.923s` | `2.393s` | `2.284s` | `2.867s` |
| `v13` | `512MB` | `4.705s` | `4.406s` | `6.005s` | `5.039s` |

Interpretation:

- `256MB`: `v13` is faster than `base` by about `35.6%`
- `512MB`: `v13` is faster than `base` by about `29.4%`

So the next suspect is:

1. batch 4
2. batch 5
3. interaction between batch 5 and merged `origin/main`

## Worktree / commit map

Existing local mapping on the original machine was:

| label | commit | meaning |
| --- | --- | --- |
| `base` | `73364e7f` | baseline before RPC optimization branch |
| `v13` | `a1e4e2fe` | batch 1-3 |
| `v134` | `59e3d09d` | batch 1-4 |
| `v1345` | `e6a9f579` | batch 1-5 |
| `v1345m` | `016fffe2` | batch 1-5 + metrics |
| `head` | `ba8d3521` | current merged branch head |

If the target machine does not have the same worktrees, recreate them with:

```bash
git worktree add ../rpcbench-base 73364e7f
git worktree add ../rpcbench-13 a1e4e2fe
git worktree add ../rpcbench-134 59e3d09d
git worktree add ../rpcbench-1345 e6a9f579
git worktree add ../rpcbench-1345m 016fffe2
git worktree add ../rpcbench-head ba8d3521
```

## Required tools

- Docker Desktop or Docker Engine
- `docker compose`
- `mc`
- Rust toolchain compatible with the repo
- `perl`

Quick checks:

```bash
docker ps
docker compose version
mc --version
cargo --version
perl -v
```

## Important environment notes

- Free disk space matters a lot. Keep at least `100GiB` free before running the full bisect.
- `target/` directories from multiple worktrees can easily exhaust disk space.
- If compilation starts failing with `No space left on device`, delete old `target/` directories first.

Useful check:

```bash
df -h .
find .. -maxdepth 3 -type d -name target -print0 | xargs -0 du -sh 2>/dev/null | sort -hr | head
```

## Benchmark strategy

The intended bisect order is:

1. compare `base` vs `v13`
2. compare `v13` vs `v134`
3. compare `v134` vs `v1345`
4. compare `v1345` vs `v1345m`
5. compare `v1345m` vs `head`

The key sizes are:

- `256MB`
- `512MB`

Each variant should run:

- 3 upload iterations per size

If time permits, add download verification later, but upload is the priority because that is where the regression was observed.

## Runtime image

Use a simple runtime image that only needs `/usr/bin/rustfs` mounted in:

```Dockerfile
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/bin/rustfs"]
```

Build example:

```bash
docker build -t rustfs-bench:base -<<'EOF'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/bin/rustfs"]
EOF
```

## Build the release binary

For each variant worktree:

```bash
cd /path/to/rpcbench-134
cargo build --release --bin rustfs
```

Expected binary:

```bash
/path/to/rpcbench-134/target/release/rustfs
```

## Benchmark script

Create a local script like this on the target machine:

```bash
#!/bin/zsh
set -euo pipefail

if [[ $# -lt 5 ]]; then
  echo "usage: $0 <variant> <image> <base_port> <output_tsv> <binary_path> [sizes]" >&2
  exit 1
fi

variant="$1"
image="$2"
base_port="$3"
out="$4"
binary="$5"
shift 5
sizes=(${@:-256 512})

root="/tmp/rustfs-rpc-bisect-${variant}"
compose="${root}/docker-compose.yml"
alias_name="bench-${variant}"
bucket="rpc-bisect-${variant}"
access_key="rustfsadmin"
secret_key="rustfsadmin"

cleanup() {
  docker compose -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

rm -rf "$root"
mkdir -p "$root"/{node1,node2,node3,node4}
for node in node1 node2 node3 node4; do
  mkdir -p "$root/$node"/disk{1,2,3,4}
done

cat > "$compose" <<YAML
services:
  node1:
    image: ${image}
    container_name: bench-${variant}-node1
    hostname: node1
    environment:
      - RUSTFS_VOLUMES=http://node{1...4}:9000/data/disk{1...4}
      - RUSTFS_ADDRESS=node1:9000
      - RUSTFS_CONSOLE_ENABLE=false
      - RUSTFS_ACCESS_KEY=${access_key}
      - RUSTFS_SECRET_KEY=${secret_key}
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - ${binary}:/usr/bin/rustfs:ro
      - ${root}/node1/disk1:/data/disk1
      - ${root}/node1/disk2:/data/disk2
      - ${root}/node1/disk3:/data/disk3
      - ${root}/node1/disk4:/data/disk4
    ports:
      - "${base_port}:9000"
    networks: [rustfs-net]
  node2:
    image: ${image}
    container_name: bench-${variant}-node2
    hostname: node2
    environment:
      - RUSTFS_VOLUMES=http://node{1...4}:9000/data/disk{1...4}
      - RUSTFS_ADDRESS=node2:9000
      - RUSTFS_CONSOLE_ENABLE=false
      - RUSTFS_ACCESS_KEY=${access_key}
      - RUSTFS_SECRET_KEY=${secret_key}
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - ${binary}:/usr/bin/rustfs:ro
      - ${root}/node2/disk1:/data/disk1
      - ${root}/node2/disk2:/data/disk2
      - ${root}/node2/disk3:/data/disk3
      - ${root}/node2/disk4:/data/disk4
    networks: [rustfs-net]
  node3:
    image: ${image}
    container_name: bench-${variant}-node3
    hostname: node3
    environment:
      - RUSTFS_VOLUMES=http://node{1...4}:9000/data/disk{1...4}
      - RUSTFS_ADDRESS=node3:9000
      - RUSTFS_CONSOLE_ENABLE=false
      - RUSTFS_ACCESS_KEY=${access_key}
      - RUSTFS_SECRET_KEY=${secret_key}
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - ${binary}:/usr/bin/rustfs:ro
      - ${root}/node3/disk1:/data/disk1
      - ${root}/node3/disk2:/data/disk2
      - ${root}/node3/disk3:/data/disk3
      - ${root}/node3/disk4:/data/disk4
    networks: [rustfs-net]
  node4:
    image: ${image}
    container_name: bench-${variant}-node4
    hostname: node4
    environment:
      - RUSTFS_VOLUMES=http://node{1...4}:9000/data/disk{1...4}
      - RUSTFS_ADDRESS=node4:9000
      - RUSTFS_CONSOLE_ENABLE=false
      - RUSTFS_ACCESS_KEY=${access_key}
      - RUSTFS_SECRET_KEY=${secret_key}
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - ${binary}:/usr/bin/rustfs:ro
      - ${root}/node4/disk1:/data/disk1
      - ${root}/node4/disk2:/data/disk2
      - ${root}/node4/disk3:/data/disk3
      - ${root}/node4/disk4:/data/disk4
    networks: [rustfs-net]
networks:
  rustfs-net:
YAML

docker compose -f "$compose" up -d >/dev/null

for _ in {1..60}; do
  if mc alias set "$alias_name" "http://127.0.0.1:${base_port}" "$access_key" "$secret_key" >/dev/null 2>&1; then
    if mc admin info "$alias_name" >/dev/null 2>&1; then
      break
    fi
  fi
  sleep 2
done

mc admin info "$alias_name" >/dev/null
mc mb --ignore-existing "$alias_name/$bucket" >/dev/null

for size in $sizes; do
  src="/tmp/rustfs-rpc-bench-data/object-${size}m.bin"
  for iter in 1 2 3; do
    obj="object-${size}m-${iter}.bin"
    mc rm --force "$alias_name/$bucket/$obj" >/dev/null 2>&1 || true
    start=$(perl -MTime::HiRes=time -e 'printf "%.6f", time')
    mc cp "$src" "$alias_name/$bucket/$obj" >/dev/null
    end=$(perl -MTime::HiRes=time -e 'printf "%.6f", time')
    elapsed=$(perl -e 'printf "%.3f", $ARGV[1]-$ARGV[0]' "$start" "$end")
    printf "%s\t%s\t%s\t%s\n" "$variant" "$size" "$iter" "$elapsed" >> "$out"
  done
done
```

Save it as:

```bash
/tmp/rustfs_bisect_bench_mount.sh
chmod +x /tmp/rustfs_bisect_bench_mount.sh
```

## Prepare test data

```bash
mkdir -p /tmp/rustfs-rpc-bench-data
dd if=/dev/urandom of=/tmp/rustfs-rpc-bench-data/object-256m.bin bs=1m count=256
dd if=/dev/urandom of=/tmp/rustfs-rpc-bench-data/object-512m.bin bs=1m count=512
shasum -a 256 /tmp/rustfs-rpc-bench-data/object-256m.bin /tmp/rustfs-rpc-bench-data/object-512m.bin
```

Then update the `src=` line in the script if needed.

## Run order

Create an output file:

```bash
out=/tmp/rpc-transfer-bisect.tsv
printf "variant\tsize_mb\titeration\tupload_sec\n" > "$out"
```

Run:

```bash
/tmp/rustfs_bisect_bench_mount.sh base  rustfs-bench:base 19100 "$out" /path/to/rpcbench-base/target/release/rustfs 256 512
/tmp/rustfs_bisect_bench_mount.sh v13   rustfs-bench:base 19110 "$out" /path/to/rpcbench-13/target/release/rustfs 256 512
/tmp/rustfs_bisect_bench_mount.sh v134  rustfs-bench:base 19120 "$out" /path/to/rpcbench-134/target/release/rustfs 256 512
/tmp/rustfs_bisect_bench_mount.sh v1345 rustfs-bench:base 19130 "$out" /path/to/rpcbench-1345/target/release/rustfs 256 512
/tmp/rustfs_bisect_bench_mount.sh v1345m rustfs-bench:base 19140 "$out" /path/to/rpcbench-1345m/target/release/rustfs 256 512
/tmp/rustfs_bisect_bench_mount.sh head  rustfs-bench:base 19150 "$out" /path/to/rpcbench-head/target/release/rustfs 256 512
```

## How to interpret results

Compute average per variant and size:

```bash
awk 'NR>1 {key=$1 FS $2; sum[key]+=$4; cnt[key]++} END {for (k in sum) printf "%s\t%.3f\n", k, sum[k]/cnt[k]}' "$out" | sort
```

Decision rule:

- if `v134` regresses relative to `v13`, the problem is in batch 4
- if `v134` is fine but `v1345` regresses, the problem is in batch 5 before metrics
- if `v1345` is fine but `v1345m` regresses, the problem is in the metrics patch
- if `v1345m` is fine but `head` regresses, the issue comes from merged `origin/main` interaction

## Recommended follow-up after locating the regression

If batch 4 is the suspect:

- focus on `rustfs/src/storage/rpc/http_service.rs`
- focus on `rustfs/src/server/http.rs`
- focus on `rustfs/src/admin/router.rs`

If batch 5 is the suspect:

- focus on `crates/common/src/internode_metrics.rs`
- focus on `crates/rio/src/http_reader.rs`
- focus on `rustfs/src/storage/rpc/http_service.rs`
- focus on `crates/ecstore/src/metrics_realtime.rs`
- focus on `crates/protos/src/lib.rs`

## Update: regression confirmed and fixed

On March 20, 2026, the bisect was completed with Linux `aarch64` binaries built in Docker and benchmarked with the script above.

Average upload times:

```text
v13        256MB  1.657s
v13        512MB  3.220s
v134       256MB  1.686s
v134       512MB  3.089s
v1345      256MB  2.062s
v1345      512MB  3.927s
currentfix 256MB  1.531s
currentfix 512MB  2.893s
```

Conclusion:

- `batch 1-3` improved upload performance relative to the original baseline
- `batch 4` did not introduce a regression
- `batch 5` was the first regressing batch
- the metrics patch and merge from `origin/main` were not required to explain the first regression

The regression in `batch 5` came from changing `handle_put_file()` in
`rustfs/src/storage/rpc/http_service.rs` from direct chunk writes to
`StreamReader + tokio::io::copy`. With Tokio `1.50.0`, `io::copy` uses an
internal `8 KiB` default buffer, which re-buffered the incoming HTTP body and
caused slower uploads.

The fix on the current branch restores direct per-chunk `write_all()` calls
while preserving byte accounting, internode metrics, and `file.flush().await`.

Targeted verification after the fix:

```bash
cargo test -p rustfs --bin rustfs http_service::tests -- --nocapture
```

The benchmark for the fixed current branch used:

```bash
/tmp/rustfs_bisect_bench_mount.sh currentfix rustfs-bench:base 19150 "$out" \
  /Users/weisd/project/github/rustfs_rpc/target-linux/release/rustfs 256 512
```

## Update: download failure root cause and validation

After the upload regression was fixed, download benchmarking exposed a separate
correctness issue: object GET requests failed with `unexpected EOF` or
`Connection closed by foreign host`, even for an `8MB` object.

The failure was traced to remote shard writes not being fully finalized. The
write path in `crates/ecstore/src/erasure_coding/encode.rs` returned after
`Erasure::encode()` finished producing shard data, but it did not call
`shutdown()` on the per-disk writers. For local files this was mostly benign,
but for remote disks the writer is `rustfs_rio::HttpWriter`, which only
finishes the HTTP `PUT` body and waits for the response during shutdown.

Observed symptom before the fix:

- object uploads appeared to succeed
- only one node's `part.1` files had the expected `699312` bytes
- the other remote `part.1` files existed but were `0` bytes
- later `read_file_stream` calls returned `500 Internal Server Error`
- outer object GETs advertised the full content length but streamed `0` bytes

The fix was committed as:

```text
f268e4e4 fix: shutdown erasure writers after encode
```

It restores explicit writer shutdown in `Erasure::encode()` and adds a
regression test that verifies writers which only commit data during shutdown
are finalized correctly.

Targeted verification after this fix:

```bash
cargo test -p rustfs-ecstore encode_shutdowns_writers_after_small_shards -- --nocapture
cargo test -p rustfs-ecstore test_bitrot_ -- --nocapture
```

Real-world validation after rebuilding the Linux `aarch64` binary locally with
`cargo zigbuild`:

```text
8MB download: success
speed: ~68.15 MiB/s
```

All sixteen `part.1` files for that validation object were `699312` bytes after
the fix, instead of only the first node containing data.

Post-fix benchmark averages:

```text
encodefixhead upload   256MB  1.608s
encodefixhead upload   512MB  2.940s
encodefixhead download 256MB  1.376s
encodefixhead download 512MB  2.654s
```

Compared to the previously measured upload-only `vectoredfix` build:

- `256MB` upload is slower (`1.461s -> 1.608s`)
- `512MB` upload is slightly slower (`2.805s -> 2.940s`)
- uploads remain much faster than the regressed `v1345`

This is expected and should be treated as the more accurate result, because the
new build waits for remote shard writers to finish correctly instead of
returning before all remote `PUT` bodies are fully finalized.

## Follow-up optimization: batch internode PUT body writes

After the correctness fixes above, the next bottleneck moved into the internode
upload server path in `rustfs/src/storage/rpc/http_service.rs`.

Profiling showed:

- the client-side `HttpWriter` sent each large `1.398MB` shard in only `2`
  HTTP body chunks
- by the time the request reached `handle_put_file()`, hyper had re-chunked the
  body into roughly `23-25` smaller chunks
- `flush()` was negligible; most time was spent in repeated
  `writer.write_all(&bytes).await` calls on the server

To address that, `write_body_chunks_to_writer()` was changed to aggregate
incoming HTTP body chunks into a `BytesMut` buffer sized to
`DEFAULT_READ_BUFFER_SIZE`, and only then write to the underlying file.

Commit:

```bash
git rev-parse --short 0d0e8fdf
# 0d0e8fdf
```

Verification:

```bash
cargo test -p rustfs-rio http_ -- --nocapture
cargo test -p rustfs --bin rustfs http_service::tests -- --nocapture
```

Both passed.

Benchmarks:

```text
profhttp3 upload 256MB   avg 1.850s
aggverify upload 256MB   avg 0.430s
agg512b   upload 512MB   avg 0.778s
encodefixhead upload 512MB avg 2.940s
```

Correctness validation for the batched-write build:

- uploaded objects are present with the expected `256MiB` size
- downloaded object MD5 matches the source file exactly

Interpretation:

- the aggregation change materially reduces server-side internode upload write
  overhead
- the gain persists beyond `256MB`; `512MB` uploads improved from `2.940s` to
  `0.778s` in the same benchmark harness

## Current repo status

At handoff time:

```bash
git branch --show-current
# rpc-transfer-optimization

git rev-parse --short HEAD
# f268e4e4
```

The merge from `origin/main` is already complete and validated with:

```bash
cargo test -p rustfs-rio http_ -- --nocapture
cargo test -p rustfs --bin rustfs admin::route_registration_test -- --nocapture
make -j1 pre-commit
```

## Notes

- Do not rely on the original machine's `/tmp/rustfs-bisect-*` layout.
- Recreate the benchmark script and test data locally.
- Keep ports distinct for each variant.
- Run variants serially, not in parallel.
- If Docker becomes unstable, restart Docker Desktop before continuing.
