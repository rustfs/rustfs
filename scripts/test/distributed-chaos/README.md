# Distributed Load And Chaos Test Kit

This directory contains a practical starting point for distributed RustFS stress, soak, and chaos testing.

It is intentionally split into two layers:

- Docker Compose for fast local iteration and reproducible bug hunting.
- Kubernetes plus Chaos Mesh for closer-to-production failure injection and nightly endurance runs.

## Recommended baseline topology

Use these topologies first because they match RustFS's current Helm and runtime conventions:

- Local Compose: 4 nodes x 4 disks each, 16 total disks, peer URLs via `RUSTFS_VOLUMES=http://rustfs-node{1...4}:9000/data/rustfs{0...3}`
- Kubernetes: 4 pods x 4 PVCs each, `replicaCount=4`, distributed mode enabled
- Future scale-up: 16 pods x 1 disk each after the 4x4 topology is stable

If your production target differs, keep the same scenario mix but scale `--objects`, `--concurrent`, and duration.

## Failure-prone scenarios to prioritize

These are the first scenarios most likely to expose hidden distributed races, lock contention, metadata corruption, or quorum edge cases:

1. Small-file metadata storm
   Run mixed PUT/GET/DELETE/STAT against 1 KiB to 1 MiB objects with very high concurrency.
   Goal: expose lock hot spots, namespace contention, bucket metadata bottlenecks, and leaked in-flight state.

2. Read-after-write and delete-after-read churn
   Run a write-heavy mixed workload while repeatedly deleting and re-reading the same hot prefixes.
   Goal: expose stale caches, conditional write races, tombstone visibility bugs, and quorum timing issues.

3. Sustained throughput soak
   Run 30 to 120 minute PUT and GET workloads with 64 MiB to 1 GiB objects.
   Goal: expose memory growth, background task starvation, long-tail latency creep, and degraded healing behavior.

4. One-node loss during mixed traffic
   Kill or isolate one node during an active mixed workload.
   Goal: validate quorum handling, request retry behavior, client-visible error rate, and recovery time.

5. Network degradation plus disk pressure
   Inject internode delay or packet loss while adding CPU or IO pressure to one node.
   Goal: expose lock acquisition amplification, internode timeout storms, and disk queue collapse under degraded coordination.

## Quick start: Docker Compose

### 1. Start a real 4-node distributed cluster

```bash
./scripts/test/distributed-chaos/run-compose.sh up
```

This stack uses the same peer URL topology RustFS expects in distributed mode. It is not the "4 independent single-node containers behind a load balancer" pattern.

If you want to verify the harness against a prebuilt image instead of building local source first:

```bash
BUILD_LOCAL=0 RUSTFS_IMAGE=rustfs/rustfs:latest ./scripts/test/distributed-chaos/run-compose.sh up
```

### 2. Run load

Small-file metadata storm:

```bash
./scripts/test/distributed-chaos/run-warp.sh small
```

For a short local smoke run, override the default duration, concurrency, and object count:

```bash
WARP_SMALL_DURATION=30s WARP_SMALL_CONCURRENT=32 WARP_SMALL_OBJECTS=2000 ./scripts/test/distributed-chaos/run-warp.sh small
```

Large-object throughput:

```bash
./scripts/test/distributed-chaos/run-warp.sh large
./scripts/test/distributed-chaos/run-warp.sh get-large
```

Balanced mixed workload:

```bash
./scripts/test/distributed-chaos/run-warp.sh mixed
```

Batch delete pressure:

```bash
./scripts/test/distributed-chaos/run-warp.sh delete-batch
```

### 2b. Reproduce mixed-workload corruption or heal regressions

Use the focused reproducer when you want a bounded run that scans Warp output and node logs for corruption, quorum, or heal failures:

```bash
WARP_MODE=docker WARP_HOST=rustfs-lb:9000 ./scripts/test/distributed-chaos/repro-mixed-corruption.sh
```

The script writes Warp output, per-node logs, and an object layout report under `artifacts/distributed-chaos/repros/`.

Turn the latest repro artifacts into a CI-friendly summary and exit code:

```bash
./scripts/test/distributed-chaos/evaluate-chaos-run.sh latest
```

Render the same run into a PR-friendly Markdown report and a browser-friendly HTML artifact:

```bash
OUT_FILE=./artifacts/distributed-chaos/repros/latest-report.md \
./scripts/test/distributed-chaos/render-chaos-report.sh markdown latest

OUT_FILE=./artifacts/distributed-chaos/repros/latest-report.html \
./scripts/test/distributed-chaos/render-chaos-report.sh html latest
```

### 2c. Inspect object placement after a suspicious run

When health probes stay green but you still suspect silent distributed write loss, inspect the object layout across all 16 local endpoints:

```bash
./scripts/test/distributed-chaos/inspect-object-layout.sh warp-benchmark-bucket 'COKUoQJk/30.KXBHw0OFFtst9YPu.rnd'
```

This is useful for mixed small-file failures where the object is still readable through quorum but one node never persisted its `xl.meta`.

### 3. Stream admin metrics and collect profiles

```bash
./scripts/test/distributed-chaos/collect-admin-metrics.sh stream
./scripts/test/distributed-chaos/collect-admin-metrics.sh pprof-status
./scripts/test/distributed-chaos/collect-admin-metrics.sh pprof-cpu 30
./scripts/test/distributed-chaos/collect-admin-metrics.sh pprof-flamegraph 30
```

If host-side SigV4 signing against `127.0.0.1:9000` is unreliable in your local Docker setup, collect metrics from inside the Docker network instead:

```bash
SIGNING_MODE=docker_awscurl BASE_URL=http://rustfs-lb:9000 ./scripts/test/distributed-chaos/collect-admin-metrics.sh once
```

### 4. Inject faults

Add 250 ms delay to one node for 5 minutes:

```bash
./scripts/test/distributed-chaos/pumba-chaos.sh delay 're2:^rustfs-node1$' 250 5m
```

Drop 10 percent of packets to two nodes for 3 minutes:

```bash
./scripts/test/distributed-chaos/pumba-chaos.sh loss 're2:^rustfs-node[12]$' 0.10 3m
```

Kill random RustFS nodes three times with a 45 second gap:

```bash
./scripts/test/distributed-chaos/pumba-chaos.sh kill 're2:^rustfs-node[1-4]$' 45s 3
```

Apply CPU pressure to one node for 2 minutes:

```bash
./scripts/test/distributed-chaos/pumba-chaos.sh stress 're2:^rustfs-node2$' 120s '--cpu 4 --timeout 120s --metrics-brief'
```

### 5. Tear down

```bash
./scripts/test/distributed-chaos/run-compose.sh down
./scripts/test/distributed-chaos/run-compose.sh reset
```

### 6. Run special scenarios

These scenarios cover the control-plane and topology transitions that are easy to miss in ordinary mixed traffic testing:

High-load write destruction on the 4-node distributed cluster:

```bash
./scripts/test/distributed-chaos/run-special-scenarios.sh write-chaos
```

Decommission while writes are still in flight on the disposable two-pool environment:

```bash
./scripts/test/distributed-chaos/run-special-scenarios.sh decommission-under-load
```

Rebalance while writes are still in flight on the disposable two-pool environment:

```bash
./scripts/test/distributed-chaos/run-special-scenarios.sh rebalance-under-load
```

Violent multi-pool mixed workload with pause and optional restart:

```bash
./scripts/test/distributed-chaos/run-special-scenarios.sh multi-pool-chaos
```

All four scenarios write a `summary.md` plus raw logs under `artifacts/distributed-chaos/special/`.
The multi-pool scenarios reuse the local two-pool harness from `scripts/test/decommission_docker.sh`, and default to a containerized `mc` fallback so they do not require a host-installed `mc`.

## Quick start: Kubernetes plus Chaos Mesh

### 1. Create a local multi-node kind cluster

```bash
kind create cluster --name rustfs-chaos --config ./scripts/test/distributed-chaos/kind-config.yaml
```

The config maps RustFS NodePorts `32000` and `32001` to localhost `9000` and `9001`.

### 2. Install RustFS in distributed mode

Pick the cluster's default storage class and pass it to Helm:

```bash
DEFAULT_SC="$(kubectl get storageclass -o jsonpath='{.items[0].metadata.name}')"

helm install rustfs ./helm/rustfs \
  -n rustfs-chaos \
  --create-namespace \
  --set mode.distributed.enabled=true \
  --set mode.standalone.enabled=false \
  --set replicaCount=4 \
  --set service.type=NodePort \
  --set service.endpoint.nodePort=32000 \
  --set service.console.nodePort=32001 \
  --set ingress.enabled=false \
  --set gatewayApi.enabled=false \
  --set storageclass.name="${DEFAULT_SC}" \
  --set secret.rustfs.access_key=rustfsadmin \
  --set secret.rustfs.secret_key=rustfsadmin
```

Wait for the cluster:

```bash
kubectl -n rustfs-chaos rollout status statefulset/rustfs --timeout=10m
kubectl -n rustfs-chaos get pods -o wide
curl -sf http://127.0.0.1:9000/health
curl -sf http://127.0.0.1:9000/health/ready
```

### 3. Install Chaos Mesh

```bash
./scripts/test/distributed-chaos/install-chaos-mesh.sh
```

### 4. Apply chaos manifests

Internode latency:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/network-delay.yaml
```

Packet loss:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/network-loss.yaml
```

Full internode partition:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/network-partition.yaml
```

One pod kill:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/pod-kill.yaml
```

Disk latency:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/io-latency.yaml
```

CPU pressure:

```bash
kubectl apply -f ./scripts/test/distributed-chaos/chaos-mesh/stress-cpu.yaml
```

Remove all experiments:

```bash
kubectl delete -f ./scripts/test/distributed-chaos/chaos-mesh
```

## Suggested execution plan

### PR smoke gate, 10 to 15 minutes

- Compose cluster up
- `run-warp.sh small`
- `pumba-chaos.sh kill ... 1`
- `collect-admin-metrics.sh once`
- Pass if cluster recovers and no object-read verification fails

### Nightly distributed gate, 45 to 90 minutes

- Compose or kind cluster
- `run-warp.sh mixed`
- `run-warp.sh large`
- `repro-mixed-corruption.sh`
- `evaluate-chaos-run.sh latest`
- `render-chaos-report.sh markdown latest`
- `render-chaos-report.sh html latest`
- network delay plus packet loss
- one pod kill
- CPU or IO stress on one node
- collect admin metrics and one CPU profile

### Weekly soak, 2 to 6 hours

- kind or k3s
- long mixed workload
- scheduled chaos every 10 to 20 minutes
- explicit post-run consistency sampling

### Weekly or manual control-plane gate

- `run-special-scenarios.sh decommission-under-load`
- `run-special-scenarios.sh rebalance-under-load`
- `run-special-scenarios.sh multi-pool-chaos`
- review `artifacts/distributed-chaos/special/*/summary.md`

## What to measure

### Client-visible outcomes

- Warp throughput in MiB/s and objects/s
- Warp latency p50, p95, p99, and max
- HTTP 4xx and 5xx error rate
- failed benchmark phases or early benchmark aborts

### Cluster health and quorum

- erasure-set overall health
- erasure-set read health and write health
- erasure-set online drives count
- drive offline count and healing drives count

### Lock and contention indicators

- `locks_read_total`
- `locks_write_total`
- request in-flight and waiting totals
- CPU time slope during small-file storms

### Internode coordination

- `internode_errors_total`
- `internode_dial_errors_total`
- `internode_dial_avg_time_nanos`
- sent and received bytes between peers

### Disk saturation

- `drive_waiting_io`
- `drive_reads_await`
- `drive_writes_await`
- `drive_reads_kb_per_sec`
- `drive_writes_kb_per_sec`
- `drive_perc_util`
- `drive_io_errors_total`

### Process stability

- resident memory bytes
- virtual memory bytes
- open file descriptors
- uptime continuity
- background profile captures when p99 spikes
- per-object placement anomalies captured by `inspect-object-layout.sh`

## Acceptance criteria

Use baseline-relative thresholds until you have production SLOs. Start with these gates:

1. Data correctness
   Zero checksum mismatches and zero unexpected missing objects in a 1,000-object post-test sample.

2. Availability during one-node failure
   GET success rate stays at or above 99.9 percent after the initial 30 second disruption window.

3. Write availability during one-node failure
   PUT success rate stays at or above 99.0 percent after the initial 30 second disruption window.

4. Recovery time
   `/health/ready` returns healthy again within 60 seconds after one-node kill or injected pause.

5. Tail latency
   p99 latency must not exceed 2x the no-chaos baseline for the same workload, except in explicitly destructive partition tests.

6. Lock recovery
   `locks_read_total` and `locks_write_total` return near baseline within 2 minutes after the workload stops.

7. Memory stability
   resident memory returns to within 15 percent of the pre-test value within 10 minutes after load ends.

8. Disk backlog
   `drive_waiting_io` and `drive_writes_await` must recover toward baseline within 5 minutes after IO chaos ends.

9. No silent cluster degradation
   erasure-set overall health, read health, and write health remain healthy after recovery.

10. No silent placement gaps
    `evaluate-chaos-run.sh latest` reports `overall_verdict=PASS`, including zero missing placements for any suspicious object it inspects.

11. Control-plane operations survive background load
    Decommission and rebalance must finish without `failed=true`, and the post-run verification objects must remain readable.

12. High-load write chaos recovers
    After the write-chaos scenario, `/health/ready` must recover and the background write workload must not abort unexpectedly.

## CI/CD recommendation

Use a tiered pipeline instead of running full chaos on every PR:

- PR: fast distributed smoke on Compose
- merge to `main`: one mixed workload plus one node kill
- nightly: kind plus Chaos Mesh delay, loss, kill, and stress
- weekly: long soak plus profile capture and artifact retention
- keep evaluation separate from artifact upload so failed runs still preserve logs and summaries
- publish both Markdown and HTML reports so engineers can review the same run in PRs and in artifact browsers

The example workflow file in this directory is a safe starting point:

```text
scripts/test/distributed-chaos/github-actions.example.yml
```

## Sources

This test kit is aligned with:

- RustFS distributed startup and lock client initialization in [main.rs](/Users/overtrue/www/rustfs/rustfs/src/main.rs)
- RustFS cluster E2E helper in [common.rs](/Users/overtrue/www/rustfs/crates/e2e_test/src/common.rs)
- RustFS admin metrics streaming in [metrics.rs](/Users/overtrue/www/rustfs/rustfs/src/admin/handlers/metrics.rs)
- RustFS profiling endpoints in [profile_admin.rs](/Users/overtrue/www/rustfs/rustfs/src/admin/handlers/profile_admin.rs)
- Helm distributed topology in [values.yaml](/Users/overtrue/www/rustfs/helm/rustfs/values.yaml)
- kind quick start [kind.sigs.k8s.io](https://kind.sigs.k8s.io/)
- MinIO Warp CLI and workload references [docs.min.io](https://docs.min.io/enterprise/minio-warp/)
- Pumba quick start and command examples [github.com/alexei-led/pumba](https://github.com/alexei-led/pumba)
- Chaos Mesh quick start and experiment docs [chaos-mesh.org](https://chaos-mesh.org/docs/quick-start/)
