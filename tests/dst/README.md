# Deterministic simulation (RustFS 1.0.7)

This harness is a single-threaded, seeded simulator for two official cluster shapes. It calls RustFS 1.0.7 code for layout, Reed-Solomon encode/decode, HighwayHash256S framing, read/write quorum reduction, object-to-set SipHash, shard distribution, and namespace-lock quorum sizes. It does not boot the HTTP server, tonic, or the on-disk `LocalDisk` path.

The official campaign host is an Apple Mac mini (M5 Pro, 24 GB unified memory, hostname `bogon`, Tailscale name `kevins-mac-mini`). A cloud VM smoke is not that campaign.

## Why this shape

`turmoil`, `madsim`, and `shuttle` are not in this tree. RustFS 1.0.7 keeps internode I/O on `tokio::net` and `tonic`, and drive I/O on the real filesystem (`openat2`, dedicated fsync lanes, `spawn_blocking`). Putting the whole server under madsim or turmoil would mean replacing the runtime and the disk stack. This harness instead drives the durability primitives those paths already call, under a virtual clock and an explicit fault schedule.

## Topologies

Both use one pool and the default set-width selector (`RUSTFS_ERASURE_SET_DRIVE_COUNT` is forced to `0` inside the harness, even if the shell has it set). Confirmed against `DisksLayout` on 1.0.7:

| Name | Volume argument | Nodes | Disks / node | Sets | Drives / set | Default parity | Read quorum | Write quorum | Exclusive lock | Shared lock |
|---|---|---|---|---|---|---|---|---|---|---|
| `4x4` | `http://node{1...4}:9000/data{1...4}` | 4 | 4 | 1 | 16 | 4 | 12 | 12 | 3 of 4 nodes | 2 of 4 nodes |
| `3x2` | `http://node{1...3}:9000/data{1...2}` | 3 | 2 | 1 | 6 | 3 | 3 | 4 | 2 of 3 nodes | 2 of 3 nodes |

Parity is `default_parity_count`. Write quorum is the data-block count, plus one when data and parity are equal (the 6-drive set). Exclusive lock quorum is `(clients / 2) + 1` with one lock client per node. Shared lock quorum is `clients - clients / 2`.

The campaign runs `4x4` to completion, then `3x2`, in one process. Do not start a second campaign next to it.

## Faults

Every choice comes from xoshiro256** expanded from the seed with splitmix64.

| Fault | Official rate | Effect |
|---|---|---|
| Message drop | 3% | The completion never happens. The client counts `DiskNotFound` at the deadline. |
| Message duplicate | 2% | A second completion. Same generation and bytes are idempotent. An older generation cannot roll forward. |
| Reorder | independent delays | Completions carry their own delay. |
| Delay | uniform `0..=120_000` virtual µs | The client deadline is `100_000` virtual µs, so some completions miss it. |
| Drive I/O error | 2% | `FaultyDisk`, ignored by the production quorum reducer, and the bytes are not applied. |
| Clock jump | 8% of operations | Virtual time jumps by at least the operation timeout. Late completions are not counted. |
| Node crash | 10% of operations, plus a campaign action | The node process stops. In-flight I/O for its drives is dropped. Durable bytes stay. |
| Node restart | campaign action | The process returns. Disk contents are whatever survived. |
| Isolate / heal partition | campaign action | Messages to and from that node are dropped until isolation is cleared. |
| Bitrot | campaign action | One stored byte is flipped. `HighwayHash256S` must reject it. |
| Disk wipe | campaign action | One drive's objects are removed. |
| Heal | campaign action | If the object is still readable, missing reachable drives are rewritten from `Erasure::encode_data`. |

A put or delete runs only when the exclusive lock quorum of reachable nodes is met. A get or list runs only when the shared lock quorum is met. Disk success and failure then go through `reduce_write_quorum_errs` / `reduce_read_quorum_errs`.

## Invariants

Checked after every campaign step, against drive media (a crashed process does not erase a drive):

- A client read that returns bytes matches the last committed payload.
- A readable object was committed. A failed quorum does not publish it.
- If at least `read_quorum` replicas still verify, the object reads back as those bytes.
- Below that count the read fails. It does not return different bytes.
- List entries are committed keys.
- The quorum reducer must not accept fewer than `quorum` successes when every other error is in `OBJECT_OP_IGNORED_ERRS`, and must not reject a full ignored-error success.

A minimum-success delete on these two shapes leaves fewer than `read_quorum` replicas (`drives - write_quorum < read_quorum`). A node crash keeps media, so a fully written object stays readable: 16−4 = 12 on `4x4`, and 6−2 = 4 which is still above the 3-disk read quorum on `3x2`. A minimum 12-of-16 or 4-of-6 write does not promise that a later node loss is still readable; the checker uses the surviving verified replicas.

## What this does not model

- The streaming multi-block encode pipeline, `xl.meta` bytes, or multipart completion. Payloads are at most 4 KiB and go through `Erasure::encode_data` / `decode_data` (one block, under the 1 MiB `BLOCK_SIZE_V2`).
- Splitting fsync from the ack. The drive applies the bytes and acknowledges them in one scheduler event. A dropped message is dropped before that point.
- Lock leases, refresh, and fencing. Only the quorum gate is enforced.
- Storage-class parity overrides. New objects use the default parity for the set width.
- Donor-frame swaps. Independent shard commitments are off by default in 1.0.7; random bit flips are the corruption the default hash is required to catch.
- A second client. One coordinator issues the operations.

## Mac mini runbook

Machine: Apple M5 Pro, 24 GB unified memory, macOS, `aarch64-apple-darwin`. OrbStack's Docker VM is about 14 GB if it is left running. UTM guests, if powered on, take more. The simulator itself stays under about 512 MB. The memory risk is `rustc` while it builds `rustfs-ecstore`.

1. Stop extra guests before compiling. If OrbStack stays up at ~14 GB, set `CARGO_BUILD_JOBS=1`. If Docker, OrbStack, and UTM are stopped, `CARGO_BUILD_JOBS=2` fits. Do not use a higher job count, and do not compile the rest of the workspace in parallel.
2. Do not start the real 16-drive or 6-drive process cluster in the same window as this compile. Run the simulator first, or compile first and then start the cluster.
3. Check out the commit this PR builds, which is 1.0.7 (`03c8a1733d57a6ffc3095f63f765d396f5d04a3a`) plus the harness. `git rev-parse HEAD` must be the commit you pass to `--commit`.
4. Unset `RUSTFS_ERASURE_SET_DRIVE_COUNT` for any real process you start beside this. The simulator forces set width `0` (the production default). A different width is a different layout.
5. Real-process endpoint arguments, if you bring a cluster up for a separate run, are the volume strings in the table above. One `rustfs` process per node. Do not run those processes in parallel with `dst-campaign`.

Smoke (after the ecstore build; the tests themselves are short):

```bash
cd /path/to/rustfs
export CARGO_BUILD_JOBS=1
git rev-parse HEAD
cargo test -p rustfs-dst -- --test-threads=1
```

Official campaign. One process, `4x4` then `3x2`, seeds 1 through 24, 400 steps each, 30 minute wall clock cap. Expect the run itself to finish well inside the cap; the cap is there so a hung seed cannot sit forever. Virtual time is not wall time.

```bash
mkdir -p "$HOME/rustfs-dst-results"
COMMIT=$(git rev-parse HEAD)
export CARGO_BUILD_JOBS=1
cargo run -p rustfs-dst --bin dst-campaign -- \
  --seeds 1-24 \
  --steps 400 \
  --budget-secs 1800 \
  --max-payload 4096 \
  --max-objects 8 \
  --commit "$COMMIT" \
  --report "$HOME/rustfs-dst-results/dst-1.0.7.md" \
  --checkpoint "$HOME/rustfs-dst-results/dst-1.0.7.checkpoint"
echo "exit=$?"
```

Exit 0 means every seed passed and the budget was not exhausted. Exit 1 means a failing seed or an early stop. The report lists each seed. Failing seeds include the first failing step, the minimized step, the invariant name, and the traced schedule.

Resume after a stop or a reboot. The checkpoint skips seeds already recorded as `ok` or `fail` for either topology, then continues. Do not delete the checkpoint if you want the failure log kept.

```bash
cargo run -p rustfs-dst --bin dst-campaign -- \
  --seeds 1-24 \
  --steps 400 \
  --budget-secs 1800 \
  --commit "$(git rev-parse HEAD)" \
  --report "$HOME/rustfs-dst-results/dst-1.0.7.md" \
  --checkpoint "$HOME/rustfs-dst-results/dst-1.0.7.checkpoint"
```

Reproduce one seed:

```bash
cargo run -p rustfs-dst --bin dst-campaign -- \
  --seeds 17 \
  --steps 400 \
  --budget-secs 300 \
  --commit "$(git rev-parse HEAD)" \
  --report "$HOME/rustfs-dst-results/seed-17.md"
```

To stop at the minimized step from the report, pass that step count as `--steps`. Add `--no-minimize` only when you already have the minimal step and want a faster replay.

Optional longer pass, still sequential, still one process:

```bash
cargo run -p rustfs-dst --bin dst-campaign -- \
  --seeds 1-48 \
  --steps 800 \
  --budget-secs 3600 \
  --commit "$(git rev-parse HEAD)" \
  --report "$HOME/rustfs-dst-results/dst-1.0.7-extended.md" \
  --checkpoint "$HOME/rustfs-dst-results/dst-1.0.7-extended.checkpoint"
```

Do not pass `--release` while OrbStack is using ~14 GB. A debug campaign is the intended run. If you have stopped every VM and still want a release binary, use `CARGO_BUILD_JOBS=1 cargo run -p rustfs-dst --release --bin dst-campaign -- …`.

## Resource limits

| Limit | Value |
|---|---|
| Host RAM | 24 GB unified |
| Leave for OrbStack if it stays up | ~14 GB |
| `CARGO_BUILD_JOBS` with OrbStack up | 1 |
| `CARGO_BUILD_JOBS` with Docker/OrbStack/UTM stopped | 2 |
| Campaign processes | 1 |
| Topologies | sequential, never two at once |
| Payload cap | 4096 bytes |
| Objects per seed | 8 |
| Official seeds | 1–24 per topology |
| Steps per seed | 400 |
| Wall budget | 1800 seconds for both topologies |
| Expected simulator RSS | under 512 MB |

## Cloud VM smoke

The cloud VM that added this harness is not the official host. This smoke ran on Linux x86_64 against the 1.0.7 tree plus this harness, before the Mac mini campaign. It is not a substitute for the runbook above.

```bash
cargo run -p rustfs-dst --bin dst-campaign -- \
  --seeds 1-4 \
  --steps 50 \
  --budget-secs 120 \
  --max-payload 4096 \
  --max-objects 8 \
  --commit 03c8a1733d57a6ffc3095f63f765d396f5d04a3a
```

Result: exit 0. 8 of 8 passed, in order: `4x4` seeds 1–4, then `3x2` seeds 1–4. No failing seed. Layout resolved to one 16-drive set (read/write quorum 12, exclusive lock 3) and one 6-drive set (read quorum 3, write quorum 4, exclusive lock 2). `cargo test -p rustfs-dst -- --test-threads=1` passed 7 tests. `cargo clippy -p rustfs-dst --all-targets -- -D warnings` passed.
