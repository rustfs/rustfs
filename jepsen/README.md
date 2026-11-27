# Jepsen tests for RustFS 1.0.7

This directory is a [Jepsen](https://github.com/jepsen-io/jepsen) 0.3.16 project. It does not reimplement Nemesis, Elle, or Knossos. Network partitions, process kill, process pause, clock skew, and packet delay or loss come from `jepsen.nemesis.combined`. The register histories are checked with Elle's `rw-register` (strict serializability of single-key transactions) and Knossos (`jepsen.checker/linearizable` on `knossos.model/cas-register`). Jepsen 0.3.16 has no disk-full or slow-disk nemesis. Packet delay is the latency fault that library does provide. Bit-flip file corruption is opt-in and off by default.

The Mac mini (24 GB, OrbStack Docker capped near 14 GB) runs **one topology at a time**. Shut the first cluster down, including its volumes, before starting the second.

## What is checked

RustFS 1.0.7 acknowledges object writes at erasure write quorum and serves the current object from read quorum. Listings are not linearizable reads. The in-tree notes for that are `rustfs/src/app/bucket_list_through.rs` and `crates/ecstore/src/data_usage/mod.rs`. Elle and Knossos therefore see puts, gets, deletes, conditional puts, and multipart completion. A list is a separate checker that only rejects an ETag no acknowledged or indeterminate write can explain. A stale list is not a failure.

`If-Match` and `If-None-Match: *` are supported on `PutObject` in 1.0.7 (`scripts/s3-tests/implemented_tests.txt` covers `test_put_object_ifmatch_*` and `test_put_object_ifnonmatch_*`). Versioning is supported. Multipart upload of a single small part is a blind write, not a conditional write. There is no multi-key transaction, so the Elle workload defaults to one micro-op per transaction. That workload runs Elle's strict-serializability search and, on each single-micro-op key, Knossos. Elle does not infer a per-key version order from real time unless `:linearizable-keys?` is set, and Elle's notes say that flag can report a cycle that is not there, so this suite leaves it off and uses Knossos for that order. `--max-txn-length` greater than 1 asks for a guarantee the server does not claim. A strict-serializability failure there is expected, and Knossos ignores those multi-micro-op transactions.

A lost response is `:info` (indeterminate). A precondition failure is `:fail`. Knossos or Elle returning `:valid? :unknown` means the history was inconclusive. `:valid? false` is a failed check.

## Topologies

Both layouts are one pool and one erasure set. `RUSTFS_VOLUMES` uses RustFS ellipsis expansion: each `{start...end}` is a dimension, and the dimensions multiply. The path is a directory on that node. The same string is passed to every node. `rustfs server` reads it from the environment. This matches `DisksLayout` in `crates/ecstore/src/layout/disks_layout.rs`, including `http://rustfs{0...15}.mydomain.net/data{0...1}`.

Default parity follows `default_parity_count` (8 or more drives → 4, 6 or 7 → 3). Read quorum is `drives - parity`. Write quorum is the data-shard count, plus one when that count equals parity (`default_write_quorum`).

| Topology | Nodes | Drives each | Endpoints | Set | Parity | Read quorum | Write quorum | Volume argument |
|---|---|---|---|---|---|---|---|---|
| `4x4` | 4 | 4 | 16 | 16 | 4 | 12 | 12 | `http://n{1...4}.orb.local:9000/var/lib/rustfs/data{1...4}` |
| `3x2` | 3 | 2 | 6 | 6 | 3 | 3 | 4 | `http://n{1...3}.orb.local:9000/var/lib/rustfs/data{1...2}` |

On `4x4`, losing one node leaves 12 drives, which still meets quorum. A kill of a minority is two nodes and leaves 8 drives, so the cluster cannot read or write until the nemesis heals. A 2-2 partition also leaves neither side at quorum 12. On `3x2`, one node down leaves 4 drives, which still meets write quorum 4. A 2-1 partition lets the two-node side write. The single node has two drives and must not acknowledge a write.

Named volumes in the compose files are directories on the OrbStack disk. They are not sixteen or six independent physical disks. UTM can attach a virtual disk per mount when that distinction matters. File corruption, off by default, targets `/var/lib/rustfs/data1` on one node.

## Mac mini: software

Install Temurin 21, Leiningen, and gnuplot on macOS. Jepsen 0.3.16 is built for Java 21. The perf checker shells out to gnuplot and fails the run if the binary is missing. The Jepsen process runs on the Mac, not inside the 14 GB Docker budget. The project JVM cap is 3 GB.

```bash
brew install --cask temurin@21
brew install leiningen gnuplot
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
java -version
cd jepsen && lein test
```

`lein test` does not start RustFS. It checks quorum numbers, the volume string, Elle on a legal and an illegal history, Knossos on a legal and an illegal history, and the listing and version-id checkers.

OrbStack  Docker 29 is the container path below. UTM is the path that can give each node its own clock.

## Docker nodes (OrbStack)

These containers are Debian SSH machines. Jepsen installs the official 1.0.7 musl zip, verifies its sha256, and starts `rustfs server` with `start-stop-daemon`. The packaged systemd unit is not used: it sets `ProtectClock=true` and `Restart=always`, which would hide clock changes and undo a kill. The image does not grant `SYS_TIME`. A clock change inside a container would move the shared OrbStack clock, so the default fault list is `partition,kill,pause,packet` and does not include `clock`.

From the repository root on the Mac:

```bash
export JEPSEN_SSH_PUBKEY="$(cat ~/.ssh/id_ed25519.pub)"
export JEPSEN_SSH_KEY="$HOME/.ssh/id_ed25519"
./jepsen/scripts/switch-topology.sh 4x4
./jepsen/scripts/preflight.sh --drives 4 \
  n1.orb.local n2.orb.local n3.orb.local n4.orb.local
```

`switch-topology.sh` runs `down -v` on the 3x2 project and both smoke stacks, then builds and starts only the 4x4 nodes. Container names are `n1` through `n4`, which OrbStack publishes as `n1.orb.local` and so on. Each container is limited to 2 GiB and 1 CPU (8 GiB for this topology). Drives are `/var/lib/rustfs/data1` through `data4`. The entrypoint writes `/etc/hosts` so `n1.orb.local` inside the network is that container's bridge address. RustFS marks an endpoint local only when that name resolves to a local IP and port 9000.

Preflight must show passwordless `sudo`, the data directories, `iptables -L`, and a local address for each `nX.orb.local`. If it prints `tc netem: NOT available`, rerun with `--faults partition,kill,pause`.

Then, still one topology:

```bash
export JEPSEN_SSH_KEY="$HOME/.ssh/id_ed25519"
./jepsen/scripts/run-topology.sh 4x4
```

That runs `elle`, `register`, `versioned`, and `list` in order, 90 seconds of client traffic each, concurrency 8, rate about 10 ops/s. It stops at the first non-zero Jepsen exit. A single workload:

```bash
cd jepsen
lein run test --topology 4x4 --workload elle --time-limit 90 --concurrency 8 \
  --username jepsen --ssh-private-key "$HOME/.ssh/id_ed25519" \
  --faults partition,kill,pause,packet --rate 10
```

Credentials are the test pair `rustfsadmin` / `rustfsadmin` (access key at least 3 bytes, secret at least 8). Override with `--access-key` and `--secret-key`. The bucket is `jepsen`. Region is `us-east-1`. The client uses path-style URLs on port 9000.

### Switch to 3x2

Do this only after the 4x4 run is finished. The script removes the 4x4 volumes so a 16-drive `format.json` cannot be opened as a 6-drive pool.

```bash
export JEPSEN_SSH_PUBKEY="$(cat ~/.ssh/id_ed25519.pub)"
./jepsen/scripts/switch-topology.sh 3x2
./jepsen/scripts/preflight.sh --drives 2 \
  n1.orb.local n2.orb.local n3.orb.local
./jepsen/scripts/run-topology.sh 3x2
```

3x2 is three containers at 2 GiB (6 GiB), drives `data1` and `data2`, concurrency 6. Container name `n4` is gone. Do not start `compose-4x4.yaml` again until this project has been removed with `down -v`.

Results are under `jepsen/store/rustfs-4x4-<workload>/latest/` and `jepsen/store/rustfs-3x2-<workload>/latest/`. `results.edn` holds `:valid?`. Elle writes a plot directory next to it. `rustfs.log` is copied from each node. `latest` is the symlink Jepsen maintains.

## Optional image smoke

`jepsen/docker/smoke-4x4.yaml` and `smoke-3x2.yaml` start `rustfs/rustfs:1.0.7` directly, with the same ellipsis shape and container names `smoke-n1` and so on, so they do not occupy `n1`. This does not install the zip, does not run Nemesis, and is not a Jepsen result. Stop it before the SSH topology. From the repo root:

```bash
docker compose -f jepsen/docker/smoke-4x4.yaml up -d
docker compose -f jepsen/docker/smoke-4x4.yaml logs -f n1
curl -fsS http://smoke-n1.orb.local:9000/health/live
docker compose -f jepsen/docker/smoke-4x4.yaml down -v
```

The smoke volume string is `http://n{1...4}:9000/data/disk{1...4}` because the official image can write under `/data`. The Jepsen nodes use `/var/lib/rustfs/data{1...4}` and the `.orb.local` names. If `rustfs/rustfs:1.0.7` is not published for arm64, the smoke file fails on pull. The Jepsen install path does not use that image. It downloads `rustfs-linux-aarch64-musl-v1.0.7.zip` (sha256 `e2c6d45105dd05df2f35547fbebfacf40dfba6ec4f8c31b86e30934faea79e76`) onto each node. The gnu zip is `--libc gnu`. An x86_64 node, including a Rosetta OrbStack machine, selects the matching pinned zip.

## UTM, or OrbStack Linux machines, when the clock must move

Use this instead of the compose files, not in addition to them. Four 2 GiB VMs plus macOS plus a 3 GB JVM is about 8 GB of guests and fits in 24 GB only when Docker is not also holding the 8 GB 4x4 set. Shut OrbStack Docker down before starting the VMs. Do not boot a fourth VM during the 3x2 run.

For UTM, create Debian 12 or 13 arm64 guests, 2 GiB RAM, 2 CPUs, and a 16 GB system disk. Attach four 1 GB virtio disks on each of the four guests for the first run and mount them at `/var/lib/rustfs/data1` through `data4`. Install `openssh-server`, `sudo`, `wget`, `unzip`, `iptables`, `iproute2`, and `netcat-openbsd`. Give the SSH user passwordless sudo. Put the same `/etc/hosts` on the Mac and on every guest, with stable addresses, for example `192.168.64.11 n1` through `192.168.64.14 n4`. The guest names must be `n1` through `n4` with no gaps.

```bash
cd jepsen
lein run test --topology 4x4 --nodes n1,n2,n3,n4 --workload elle \
  --time-limit 90 --concurrency 8 --username debian \
  --ssh-private-key "$HOME/.ssh/id_ed25519" \
  --faults partition,kill,pause,packet,clock --rate 10
```

`-n` / `--node` adds a single host each time. A comma-separated list belongs on `--nodes`. The volume string becomes `http://n{1...4}:9000/var/lib/rustfs/data{1...4}`.

Before calling that a clock result, confirm the clocks are independent: on n1, `sudo date -s '+30 seconds'`, and on n2, `date`. If n2 jumps as well, OrbStack or UTM is sharing one clock and `--faults` must omit `clock`. The DB setup stops `systemd-timesyncd` and `chronyd` when those units exist. Reset the n1 clock with `sudo chronyc makestep` or a reboot after the probe.

For 3x2, power off n4. On n1, n2, and n3, unmount and ignore `data3` and `data4`, and delete the contents of `data1` and `data2` so the 16-drive format is not reused. Then repeat the command with `--topology 3x2 --nodes n1,n2,n3 --concurrency 6` and two mounted disks per guest.

OrbStack Linux machines are the same contract if `date -s` stays local to the machine. Create them only while the Docker Jepsen stack is down, at 2 GiB each:

```bash
orb create --memory 2G --cpus 1 --disk 20G debian n1
```

Repeat for n2, n3, and n4. If `debian` is not a distro name, the `orb create` help lists images. Use Debian 12 or 13 arm64. Install sshd inside the machine, add the Mac pubkey, and pass the machine names to `--nodes` if those names resolve from the Mac and from the other machines. The names still have to be numbered `n1` through `n4`.

## Faults and how to read a failure

| Fault | Library | On these topologies |
|---|---|---|
| `partition` | `jepsen.nemesis.combined` partition package (`:one`, `:majority`, `:majorities-ring`) | 4x4 majority is a 2-2 split and neither side has quorum. 3x2 majority is 2-1 and the larger side can write. |
| `kill`, `pause` | DB `Kill` and `Pause` (`SIGKILL` / `SIGSTOP` on the `rustfs` process) | Targets are one node, or a minority. On 4x4 a minority is two nodes and drops below quorum. On 3x2 a minority is one node and quorum remains. |
| `packet` | combined packet package, `tc netem` delay and 20% loss | One node. This is the stand-in for a slow disk. |
| `clock` | `jepsen.nemesis.time` via the combined clock package | One node. Only on VMs whose clocks move independently. Off for Docker. |
| `file-corruption` | combined bitflip of `/var/lib/rustfs/data1` | Off unless added to `--faults`. It can destroy a shard. |

Nemesis ops use `:info`. Client crashes and timeouts use `:info`. After the fault interval Jepsen heals, waits 10 seconds, and runs the workload's final reads.

Open `store/<test>/latest/results.edn`.

- `:valid? true` means that checker accepted the history.
- `:valid? false` under `:workload` is the consistency failure. Elle's `:anomaly-types` names the cycle (`G1a`, `G1b`, internal, stale read, and so on). Knossos reports the operation it could not linearize. `:listing` false means a list returned an ETag that was never written, a duplicate key in one page, or a size that does not match the write of that ETag. `:versions` false means a successful versioned put or CAS returned no version id, or two successes shared one.
- `:valid? :unknown` means Elle or Knossos could not decide, usually because too many operations were indeterminate. Extend the quiet period or rerun. It is not a passing result and it is not, by itself, a bug report.
- A process that never comes back, or `results.edn` missing, is a harness or environment failure. Read `jepsen.log` and `rustfs.log` before treating it as a storage bug.

The perf plot shows partition, kill, pause, packet, and clock intervals from the Nemesis package. A spike of errors during a 4x4 two-node kill is the quorum going away. The check fails when a value that was acknowledged cannot be ordered with a later read, or when a side that should not have quorum acknowledges a write that disagrees with the other side.

## Blockers on Apple Silicon

Docker containers share the OrbStack VM clock. Do not add `SYS_TIME`, and do not put `clock` in `--faults` for the compose path.

`tc qdisc netem` needs the sch_netem module in the OrbStack kernel. Preflight says so when it is missing. Drop `packet` in that case. Partitions use `iptables` in the container namespace and need `NET_ADMIN`, which the compose file adds. If preflight's `iptables -L` fails, partitions will not isolate nodes and the run is not a fault test.

`n1.orb.local` is an OrbStack name. Docker Desktop alone does not publish it. Use the UTM `/etc/hosts` path there, or OrbStack.

Port 9000 is shared by design: each node is reached by its own DNS name, not by `localhost:9000`. Four containers cannot all publish host port 9000.

The musl zip is the default because a gnu binary built on a newer glibc may not start on Debian 12. `--libc gnu` selects the gnu zip and its pinned sha256.

A 4x4 format on disk will not boot as 3x2. `switch-topology.sh` deletes volumes. A hand-run `down` without `-v` leaves the old format in place and the next start fails closed.

Running both compose projects, or the Docker stack plus four UTM guests, exceeds the 14 GB Docker cap or the 24 GB machine. The scripts refuse to leave the other Jepsen project up.

## Artifacts

Release page: <https://github.com/rustfs/rustfs/releases/tag/1.0.7>.

| Zip | sha256 |
|---|---|
| `rustfs-linux-aarch64-musl-v1.0.7.zip` | `e2c6d45105dd05df2f35547fbebfacf40dfba6ec4f8c31b86e30934faea79e76` |
| `rustfs-linux-aarch64-gnu-v1.0.7.zip` | `147ef56079be712a242c4db43866ab269a6441a14214d66bac7ed22ef5db419e` |
| `rustfs-linux-x86_64-musl-v1.0.7.zip` | `c90a40ba8cdc1468c71029a4d65fdd13449720183d3a8e00bc61ab09fcd448a5` |
| `rustfs-linux-x86_64-gnu-v1.0.7.zip` | `4cd2ee55b75f7cb7c935eeb20f8c22f17e1453009cf1af1ff8b42f7986cba201` |

`--rustfs-url` requires `--rustfs-sha256`. The installer deletes a zip that does not match and will not start it.
