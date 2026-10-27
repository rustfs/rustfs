# Workflow runners: what is billed and where a job belongs

**Use this when:** adding a job, changing a `runs-on`, moving a lane between runner pools, or deciding whether a new check is worth its runner cost.
**Source of truth:** `runs-on` in each workflow file for current placement; `.github/actionlint.yaml` for the declared self-hosted labels; the organisation's Settings → Billing → Usage page for what is actually charged; `docs/testing/ci-gates.md` for which checks block a merge (that does not change when a runner changes).

## Billing facts

This repository is public. For a public repository, GitHub's standard hosted runners (`ubuntu-latest`, `windows-latest`, `macos-latest`) cost nothing and have no minute cap, whatever plan the organisation is on. GitHub bills only two things: larger hosted runners, which have custom labels and are not used here, and standard runners in private repositories.

Every other label in this repository is capacity RustFS pays for. ARC pods and lab machines accrue cost while the machine exists. A Ubicloud label is metered for the minutes a job holds its VM:

| Label | Paid by | What it is | Use it for |
|---|---|---|---|
| `ubuntu-latest` | nobody | GitHub-hosted, Ubuntu 24.04, 4 vCPU, 16 GB RAM, about 14 GB of free disk. Ships a C toolchain, `docker`, `gh`, `curl`, `wget`, `jq`, Python. Can `mount` tmpfs and loop devices; io_uring works | the default for every job |
| `windows-latest`, `macos-latest` | nobody | GitHub-hosted standard runners | Windows and macOS targets |
| `sm-standard-2` | RustFS | ARC pod, Ubuntu 24.04, minimal image: no `gh`, no C toolchain; Docker only on the `dind-` label | only a job that a hosted runner cannot run (see the placement rule) |
| `sm-standard-4` | RustFS | ARC pod, Ubuntu 24.04, 14 CPUs and 28 GB (the label is not the core count), large disk | workspace-scale Cargo builds |
| `ubicloud-standard-16-arm` | RustFS | Ubicloud VM, alias of `ubicloud-standard-16-arm-ubuntu-2404`: Ubuntu 24.04 arm64, 16 vCPU, 48 GB, 300 GB disk. Reached through the `build.yml` matrix, so `actionlint.yaml` does not list it | native aarch64 release builds (`linux-aarch64-musl`, `linux-aarch64-gnu`) |
| `dind-sm-standard-2` | RustFS | ARC pod with Docker-in-Docker | image builds that need `buildx` and a registry push |
| `smoke-testing`, `pf-testing` | RustFS | long-lived lab machines | the packaged functional chain and performance suites |

The self-hosted pods and the hosted image are both Ubuntu 24.04, so a binary built on one runs on the other; `ci.yml` builds the debug binary on `sm-standard-4` and runs it on both pools.

## Placement rule

A job runs on `ubuntu-latest` unless it has one of these reasons, and the reason goes in a comment next to its `runs-on`:

1. **Disk.** It restores or produces the workspace `target/`: `cargo build` or `cargo test` with `--all-targets`, `cargo nextest run --all`, or any reader of the `ci-dev` / `ci-feat-*` caches. The workspace test build leaves about 64 GB in `target/`; a hosted runner has about 14 GB.
2. **Critical-path CPU.** It is a build that several downstream lanes wait on and would blow its timeout at 4 vCPU (`build-rustfs-debug-binary` gates the e2e and s3-tests lanes).
3. **Hardware.** Native aarch64, Docker-in-Docker, or the lab machines.
4. **Cache ownership.** It reads a `rust-cache` key that a self-hosted job writes (see the cache rule below).

Jobs that do only the following have no reason to be self-hosted: shell or Python scripts, `cargo fmt`, `cargo deny`, typos, actionlint, API calls (`gh`, dependency review, issue and stale bots, CLA checks), downloading an artifact and running a prebuilt binary, or a `cargo build -p rustfs` with its own cache key. `e2e-distributed.yml` and `e2e-upgrade.yml` build the server and the `e2e_test` harness on `ubuntu-latest` inside their budgets, so "it compiles something" alone is not a reason.

A hosted runner is also the only choice for a few things the pods cannot do: `mount -o loop` and `mount -t tmpfs` fail inside the ARC pods, and a container runtime cannot guarantee that `io_uring_setup` is allowed. `uring-integration` and `e2e-distributed` are on `ubuntu-latest` for that reason, not for cost.

## Cache rule

`Swatinem/rust-cache` builds its key from the OS, architecture, toolchain and lockfile. It does not include the runner label or image, so a cache written on an `sm-standard-4` pod and one written on `ubuntu-latest` would overwrite each other under the same `shared-key`.

- Every `shared-key` has exactly one writer. `cache-warm.yml` owns `ci-dev`, `ci-feat-rio`, `ci-feat-proto` and `ci-uring`; every other `./.github/actions/setup` call states `cache-save-if` explicitly, and `scripts/security/check_cache_save_if.sh` fails the build when one does not.
- All readers of a key run on the same image as its writer. Keys that live on hosted runners say so in their name (`ci-uring` is warmed on `ubuntu-latest`; `ci-e2e-distributed-hosted`, `e2e-upgrade-server-hosted`, `e2e-upgrade-tests-hosted`).
- Moving a job between pools therefore means renaming its key. The new key is empty until a run from `main` (a scheduled run, or a manual dispatch of that workflow from `main`) saves it; until then that lane does a cold build. The `build.yml` aarch64 release legs append `-ubicloud-arm` for this reason. `cache-warm.yml` does not write those keys.
- `CARGO_BUILD_JOBS` and every other `CARGO*`, `CC*`, `CFLAGS*`, `CXX*`, `CMAKE*` and `RUST*` variable is hashed into the key. `ci.yml` sets `CARGO_BUILD_JOBS` at step level for that reason; promoting it to job level rotates the key for every lane that shares it.

## Where self-hosted minutes leak

These are the patterns that have cost real money here without validating anything more:

- An `if: always()` summary or alert job on a self-hosted label inside a `workflow_run`-triggered workflow. The upstream workflow completes many times a day, the real jobs skip, and the summary still starts a pod each time. Gate it on the upstream job having run, and keep it on `ubuntu-latest`.
- Per-push workflows on `main`. A merge triggers the full `ci.yml` matrix, `cache-warm.yml` and the `build.yml` development build; with tens of merges a day that is as much self-hosted time as all pull requests together. Anything that does not verify the merged tree itself belongs on a schedule or behind a `paths` filter.
- `apt-get install` of tools the hosted image already ships. It is only needed on the minimal pod images.
- A throttled `CARGO_BUILD_JOBS` on a 14-CPU pod. The limit exists for link-storm reasons documented in `ci.yml`; measure before raising it, and measure before lowering it further.
- Tiny jobs on ARC pods. Each one pays roughly a minute of pod start-up before its first step; a compile-free job that takes ten seconds costs more to schedule than to run.

## Measuring a run

To see which pool each job of a run used and for how long:

```bash
gh api "repos/rustfs/rustfs/actions/runs/<run_id>/jobs?per_page=100" \
  --jq '.jobs[] | "\(.conclusion)\t\(.labels | join(","))\t\(.started_at)\t\(.completed_at)\t\(.name)"'
```

Only rows whose label starts with `sm-`, `dind-`, `ubicloud-`, `smoke-` or `pf-` cost anything. At the October 2026 review, a full pull-request run of `ci.yml` spent about 150 self-hosted minutes, a third of it in jobs that never compiled; treat that as the scale of what a placement mistake costs, not as a current figure.

## Checklist for a new or moved job

- `runs-on` is a literal label. A new self-hosted label is declared in `.github/actionlint.yaml` in the same change (`.github/AGENTS.md`).
- The job declares `timeout-minutes` (`scripts/security/check_job_timeouts.sh`).
- `actionlint` reports nothing.
- A self-hosted `runs-on` carries a comment naming its reason from the placement rule.
- Every `./.github/actions/setup` call states `cache-save-if`, and a key read from one pool is not written from the other.
- Moving a job does not change its check name, so the `main` ruleset and `docs/testing/ci-gates.md` stay as they are; renaming it does, and then that document's change checklist applies.
