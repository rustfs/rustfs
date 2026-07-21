# Drive Timeout Tuning

This document describes the per-operation drive timeout knobs and the
drive-timeout profile. It is written for operators running RustFS on slow or
high-latency storage (HDD-class disks, network block devices, throttled
containers) who see `ListObjects`/`ListObjectsV2` requests fail on large
prefixes, or who want to widen drive liveness budgets before a walk on a
healthy-but-slow disk is treated as a stall.

If you are debugging a listing that returns *fewer* keys than the bucket holds,
read [Listing truncation and the walk stall budget](#listing-truncation-and-the-walk-stall-budget)
first — that is the case this document exists for.

## Background: what these timeouts bound

Every foreground drive operation carries a liveness budget so a hung disk fails
fast instead of parking the request forever. The budget answers "is the drive
still *answering*", not "how much total work is there" — a healthy disk that is
merely busy keeps making progress and is not timed out.

The most important of these for listings is the **walk stall budget**. A
directory walk (the filesystem enumeration behind every `ListObjects`) bounds
each individual filesystem call — `readdir`, `stat`, `xl.meta` read — by the
stall budget. A call that stops answering for longer than the budget fails with
a drive timeout; time the walk spends blocked on a slow *consumer* (a client
draining the listing slowly) does not count against it.

## Configuration and precedence

Each knob is resolved in this order, highest priority first:

1. Its explicit per-operation environment variable
   (`RUSTFS_DRIVE_*_TIMEOUT_SECS`).
2. The legacy global fallback `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION` (deprecated;
   applies to every per-operation knob that has no explicit override).
3. The drive-timeout profile default (see below).
4. The built-in default.

Values are whole seconds. Changes take effect on process restart.

### Drive-timeout profile

`RUSTFS_DRIVE_TIMEOUT_PROFILE` selects a preset that raises several defaults at
once, so slow-storage deployments do not have to set each knob individually:

| Value | Effect |
|---|---|
| `default` | Built-in defaults (see the table below). |
| `high_latency` | Raises the default for every profile-aware knob to **60s**. |

Explicit per-operation overrides always win over the profile, so you can select
`high_latency` and still pin one knob to a specific value.

## Knobs

| Environment variable | Default | `high_latency` default | Bounds |
|---|---:|---:|---|
| `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS` | `5` | `60` | Max time a single walk filesystem call may go without answering during a listing walk. **The knob for listing failures on large prefixes.** |
| `RUSTFS_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS` | `10` | `120` | Max time the metacache merge consumer waits for the next visible entry from a walk reader. Values below the resolved stall timeout are clamped up to the stall timeout. |
| `RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS` | `5` | `60` | Total wall-clock timeout for a `walk_dir`. Retained for non-foreground callers; the foreground listing path no longer uses it (see below). |
| `RUSTFS_DRIVE_LIST_DIR_TIMEOUT_SECS` | `5` | `60` | Timeout for a standalone `list_dir` metadata listing. |
| `RUSTFS_DRIVE_METADATA_TIMEOUT_SECS` | `5` | `60` | Timeout for metadata reads such as `read_metadata`. |
| `RUSTFS_DRIVE_DISK_INFO_TIMEOUT_SECS` | `5` | `60` | Timeout for `disk_info()` calls. |
| `RUSTFS_OBJECT_DISK_READ_TIMEOUT` | `10` | `60` | Per-read stall budget while streaming an object body from disk. |
| `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION` | `30` | — | Deprecated global fallback for every per-operation knob without an explicit override. Prefer the per-operation knobs. |

The health-transition and probe knobs (`RUSTFS_DRIVE_TIMEOUT_HEALTH_ACTION`,
`RUSTFS_DRIVE_ACTIVE_CHECK_*`, `RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD`, and the
returning/offline classification knobs) govern how a timeout maps to drive
health state. They are out of scope here; see `crates/config/src/constants/drive.rs`
for the full list and defaults.

## Listing truncation and the walk stall budget

### Symptom

`ListObjects`/`ListObjectsV2` on a large prefix either:

- returns `500 InternalError` with `Io error: timeout`; or
- (on older builds) returns HTTP 200 with `IsTruncated=false` after fewer keys
  than the bucket actually holds — a **silent** truncation that S3 clients
  (`mc`, minio-go, SDK pagination loops) cannot detect, because
  `IsTruncated=false` is the protocol's only end-of-listing signal.

Every "missing" object remains readable by exact key via `GetObject` /
`StatObject`; only the listing is affected.

### Why it happens

A listing walk is bounded by the stall budget. Because a whole-directory
enumeration (`list_dir` reading every immediate child in one pass) is bounded by
that budget **as a single unit**, a very wide *flat* directory — one prefix
holding hundreds of thousands or millions of immediate children — can make a
single `readdir` exceed the budget on a perfectly healthy disk, especially on
HDD-class or throttled storage. That trips a drive timeout, which the listing
path escalates and surfaces to the client.

### The silent variant is fixed; the loud 500 is tuned away

As of the walk-stall rework (merged to `main`, first released in **1.0.0-beta.9**):

- **The silent variant is eliminated.** A walk that dies mid-stream can no
  longer be consumed as a clean end-of-listing. Once a walk has streamed any
  entries and then stalls, the failure is recorded as a hard drive timeout and
  escalated on that erasure set, so the client always sees an error — never a
  well-formed short page. This is locked by the
  `list_path_raw_returns_timeout_when_producer_fails_after_partial_entry`
  regression test in `crates/ecstore/src/cache_value/metacache_set.rs`.
- **The remaining 500 is an operator-tunable, not a data-integrity bug.** A
  genuinely wide flat directory can still exhaust the default 5s stall budget on
  slow storage and fail the listing loudly. The supported mitigation is to widen
  the budget.

### Mitigation

Raise the walk stall budget, or select the high-latency profile:

```bash
# widen just the listing walk budget
-e RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS=60

# widen the metacache reader wait budget when drives keep progressing but do
# not publish visible entries quickly enough for the merge consumer
-e RUSTFS_DRIVE_WALKDIR_PEEK_TIMEOUT_SECS=120

# or raise every profile-aware drive default at once
-e RUSTFS_DRIVE_TIMEOUT_PROFILE=high_latency
```

> Note: on releases at or before `1.0.0-beta.8`, the foreground listing path was
> bounded by the *total* wall-clock knob `RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS`
> instead of the stall budget. If you cannot upgrade, raise that knob — but
> upgrading to `1.0.0-beta.9` or later is strongly preferred, because only the
> newer builds convert the *silent* truncation into a detectable error.

The most durable fix for pathologically wide directories is to shard keys under
additional prefix levels so no single directory holds an enormous flat child
set; the stall budget then never has to bound one giant `readdir`.

### Diagnosing

Server logs at the failure show the walk timeout escalating through the listing
pipeline:

```
WARN  Metacache reader peek timed out            state=peek_timed_out  drive=<endpoint>
ERROR Metacache listing quorum failed            state=quorum_failed
```

The `rustfs_list_path_raw_stall_total` counter (labelled by `drive`) increments
on every walk stall, and gives you a per-drive signal that a budget is being
hit before any client-visible failure. Watch it after tuning to confirm the
stalls have stopped.
