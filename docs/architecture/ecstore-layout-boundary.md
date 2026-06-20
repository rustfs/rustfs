# ECStore Layout Boundary

This document records the `E-001` and `E-SET-001` foundation slice for the
architecture migration.

## Directory Skeleton

The ECStore migration uses these internal ownership buckets before any pure
file moves:

- `api`: facade and compatibility re-export ownership.
- `core`: store facade, object, bucket, list, multipart, and heal paths.
- `layout`: static endpoint, disk, pool, and set layout descriptions.
- `disk`: local disk, format, health, and disk error ownership.
- `erasure`: erasure coding and bitrot ownership.
- `metadata`: bucket metadata, config object-store, and data-usage ownership.
- `cluster`: remote disk, peer, lock, membership, and health-control-plane
  ownership.
- `services`: lifecycle, replication, tier, notification, rebalance, and
  metrics service ownership.

## Static Set Layout

Static layout is derived from persisted `FormatV3` data and input endpoint
expansion. It may describe:

- deployment id;
- set count and drives per set;
- disk UUID positions inside `format.erasure.sets`;
- distribution algorithm;
- endpoint grouping produced before runtime disk initialization.

Static layout must not own disk handles, lock clients, reconnect loops, repair
state, or shutdown signaling.

## Runtime Set Orchestration

Runtime orchestration remains owned by `Sets` and `SetDisks` until a later pure
move. It may describe:

- flat disk index to `(set_index, disk_index)` mapping;
- per-set local disk replacement after distributed setup detection;
- per-set lock-client host deduplication;
- endpoint reconnect monitoring and runtime shutdown signaling;
- read/write/heal/list orchestration over initialized disks.

## Preservation Rules

- Object-to-set hashing and distribution algorithm selection must not change.
- Format `sets` ordering and disk UUID position lookup must not change.
- Local disk replacement and lock-client mapping stay runtime-only.
- Later file moves must keep old public paths or add explicit compatibility
  coverage before deleting them.
