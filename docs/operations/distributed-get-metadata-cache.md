# Distributed GetObject metadata cache proof experiment

`RUSTFS_EXPERIMENTAL_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_PROOF_ENABLE` opts a test or `e2e-test-hooks` build into proof-validated GetObject metadata caching for distributed erasure sets. It defaults to `false` and requires a restart to change. Ordinary production builds ignore the setting and keep the distributed cache hard-disabled until the mutation-authority audit described below is complete.

The experiment does not add a cache-control RPC or change write admission. Capable namespace-lock voters advance a bounded mutation-generation shard on every successful exclusive grant. An eligible GET requests stamps through its existing shared-lock quorum and may use a cached metadata entry only when the current proof exactly matches the proof recorded at cache publication.

The read falls back to authoritative metadata quorum without failing the S3 request when the setting is disabled or any required proof property is unavailable, including a legacy voter, malformed or unknown stamp, duplicate voter identity, lock loss, voter restart, generation exhaustion, changed quorum cohort, or proof mismatch.

Only request shapes already eligible for the process-local metadata cache participate: latest unversioned object reads whose ECStore entrypoint holds the fixed object namespace read guard, excluding direct SetDisks calls, system buckets, explicit versions, versioned or suspended buckets, delete-marker reads, part-number or part-checksum reads, and data-movement paths. Local erasure deployments retain the existing process-local generation behavior.

The proof state is in memory and has no on-disk migration. Hash-shard collisions cause conservative cache misses only. Mixed-version operation preserves lock and storage availability because old peers can grant locks without a stamp; a cache hit requires a complete capable read quorum.

Keep the experiment disabled for production rollout until every cache-visible metadata mutation path has been verified to hold the same exclusive object namespace authority and the multi-node fault and performance acceptance matrix has passed. Disabling the setting and restarting clears all process-local proof-bound cache state.

The current production-activation blockers include delete-recovery reconciliation, exact-marker cleanup, and selected restore/heal metadata paths that can operate under a set-local, lifecycle, or process-local fence instead of the ECStore fixed object lock domain. These paths must either acquire that fixed exclusive authority before publication or gain an equivalent quorum-stamp transition before the production gate is removed. Legacy transition-state reconciliation is not a blocker because it already acquires the fixed store domain followed by every distinct physical set domain before disk metadata publication.
