# Concurrency and Durability Lens

- For every changed lock, enumerate overlapping lock sets and construct the
  ABBA interleaving. Multiple-lock order must be documented and consistent.
- Mark guard lifetimes and every `.await`, disk, and RPC call inside them.
  Estimate contention and timeout behavior under concurrent requests.
- Object commits remain fenced if the distributed lock is lost after shard
  writes and before metadata rename.
- For write/rename changes, trace `write tmp -> sync tmp -> rename -> sync parent
  -> sync required ancestors`; simulate a crash after each step and honor the
  configured durability gate.
- Multi-disk fan-out counts every result. Quorum-minus-one cannot become success;
  heal remains best-effort per target where that is the established contract.
- At every new cancellable await between mutation and cleanup/commit, drop the
  future and inspect leftover files, counters, permits, and replay state.
- Multipart operations on the same upload ID are serialized where required;
  abort/complete/list races cannot delete parts before durable commit.
- Post-commit cleanup is best-effort, retry-safe, and cannot fail an already
  committed write or delete the last surviving copy.
- Persisted read-modify-write uses serialization/CAS. Queue replay is crash-safe
  and duplicate delivery has an idempotency contract.
- Streaming reconstruction failures after partial output surface as errors, not
  successful EOF.
