# Correctness Lens

Attack the changed behavior, not every subsystem in the repository.

- Trace new error paths to the caller. Inject the ignored/wildcard variants and
  verify they cannot become success, not-found, or a plausible default.
- Exercise zero/empty/missing, maximum, and exact-boundary inputs for every
  changed count, size, index, page limit, or optional value.
- For aggregation/quorum changes, test exactly quorum and quorum-minus-one with
  mixed disk errors and nil/placeholder entries.
- For listing/pagination, test `n == max`, `n == max + 1`, delimiter folding,
  continuation markers, and object/prefix name collisions.
- For EC/read/streaming changes, inject failure after partial output and verify
  the client receives an error rather than a clean truncated body. Assert exact
  bytes and length.
- For multipart/object commits, fail before/after rename and cleanup; committed
  data must remain readable and pre-commit cleanup must not destroy parts.
- For version/index ordering, test `len - 1`, `len`, equal timestamps, missing
  versions, and deterministic tie-breaking.
- For directory-object behavior, trace `__XLDIR__` at the store layer; branches
  below the layer that sees trailing slashes are dead.
- For binary UUID metadata, absent, empty, and nil all mean no value. Never send
  nil/empty `versionId` to an unversioned tier.
- For agent rules/skill routers, test a trigger matrix covering ordinary
  inquiry, low-risk implementation, explicit review, high-risk code, PR
  creation, release, and post-PR monitoring. Each case must select only the
  intended workflow and retain required safety/authorization boundaries.

Null verdicts name only the probes relevant to the diff.
