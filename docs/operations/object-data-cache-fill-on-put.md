# Object data cache: fill on PUT (`RUSTFS_OBJECT_DATA_CACHE_FILL_ON_PUT`)

The object data cache (`RUSTFS_OBJECT_DATA_CACHE_*`) fills on GET: the first
reader of an object pays the erasure read and the fill, later readers hit.
For write-once data that is read right after it is written, such as KV-cache
chunks pushed after prefill, the first read is the one that matters.

With `RUSTFS_OBJECT_DATA_CACHE_FILL_ON_PUT=true` a PUT keeps a copy of its
plaintext body while it streams to the erasure encoder and, once the object
is committed, inserts it under the same key the GET path plans for that
version (after the usual post-put invalidation). The first GET is then a hit.

Only PUTs that meet all of the following are filled:

- the cache is enabled in a fill mode (`fill_buffered_only` or
  `fill_materialize_enabled`);
- the body takes the plain streaming path: no compression, no ciphertext
  passthrough, no SSE request headers (bucket-default encryption is caught by
  the plan after the commit, and such objects are not inserted);
- the object size is known and at most the cache's fill ceiling
  (`RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES`, clamped like a GET fill).

Cost: one extra in-memory copy of the body for the duration of the PUT (the
frames are released after the insert), and the insert itself. Nothing changes
for PUTs that do not qualify. Default: off.

Measured with 48 MiB chunks over cuObject RDMA GETs, the first GET after a
PUT then comes from memory instead of the drives.
