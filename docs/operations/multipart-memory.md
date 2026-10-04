# Multipart upload memory diagnosis

**Use this when:** sustained multipart uploads increase process memory or end in an OOM kill, and you need to distinguish request buffers, background work, and allocator retention.

## Record the actual workload

Record the image digest, source revision, architecture, allocator version, I/O backend, data filesystem and mounts, container memory/swap limits, and the client version. Docker directories sharing one device are not equivalent to separate physical disks. Keep any local-test disk-check bypass out of production configuration.

Count active UploadPart requests independently from file/upload concurrency. Record part size, parts per upload, retries, completed uploads, aborted uploads, TLS, throughput, and request latency. A distributed client's concurrency applies per client. Warp multipart-put also performs create and complete operations; its PUTPART summary alone does not count successful completions.

Run a short baseline followed by a sustained isolated run long enough to cross the reported growth window. Keep a defined idle observation window after uploads, completion, abort and cleanup. Bound disk use as well as memory: a successful long upload test can fill the test filesystem.

## Separate memory measurements

Collect these measurements on the same time axis:

- Process RssAnon/Pss_Anon, file RSS, swap and total RSS from Linux procfs.
- Cgroup memory.current, memory.stat anon/file and memory.events. Docker stats is not a substitute for this split.
- Allocator reserved and committed bytes, and requested bytes only when their live statistics are available.
- Active requests, EC producer/queue/writer bytes, GET buffered bytes, tasks, threads, file descriptors, and scanner/heal/replication activity.

The EC queue budget controls queued encoded blocks, not complete per-request or process memory. Admission permits constrain concurrent operations; they do not establish an RSS limit or release allocations retained by completed work.

### Allocator statistics

Mimalloc JSON count statistics have direct current, peak and total fields. Scalar counters contain only cumulative totals. In particular, a scalar malloc_requested must never be interpreted as current live bytes or a peak. The malloc_normal/huge statistics are not a reliable replacement for requested live bytes in release builds.

Use rustfs_memory_allocator_malloc_requested_bytes only when rustfs_memory_allocator_malloc_requested_bytes_available equals 1. A value of 0 for availability means the live statistic could not be established; it does not mean the heap is empty. An all-zero requested count may mean accounting was compiled out. A supported count whose current becomes zero while peak/total remain nonzero is a valid drained heap observation.

A previously published gauge can remain visible after a statistic becomes unavailable. Filter it by the availability metric. The malloc_requested_total_bytes metric is cumulative allocation activity and can grow indefinitely in a healthy process. Do not use its slope as evidence of retained memory.

When live requested bytes are unavailable, capture a supported heap profile or allocation/free trace. Preserve attribution to allocation stacks and lifetime; committed bytes and RSS alone cannot establish a leak. Other raw allocator statistics, including page accounting, depend on the allocator build and are not interchangeable with live application bytes.

### Idle reclaim

The reclaim loop waits for request, delete-tail, scanner, heal, EC and GET-buffer activity to become idle. Continuous traffic can skip reclaim. Check the activity gauges, skipped reasons and idle streak rather than assuming that waiting a fixed period guarantees collection.

A successful MiMalloc collect call collects the executing thread's heap. The reclaim ok counter does not prove that all Tokio worker or blocking-thread heaps have been collected. It cannot by itself rule out allocator retention. Avoid adding forced collection to each request without a measured cause and performance evidence.

## Isolate the retaining path

Compare the same upload ID and part number overwritten repeatedly with new upload IDs and parts. Test UploadPart without completion, complete cycles, abort, disconnect, timeout and slow storage separately. Compare TLS and plaintext only at matched workload and throughput. Correlate background activity rather than disabling durability or checksums.

A live-heap leak requires evidence that allocations stay reachable or unfreed after their owner should finish. Stable live allocations with growing committed/RSS point toward allocator retention or fragmentation. Growth tracking metadata cardinality or background queues needs a bounded-state investigation. Accept stable caches and explain their bounds; do not require every byte of RSS to disappear after idle.

Verify any fix with the same workload, failure paths and idle window. Preserve S3 part/checksum semantics, cancellation cleanup, commit fencing, write quorum and durability. Raising memory limits or reducing concurrency can be a diagnostic control, but does not prove a leak is fixed.
