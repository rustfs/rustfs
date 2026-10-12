# Bucket creation and deletion admission

`CreateBucket` and `DeleteBucket` share a process-wide execution budget of eight active storage mutations. Short bursts wait in a FIFO queue with space for 128 waiting requests. Each request can wait up to 30 seconds for an execution slot. A separate total budget bounds all unfinished operations to 136, including queued requests, mutations, and post-commit peer reloads. These limits protect storage, namespace-lock, and notification resources while allowing parallel test workers to create isolated buckets.

A request that finds the queue full, exceeds the queue wait, or reaches the total in-flight limit returns HTTP 503 with the S3 error code `SlowDown` and `Retry-After: 1`. Admission rejection happens before storage mutation begins. This is an overload response, not a bucket-name or filesystem error. The limits are shared by creation and deletion and are currently fixed.

Waiting requests do not start storage mutations. Disconnecting while waiting releases both the queue slot and its total-operation slot. Once admitted, the detached task continues even when its caller disconnects. Creation releases its execution slot after the storage mutation, site-replication hook, and associated locks finish, then waits for peer metadata reloads before returning. Its total-operation slot remains held until that work finishes, so slow reloads cannot accumulate unbounded background tasks or occupy every storage execution slot. Deletion holds its execution slot through its usecase; its existing asynchronous peer cache invalidation is unchanged.

For automated tests and applications:

- Keep SDK retries enabled and use bounded exponential backoff with jitter for `SlowDown`. Honor `Retry-After` as a minimum delay when implementing your own retry policy.
- Set the client request timeout above the queue wait plus the expected operation time. The 30-second limit covers admission waiting only; it does not limit transaction execution.
- Reduce the client concurrency if bursts exceed the waiting capacity or repeatedly time out. A bucket per test worker remains a supported isolation pattern.
- After an ambiguous network timeout, check the bucket state before assuming the operation failed: an admitted transaction may finish after the client stops waiting.

The serving implementation is `BucketOperationAdmission` in `rustfs/src/app/bucket_usecase.rs`. This budget does not change object PUT/GET admission, erasure quorum, or persistence rules.
