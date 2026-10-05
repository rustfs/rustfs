# Bucket creation and deletion admission

`CreateBucket` and `DeleteBucket` share a process-wide execution budget of eight active transactions. Short bursts wait in a FIFO queue with space for 128 waiting requests. Each request can wait up to 30 seconds for an execution slot. These limits protect storage and namespace-lock resources while allowing parallel test workers to create isolated buckets.

A request that finds the queue full or exceeds the queue wait returns HTTP 503 with the S3 error code `SlowDown` and `Retry-After: 1`. This is an overload response, not a bucket-name or filesystem error. The active and waiting limits are shared by creation and deletion and are currently fixed.

Waiting requests do not start storage mutations. Disconnecting while waiting releases the queue slot. Once admitted, the transaction owns its execution slot until the mutation and post-commit hooks finish, including when its caller disconnects.

For automated tests and applications:

- Keep SDK retries enabled and use bounded exponential backoff with jitter for `SlowDown`. Honor `Retry-After` as a minimum delay when implementing your own retry policy.
- Set the client request timeout above the queue wait plus the expected operation time. The 30-second limit covers admission waiting only; it does not limit transaction execution.
- Reduce the client concurrency if bursts exceed the waiting capacity or repeatedly time out. A bucket per test worker remains a supported isolation pattern.
- After an ambiguous network timeout, check the bucket state before assuming the operation failed: an admitted transaction may finish after the client stops waiting.

The serving implementation is `BucketOperationAdmission` in `rustfs/src/app/bucket_usecase.rs`. This budget does not change object PUT/GET admission, erasure quorum, or persistence rules.
