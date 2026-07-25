# Replication target check

`GET /BUCKET?replication-check` is a signed S3 extension for validating every
replication target referenced by a bucket replication configuration.

## Active mutation warning

Despite using `GET`, this operation is **not read-only**. On each target it:

1. writes an 8-byte object under `.rustfs.sys/replication-check/<uuid>/<uuid>`;
2. creates a replicated delete marker;
3. permanently deletes the probe object version; and
4. enumerates that exact probe key and attempts to delete every remaining
   object version and delete marker.

Callers should obtain operator confirmation before sending the request. Probe
keys use a reserved namespace and two independent random UUIDs. Before writing,
the server verifies that no version or delete marker exists at the exact key,
then uses an atomic `If-None-Match: *` write so it cannot overwrite a key created
concurrently by an application.

## Response contract

The route returns HTTP 200 with JSON after all configured targets have been
checked. `Status` is `FAILED` when any target or cleanup phase failed; successful
target results remain present when another target fails.

```json
{
  "Status": "FAILED",
  "ActiveMutation": true,
  "MutationDescription": "Writes a probe object, creates a delete marker, deletes the probe version, and cleans up all probe artifacts on each target.",
  "ProbeNamespace": ".rustfs.sys/replication-check/",
  "Targets": [
    {
      "Arn": "arn:minio:replication::target",
      "Bucket": "replica",
      "Status": "FAILED",
      "Error": "probe cleanup failed: target delete object version check failed: AccessDenied",
      "Phases": {
        "Bucket": { "Status": "OK" },
        "Versioning": { "Status": "OK" },
        "ObjectLock": { "Status": "OK" },
        "Put": { "Status": "OK" },
        "DeleteMarker": { "Status": "OK" },
        "VersionDelete": { "Status": "OK" },
        "Cleanup": {
          "Status": "FAILED",
          "Error": "target delete object version check failed: AccessDenied"
        }
      }
    }
  ]
}
```

Phase states are `OK`, `FAILED`, or `SKIPPED`. Errors are single-line, bounded
to 512 bytes, and omit remote messages, endpoints, credentials, signatures, and
authorization material. A cleanup failure is always explicit; it is never
reported as a successful check.
