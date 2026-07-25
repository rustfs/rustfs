# Atomic object undo precondition

RustFS supports a destination-side version precondition for the two S3
operations used to undo changes in a versioned bucket:

```text
x-rustfs-expected-current-version-id: <version-id>
```

This is a RustFS extension, not a standard S3 header.

## Restore a historical object version

Send a same-object `CopyObject` request whose copy source includes the
historical `versionId`, and set the extension header to the version that was
current when the undo was planned. RustFS holds the destination namespace write
lock while it compares the current version and creates the restored version.

The request fails without creating a version when the source is not a
historical version, the source and destination differ, or the destination
current version no longer matches.

## Remove a delete marker

Send a version-specific `DeleteObject` request for the delete marker and set the
extension header to the same version ID. RustFS removes it only when it is still
the current version and is a delete marker. The namespace write lock covers the
check and deletion.

## Responses and retries

- A stale, missing, non-current, or non-delete-marker version returns HTTP 412
  `PreconditionFailed` and does not mutate object history.
- An empty or malformed version header returns HTTP 400 `InvalidArgument`.
- A mismatched operation shape returns HTTP 400 `InvalidRequest`.
- A successful retry using an old expected version returns HTTP 412 because the
  first successful request changed the current version.

Object Lock retention, legal hold, authorization, replication, and ordinary S3
behavior remain unchanged. The extension only adds a precondition; it does not
bypass existing validation.
