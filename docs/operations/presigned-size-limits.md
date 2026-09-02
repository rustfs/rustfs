# Presigned upload size limits

**Use this when:** a backend issues SigV4-presigned upload URLs to browsers and must cap how much a client can upload with one URL — per request (`PutObject`) or per multipart upload.
**Source of truth:** `rustfs/src/auth.rs` (`RUSTFS_MAX_CONTENT_LENGTH_QUERY`, `RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY`, `parse_presigned_put_max_content_length`, `parse_presigned_multipart_max_total_object_size`); enforcement in `rustfs/src/app/object/put.rs` (`MaxContentLengthStream`), `rustfs/src/app/multipart_usecase.rs` (`multipart_max_total_object_size`), and `crates/ecstore/src/set_disk/ops/multipart.rs` (`multipart_size_limit_from_metadata`, `admitted_multipart_size`).

Both limits are RustFS-specific query parameters carried inside the SigV4 canonical query.

## Shared signing rule

1. The backend appends the parameter to the request URI **before** computing the SigV4 presigned signature. It is part of the canonical query, so adding, removing, or changing it afterwards invalidates the signature; the browser cannot alter it.
2. RustFS parses the parameter only after the request has been accepted as SigV4-signed (the `VerifiedPresignedRequest` / `VerifiedSigV4Request` request markers). The same query string on an unsigned request is rejected with `InvalidRequest`.
3. The value is an unsigned 64-bit integer. Duplicate, case-variant, malformed, negative, or overflowing values return `InvalidRequest`.
4. Requests that do not carry the parameter, including ordinary authenticated or anonymous uploads, keep their existing behavior.

| | V1 per-request | V2 per-upload |
| --- | --- | --- |
| Query parameter | `x-rustfs-max-content-length=<u64>` | `x-rustfs-max-total-object-size=<u64>` |
| Accepted on | SigV4 presigned `PutObject` only | `CreateMultipartUpload` only (signed or presigned SigV4) |
| Any other operation carrying it | `InvalidRequest` (`CopyObject`, multipart, `GET`, `HEAD`, `DELETE`, bucket operations) | `InvalidRequest` (`UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, listing, copy — these read the persisted session state instead) |
| What is measured | Decoded request body bytes of that one request | Logical object bytes (`actual_size`) summed across the upload's parts; not erasure, encryption, or compression bytes |
| Where the limit lives | The request only | Multipart session metadata (`SUFFIX_MAX_TOTAL_OBJECT_SIZE`), written at create time |
| Over-limit result | `EntityTooLarge`; the object is not published | `EntityTooLarge` on the offending `UploadPart`, and on `CompleteMultipartUpload` if the recorded parts exceed the limit |

## V1: `x-rustfs-max-content-length`

```text
uri = "/photos/avatar.png"
uri += "?x-rustfs-max-content-length=10485760"
presigned_url = sigv4_presign("PUT", uri, credentials)
# Return presigned_url to the browser. Never append the parameter afterwards.
```

- A declared `Content-Length` above the limit is rejected before storage. A body that streams more bytes than the limit is cut off with `EntityTooLarge` and nothing is published.
- Not combinable with archive auto-extraction (`x-amz-meta-snowball-auto-extract`): `InvalidRequest`.
- Unknown-length and SigV4 streaming-chunked uploads stay outside the existing PutObject admission contract; this parameter does not enable them.
- Per request only: it is neither a cumulative cap across several PUTs nor a multipart limit.

## V2: `x-rustfs-max-total-object-size`

```text
uri = "/photos/archive.zip?uploads"
uri += "&x-rustfs-max-total-object-size=104857600"
presigned_url = sigv4_presign("POST", uri, credentials)
# Return presigned_url to the browser. Never append the parameter afterwards.
```

1. RustFS verifies the SigV4 request and persists the limit with the upload ID.
2. The browser uploads parts with the returned upload ID; part requests carry no custom parameter.
3. Each `UploadPart` (and each `UploadPartCopy` into the upload) is admitted only if the upload's running logical total plus this part fits the budget. Replacing an existing part number uses replacement semantics: the old part's size is released before the new size is admitted.
4. `CompleteMultipartUpload` re-sums the recorded parts and rejects the completion if they exceed the limit.

Properties of a capped upload:

- Unknown-length or negative-length parts are rejected with `UnexpectedContent` rather than buffered without a bound.
- Capped parts are admitted under an upload-wide write lock before temporary shards are created, and hold a per-upload staging permit that bounds local in-flight data. The lock is released while the body is read and reacquired for the final check and rename, so `Complete` and `Abort` are not blocked behind a slow upload. The request-body stall timeout releases the staging permit when a client stops sending.
- An upload created without the parameter stays unlimited.
- Enforcement runs in the multipart data plane on every node. During a rolling upgrade, route capped uploads only to nodes that carry the V2 implementation; a node without it treats the internal metadata as unknown and cannot enforce the limit.
