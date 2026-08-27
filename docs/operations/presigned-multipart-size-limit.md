# Presigned multipart total-size limit

RustFS V2 supports an optional capability on a signed or SigV4-presigned
`CreateMultipartUpload` request:

```text
x-rustfs-max-total-object-size=<unsigned 64-bit integer>
```

The backend must include the parameter before calculating the SigV4
signature. It is part of the canonical query and cannot be added, removed, or
changed by the browser. RustFS stores the verified limit in the multipart
upload session and applies it to every `UploadPart` and to
`CompleteMultipartUpload`.

Backend pseudocode (the custom query must be present before signing):

```text
uri = "/photos/archive.zip?uploads"
uri += "&x-rustfs-max-total-object-size=104857600"
presigned_url = sigv4_presign("POST", uri, credentials)
# Return presigned_url to the browser. Never append the parameter afterwards.
```

The resulting flow is:

1. The backend signs `CreateMultipartUpload?...&x-rustfs-max-total-object-size=104857600`.
2. RustFS verifies the SigV4 request and persists the limit with the upload ID.
3. The browser uploads parts using the returned upload ID.
4. RustFS rejects a part whose declared logical size would exceed the remaining
   budget and rejects completion if the server-side part metadata exceeds the
   limit.

The limit is measured in logical object bytes (`actual_size`), not erasure,
encryption, or compression bytes. Replacing an existing part uses replacement
semantics: the old part size is removed before the new part size is admitted.
Unknown-length parts are rejected for capped sessions rather than buffered
without a bound. Capped parts are admitted under an upload-wide write lock
before temporary shards are created and use a per-upload staging permit to
bound local in-flight data. The distributed lock is released while the body is
read and reacquired for the final check/rename, so Complete and Abort are not
blocked behind a slow upload. The normal request-body stall timeout releases
the staging permit when a client stops sending.

The parameter is accepted only on `CreateMultipartUpload`. Supplying it on
`UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, listing, or
copy operations returns `InvalidRequest`; those requests use the persisted
session state. A multipart upload created without this parameter remains
unlimited for backward compatibility. The V1 single-request capability
(`x-rustfs-max-content-length`) is independent and is not a multipart limit.

Because enforcement happens in the multipart data plane, every node that may
receive requests for a capped upload must run the V2 implementation. During a
rolling upgrade, route capped uploads only to upgraded nodes; older nodes treat
the internal metadata as unknown and cannot enforce the limit.
