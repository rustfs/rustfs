# Presigned PutObject size limit

RustFS V1 supports an optional, RustFS-specific capability on a SigV4
presigned `PutObject` URL:

```text
x-rustfs-max-content-length=<unsigned 64-bit integer>
```

The backend that creates the URL must add this query parameter to the request
URI before calculating the SigV4 presign. It is part of the canonical query;
adding, removing, or changing it after signing invalidates the signature. A
browser can then upload with a plain `PUT` and does not need a custom size
header.

RustFS validates the capability after SigV4 authentication and enforces it on
the decoded request body. A declared `Content-Length` above the limit is
rejected before storage. If the body produces more bytes than the limit while
streaming, RustFS returns `EntityTooLarge` and does not publish the object.

The V1 contract is deliberately narrow:

- The parameter is accepted only on a SigV4 presigned `PutObject` request.
- Duplicate, case-variant, malformed, negative, or overflowing values return
  `InvalidRequest`.
- Requests without the parameter, including ordinary authenticated or
  anonymous `PUT`, keep the existing behavior.
- The parameter on `CopyObject`, multipart, `GET`, `HEAD`, `DELETE`, bucket, or
  other operations returns `InvalidRequest`.
- Unknown-length and SigV4 streaming-chunked uploads remain unsupported by the
  existing PutObject admission contract and are not enabled by this feature.

This capability is per request; it is not a cumulative multipart-upload cap.
Multipart session limits are planned for V2 under a separate query/API
contract.
