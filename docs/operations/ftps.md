# FTPS uploads

FTPS `STOR` buffers at most one 16 MiB payload chunk per active upload in the
protocol driver. It waits for the storage backend to consume each chunk before
reading the next. Backend, TLS, connection, and allocator overhead are additional;
this is not a process-wide memory limit. Concurrent uploads each have their own
buffer.

Files smaller than 16 MiB, including empty files, use `PutObject`. Files at or
above that threshold use sequential S3 multipart uploads, so they can exceed the
5 GiB single-`PutObject` limit. Multipart ETags differ from single-PUT ETags and
must not be interpreted as a whole-file MD5 checksum. An upload is successful only
after the multipart completion succeeds. The fixed part size and S3's 10,000-part
limit allow up to 156.25 GiB per FTPS upload; a larger input fails and cleanup is
attempted. Resuming or appending with a nonzero offset remains unsupported.

Each write operation requires `s3:PutObject`. Failed or cancelled transfers also
attempt `s3:AbortMultipartUpload` using the authenticated user's permissions;
cleanup does not bypass IAM. Grant that permission on the upload prefix to allow
immediate cleanup. Cancellation cleanup has a bounded task count and timeout.
Configure an `AbortIncompleteMultipartUpload` bucket lifecycle rule as a fallback
for denied/failed cleanup, process crashes, or losing the upload ID while upload
initiation is in flight. Before completion, received parts do not replace an existing completed object.
A lost or failed completion response may have an ambiguous outcome; clients
should verify the destination before retrying.
