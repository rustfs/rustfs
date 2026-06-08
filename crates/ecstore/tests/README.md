# ecstore Integration Tests

## MinIO-generated encrypted fixtures

`minio_generated_read_test.rs` validates the `bitrot -> GetObjectReader` path against raw MinIO backend data captured by
`.\rustfs\scripts\minio_fixture_lab\lab.py`.

It currently covers multipart fixtures for:

- `sse-s3-multipart-8m`
- `sse-kms-multipart-8m`

Required environment variables:

- `RUSTFS_MINIO_FIXTURE_ROOT`
- `RUSTFS_MINIO_STATIC_KMS_KEY_B64`

Example:

```powershell
$env:RUSTFS_MINIO_FIXTURE_ROOT = '.\rustfs\tmp\minio-fixture-lab-local-key'
$env:RUSTFS_MINIO_STATIC_KMS_KEY_B64 = '<base64-32-byte-local-minio-kms-key>'
cargo +1.95.0 test -p rustfs-ecstore --features rio-v2 --test minio_generated_read_test -- --ignored
```
