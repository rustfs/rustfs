# Server-Side Encryption Integration

RustFS implements Amazon S3-compatible server-side encryption semantics. This document outlines how each mode maps to KMS operations and how clients should format requests.

## Supported Modes

| Mode | Request Headers | Managed by | Notes |
|------|-----------------|------------|-------|
| `SSE-S3` | `x-amz-server-side-encryption: AES256` | RustFS KMS using the configured default key. | Simplest option; clients do not manage keys. |
| `SSE-KMS` | `x-amz-server-side-encryption: aws:kms`<br>`x-amz-server-side-encryption-aws-kms-key-id: <key-id>` (optional) | RustFS KMS + backend (Vault/Local). | Specify a key-id to override the default. |
| `SSE-C` | `x-amz-server-side-encryption-customer-algorithm: AES256`<br>`x-amz-server-side-encryption-customer-key: <Base64 key>`<br>`x-amz-server-side-encryption-customer-key-MD5: <Base64 MD5>` | Customer provided | RustFS never stores the plaintext key; clients must supply it on every request. |

## Request Examples

### SSE-S3 Upload & Download

```bash
# Upload
aws s3api put-object \
  --endpoint-url http://localhost:9000 \
  --bucket demo --key obj.txt --body file.txt \
  --server-side-encryption AES256

# Download
aws s3api get-object \
  --endpoint-url http://localhost:9000 \
  --bucket demo --key obj.txt out.txt
```

### SSE-KMS with Explicit Key ID

```bash
aws s3api put-object \
  --endpoint-url http://localhost:9000 \
  --bucket demo --key report.csv --body report.csv \
  --server-side-encryption aws:kms \
  --ssekms-key-id rotation-2024-09
```

If `--ssekms-key-id` is omitted, RustFS uses the configured `default_key_id`.

### SSE-C Multipart Upload

SSE-C requires additional care:

1. Generate a 256-bit key and compute its MD5 digest.
2. For multipart uploads, every request (initiate, upload-part, complete, GET) must include the SSE-C headers.
3. Keep part sizes ≥ 5 MiB to avoid falling back to inline storage which complicates key handling.

```bash
KEY="01234567890123456789012345678901"
KEY_B64=$(echo -n "$KEY" | base64)
KEY_MD5=$(echo -n "$KEY" | md5 | awk '{print $1}')

aws s3api create-multipart-upload \
  --endpoint-url http://localhost:9000 \
  --bucket demo --key video.mp4 \
  --server-side-encryption-customer-algorithm AES256 \
  --server-side-encryption-customer-key "$KEY_B64" \
  --server-side-encryption-customer-key-MD5 "$KEY_MD5"
# Upload all parts with the same trio of headers
```

On download, supply the same headers; otherwise the request fails with `AccessDenied`.

## Response Headers

| Header | SSE-S3 | SSE-KMS | SSE-C |
|--------|--------|---------|-------|
| `x-amz-server-side-encryption` | `AES256` | `aws:kms` | _absent_ |
| `x-amz-server-side-encryption-aws-kms-key-id` | _default key id_ | Provided key id | _absent_ |
| `x-amz-server-side-encryption-customer-algorithm` | _absent_ | _absent_ | `AES256` |
| `x-amz-server-side-encryption-customer-key-MD5` | _absent_ | _absent_ | MD5 of supplied key |

## Error Scenarios

| Scenario | Error | Resolution |
|----------|-------|------------|
| SSE-C key/MD5 mismatch | `AccessDenied` | Regenerate the MD5 digest, ensure Base64 encoding is correct. |
| Missing SSE-C headers on GET | `InvalidRequest` | Provide the same `sse-c` headers used during upload. |
| Invalid key id for SSE-KMS | `NotFound` | Call `GET /kms/keys` to retrieve the valid IDs or create one via the admin API. |
| KMS backend offline | `InternalError` | Check `/kms/status`, restart or reconfigure the backend. |

## Best Practices

- Always use HTTPS endpoints when supplying SSE-C headers.
- Log the key-id used for SSE-KMS uploads to simplify forensic analysis.
- For compliance workloads, disable cache or lower cache TTL via `/kms/reconfigure` so data keys are short-lived.
- Test multipart SSE-C flows regularly; the e2e suite (`test_comprehensive_kms_full_workflow`) covers this scenario.

For the administrative API and configuration specifics, refer to [http-api.md](http-api.md) and [configuration.md](configuration.md).
