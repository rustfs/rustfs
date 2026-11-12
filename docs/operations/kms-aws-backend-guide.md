# AWS KMS backend: setup, verification, and operations

**Use this when:** connecting RustFS to AWS KMS for the first time, verifying an existing AWS-backed deployment, upgrading one from 1.0.4 or 1.0.5, or deciding how to run and protect the AWS keys RustFS depends on.
**Source of truth:** `crates/kms/src/backends/aws.rs` (backend behavior and its deviations), `crates/kms/src/config.rs` (`ENV_KMS_AWS_*`), `crates/kms/src/policy.rs` (retry, deadline and circuit breaker), `rustfs/src/storage/sse.rs` (SSE data path). 中文版: [kms-aws-backend-guide.zh-CN.md](kms-aws-backend-guide.zh-CN.md).

The statements about behavior on this page were observed against the real AWS KMS service (region `eu-north-1`) and, for failures AWS cannot be asked to produce on demand, against a loopback stand-in that speaks the AWS KMS wire protocol. Where a statement is guidance that was not exercised, it says so. For the contract-level differences between AWS and the other backends, see [KMS backend security properties](kms-backend-security.md#aws-kms-deviations-from-the-shared-backend-contract).

## What RustFS does with AWS KMS

RustFS uses envelope encryption. For each encrypted object it asks AWS KMS for a data key (`GenerateDataKey`), encrypts the object with the plaintext data key, and stores only the wrapped data key next to the object. To read the object it sends the wrapped key back (`Decrypt`). The KMS key itself never leaves AWS, and RustFS never stores AWS credentials.

Consequences that shape everything below:

- **AWS KMS is on the read path.** If the key is disabled, scheduled for deletion, deleted, or AWS is unreachable, encrypted objects cannot be read. Nothing stored in RustFS can substitute for the key.
- **Every encrypted write and every encrypted read is one AWS request.** Multipart uploads add one `Decrypt` per uploaded part. Plan request volume and cost accordingly.
- **Both `aws:kms` and `AES256` objects use the key.** With a KMS backend configured, SSE-S3 (`AES256`) wraps its data keys under the configured default key, so SSE-S3 objects depend on AWS exactly as SSE-KMS objects do.

## Before you start

| Item | Requirement |
| --- | --- |
| RustFS release | One that contains the AWS read-path fix. Releases 1.0.4 and 1.0.5 can write but not read AWS-backed objects; see [Upgrading from 1.0.4 or 1.0.5](#upgrading-from-104-or-105). |
| KMS key | Symmetric, usage "Encrypt and decrypt", in the region RustFS will be configured with. |
| Network | Every RustFS node must reach `kms.<region>.amazonaws.com` over HTTPS (or a VPC interface endpoint for KMS). The AWS SDK inside RustFS does not use `HTTP_PROXY` / `HTTPS_PROXY`. |
| Identity | One IAM identity per deployment that every node can obtain credentials for. |
| Clock | Node clocks in sync; AWS rejects requests whose signature time is off by more than a few minutes. |

## Step 1: create the KMS key

In the AWS console: **KMS → Customer managed keys → Create key**.

1. Key type **Symmetric**, key usage **Encrypt and decrypt**. Leave "Advanced options" at their defaults (key material origin KMS, single-Region) unless you have decided on multi-Region keys for disaster recovery.
2. Alias and description: for humans only. RustFS addresses the key by its **key ID or ARN**, never by alias.
3. **Key administrators**: select only the human role that will manage this key (disable, rotate, schedule deletion). Do not select AWS service-linked roles (`AWSServiceRoleFor…`) or organization-managed roles. Keep "Allow key administrators to delete this key" only if those administrators should hold that power.
4. **Key users**: select the IAM identity RustFS runs as (created in step 2; you can come back and add it). This grants `kms:Encrypt`, `kms:Decrypt`, `kms:ReEncrypt*`, `kms:GenerateDataKey*` and `kms:DescribeKey` on this key through the key policy.
5. Finish, then record the **region** and the **key ARN**.

Administrators and users should be different identities. The identity RustFS runs as needs to use the key, not to manage it.

## Step 2: create the identity RustFS runs as

Prefer short-lived role credentials over long-lived access keys:

| Where RustFS runs | Recommended identity |
| --- | --- |
| EC2 | An instance profile role |
| EKS | IAM Roles for Service Accounts (IRSA) or EKS Pod Identity |
| ECS | A task role |
| Outside AWS | A dedicated IAM user with an access key, rotated on a schedule, or IAM Roles Anywhere |

RustFS resolves credentials through the standard AWS provider chain: environment variables, the shared profile (`AWS_PROFILE`, `~/.aws/credentials`), then container and instance metadata. It never persists them, and the KMS admin API rejects credential fields in a configuration body.

Attach this identity policy. `kms:ListKeys` cannot be granted by a key policy because it is not scoped to one key, and RustFS calls it as its startup health check; without it the KMS backend does not start.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RustfsKmsHealthCheck",
      "Effect": "Allow",
      "Action": "kms:ListKeys",
      "Resource": "*"
    }
  ]
}
```

If you would rather not rely on the console's "key users" list, grant the data-path actions in the same identity policy, scoped to the key:

```json
{
  "Sid": "RustfsKmsDataPath",
  "Effect": "Allow",
  "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
  "Resource": "arn:aws:kms:<region>:<account-id>:key/<key-id>"
}
```

Only if operators will manage AWS keys **through the RustFS admin API** does the identity also need the management actions below. Most deployments should leave these out and manage keys in AWS directly, so that a compromised RustFS node cannot disable or delete keys.

| RustFS admin operation | AWS action |
| --- | --- |
| Create key | `kms:CreateKey` |
| Enable / disable | `kms:EnableKey`, `kms:DisableKey` |
| Rotate | `kms:RotateKeyOnDemand` |
| Schedule / cancel deletion | `kms:ScheduleKeyDeletion`, `kms:CancelKeyDeletion` |

## Step 3: configure RustFS

Set these on every node and restart:

```bash
RUSTFS_KMS_ENABLE=true
RUSTFS_KMS_BACKEND=aws
RUSTFS_KMS_AWS_REGION=eu-north-1
RUSTFS_KMS_DEFAULT_KEY_ID=arn:aws:kms:eu-north-1:<account-id>:key/<key-id>
```

Notes:

- `RUSTFS_KMS_DEFAULT_KEY_ID` is the key used when a request asks for encryption without naming a key, and the key that wraps every SSE-S3 data key. The key ID and the ARN are both accepted.
- Do **not** set `RUSTFS_SSE_S3_MASTER_KEY`. It configures the no-KMS fallback and has no role when a KMS backend is active.
- `RUSTFS_KMS_AWS_ENDPOINT_URL` is only for emulators and non-default endpoints. A plain `http://` endpoint is refused unless `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true`, which is for development only. With a VPC interface endpoint and private DNS enabled, the default regional name already resolves to the endpoint and nothing needs to be set.
- The same configuration can be applied at runtime through the admin API (`POST /rustfs/admin/v3/kms/configure`, body `{"backend_type": "AWS", "region": "...", "default_key_id": "..."}`). Credentials still come from each node's own environment.

Startup is fail-closed. If the health check cannot reach AWS, or the endpoint is plain HTTP without the development switch, the RustFS process exits during startup instead of serving without encryption.

## Step 4: verify

Write, inspect and read back one object. With any S3 client:

```bash
aws s3api put-object --endpoint-url https://rustfs.example.com --bucket demo --key kms-check \
  --body ./sample.bin --server-side-encryption aws:kms
aws s3api head-object --endpoint-url https://rustfs.example.com --bucket demo --key kms-check
aws s3api get-object  --endpoint-url https://rustfs.example.com --bucket demo --key kms-check ./readback.bin
cmp ./sample.bin ./readback.bin
```

Expected: all three calls succeed, `head-object` reports `ServerSideEncryption: aws:kms` and the key you configured, and the read-back bytes are identical.

Then confirm the round trip in AWS, which proves RustFS really used the key rather than a local fallback: in **CloudTrail → Event history**, filter by event source `kms.amazonaws.com`. A write produces `GenerateDataKey`; a read produces `Decrypt`. Each event carries the encryption context RustFS sent, whose entry maps the bucket name to `bucket/object-key`, so CloudTrail shows which object each KMS call served. **A read that returns data without a matching `Decrypt` event, or an error with no `Decrypt` attempt, means the read never reached AWS.**

Check the service state:

```bash
GET /rustfs/admin/v3/kms/status
```

`backend_status` should be `healthy`, and `capabilities` should report `rotate`, `enable_disable` and `schedule_deletion` true with `versioning`, `physical_delete` and `rewrap` false.

## Behavior reference

Observed on the AWS backend. Status codes are what an S3 client receives.

### Key identifiers

| Situation | Result |
| --- | --- |
| Key named by ID or by ARN in `x-amz-server-side-encryption-aws-kms-key-id` | Accepted. The response and later `HEAD`/`GET` echo the same spelling the write used. |
| Key that does not exist, or an ARN in another region | `400 KMS.NotFoundException`; no object is created. |
| Create a key through RustFS with a chosen name (including the `mc`-compatible `create-key?key-id=`) | `501 NotImplemented`. AWS assigns key IDs; create without a name and use the returned ID. |
| Describe a key by ARN through `GET /rustfs/admin/v3/kms/keys/{key_id}` | Works when the ARN is percent-encoded into the path segment; the `describe-key?keyId=` form takes it as a query value. |

### Pairing with a Vault site under site replication

**Delivery requirement.** Site replication synchronizes key names verbatim and never synchronizes keys, so every site must hold a key of the same name ([details](site-replication-operations.md)). AWS assigns key IDs and this backend does not manage aliases, so in a Vault and AWS pair the Vault site must follow the AWS names:

1. Create the key in AWS first and note its key ID.
2. On each Vault site (`vault-transit` or `vault-kv2`), create a key whose name is that bare key ID, for example `74e5921e-107a-4584-bfda-a5dadeb3e13e`.
3. Set `RUSTFS_KMS_DEFAULT_KEY_ID` to the same bare key ID on every site, the AWS site included. Do not configure the AWS site's default key as an ARN: the configured spelling is what gets synchronized, and only the bare-ID form was verified to be usable as a Vault key name.
4. Repeat steps 1 and 2 for every additional key that a bucket default or an IAM policy will name.

Observed with this layout on `vault-transit` + AWS and `vault-kv2` + AWS: bucket defaults naming a key, and `aws:kms` defaults naming none, work for writes on both sites and replicate in both directions, with each replica sealed by the receiving site's own KMS. Without the alignment the receiving site answers `400 KMS.NotFoundException` and replication of the affected objects stays `FAILED`.

### Key state

| AWS key state | `PUT` (encrypt) | `GET` / range / copy source (decrypt) | `HEAD` |
| --- | --- | --- | --- |
| Enabled | 200 | 200 | 200 |
| Disabled | `400 InvalidRequest` (`DisabledException`) | `400 InvalidRequest` (`DisabledException`) | 200 |
| Pending deletion | `400 InvalidRequest` (`KMSInvalidStateException`) | `400 InvalidRequest` (`KMSInvalidStateException`) | 200 |
| Deleted | `400 KMS.NotFoundException` | fails; the data is unrecoverable | 200 |

This is the sharpest difference from the RustFS-managed backends, which keep reads working under a disabled key. On AWS, **disabling a key takes every object under it offline until the key is enabled again**; re-enabling restores access with no change on the RustFS side. AWS key state changes can take a few seconds to be visible to all requests.

Cancelling a scheduled deletion leaves the key **Disabled** in AWS. Enable it explicitly before expecting reads or writes to work.

### Operations the AWS backend does not offer

| Operation | Result | Why |
| --- | --- | --- |
| Immediate deletion (`force_immediate`) | `501` | AWS only offers a 7–30 day waiting period. |
| Tag, untag, update description | `501` | Not implemented for this backend; manage tags in AWS. |
| Bulk rekey / rewrap | refused | AWS key versions are opaque; AWS decrypts with the right backing key automatically. |
| KMS backup export | not applicable | The key material lives in AWS and cannot be exported. |
| SSE-S3 key auto-creation, synthetic KMS probe | unavailable | Both address a key by a name RustFS chooses. |

### When AWS misbehaves

| Condition | Client sees | Notes |
| --- | --- | --- |
| A single throttled call | 200 | Retried transparently. |
| Sustained throttling, AWS `5xx`, or the endpoint unreachable | `503 ServiceUnavailable` within a fraction of a second | Retryable. No partial object is left behind by a refused write. |
| AWS answers `AccessDeniedException` | `403 AccessDenied` | Usually a missing IAM or key-policy grant. |
| During any of the above | Unencrypted objects and `HEAD` on encrypted objects keep working | Only operations that need the key fail. |
| After AWS recovers | Encrypted reads and writes resume **without a restart**, about 30 seconds later | The backend circuit breaker stays open for 30 seconds before it probes again. |

### Per-key authorization

RustFS can check `kms:GenerateDataKey` / `kms:Decrypt` against RustFS identity policies per key (`RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true`; see [Per-key KMS authorization](kms-per-key-authorization.md)). Write policy resources with the **key ID** (`arn:aws:kms:::key/<key-id>`). A request, or a stored object, may name the key by its ID or by its ARN; both are evaluated as the same key, so an `Allow` or `Deny` on the key ID holds for either spelling. A request that names a key by alias is treated as possibly any key: every key-scoped `Deny` applies to it and no key-scoped `Allow` does, so use the key ID or ARN in requests wherever key-scoped policies are in force. AWS-side controls (key policy, IAM, encryption-context conditions) remain the outer boundary that holds even if a RustFS node is compromised.

## Upgrading from 1.0.4 or 1.0.5

Releases 1.0.4 and 1.0.5 store the AWS data key in a form their own read path does not recognize. On those releases, with the AWS backend:

- `PUT` succeeds and `HEAD` reports `aws:kms`, but `GET` fails with `400 InvalidRequest` and a message asking for `RUSTFS_SSE_S3_MASTER_KEY`.
- `UploadPart` of an SSE-KMS multipart upload fails the same way.
- CloudTrail shows `GenerateDataKey` for the write and no `Decrypt` for the read.

Setting `RUSTFS_SSE_S3_MASTER_KEY` does not help and should not be done.

The objects those releases wrote are intact. After upgrading to a fixed release, on the same data directories and with the same key and credentials:

- Existing objects read back unchanged, including range reads. No migration, rewrite or re-upload is needed.
- A server-side copy of such an object onto itself rewrites it in the current format; this is optional.
- New objects are written in the current format. **A release older than the fix cannot read them**, so do not roll back after writing with the fixed release; the older releases could not read their own objects either.

Verify an upgrade by reading one object that was written before it and checking for the `Decrypt` event in CloudTrail.

## Operating AWS KMS safely

The key is the data. Every recommendation here follows from that.

### Protect the key from deletion and disablement

- **Deleting the KMS key destroys every object encrypted under it.** There is no recovery path in RustFS or in AWS once the waiting period ends. Treat `kms:ScheduleKeyDeletion` as you would treat permission to delete all data.
- Use the longest waiting period (30 days) and alert on the request, not the deletion: create an EventBridge rule for the CloudTrail events `ScheduleKeyDeletion` and `DisableKey` on your keys and route it to the on-call channel. The waiting period only helps if someone notices.
- Deny `kms:ScheduleKeyDeletion` and `kms:DisableKey` for everyone except a small break-glass role, with an organization SCP or an explicit `Deny` in the key policy. The identity RustFS runs as should never hold either.
- Before retiring a key, prove nothing depends on it: disable it first and watch for `400 InvalidRequest` (`DisabledException`) on reads for at least one full business cycle. Disabling is reversible; deletion is not.
- RustFS reports which bucket default-encryption configurations reference a key when you schedule its deletion through the RustFS admin API. That report covers configuration, **not objects**: an empty report does not mean no object uses the key.

### Least privilege and separation of duties

- Three roles, three identities: key administrators (manage the key, cannot use it), the RustFS service identity (uses the key, cannot manage it), and auditors (read CloudTrail and key metadata).
- Scope the service identity to the specific key ARNs. Avoid `kms:*` and avoid `Resource: "*"` for anything except `kms:ListKeys`.
- Do not use the account root user or a human administrator's credentials for the service.
- Add an encryption-context condition where a key should serve only certain buckets. RustFS sends the bucket name as an encryption-context key, so `kms:EncryptionContextKeys` can restrict a key to named buckets even if the service identity is compromised. This was not exercised in the test run behind this page; validate it in a non-production account first.
- Keep AWS keys per environment, and consider per tenant or per data class. A separate key gives a separate blast radius and a separate off switch.

### Credentials

- Prefer instance, pod or task roles. They rotate automatically and leave no secret on disk.
- If a static access key is unavoidable: one key per deployment, stored with file mode `0600` or in a secret manager, rotated on a schedule, and never placed in RustFS configuration, images or version control.
- Every node needs the same permissions and region. A node with missing permissions fails its own requests while the others succeed, which looks like intermittent errors.

### Rotation

- AWS automatic key rotation is transparent to RustFS: AWS keeps every earlier backing key and decrypts old data keys with the right one. RustFS neither enables nor reports on it; turn it on in AWS.
- On-demand rotation through RustFS calls `RotateKeyOnDemand`. AWS limits how many on-demand rotations a key may have and bills beyond a free allowance; check the current AWS limits before automating it.
- Rotation protects future data keys. It does not re-encrypt existing data keys or objects, and RustFS bulk rekey is not available on this backend. If a key is suspected to be compromised, the response is a new key plus rewriting the affected objects, not rotation.

### Availability and disaster recovery

- AWS KMS regional availability is now part of your storage availability. During an AWS KMS outage, encrypted reads and writes return `503` and resume about 30 seconds after AWS recovers, without a restart.
- Use a VPC interface endpoint for KMS when RustFS runs in a VPC: it removes the dependency on internet egress and lets an endpoint policy restrict which keys and principals may be used from the network.
- For a disaster-recovery site in another region, AWS multi-Region keys let the replica key decrypt data keys wrapped by the primary. This was not exercised in the test run behind this page; test a full restore in the recovery region before depending on it.
- RustFS replication re-encrypts at the target, so each site uses its own KMS configuration. Losing the source site's key does not affect objects already replicated.

### Monitoring and audit

- Enable CloudTrail in every region you use and retain KMS events. Each `GenerateDataKey` and `Decrypt` records the caller and the encryption context, which names the bucket and object.
- Alert on a sustained rise of `AccessDeniedException`, `DisabledException`, `KMSInvalidStateException` and `ThrottlingException` for your keys.
- On the RustFS side, watch the KMS backend metrics and the circuit breaker gauge `rustfs_kms_backend_circuit_open`; see the [KMS observability runbook](kms-observability-runbook.md).
- Request an AWS KMS request-quota increase before a large migration. Symmetric cryptographic operations share one per-region rate quota across the account, and RustFS issues one request per encrypted object operation.

### Cost

- Each customer managed key has a fixed monthly charge, and each API request is billed. Encrypted object traffic translates one-to-one into KMS requests: one per write, one per read, one more per multipart part.
- Keys pending deletion are not billed. Keys left behind by tests are.

## Troubleshooting

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| `GET` returns `400 InvalidRequest` mentioning `RUSTFS_SSE_S3_MASTER_KEY`, with KMS configured | A release before the AWS read-path fix | Upgrade. Do not set the variable. |
| RustFS exits at startup with the AWS backend | Health check failed: no credentials, `kms:ListKeys` not granted, wrong region, or no route to the endpoint | Check the startup log for the KMS error, then credentials, the identity policy, and network reachability from that node. |
| `400 KMS.NotFoundException` on write | Key ID typo, key in another region or account, or the key was deleted | Compare the key ARN with `RUSTFS_KMS_AWS_REGION` and the account. |
| `403 AccessDenied` on encrypted reads or writes only | The service identity lacks `kms:GenerateDataKey` or `kms:Decrypt` on that key | Fix the key policy or identity policy; no restart needed. |
| `400 InvalidRequest` with `DisabledException` or `KMSInvalidStateException` | The key is disabled or pending deletion | Enable the key (after cancelling deletion if needed). |
| `503 ServiceUnavailable` on encrypted operations | AWS KMS unreachable or throttling | Check AWS health and quotas. Recovery is automatic about 30 seconds after AWS responds again. |
| Encrypted operations fail on one node only | That node's credentials, permissions or network differ | Compare the node's identity and reachability with a working node. |
| `501 NotImplemented` creating a key by name | AWS assigns key IDs | Create without a name, or create the key in AWS and reference its ID. |

## Related documents

- [KMS backend security properties](kms-backend-security.md)
- [KMS admin API contract](kms-admin-contract.md)
- [Per-key KMS authorization](kms-per-key-authorization.md)
- [KMS observability runbook](kms-observability-runbook.md)
