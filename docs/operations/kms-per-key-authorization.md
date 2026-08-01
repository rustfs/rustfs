# Per-key KMS authorization

RustFS authorizes KMS access with identity policies. A statement may name the keys it applies to, so a grant such as `kms:DisableKey` no longer implies every key in the cluster, and an SSE-KMS request is checked against the key it actually resolves to.

This page covers the resource grammar, the built-in role templates, the two enforcement planes, and the migration path. For where master key material lives per backend, see [KMS backend security properties](kms-backend-security.md).

## Resource grammar

KMS resources use the same empty-account ARN form as S3 resources:

| Pattern | Matches |
| --- | --- |
| `arn:aws:kms:::key/<key_id>` | exactly that key |
| `arn:aws:kms:::key/app-*` | every key whose id starts with `app-` |
| `arn:aws:kms:::*` | every key |
| `arn:aws:kms:::alias/<name>` | reserved; matches nothing until alias resolution ships |

Rules worth knowing before writing a policy:

- A statement mixing `kms:` actions with `s3:` actions is rejected. Put KMS grants in their own statement.
- A statement carrying a KMS resource with non-KMS actions is rejected.
- A KMS statement with **no** resource matches every key. This is the legacy form and stays valid, so existing policies keep their current effect.
- A KMS statement whose resources are S3 ARNs predates KMS resources. It is still loadable, but the S3 patterns never constrained key access, so evaluation ignores them, treats the statement as unscoped, and logs a warning. Rewrite these with real KMS resources.
- Bucket policies reject `kms:` statements outright. KMS grants belong to identities.
- `Deny` wins, as everywhere else: a `Deny` on `arn:aws:kms:::key/payroll-*` overrides an `Allow` on `arn:aws:kms:::*`.
- The key identifier matched is the one the request names, before any alias resolution.

## Built-in role templates

Three canned policies ship alongside `readwrite`, `readonly`, `writeonly`, `diagnostics` and `consoleAdmin`:

| Policy | Grants | Intended holder |
| --- | --- | --- |
| `KMSKeyAdministrator` | `kms:DescribeKey`, `kms:ListKeys`, `kms:EnableKey`, `kms:DisableKey`, `kms:RotateKey`, `kms:DeleteKey` | the team that governs key lifecycle |
| `KMSKeyUser` | `kms:GenerateDataKey`, `kms:Decrypt`, `kms:DescribeKey` | workloads that read and write SSE-KMS objects |
| `KMSAuditor` | `kms:DescribeKey`, `kms:ListKeys` | reviewers who need visibility and nothing else |

They express separation of duties: `KMSKeyAdministrator` can disable or delete a key but can never encrypt or decrypt with it, and `KMSKeyUser` can use a key but can never change its state.

None of the three grants `kms:Configure`, `kms:ServiceControl`, `kms:ClearCache`, `kms:Backup` or `kms:Restore`. Those act on the KMS service or on the material of every key at once, which is a cluster-administration power rather than a key-management one; they stay with `consoleAdmin`. Key creation currently shares `kms:Configure` with backend configuration, so creating keys is a `consoleAdmin` operation too.

The templates carry no S3 or admin grants, so combine one with a data-plane policy. Policy mappings accept a comma-separated list:

```
PUT /rustfs/admin/v3/set-user-or-group-policy?policyName=readwrite,KMSKeyUser&userOrGroup=app-writer&isGroup=false
```

They are also unscoped (`arn:aws:kms:::*`) so they behave like the other canned policies. For least privilege, copy one and narrow it per workload:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::reports", "arn:aws:s3:::reports/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
      "Resource": ["arn:aws:kms:::key/reports-*"]
    }
  ]
}
```

## Enforcement planes

### Admin endpoints

Every `/rustfs/admin/v3/kms/...` endpoint that names a key authorizes against that key. The identifier is taken from the request body's `key_id`, falling back to the `keyId` query parameter. An endpoint that names no key (for example `GET /rustfs/admin/v3/kms/keys`) matches any KMS resource in the policy, as before.

This plane is always on. It needs no migration: a policy that never mentioned a KMS resource keeps matching every key.

### SSE-KMS data path

An SSE-KMS write additionally requires `kms:GenerateDataKey`, and an SSE-KMS read additionally requires `kms:Decrypt`, both evaluated as the requesting identity against the key the request resolves to. This closes the confused-deputy gap where the server called KMS with its own authority on behalf of any caller holding `s3:PutObject`.

Scope and exemptions:

- **SSE-KMS only.** SSE-S3 wraps its data key with a server-owned key the caller never names, and SSE-C never reaches KMS; both are exempt, matching AWS.
- **The resolved key**, not the header. A bucket default encryption rule naming a KMS key is authorized the same way an explicit `x-amz-server-side-encryption-aws-kms-key-id` header is.
- **Anonymous requests are exempt.** They have no identity policy to evaluate, and denying them would break public buckets holding SSE-KMS objects. They remain governed by bucket policy.
- **Internal work is exempt.** Replication, lifecycle transitions, healing and the scanner run as the system principal.
- **Authorization runs before key state is checked**, so a denial cannot be used to probe whether a key exists, is disabled, or is pending deletion. The response is always `AccessDenied`.
- **Multipart uploads are authorized at create time**, where the session data key is generated. Part uploads and completion reuse that envelope and are not re-authorized against the destination key.
- **Copies are checked on both ends**: `kms:Decrypt` on the source object's key and `kms:GenerateDataKey` on the destination key. `UploadPartCopy` authorizes its source read the same way.

## Migration

Data-path enforcement is **off by default in this release** because it changes the outcome of requests that succeed today: an identity holding only `s3:PutObject` can currently encrypt under any key. Turning it on without preparing policies will produce `AccessDenied` on working workloads.

```bash
RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true
```

The server logs the configured mode once at startup, and warns while enforcement is off. A later release defaults it to enabled.

Recommended sequence:

1. Leave enforcement off. Inventory which identities read and write SSE-KMS objects and under which keys — the KMS fields on S3 audit entries (`sseType`, `kmsKeyId`, `kmsOutcome`) report the key each request used.
2. Attach `KMSKeyUser`, or a narrowed copy, to those identities. This is a no-op while enforcement is off, so it can be rolled out ahead of time.
3. Enable enforcement on one node and confirm the workloads still succeed.
4. Roll it out cluster-wide.

If a workload starts failing with `AccessDenied` on an SSE-KMS object after step 3, the identity is missing `kms:GenerateDataKey` (writes) or `kms:Decrypt` (reads) on the key named in the audit entry.
