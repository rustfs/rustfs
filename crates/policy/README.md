[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Policy - Policy Engine

<p align="center">
  <strong>Advanced policy engine and access control system for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Policy** provides advanced policy engine and access control capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- AWS-compatible bucket policy engine
- Fine-grained resource-based access control
- Condition-based policy evaluation
- Policy validation and syntax checking
- Role-based access control integration
- Dynamic policy evaluation with context

## Stored bucket tags in OPA input

When the OPA authorization plugin is enabled, IAM resolves the addressed bucket's
stored tags before sending its decision request. Tags appear in the existing
conditions map; no additional S3 calls or credentials are needed by OPA:

```json
{
  "input": {
    "resource": { "bucket": "financial-reports", "object": "annual/report.parquet" },
    "action": "s3:GetObject",
    "context": {
      "conditions": {
        "ExistingBucketTag/department": ["finance"],
        "ExistingBucketTag/environment": ["production"]
      }
    }
  }
}
```

The example omits unchanged identity and context fields. Tag names and values
are case-sensitive. Values use single-element string arrays, including `[""]`
for an empty tag value. A simple resource predicate is:

```rego
package rustfs.example
import rego.v1

finance_bucket if {
    input.context.conditions["ExistingBucketTag/department"] == ["finance"]
}
```

Combine this predicate with the policy's identity and action checks; tags do not
establish caller roles or grant access by themselves. Restrict `PutBucketTagging`
and `DeleteBucketTagging` separately when tags control access.

- Only server-resolved, stored tags populate `ExistingBucketTag/<key>`. Incoming
  conditions in that namespace (including case variants and the `s3:` alias)
  are discarded before enrichment. Headers, claims, requested replacement tags
  and object tags cannot override it.
- A confirmed missing bucket, or a bucket with no stored tags, contributes no
  bucket-tag conditions. New bucket creation therefore has no existing tags;
  a create request naming an existing bucket uses its stored tags. Bucketless
  requests, including STS and the global ListBuckets check, have no bucket tags.
- Bucket metadata lookup failures, unreadable tag XML, and ambiguous/incomplete
  tags fail closed, even if a separate bucket policy would allow the request.
  This applies to requests evaluated by OPA even if the policy does not inspect tags.
  Boolean IAM callers deny on a lookup error; S3 access checks propagate it.
- Object reads/writes, versions, listing and multipart checks use their addressed
  bucket. CopyObject and UploadPartCopy authorize source and destination with
  each bucket's own tags. Tag replacement/removal uses the **existing** stored state.
- ListBuckets retains its existing contract: a global `s3:ListAllMyBuckets` allow
  lists all buckets. Otherwise, per-bucket `s3:ListBucket`/`s3:GetBucketLocation`
  checks carry each candidate's tags and can filter the result.
- Resolution reuses the IAM storage instance's authoritative metadata reader and
  existing metadata cache/reload machinery, not a separate OPA cache. Local tag
  updates/removal update the local cache, and tag mutations wait for healthy-peer
  reload attempts before responding. Failed peer notification can leave that
  peer's cached tags stale until a later reload or distributed metadata refresh
  (normally every 15 minutes). This adds no stronger consistency barrier or
  partition-time revocation guarantee, and does not revoke in-flight requests.

This is an OPA input extension, not a new native IAM condition key. Native policy
evaluation, owner/anonymous handling, and existing authorization combination rules
remain unchanged. For S3 authorization, an OPA `false` decision is an implicit IAM
denial that an applicable bucket-policy Allow may supplement. A tag mismatch alone
therefore does not revoke access granted by such a bucket policy; policy authors
must account for those grants when relying on tags to restrict access. ListBuckets
filtering remains IAM-only. Lookup errors on the OPA path abort authorization and
never fall through to a bucket-policy Allow. Custom IAM `Store` implementations
must implement `load_bucket_tags` to support bucket-scoped OPA requests; the default
fails closed.

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
