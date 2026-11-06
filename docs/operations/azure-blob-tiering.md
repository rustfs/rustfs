# Azure Blob Remote Tiers

New Azure tiers use the native Azure Blob REST API with storage-account Shared Key authentication. Azure public cloud and Azure Stack Hub use the same driver; Stack Hub requires its own account Blob endpoint. The driver uses service version 2020-10-02, which Microsoft lists for Stack Hub 2301 and later. See [Microsoft's storage compatibility matrix](https://learn.microsoft.com/en-us/azure-stack/user/azure-stack-acs-differences).

The implementation is in [warm_backend_azure_blob.rs](../../crates/ecstore/src/services/tier/warm_backend_azure_blob.rs). Account-key signing is shared with the on-demand migration reader through [request_signature_azure.rs](../../crates/signer/src/request_signature_azure.rs); the two features have different storage-operation and service-version contracts.

## Configuration

Create the destination container first. Reserve its selected prefix for RustFS tier data, and do not modify the generated blobs with another writer.

| Console/API field | Azure meaning |
|---|---|
| Azure Blob | Select the Azure tier provider |
| Access Key / accessKey | Storage account name |
| Secret Key / secretKey | Base64 storage-account key |
| Bucket / bucket | Existing container name |
| Endpoint / endpoint | Account Blob service origin; an empty value derives the public-cloud endpoint |
| Prefix / prefix | Optional prefix reserved for this tier |
| Region / region | Not used by native Blob signing or routing |
| Storage Class / storageClass | Leave empty; the Console placeholder STANDARD is normalized to the default |

For Stack Hub, use the complete account endpoint supplied by its portal, such as https://account.blob.region.example. Do not supply a container path or a SAS query string as part of the endpoint. HTTPS uses the server's trusted CA roots, including the deployment's installed private CA bundle.

The existing Add Tier API accepts this shape:

~~~json
{
  "type": "azure",
  "azure": {
    "name": "AZURE_COLD",
    "endpoint": "https://account.blob.region.example",
    "accessKey": "account",
    "secretKey": "<base64-account-key>",
    "bucket": "archive",
    "prefix": "rustfs"
  }
}
~~~

Only storage-account keys are supported by this tier driver. Service-principal credentials, SAS tokens and explicit Hot/Cool/Archive access-tier selection are not enabled. Stack Hub's own support matrix also differs from public Azure for authentication and access tiers; selecting Azure Blob does not promise every Azure feature.

## Upload, Read, and Cleanup

The driver writes Block Blobs. Small objects use Put Blob; larger objects use bounded Put Block requests followed by one conditional Put Block List. Each block has Content-MD5 and a unique upload-specific ID. The complete declared input must be consumed before commit, and the driver does not automatically replay failed requests. The Stack Hub limit of 50,000 blocks with at most 100 MiB per block bounds the accepted object size.

For a container with Blob versioning enabled, Azure returns timestamp version IDs rather than UUIDs. Before transitioning objects into such a container, upgrade every participating reader, writer and recovery worker, enable both `RUSTFS_TIER_REMOTE_VERSION_STATE_WRITE=true` and `RUSTFS_TIER_REMOTE_VERSION_STATE_FLEET_CONFIRMED=true` on the serving fleet, and ensure its live remote-version-state capability proof is current. The flags are operator attestation and a write request; they do not replace the live proof. With either flag disabled or the proof absent/stale, RustFS refuses to commit a new transition carrying an opaque Azure version and retains opaque-version recovery cleanup. See [opaque remote-version metadata requirements](tier-ilm-debugging.md#metadata-key-conventions).

The native Azure tier's all-peer Prepare check does not enable this separate gate. A successful Add/Verify probe checks the Blob transport and cleanup, so it does not by itself establish that versioned lifecycle writes can commit.

RustFS retains an opaque Azure version ID when the service returns one. Reads and ordinary deletes of that object explicitly address that version. If Azure rejects deleting the current version with OperationNotAllowedOnRootBlob, cleanup verifies the base blob still has the target version, conditionally deletes that base with its strong ETag, then retries the exact-version delete. A missing base also permits only an exact-version retry. A replacement version or a failed condition stops this fallback. A partial completion or lost acknowledgement remains an error until a later exact cleanup succeeds. An empty remote version sends no versionid parameter; unversioned cleanup also obtains a strong ETag and uses If-Match. Snapshots and immutability policies may prevent deletion and leave the normal tier cleanup record pending.

Metadata is encoded into legal Azure metadata names and values. The transaction and destination identities retain both RustFS and MinIO aliases. Recovery checks those identities before recognizing an uncertain upload as its candidate. Encoded metadata is limited to 8 KiB. The complete destination blob name, including the configured prefix, must fit the Stack Hub limit of 880 characters and 1,760 UTF-8 bytes; standalone dot path segments are rejected before I/O. Configuration validation normalizes one trailing prefix slash and reserves the entire canonical lifecycle object-name suffix before running a remote probe, so a prefix that fits a short probe but cannot fit actual transitions is rejected when constructing the tier.

Interrupted block staging can leave uncommitted blocks that Azure expires according to its own policy. Lifecycle uploads retain the existing transaction-journal recovery contract for a lost commit acknowledgement. Add/Edit/Verify probes retain the existing bounded in-process compensation: killing the process during a validation probe is not covered by a durable probe recovery worker. See [validation probe recovery status](tier-ilm-debugging.md#validation-probe-crash-recovery-status).

## Upgrade and Rollback

Existing Azure configurations retain their legacy S3-compatible transport. New Add operations select native Blob and persist a distinct protocol version and an encoded endpoint marker inside the existing binary configuration shape. A legacy destination and a native destination have different backend identities even if their account, endpoint, container and prefix are identical. Credential edits do not change the stored protocol.

Upgrade every topology member before adding a native Azure tier. A mutation whose candidate configuration contains a native Azure tier carries the immutable requires_native_azure intent extension. Earlier peers reject that extension during Prepare, before the new configuration can be committed. Ordinary tier mutations without native Azure keep their previous wire shape and checksums.

A rejected attempt during a mixed-version upgrade may retain a coordinator recovery record until every peer can read the intent and complete its normal abort/recovery exchange. Finish upgrading the remaining nodes before retrying; do not manually remove mutation records.

Do not downgrade a cluster while a native Azure tier or its recovery records remain in use. An older server can preserve and rewrite the existing Azure payload, but its S3 driver rejects the encoded native endpoint before making a request. Restore or migrate the referenced data and complete normal tier removal and recovery before rollback. Do not manually remove the native endpoint marker or protocol version, or delete tier metadata to bypass reference checks.

## Diagnostics and Verification

Native service failures retain a bounded HTTP status, Azure service error code and x-ms-request-id. Credential failures and deterministic rejected writes are reported directly; they do not cause a cleanup attempt against an object the write did not create. A transport failure or lost acknowledgement still requires candidate reconciliation, and an unconfirmed cleanup remains an error. Credentials, signed authorization values and raw response bodies are excluded from these diagnostics.

Local regression tests cover the signed Blob wire requests, block commit boundaries, empty and truncated input, ranged and versioned reads, conditional cleanup, identity-bound recovery, service errors, bounded XML parsing, persisted protocol selection and rejection by the previous peer reader. Before certifying a Stack Hub release, run Add/Verify, lifecycle transition, read-back/restore, expiry and restart/failure tests against that actual Stack Hub version. Mock protocol tests and Azure public-cloud results alone do not establish Stack Hub interoperability.
