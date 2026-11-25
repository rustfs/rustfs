# S3 stack upgrade and rollback

The gateway release serves S3 with rustfs/gateway. Remove `RUSTFS_S3_STACK` from service units, container environments and Helm values before upgrading. Even an empty setting prevents startup. Keep the previous release binary or image digest with the deployment configuration.

## Roll back to the previous release

1. Check the target release against the [storage format version floors](rolling-restart.md#version-floors-that-break-mixed-version-fleets). If the cluster has written data or metadata that the target cannot read, stop here. The S3 stack switch itself requires no data migration.
2. Stop one node, restore the previous RustFS binary or image and start it with the same volumes and configuration. Follow the [rolling restart procedure](rolling-restart.md#rolling-restart-procedure). Wait for the node to report ready and for its peers to report its drives online before replacing another node.
3. Through the restored node, read an object written after the upgrade and compare its contents or checksum. Read a bucket configuration used by the application. If an upload was in progress, list its parts and compare their numbers, sizes and ETags with the client's record. Pause the rollout if any check fails.

## Client-visible error change

| Conformance case | What clients see |
| --- | --- |
| `c-sig-0598` (R-053) | For an access key the S3 parser cannot read, both versions return HTTP 403. The gateway returns the XML error code `AccessDenied`. The legacy binary returns `InvalidAccessKeyId`. Clients that classify these errors by code need to handle both during a rollout. |

The case is recorded in the [pinned gateway conformance rulings](https://github.com/rustfs/gateway/blob/4a4360da67abb59868e57a8da5c0b64cf924ef0a/conformance/rulings/rustfs-profile.toml); host-assembled conformance rulings are tracked in [`.config/conformance-rulings.toml`](../../.config/conformance-rulings.toml).
