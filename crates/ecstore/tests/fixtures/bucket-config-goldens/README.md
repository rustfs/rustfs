# Persisted bucket-configuration XML goldens (rustfs/backlog#2744)

Byte-level goldens for the 13 XML families that `BucketMetadata` persists as
`*_config_xml`. They pin what `rustfs_ecstore::api::bucket::utils::deserialize`
followed by `serialize` emits today, so that the DTO/codec replacement (T1.4)
and the persisted-bytes comparison (T3.7) can be proven byte-identical: an
older binary must keep reading what a newer one writes, and the reverse.

Generated at commit: `6b1554003ebf8f2037ffb7da9c9b906527e758da` (2026-10-07T17:40:56Z)

Checked by `crates/ecstore/tests/bucket_config_goldens.rs`
(`cargo nextest run -p rustfs-ecstore bucket_config_goldens`, 39 tests).
Generated once by `scripts/gen_bucket_config_goldens.sh`, which records the
commit above and refuses to run again unless `--force` is passed. Do not
regenerate after a DTO or codec replacement: at that point a diff here is the
finding, not noise.

## Layout

`<family>/<shape>.in.xml` is an input; `<family>/<shape>.out.xml` is the exact
bytes of `serialize(deserialize(in))` on the generating commit. The test
asserts `bytes == out` (never structural equality), that `out` is a fixed point
of the same round trip, and for the `rustfs` shape that `serialize(typed
value)` still equals `rustfs.in.xml`.

| Shape | Source |
| --- | --- |
| `aws` | Written for this corpus from the public S3 XML schema in the shape of the AWS documentation examples: XML declaration, `xmlns` on the root, indented. No AWS prose is copied. |
| `minio` | The bytes a MinIO server persists. Seven families are carved verbatim out of `../minio/bucket_metadata.blob.hex` (MinIO `RELEASE.2025-07-23T15-54-02Z`); the rest are hand-shaped as compact Go `encoding/xml` output with the S3 namespace on the root, because MinIO does not persist those families. |
| `rustfs` | The bytes RustFS writes today through `serialize` for a typed value (`rustfs_values` in the test). This shape carries the MinIO-only members RustFS accepts (`DelMarkerExpiration`, `ExcludeFolders`, `ExcludedPrefixes`, `DeleteReplication`). |

| Family | Root element | `minio.in.xml` provenance |
| --- | --- | --- |
| `notification` | `NotificationConfiguration` | MinIO fixture |
| `lifecycle` | `LifecycleConfiguration` | MinIO fixture (carries the MinIO `ExpiryUpdatedAt` extension) |
| `object_lock` | `ObjectLockConfiguration` | MinIO fixture |
| `versioning` | `VersioningConfiguration` | MinIO fixture |
| `encryption` | `ServerSideEncryptionConfiguration` | MinIO fixture |
| `tagging` | `Tagging` | MinIO fixture |
| `replication` | `ReplicationConfiguration` | MinIO fixture |
| `cors` | `CORSConfiguration` | hand-shaped |
| `logging` | `BucketLoggingStatus` | hand-shaped |
| `website` | `WebsiteConfiguration` | hand-shaped |
| `accelerate` | `AccelerateConfiguration` | hand-shaped |
| `request_payment` | `RequestPaymentConfiguration` | hand-shaped |
| `public_access_block` | `PublicAccessBlockConfiguration` | hand-shaped |

## Unknown elements

`replication/aws.in.xml` is the unknown-element sample from
`crates/replication/src/config.rs`
(`s3_xml_parser_discards_unknown_replication_elements_before_validation`),
re-indented to two spaces. Its golden proves the root-level `FutureTopLevel`
element is skipped on read and never re-emitted on write, which is the
persisted-configuration exception recorded in the root `AGENTS.md`. On the
generating commit every one of the 13 root elements skips unknown children;
nested members (`Rule`, `Filter`, `Destination`, ...) reject them. A
replacement codec must keep that boundary where it is.
