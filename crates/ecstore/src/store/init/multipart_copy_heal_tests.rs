// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

mod multipart_copy_heal_tests {
    use super::{build_isolated_test_store, run_large_stack_async_test, without_storage_class_env};
    use crate::{
        disk::{
            DeleteOptions, DiskAPI as _, DiskStore, RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, ReadOptions,
            error::DiskError,
            multipart_source::{
                MULTIPART_SOURCE_PROTOCOL_VERSION, MultipartProofKind, MultipartSourceOperation, MultipartSourcePhase,
                MultipartSourceProof, MultipartSourceRequest, MultipartSourceResponse, PublicationIdentity, SourceConditionV1,
                is_publication_metadata_key, publication_identity, publication_identity_from_metadata,
            },
        },
        object_api::{ObjectOptions, PutObjReader, WriteCompletion},
        runtime::instance::InstanceContext,
        storage_api_contracts::{
            bucket::{BucketOperations as _, MakeBucketOptions},
            heal::HealOperations,
            multipart::{CompletePart, MultipartOperations as _},
            object::{ObjectIO as _, ObjectOperations as _},
        },
        store::ECStore,
    };
    use http::HeaderMap;
    use rustfs_filemeta::{FileInfo, FileMeta};
    use rustfs_heal_contracts::heal_channel::{DriveState, HealOpts, HealScanMode};
    use sha2::{Digest, Sha256};
    use std::{path::PathBuf, sync::Arc, time::Duration};
    use time::OffsetDateTime;
    use tokio::io::AsyncReadExt as _;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    const ANCHOR: &str = ".mpu-source-retirement";

    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(deny_unknown_fields)]
    struct AnchorBytes {
        protocol_version: u16,
        disk_id: Uuid,
        physical_slot: usize,
        publication: PublicationIdentity,
        source_original_sha256: [u8; 32],
        source_validation_receipt_id: Uuid,
    }

    struct CopyHealFixture {
        ctx: Arc<InstanceContext>,
        store: Arc<ECStore>,
        shutdown: CancellationToken,
        disks: Vec<DiskStore>,
        bucket: String,
        object: String,
        publication: PublicationIdentity,
        original_options: ObjectOptions,
        body: Vec<u8>,
        anchors: Vec<Vec<u8>>,
        _root: tempfile::TempDir,
    }

    impl Drop for CopyHealFixture {
        fn drop(&mut self) {
            self.shutdown.cancel();
        }
    }

    fn local_path(disk: &DiskStore, bucket: &str, path: &str) -> PathBuf {
        disk.get_object_path_if_local(bucket, path)
            .expect("actual local participant")
            .expect("valid physical fixture path")
    }

    impl CopyHealFixture {
        async fn new(label: &str) -> Self {
            let root = tempfile::tempdir().expect("real Copy/heal store roots");
            let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(root.path(), label, &[4])).await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            let bucket = format!("copy-heal-source-{}", Uuid::new_v4());
            let object = "native-completion-with-shared-data-directory".to_owned();
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("actual versioned source bucket metadata");
            let mut seed = 0x6d70_752d_636f_7079_u64;
            let body = (0..1 << 20)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed as u8
                })
                .collect::<Vec<_>>();
            let options = ObjectOptions {
                versioned: true,
                mod_time: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed native C timestamp")),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            let upload = store.pools[0]
                .new_multipart_upload(&bucket, &object, &options)
                .await
                .expect("actual native C Init");
            let part = store.pools[0]
                .put_object_part(
                    &bucket,
                    &object,
                    &upload.upload_id,
                    1,
                    &mut PutObjReader::from_vec(body.clone()),
                    &options,
                )
                .await
                .expect("actual C erasure shards and selected part generation");
            let completed = store.pools[0]
                .clone()
                .complete_multipart_upload(
                    &bucket,
                    &object,
                    &upload.upload_id,
                    vec![CompletePart {
                        part_num: part.part_num,
                        etag: part.etag,
                        ..Default::default()
                    }],
                    &options,
                )
                .await
                .expect("actual native C quorum publication");
            tokio::time::timeout(Duration::from_secs(10), async {
                while ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("native completion physical tails drain");
            let version = completed.version_id.expect("actual native C version");
            let original_options = ObjectOptions {
                versioned: true,
                version_id: Some(version.to_string()),
                ..Default::default()
            };
            let current = store
                .get_object_info(&bucket, &object, &original_options)
                .await
                .expect("actual committed C metadata");
            let publication = publication_identity_from_metadata(&current.user_defined)
                .expect("actual C tuple is strict typed metadata")
                .expect("actual C carries native provenance");
            assert_eq!(publication.version_id, Some(version));
            let disks = store.pools[0].disk_set[0]
                .disk_inventory()
                .await
                .into_iter()
                .map(|disk| disk.expect("all actual formatted participants online"))
                .collect::<Vec<_>>();
            let mut anchors = Vec::with_capacity(4);
            for (slot, disk) in disks.iter().enumerate() {
                let fi = disk
                    .read_version("", &bucket, &object, &version.to_string(), &ReadOptions::default())
                    .await
                    .expect("actual C metadata on each producer");
                assert_eq!(publication_identity(&fi).expect("actual participant C tuple"), Some(publication.clone()));
                assert_eq!(fi.data_dir, Some(publication.final_data_dir));
                assert!(
                    !local_path(disk, RUSTFS_META_MULTIPART_BUCKET, &publication.lineage.path).exists(),
                    "normal Complete removes the actual source root before these RootMissing tests"
                );
                let raw = std::fs::read(local_path(disk, &bucket, &format!("{object}/{}/{ANCHOR}", publication.final_data_dir)))
                    .expect("actual producer's version-owned anchor");
                let anchor: AnchorBytes = rmp_serde::from_slice(&raw).expect("actual native anchor record");
                assert_eq!(
                    (anchor.disk_id, anchor.physical_slot),
                    (disk.get_disk_id().await.expect("actual format ID").expect("formatted disk"), slot)
                );
                assert_eq!(anchor.publication, publication);
                anchors.push(raw);
            }
            Self {
                ctx,
                store,
                shutdown,
                disks,
                bucket,
                object,
                publication,
                original_options,
                body,
                anchors,
                _root: root,
            }
        }

        async fn drain(&self) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("real Copy/heal physical tails drain");
        }

        async fn version_copy(&self) -> ObjectOptions {
            self.version_copy_with_replacement(false).await
        }

        async fn version_copy_with_replacement(&self, replace_metadata: bool) -> ObjectOptions {
            let mut source = self
                .store
                .get_object_info(&self.bucket, &self.object, &self.original_options)
                .await
                .expect("actual original version for CopyObject");
            source.metadata_only = true;
            source.put_object_reader = None;
            if replace_metadata {
                // This is the real storage producer input for MetadataDirective
                // REPLACE: CopyObject receives the caller's replacement map.
                source.user_defined = Arc::new(std::collections::HashMap::new());
            }
            Arc::make_mut(&mut source.user_defined).insert("x-amz-meta-version-copy".to_owned(), "D".to_owned());
            let version = Uuid::new_v4();
            let options = ObjectOptions {
                versioned: true,
                version_id: Some(version.to_string()),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            self.store
                .copy_object(
                    &self.bucket,
                    &self.object,
                    &self.bucket,
                    &self.object,
                    &mut source,
                    &self.original_options,
                    &options,
                )
                .await
                .expect("actual metadata/version-only CopyObject creates D");
            assert!(source.version_only, "the real ECStore producer selected the version-only branch");
            self.drain().await;
            for disk in &self.disks {
                let d = disk
                    .read_version("", &self.bucket, &self.object, &version.to_string(), &ReadOptions::default())
                    .await
                    .expect("actual D version on each disk");
                assert_eq!(d.version_id, Some(version));
                assert_ne!(d.version_id, self.publication.version_id);
                assert_ne!(
                    d.mod_time.expect("actual D timestamp").unix_timestamp_nanos(),
                    self.publication.modified_unix_nanos
                );
                assert_eq!(
                    d.data_dir,
                    Some(self.publication.final_data_dir),
                    "actual version-only Copy shares C's physical DD"
                );
                assert_eq!(
                    publication_identity(&d).expect("strict metadata from the actual Copy producer"),
                    if replace_metadata {
                        None
                    } else {
                        Some(self.publication.clone())
                    },
                    "metadata replacement removes the historical tuple; ordinary version Copy preserves it"
                );
            }
            options
        }

        async fn confirm(
            &self,
            slot: usize,
            publication: &PublicationIdentity,
        ) -> std::result::Result<MultipartSourceProof, DiskError> {
            let response = self.disks[slot]
                .recover_multipart_source(MultipartSourceRequest {
                    protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
                    path: publication.lineage.path.clone(),
                    operation: MultipartSourceOperation::ConfirmTerminal {
                        expected: None,
                        publication: publication.clone(),
                        challenge: Uuid::new_v4(),
                    },
                })
                .await?;
            let MultipartSourceResponse::TerminalConfirmed(proof) = response else {
                panic!("strict terminal native response");
            };
            proof.verify_with_authority(&self.ctx.multipart_proof_verification_context())?;
            assert_eq!(proof.kind, MultipartProofKind::RootMissingTerminal);
            assert_eq!(proof.snapshot.phase, MultipartSourcePhase::RootMissing);
            assert_eq!(proof.snapshot.publication.as_ref(), Some(publication));
            assert_eq!(proof.snapshot.physical_slot, slot);
            Ok(proof)
        }

        fn anchor_path(&self, slot: usize) -> PathBuf {
            local_path(
                &self.disks[slot],
                &self.bucket,
                &format!("{}/{}/{ANCHOR}", self.object, self.publication.final_data_dir),
            )
        }

        async fn force_deep_heal(&self, slot: usize, bucket: &str, object: &str, options: &ObjectOptions, data_dir: Uuid) {
            let shard = local_path(&self.disks[slot], bucket, &format!("{object}/{data_dir}/part.1"));
            let original = std::fs::read(&shard).expect("real producer's physical shard before corruption");
            assert!(!original.is_empty(), "noninline one-MiB object has a real bitrot shard");
            let mut corrupt = original.clone();
            corrupt[0] ^= 0x80;
            std::fs::write(&shard, &corrupt).expect("actual bitrot corruption in shared physical shard");
            std::fs::File::open(&shard)
                .expect("open corrupted actual shard")
                .sync_all()
                .expect("persist actual fault bytes");
            assert_ne!(std::fs::read(&shard).expect("actual corrupt bytes"), original);
            let (healed, error) = HealOperations::heal_object(
                self.store.pools[0].disk_set[0].as_ref(),
                bucket,
                object,
                options.version_id.as_deref().expect("exact actual Copy version"),
                &HealOpts {
                    scan_mode: HealScanMode::Deep,
                    ..Default::default()
                },
            )
            .await
            .expect("actual DeepHeal must accept historical native metadata on CopyObject");
            assert!(error.is_none(), "actual DeepHeal succeeds: {error:?}");
            assert!(
                matches!(healed.before.drives[slot].state.as_str(), "corrupt" | "missing"),
                "forced corruption must take the real repair path rather than a healthy no-op: {:?}",
                healed.before.drives[slot]
            );
            assert_eq!(healed.after.drives[slot].state, DriveState::Ok.to_string());
            self.drain().await;
            assert_eq!(
                std::fs::read(&shard).expect("actual rebuilt physical shard"),
                original,
                "DeepHeal reconstructed the exact original physical slot, not a donor shard"
            );
            assert!(
                !local_path(&self.disks[slot], RUSTFS_META_MULTIPART_BUCKET, &self.publication.lineage.path).exists(),
                "ordinary Copy healing cannot recreate the consumed native source"
            );
        }

        async fn body_is_exact(&self, bucket: &str, object: &str, options: &ObjectOptions) {
            let mut reader = self
                .store
                .get_object_reader(bucket, object, None, HeaderMap::new(), options)
                .await
                .expect("actual version/body after Copy and DeepHeal");
            let expected_version = options
                .version_id
                .as_deref()
                .map(|value| Uuid::parse_str(value).expect("fixture version"));
            assert_eq!(reader.object_info.version_id, expected_version);
            let mut actual = Vec::new();
            reader
                .stream
                .read_to_end(&mut actual)
                .await
                .expect("consume actual healed object body");
            assert_eq!(actual, self.body);
        }

        async fn versions_are_exact(&self, slot: usize, bucket: &str, object: &str, expected: &[Uuid]) {
            let raw = self.disks[slot]
                .read_all(bucket, &format!("{object}/xl.meta"))
                .await
                .expect("actual physical version stack after repair");
            let metadata = FileMeta::load(&raw).expect("valid repaired FileMeta inventory");
            let mut versions = metadata
                .versions
                .iter()
                .map(|version| version.header.version_id.expect("actual concrete version slot"))
                .collect::<Vec<_>>();
            versions.sort_unstable();
            let mut expected = expected.to_vec();
            expected.sort_unstable();
            assert_eq!(versions, expected, "repair preserves each actual version exactly once");
        }

        fn projected_copy_request(&self, fi: &FileInfo, bucket: &str, object: &str) -> PublicationIdentity {
            // This is an untrusted request derived from the actual Copy FI,
            // never a replacement FI or a record written to any disk.
            let mut lineage = self.publication.lineage.clone();
            if lineage.bucket != bucket || lineage.object != object {
                lineage.bucket = bucket.to_owned();
                lineage.object = object.to_owned();
                let hash = hex_simd::encode_to_string(
                    Sha256::digest(format!("{bucket}/{object}").as_bytes()),
                    hex_simd::AsciiCase::Lower,
                );
                lineage.path = format!(
                    "{hash}/{}",
                    self.publication
                        .lineage
                        .path
                        .rsplit('/')
                        .next()
                        .expect("actual upload component")
                );
            }
            let condition = SourceConditionV1 {
                lineage,
                generation: self.publication.source_generation,
                attempt: self.publication.attempt,
            };
            condition
                .attach(&mut fi.clone())
                .expect("well-formed request for the actual Copy identity")
        }
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn native_multipart_copy_heal_version_copy_preserves_original_producer_anchor() {
        run_large_stack_async_test("multipart-copy-heal-shared-dd", || async {
            let fixture = CopyHealFixture::new("native-multipart-copy-heal-shared-dd").await;
            let d = fixture.version_copy().await;
            fixture
                .force_deep_heal(0, &fixture.bucket, &fixture.object, &d, fixture.publication.final_data_dir)
                .await;
            assert_eq!(
                std::fs::read(fixture.anchor_path(0)).expect("original owned anchor after actual D repair"),
                fixture.anchors[0]
            );
            fixture
                .confirm(0, &fixture.publication)
                .await
                .expect("exact C remains a strict own-producer vote");
            let current_d = fixture.disks[0]
                .read_version(
                    "",
                    &fixture.bucket,
                    &fixture.object,
                    d.version_id.as_deref().expect("D"),
                    &ReadOptions::default(),
                )
                .await
                .expect("actual D after real heal");
            let d_request = fixture.projected_copy_request(&current_d, &fixture.bucket, &fixture.object);
            assert_ne!(d_request, fixture.publication);
            assert!(
                fixture.confirm(0, &d_request).await.is_err(),
                "historical C metadata cannot become fresh D authority"
            );
            fixture
                .body_is_exact(&fixture.bucket, &fixture.object, &fixture.original_options)
                .await;
            fixture
                .versions_are_exact(
                    0,
                    &fixture.bucket,
                    &fixture.object,
                    &[
                        fixture.publication.version_id.expect("actual native C version"),
                        Uuid::parse_str(d.version_id.as_deref().expect("actual D version")).expect("D UUID"),
                    ],
                )
                .await;
            fixture.body_is_exact(&fixture.bucket, &fixture.object, &d).await;
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn native_multipart_copy_heal_preserves_own_anchor_while_original_metadata_is_missing() {
        run_large_stack_async_test("multipart-copy-heal-missing-original", || async {
            for replace_metadata in [false, true] {
                let fixture = CopyHealFixture::new("native-multipart-copy-heal-missing-original").await;
                let d = fixture.version_copy_with_replacement(replace_metadata).await;
                let c_version = fixture.original_options.version_id.as_deref().expect("actual C version");
                let c = fixture.disks[0]
                    .read_version("", &fixture.bucket, &fixture.object, c_version, &ReadOptions::default())
                    .await
                    .expect("actual C before one-disk metadata loss");
                fixture.disks[0]
                    .delete_version(&fixture.bucket, &fixture.object, c, false, DeleteOptions::default())
                    .await
                    .expect("remove only this participant's actual C version; D still references the DD");
                assert!(matches!(
                    fixture.disks[0]
                        .read_version("", &fixture.bucket, &fixture.object, c_version, &ReadOptions::default())
                        .await,
                    Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)
                ));
                assert_eq!(
                    std::fs::read(fixture.anchor_path(0)).expect("shared D reference retains actual anchor"),
                    fixture.anchors[0]
                );
                fixture
                    .force_deep_heal(0, &fixture.bucket, &fixture.object, &d, fixture.publication.final_data_dir)
                    .await;
                assert_eq!(
                    std::fs::read(fixture.anchor_path(0)).expect("D repair keeps the original record byte-for-byte"),
                    fixture.anchors[0]
                );
                assert!(
                    matches!(
                        fixture.disks[0]
                            .read_version("", &fixture.bucket, &fixture.object, c_version, &ReadOptions::default())
                            .await,
                        Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)
                    ),
                    "D heal cannot synthesize C metadata"
                );
                assert!(
                    fixture.confirm(0, &fixture.publication).await.is_err(),
                    "retained record alone cannot vote while exact C metadata is absent"
                );
                let current_d = fixture.disks[0]
                    .read_version(
                        "",
                        &fixture.bucket,
                        &fixture.object,
                        d.version_id.as_deref().expect("D"),
                        &ReadOptions::default(),
                    )
                    .await
                    .expect("actual D while C is absent");
                assert_eq!(
                    publication_identity(&current_d).expect("actual D metadata after repair"),
                    if replace_metadata {
                        None
                    } else {
                        Some(fixture.publication.clone())
                    },
                    "heal preserves the real Copy metadata and does not mint a publication tuple"
                );
                assert!(
                    fixture
                        .confirm(0, &fixture.projected_copy_request(&current_d, &fixture.bucket, &fixture.object))
                        .await
                        .is_err(),
                    "actual D never inherits C's producer authority"
                );
                let (healed, error) = HealOperations::heal_object(
                    fixture.store.pools[0].disk_set[0].as_ref(),
                    &fixture.bucket,
                    &fixture.object,
                    c_version,
                    &HealOpts {
                        scan_mode: HealScanMode::Deep,
                        ..Default::default()
                    },
                )
                .await
                .expect("legitimate C heal from actual original quorum");
                assert!(error.is_none(), "actual C metadata repair succeeds: {error:?}");
                assert_ne!(
                    healed.before.drives[0].state,
                    DriveState::Ok.to_string(),
                    "C is actually missing on the repaired participant"
                );
                assert_eq!(healed.after.drives[0].state, DriveState::Ok.to_string());
                fixture.drain().await;
                assert_eq!(
                    std::fs::read(fixture.anchor_path(0)).expect("original record survives legitimate C repair"),
                    fixture.anchors[0]
                );
                fixture
                    .confirm(0, &fixture.publication)
                    .await
                    .expect("restored exact C metadata revives only its original strict vote");
                fixture
                    .body_is_exact(&fixture.bucket, &fixture.object, &fixture.original_options)
                    .await;
                fixture
                    .versions_are_exact(
                        0,
                        &fixture.bucket,
                        &fixture.object,
                        &[
                            fixture.publication.version_id.expect("actual native C version"),
                            Uuid::parse_str(d.version_id.as_deref().expect("actual D version")).expect("D UUID"),
                        ],
                    )
                    .await;
                fixture.body_is_exact(&fixture.bucket, &fixture.object, &d).await;
            }
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn native_multipart_copy_heal_full_copy_repairs_new_scope_without_minting_an_anchor() {
        run_large_stack_async_test("multipart-copy-heal-materialized", || async {
            let fixture = CopyHealFixture::new("native-multipart-copy-heal-materialized").await;
            let bucket = format!("copy-heal-destination-{}", Uuid::new_v4());
            let object = "materialized-copy-in-a-new-key";
            fixture
                .store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("real distinct destination bucket");
            let reader = fixture
                .store
                .get_object_reader(&fixture.bucket, &fixture.object, None, HeaderMap::new(), &fixture.original_options)
                .await
                .expect("actual source stream for full CopyObject");
            let mut source = reader.object_info.clone();
            let size = source.get_actual_size().expect("actual full Copy body length");
            source.put_object_reader = Some(PutObjReader::new(
                rustfs_rio::HashReader::from_stream(reader.stream, size, size, None, None, false)
                    .expect("actual materialized Copy reader"),
            ));
            let d = ObjectOptions {
                versioned: true,
                version_id: Some(Uuid::new_v4().to_string()),
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            fixture
                .store
                .copy_object(
                    &fixture.bucket,
                    &fixture.object,
                    &bucket,
                    object,
                    &mut source,
                    &fixture.original_options,
                    &d,
                )
                .await
                .expect("actual full stream CopyObject across bucket and key");
            drop(source);
            fixture.drain().await;
            let actual = fixture.disks[0]
                .read_version(
                    "",
                    &bucket,
                    object,
                    d.version_id.as_deref().expect("actual full Copy version"),
                    &ReadOptions::default(),
                )
                .await
                .expect("actual materialized Copy FI");
            let data_dir = actual.data_dir.expect("actual full Copy DD");
            assert_ne!(data_dir, fixture.publication.final_data_dir);
            assert_ne!(actual.version_id, fixture.publication.version_id);
            assert_eq!(
                publication_identity(&actual).expect("inherited historical tuple"),
                Some(fixture.publication.clone())
            );
            for disk in &fixture.disks {
                assert!(
                    !local_path(disk, &bucket, &format!("{object}/{data_dir}/{ANCHOR}")).exists(),
                    "ordinary full Copy never imports native provenance"
                );
            }
            fixture.force_deep_heal(0, &bucket, object, &d, data_dir).await;
            assert!(
                !local_path(&fixture.disks[0], &bucket, &format!("{object}/{data_dir}/{ANCHOR}")).exists(),
                "actual full-Copy DeepHeal cannot mint or rebind an anchor"
            );
            let after = fixture.disks[0]
                .read_version("", &bucket, object, d.version_id.as_deref().expect("D"), &ReadOptions::default())
                .await
                .expect("actual healed full Copy");
            assert!(
                fixture
                    .confirm(0, &fixture.projected_copy_request(&after, &bucket, object))
                    .await
                    .is_err(),
                "historical tuple cannot confirm the new target scope"
            );
            fixture
                .confirm(0, &fixture.publication)
                .await
                .expect("original C remains independent native provenance");
            fixture
                .versions_are_exact(
                    0,
                    &bucket,
                    object,
                    &[Uuid::parse_str(d.version_id.as_deref().expect("actual full Copy version")).expect("D UUID")],
                )
                .await;
            fixture.body_is_exact(&bucket, object, &d).await;
        });
    }

    fn stage_actual_tmp_dd(fixture: &CopyHealFixture, fi: &FileInfo, include_anchor: bool) -> String {
        let path = format!("copy-heal-negative-{}", Uuid::new_v4());
        let data_dir = fi.data_dir.expect("actual selected DD");
        let source = local_path(&fixture.disks[0], &fixture.bucket, &format!("{}/{data_dir}", fixture.object));
        let target = local_path(&fixture.disks[0], RUSTFS_META_TMP_BUCKET, &format!("{path}/{data_dir}"));
        std::fs::create_dir_all(&target).expect("actual ordinary TMP repair directory");
        for entry in std::fs::read_dir(&source).expect("actual native shard, sidecar and proof inventory") {
            let entry = entry.expect("native DD entry");
            if entry.file_name() == ANCHOR && !include_anchor {
                continue;
            }
            assert!(entry.file_type().expect("native regular-file type").is_file());
            std::fs::copy(entry.path(), target.join(entry.file_name())).expect("real immutable repair input bytes");
        }
        path
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn native_multipart_copy_heal_rejects_malformed_aliases_and_reserved_incoming_record_before_mutation() {
        run_large_stack_async_test("multipart-copy-heal-negative-input", || async {
            let fixture = CopyHealFixture::new("native-multipart-copy-heal-negative-input").await;
            let d = fixture.version_copy().await;
            let actual = fixture.disks[0]
                .read_version(
                    "",
                    &fixture.bucket,
                    &fixture.object,
                    d.version_id.as_deref().expect("D"),
                    &ReadOptions::default(),
                )
                .await
                .expect("real Copy FI for malformed input probes");
            let metadata_path = local_path(&fixture.disks[0], &fixture.bucket, &format!("{}/xl.meta", fixture.object));
            let metadata = std::fs::read(&metadata_path).expect("actual C+D stack before rejected inputs");
            let shard_path = local_path(
                &fixture.disks[0],
                &fixture.bucket,
                &format!("{}/{}/part.1", fixture.object, fixture.publication.final_data_dir),
            );
            let shard = std::fs::read(&shard_path).expect("actual shared shard before rejected inputs");
            for case in [
                "short-distribution",
                "overflow-geometry",
                "alias-conflict",
                "incoming-reserved",
            ] {
                let mut incoming = actual.clone();
                incoming.set_healing();
                let keys = incoming
                    .metadata
                    .keys()
                    .filter(|key| is_publication_metadata_key(key))
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(keys.len(), 2, "actual producer writes both native internal metadata aliases");
                let mut malformed = fixture.publication.clone();
                match case {
                    "short-distribution" => malformed.lineage.geometry.distribution.clear(),
                    "overflow-geometry" => {
                        malformed.lineage.geometry.data_blocks = usize::MAX;
                        malformed.lineage.geometry.parity_blocks = 1;
                    }
                    "alias-conflict" => malformed.attempt = Uuid::new_v4(),
                    _ => {}
                }
                if case != "incoming-reserved" {
                    let encoded = hex_simd::encode_to_string(
                        rmp_serde::to_vec_named(&malformed).expect("untrusted malformed typed tuple"),
                        hex_simd::AsciiCase::Lower,
                    );
                    for key in keys.iter().take(if case == "alias-conflict" { 1 } else { keys.len() }) {
                        incoming.metadata.insert(key.clone(), encoded.clone());
                    }
                }
                let tmp = stage_actual_tmp_dd(&fixture, &actual, case == "incoming-reserved");
                let result = fixture.disks[0]
                    .rename_data(RUSTFS_META_TMP_BUCKET, &tmp, incoming, &fixture.bucket, &fixture.object)
                    .await;
                let error: DiskError = result.expect_err(
                    "malformed geometry, alias conflict or reserved incoming record is a typed failure, never a panic",
                );
                if case == "incoming-reserved" {
                    assert!(
                        matches!(&error, DiskError::Io(io) if io.kind() == std::io::ErrorKind::PermissionDenied)
                            || matches!(error, DiskError::FileAccessDenied | DiskError::DiskAccessDenied),
                        "reserved incoming native record is an access rejection: {error:?}"
                    );
                } else {
                    assert!(
                        matches!(error, DiskError::FileCorrupt | DiskError::ConditionalDeleteConflict),
                        "malformed typed metadata must not become an executor panic or generic I/O error: {error:?}"
                    );
                }
                assert_eq!(std::fs::read(&metadata_path).expect("target after rejected ordinary TMP input"), metadata);
                assert_eq!(std::fs::read(&shard_path).expect("shared shard after rejected input"), shard);
                assert_eq!(
                    std::fs::read(fixture.anchor_path(0)).expect("original anchor after rejected input"),
                    fixture.anchors[0]
                );
            }
            fixture
                .confirm(0, &fixture.publication)
                .await
                .expect("invalid repair input never damages the original strict vote");
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn native_multipart_copy_heal_never_rebinds_foreign_disk_or_scope_records() {
        run_large_stack_async_test("multipart-copy-heal-foreign-records", || async {
            for foreign in ["disk", "scope"] {
                let fixture = CopyHealFixture::new("native-multipart-copy-heal-foreign-records").await;
                let d = fixture.version_copy().await;
                let raw = if foreign == "disk" {
                    fixture.anchors[1].clone()
                } else {
                    let mut anchor: AnchorBytes =
                        rmp_serde::from_slice(&fixture.anchors[0]).expect("actual native anchor before scope corruption");
                    anchor.publication.lineage.object.push_str("-foreign-scope");
                    rmp_serde::to_vec_named(&anchor).expect("untrusted wrong-scope record")
                };
                std::fs::write(fixture.anchor_path(0), &raw).expect("actual foreign-record fault in this physical DD");
                fixture
                    .force_deep_heal(0, &fixture.bucket, &fixture.object, &d, fixture.publication.final_data_dir)
                    .await;
                assert!(
                    !fixture.anchor_path(0).exists(),
                    "ordinary heal never retains or rebinds foreign-disk or foreign-key provenance"
                );
                assert!(
                    fixture.confirm(0, &fixture.publication).await.is_err(),
                    "the repaired donor cannot become an original producer"
                );
                let actual = fixture.disks[0]
                    .read_version(
                        "",
                        &fixture.bucket,
                        &fixture.object,
                        d.version_id.as_deref().expect("D"),
                        &ReadOptions::default(),
                    )
                    .await
                    .expect("actual repaired D with no native origin");
                assert!(
                    fixture
                        .confirm(0, &fixture.projected_copy_request(&actual, &fixture.bucket, &fixture.object))
                        .await
                        .is_err()
                );
                fixture
                    .versions_are_exact(
                        0,
                        &fixture.bucket,
                        &fixture.object,
                        &[
                            fixture.publication.version_id.expect("actual native C version"),
                            Uuid::parse_str(d.version_id.as_deref().expect("actual D version")).expect("D UUID"),
                        ],
                    )
                    .await;
                fixture.body_is_exact(&fixture.bucket, &fixture.object, &d).await;
            }
        });
    }
}
