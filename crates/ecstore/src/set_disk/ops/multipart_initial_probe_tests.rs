use super::*;
use crate::set_disk::core::io_primitives::disk_call_counters;

#[tokio::test]
async fn upgraded_legacy_publication_cold_retry_keeps_the_exact_completed_target() {
    for versioned in [false, true] {
        let (dirs, disks, set) = Box::pin(hermetic_set_disks(4)).await;
        let bucket = "initial-source-cold-legacy";
        let object = if versioned {
            "legacy-cold-origin-versioned"
        } else {
            "legacy-cold-origin-unversioned"
        };
        let opts = ObjectOptions {
            versioned,
            ..Default::default()
        };
        make_bucket_on_all(&disks, bucket).await;
        let expected_body = vec![0x73; 4096];
        let (upload, parts) = Box::pin(stage_upload_with_create_opts(&set, bucket, object, &expected_body, &opts)).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        pin_historical_unmarked_upload(&disks, bucket, &path).await;
        let source_before = originals(&disks, &path).await;
        let completed_at = OffsetDateTime::now_utc();
        let etag = get_complete_multipart_md5(&parts);
        let staging = format!("legacy-complete-{}", Uuid::new_v4());
        let mut completed_version = None;
        let mut target_before = Vec::with_capacity(disks.len());
        // Use the real legacy publication primitive on actual selected part
        // inodes. No native Complete, source receipt, or target PUB is created
        // and no consumed native source head is restored by this fixture.
        for (slot, disk) in disks.iter().enumerate() {
            let mut final_info = disk
                .read_version("", RUSTFS_META_MULTIPART_BUCKET, &path, "", &ReadOptions::default())
                .await
                .expect("read the actual unmarked physical source");
            assert!(!recognized_native_source_controls(&final_info).expect("genuine legacy schema"));
            let data_dir = final_info.data_dir.expect("actual upload data directory");
            let part_path = format!("{path}/{data_dir}/part.1.meta");
            let mut selected = disk
                .read_parts(RUSTFS_META_MULTIPART_BUCKET, &[part_path])
                .await
                .expect("decode actual selected physical part record");
            assert_eq!(selected.len(), 1);
            let part = selected.pop().expect("one actual selected part");
            assert_eq!(Some(&part.etag), parts[0].etag.as_ref());
            let source_dir = dirs[slot]
                .path()
                .join(RUSTFS_META_MULTIPART_BUCKET)
                .join(&path)
                .join(data_dir.to_string());
            let staged_dir = dirs[slot]
                .path()
                .join(RUSTFS_META_TMP_BUCKET)
                .join(&staging)
                .join(data_dir.to_string());
            std::fs::create_dir_all(&staged_dir).expect("create a real legacy private staging tree");
            let mut files = vec!["part.1".to_owned(), "part.1.meta".to_owned()];
            if let Some(integrity) = &part.integrity {
                files.push(integrity.file_name());
            }
            for file in files {
                std::fs::hard_link(source_dir.join(&file), staged_dir.join(&file))
                    .expect("stage the actual selected shard, record, and proof without replacing source metadata");
            }
            final_info.size = i64::try_from(part.size).expect("fixture part size fits FI");
            final_info.parts = vec![part];
            final_info.mod_time = Some(completed_at);
            final_info.versioned = versioned;
            final_info.data = None;
            strip_internal_multipart_metadata(&mut final_info.metadata);
            rustfs_utils::http::remove_str(&mut final_info.metadata, rustfs_filemeta::shard_integrity::SUFFIX_UPLOAD_INTEGRITY);
            rustfs_utils::http::remove_str(&mut final_info.metadata, SUFFIX_INLINE_DATA);
            insert_str(&mut final_info.metadata, SUFFIX_ACTUAL_SIZE, expected_body.len().to_string());
            insert_str(&mut final_info.metadata, rustfs_utils::http::SUFFIX_MULTIPART_UPLOAD_ID, upload.clone());
            final_info.metadata.insert("etag".to_owned(), etag.clone());
            final_info
                .persist_shard_integrity()
                .expect("persist the actual selected part integrity layout");
            if slot == 0 {
                completed_version = final_info.version_id;
            } else {
                assert_eq!(final_info.version_id, completed_version);
            }
            disk.write_metadata(bucket, RUSTFS_META_TMP_BUCKET, &staging, final_info.clone())
                .await
                .expect("write actual legacy staged FileMeta");
            let result = disk
                .rename_data(RUSTFS_META_TMP_BUCKET, &staging, final_info, bucket, object)
                .await
                .expect("publish the real legacy target through the ordinary metadata/data commit");
            assert!(
                result.multipart_source.is_none(),
                "legacy TMP publication must not fabricate a native receipt"
            );
            let persisted = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("real legacy target");
            assert!(
                publication_identity(&persisted)
                    .expect("strict legacy target decode")
                    .is_none()
            );
            assert_eq!(persisted.erasure.index, persisted.erasure.distribution[slot]);
            target_before.push(
                disk.read_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
                    .await
                    .expect("published raw target"),
            );
        }
        assert_eq!(
            originals(&disks, &path).await,
            source_before,
            "legacy publisher retained canonical sources; none were restored"
        );
        assert_eq!(body(&set, bucket, object, &opts).await, expected_body);
        {
            let _guard = set
                .acquire_multipart_upload_write_lock("legacy_cold_upgrade", bucket, object, &upload, &opts)
                .await
                .expect("upgrade source guard");
            let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
            upgrade_legacy_source(&physical, bucket, object, &path, &set.multipart_proof_verification_context())
                .await
                .expect("actual all-participant guarded legacy upgrade");
        }
        let mut upgraded = Vec::with_capacity(disks.len());
        for disk in &disks {
            let info = disk
                .read_version("", RUSTFS_META_MULTIPART_BUCKET, &path, "", &ReadOptions::default())
                .await
                .expect("actual upgraded head");
            assert_eq!(
                get_consistent_str(&info.metadata, crate::disk::multipart_source::NATIVE_SOURCE_PROTOCOL_SUFFIX),
                Some("native-v1")
            );
            assert!(!is_initial_native_source(&info).expect("upgraded origin is always conservative"));
            upgraded.push(info);
        }
        assert!(!initial_multipart_source_observations(&upgraded[0], &upgraded));
        let (reopened, cold) = Box::pin(cold_reopen(disks, set)).await;
        let calls = disk_call_counters::observe(object);
        let replay = cold
            .clone()
            .complete_multipart_upload(bucket, object, &upload, parts, &opts)
            .await
            .expect("cold retry must discover the pre-upgrade completed legacy quorum");
        assert!(
            replay.multipart_completion_replayed,
            "UpgradeLegacy cannot turn a preserved source into a new publication"
        );
        assert_eq!(replay.version_id.filter(|id| !id.is_nil()), completed_version.filter(|id| !id.is_nil()));
        assert_eq!(replay.etag, Some(etag));
        let probes = calls.total(disk_call_counters::KIND_READ_VERSION);
        assert!(
            (4..=8).contains(&probes),
            "ambiguous upgraded origin must keep the bounded destination write-quorum replay fanout: {probes}"
        );
        drop(calls);
        assert_eq!(body(&cold, bucket, object, &opts).await, expected_body);
        for (slot, disk) in reopened.iter().enumerate() {
            assert_eq!(
                disk.read_all(bucket, &format!("{object}/{STORAGE_FORMAT_FILE}"))
                    .await
                    .expect("unchanged completed target"),
                target_before[slot],
                "cold retry must not add a version or rewrite the existing target"
            );
        }
    }
}
