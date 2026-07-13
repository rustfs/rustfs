use crate::disk::endpoint::Endpoint;
use crate::disk::format::FormatV3;
use crate::disk::{DiskAPI, DiskOption, DiskStore, WalkDirOptions, new_disk};
use crate::error::Error;
use crate::io_support::rio::HashReader;
use crate::object_api::{BLOCK_SIZE_V2, ObjectOptions, PutObjReader};
use crate::set_disk::SetDisks;
use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage_api_contracts::range::HTTPRangeSpec;
use crate::store::init_format::save_format_file;
use http::HeaderMap;
use rustfs_filemeta::{MetacacheReader, MetacacheWriter};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

/// Returns the backing [`tempfile::TempDir`]s alongside the set so callers keep
/// them alive for the test's duration and the directories are removed on drop.
pub(crate) async fn make_local_set_disks(drive_count: usize, parity_count: usize) -> (Vec<tempfile::TempDir>, Arc<SetDisks>) {
    let format = FormatV3::new(1, drive_count);
    let mut dirs = Vec::with_capacity(drive_count);
    let mut endpoints = Vec::with_capacity(drive_count);
    let mut disks = Vec::with_capacity(drive_count);

    for disk_idx in 0..drive_count {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_idx);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][disk_idx];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        dirs.push(dir);
        endpoints.push(endpoint);
        disks.push(Some(disk));
    }

    let set_disks = SetDisks::new(
        "ecstore-validation-blackbox".to_string(),
        Arc::new(RwLock::new(disks)),
        drive_count,
        parity_count,
        0,
        0,
        endpoints,
        format,
        Vec::new(),
    )
    .await;

    (dirs, set_disks)
}

async fn first_shard_part_path(disk: &DiskStore, bucket: &str, object: &str) -> PathBuf {
    let object_dir = disk.path().join(bucket).join(object);
    let mut entries = fs::read_dir(&object_dir)
        .await
        .unwrap_or_else(|err| panic!("object data dir should be readable at {object_dir:?}: {err}"));
    while let Some(entry) = entries.next_entry().await.expect("object data dir entry should be readable") {
        if entry
            .file_type()
            .await
            .expect("object data entry type should be readable")
            .is_dir()
        {
            let part_path = entry.path().join("part.1");
            if fs::metadata(&part_path).await.is_ok() {
                return part_path;
            }
        }
    }
    panic!("part.1 shard should exist under {object_dir:?}");
}

async fn shard_part_paths(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) -> Vec<PathBuf> {
    let disks = set_disks.disks.read().await;
    let mut paths = Vec::new();
    for disk in disks.iter().flatten() {
        paths.push(first_shard_part_path(disk, bucket, object).await);
    }
    paths
}

#[tokio::test]
async fn blackbox_put_unknown_actual_size_restores_body_and_records_written_size() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "bb-unknown-actual-size";
    let object = "object.bin";
    let payload = (0..(BLOCK_SIZE_V2 + 123))
        .map(|idx| ((idx * 17) % 251) as u8)
        .collect::<Vec<_>>();
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };

    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let stream = HashReader::from_stream(Cursor::new(payload.clone()), payload.len() as i64, -1, None, None, false)
        .expect("hash reader should accept unknown actual size");
    let mut reader = PutObjReader::new(stream);
    let written = set_disks
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("object should be written");

    assert_eq!(written.size, payload.len() as i64);
    assert_eq!(written.parts[0].actual_size, payload.len() as i64);

    let mut get_reader = set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
        .await
        .expect("object reader should open");
    let mut restored = Vec::new();
    get_reader
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("object should stream");
    assert_eq!(restored, payload);
}

#[tokio::test]
async fn blackbox_get_restores_body_after_one_shard_file_is_removed() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "bb-missing-shard-file";
    let object = "object.bin";
    let payload = (0..(BLOCK_SIZE_V2 + 321))
        .map(|idx| ((idx * 19) % 251) as u8)
        .collect::<Vec<_>>();
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };

    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(payload.clone());
    set_disks
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("object should be written");

    let disk = {
        let disks = set_disks.disks.read().await;
        disks[0].clone().expect("first disk should exist")
    };
    let shard_path = first_shard_part_path(&disk, bucket, object).await;
    fs::remove_file(&shard_path)
        .await
        .unwrap_or_else(|err| panic!("test shard should be removable at {shard_path:?}: {err}"));

    let mut get_reader = set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
        .await
        .expect("object should remain readable after one shard file is removed");
    let mut restored = Vec::new();
    get_reader
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("degraded object should stream");

    assert_eq!(restored, payload);
}

#[tokio::test]
// Serialized: forces the reader-setup strategy through a process-global env var.
#[serial_test::serial]
async fn blackbox_get_restores_body_and_enqueues_repair_after_one_corrupt_shard() {
    use rustfs_common::heal_channel::{HealAdmissionResult, HealChannelCommand, HealChannelPriority, HealRequestSource};

    // Own the process-global heal channel so the read path's repair submission
    // becomes observable. init_heal_channel() succeeds exactly once per test
    // binary: this must stay the only ecstore unit test that takes the receiver.
    // Submissions from other (non-serial) tests queue in the unbounded channel
    // while this test is setting up, get drained and dropped by the loop below
    // (failing their submitter, which releases their dedup reservation), and
    // fail fast once the receiver drops at test end. Tests that must observe a
    // deterministic channel state serialize under the same serial key.
    let mut heal_rx = rustfs_common::heal_channel::init_heal_channel()
        .expect("this must be the only ecstore test that owns the heal channel receiver");

    // Keep data-blocks-first reader setup explicit for this deterministic
    // repair assertion (see ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP in
    // set_disk/core/io_primitives.rs): if a caller opts back into all-shards,
    // the corrupt shard's failed open can lose the setup-quorum race, making
    // the enqueue assertion below a coin flip.
    temp_env::async_with_vars([("RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP", Some("true"))], async {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "bb-corrupt-shard-repair";
        let object = "object.bin";
        let payload = (0..(BLOCK_SIZE_V2 + 777))
            .map(|idx| ((idx * 29) % 251) as u8)
            .collect::<Vec<_>>();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("object should be written");

        // Corrupt the disk holding DATA shard 1, not blindly disk 0 (which holds
        // a parity shard for this key): a data-blocks-first read must consume the
        // corrupt data shard, making the missing-shard detection deterministic.
        // The write path derives the shard distribution the same way (ops/object.rs).
        let distribution = rustfs_filemeta::FileInfo::new(&format!("{bucket}/{object}"), 2, 2)
            .erasure
            .distribution;
        let corrupt_disk = distribution
            .iter()
            .position(|&shard| shard == 1)
            .expect("distribution should place data shard 1 on a disk");
        let paths = shard_part_paths(&set_disks, bucket, object).await;
        fs::write(&paths[corrupt_disk], b"corrupt shard bytes")
            .await
            .unwrap_or_else(|err| panic!("test shard should be corruptible at {:?}: {err}", paths[corrupt_disk]));

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object should remain readable after one corrupt shard");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("corrupt-shard object should stream from parity");

        assert_eq!(restored, payload);

        let request = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                match heal_rx.recv().await.expect("heal channel should stay open") {
                    HealChannelCommand::Start { request, response_tx } if request.bucket == bucket => {
                        let _ = response_tx.send(Ok(HealAdmissionResult::Accepted));
                        break request;
                    }
                    _ => continue,
                }
            }
        })
        .await
        .expect("corrupt-shard GET should enqueue a read-repair heal request");

        assert_eq!(request.source, HealRequestSource::ReadRepair);
        assert_eq!(request.object_prefix.as_deref(), Some(object));
        assert_eq!(request.object_version_id, None, "unversioned PUT must submit repair without a version id");
        assert_eq!(request.pool_index, Some(0));
        assert_eq!(request.set_index, Some(0));
        assert_eq!(request.priority, HealChannelPriority::Low);
        assert_eq!(request.recreate_missing, Some(true));
    })
    .await;
}

#[tokio::test]
async fn blackbox_range_read_restores_exact_slice_with_one_offline_disk() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "bb-range-offline-disk";
    let object = "object.bin";
    let payload = (0..(BLOCK_SIZE_V2 + 4096))
        .map(|idx| ((idx * 23) % 251) as u8)
        .collect::<Vec<_>>();
    let range_start = 513usize;
    let range_len = 8192usize;
    let opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };

    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(payload.clone());
    set_disks
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("object should be written");

    {
        let mut disks = set_disks.disks.write().await;
        disks[2] = None;
    }

    let range = HTTPRangeSpec {
        is_suffix_length: false,
        start: range_start as i64,
        end: (range_start + range_len - 1) as i64,
    };
    let mut get_reader = set_disks
        .get_object_reader(bucket, object, Some(range), HeaderMap::new(), &opts)
        .await
        .expect("range reader should open with one offline disk");
    let mut restored = Vec::new();
    get_reader
        .stream
        .read_to_end(&mut restored)
        .await
        .expect("range body should stream");

    assert_eq!(restored, payload[range_start..range_start + range_len]);
}

#[tokio::test]
async fn blackbox_delete_marker_hides_object_body_without_erasing_prior_version_metadata() {
    let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
    let bucket = "bb-delete-marker-read-negative";
    let object = "object.bin";
    let opts = ObjectOptions {
        no_lock: true,
        version_suspended: true,
        ..Default::default()
    };

    set_disks
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    let mut reader = PutObjReader::from_vec(b"body hidden by delete marker".to_vec());
    set_disks
        .put_object(bucket, object, &mut reader, &opts)
        .await
        .expect("object should be written before delete marker");

    let marker = set_disks
        .delete_object(bucket, object, opts.clone())
        .await
        .expect("version suspended delete should create a marker");
    assert!(marker.delete_marker);

    let err = match set_disks
        .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
        .await
    {
        Ok(_) => panic!("delete marker must hide the object body"),
        Err(err) => err,
    };
    assert!(
        matches!(&err, Error::ObjectNotFound(b, o) if b == bucket && o == object),
        "latest delete marker read must map to ObjectNotFound for the requested key, got {err:?}"
    );
}

#[tokio::test]
async fn blackbox_local_disk_walk_dir_emits_metadata_entries_with_prefix_forward_and_limit() {
    let dir = tempfile::tempdir().expect("tempdir should be created");
    let bucket = "bb-local-walk";
    let endpoint = Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("disk should be created");
    disk.make_volume(bucket).await.expect("bucket volume should be created");

    for object in ["prefix/a", "prefix/b", "prefix/c", "other/d"] {
        disk.write_all(bucket, &format!("{object}/xl.meta"), bytes::Bytes::from_static(b"meta"))
            .await
            .expect("metadata object should be written");
    }

    let (reader, mut writer) = tokio::io::duplex(4096);
    let opts = WalkDirOptions {
        bucket: bucket.to_string(),
        base_dir: "prefix/".to_string(),
        recursive: true,
        forward_to: Some("prefix/b".to_string()),
        limit: 2,
        ..Default::default()
    };

    disk.walk_dir(opts, &mut writer)
        .await
        .expect("walk_dir should stream metadata entries");
    MetacacheWriter::new(&mut writer)
        .close()
        .await
        .expect("metacache stream should close");
    drop(writer);

    let mut reader = MetacacheReader::new(reader);
    let entries = reader.read_all().await.expect("metacache stream should decode");
    let names = entries
        .into_iter()
        .filter(|entry| !entry.metadata.is_empty())
        .map(|entry| entry.name)
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["prefix/b".to_string(), "prefix/c".to_string()]);
}

#[tokio::test]
#[serial_test::serial]
async fn blackbox_issue3031_diag_covers_put_success_cleanup_and_error_summary() {
    temp_env::async_with_vars([("RUSTFS_ISSUE3031_DIAG_ENABLE", Some("true"))], async {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "bb-issue3031-diag";
        let object = "object.bin";
        let first_payload = b"first diagnostic body".to_vec();
        let second_payload = b"second diagnostic body that replaces the first".to_vec();
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");

        let mut first_reader = PutObjReader::from_vec(first_payload);
        set_disks
            .put_object(bucket, object, &mut first_reader, &opts)
            .await
            .expect("first diagnostic write should succeed");

        let mut second_reader = PutObjReader::from_vec(second_payload.clone());
        set_disks
            .put_object(bucket, object, &mut second_reader, &opts)
            .await
            .expect("overwrite should succeed and report cleanup-present diagnostics");

        let mut get_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("overwritten object reader should open");
        let mut restored = Vec::new();
        get_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("overwritten object should stream");
        assert_eq!(restored, second_payload);

        let (_failed_dirs, failed_set) = make_local_set_disks(1, 0).await;
        let failed_bucket = "bb-issue3031-fail";
        failed_set
            .make_bucket(failed_bucket, &MakeBucketOptions::default())
            .await
            .expect("failure bucket should be created");
        {
            let mut disks = failed_set.disks.write().await;
            disks[0] = None;
        }
        let mut failed_reader = PutObjReader::from_vec(b"must fail closed".to_vec());
        let err = failed_set
            .put_object(failed_bucket, "object.bin", &mut failed_reader, &opts)
            .await
            .expect_err("offline only disk must fail writer setup under diagnostics");
        assert!(
            matches!(err, Error::InsufficientWriteQuorum(_, _) | Error::ErasureWriteQuorum),
            "diagnostic failure should still report a write-quorum style error, got {err:?}"
        );
    })
    .await;
}

/// rustfs/backlog#1009: the `put_object` old-size backfill must reproduce, for
/// every PUT shape, exactly what the app layer's pre-PUT `get_object_info`
/// lookup would have observed — that is the invariant that lets the app skip
/// the lookup without changing a single usage-accounting number.
mod old_current_size_backfill {
    use super::*;
    use crate::disk::OldCurrentSize;
    use crate::error::is_err_object_not_found;
    use crate::set_disk::SetDisks;

    /// What the pre-PUT lookup would report right now for `object`.
    async fn prelookup_expectation(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) -> OldCurrentSize {
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        match set_disks.get_object_info(bucket, object, &opts).await {
            Ok(object_info) => OldCurrentSize::Present(object_info.size),
            Err(err) => {
                assert!(is_err_object_not_found(&err), "prelookup expectation hit unexpected error: {err:?}");
                OldCurrentSize::Absent
            }
        }
    }

    async fn put_and_backfill(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        len: usize,
        opts: &ObjectOptions,
    ) -> Option<OldCurrentSize> {
        let payload = (0..len).map(|idx| ((idx * 31) % 251) as u8).collect::<Vec<_>>();
        let mut reader = PutObjReader::from_vec(payload);
        let (_object_info, backfill) = set_disks
            .put_object_with_old_current_size(bucket, object, &mut reader, opts)
            .await
            .expect("put_object should succeed");
        backfill
    }

    #[tokio::test]
    async fn backfill_matches_prelookup_for_every_put_shape() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "bb-old-current-size";
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        // Fresh key (inline-sized): both the lookup and the backfill say Absent.
        let object = "object.bin";
        assert_eq!(prelookup_expectation(&set_disks, bucket, object).await, OldCurrentSize::Absent);
        assert_eq!(
            put_and_backfill(&set_disks, bucket, object, 1024, &opts).await,
            Some(OldCurrentSize::Absent)
        );

        // Unversioned inline overwrite: the previous live size must surface.
        let expected = prelookup_expectation(&set_disks, bucket, object).await;
        assert_eq!(expected, OldCurrentSize::Present(1024));
        assert_eq!(put_and_backfill(&set_disks, bucket, object, 2048, &opts).await, Some(expected));

        // Non-inline overwrite (payload above the inline block threshold).
        let expected = prelookup_expectation(&set_disks, bucket, object).await;
        assert_eq!(expected, OldCurrentSize::Present(2048));
        assert_eq!(
            put_and_backfill(&set_disks, bucket, object, BLOCK_SIZE_V2 + 123, &opts).await,
            Some(expected)
        );

        // Overwriting a non-inline object reports its size back.
        let expected = prelookup_expectation(&set_disks, bucket, object).await;
        assert_eq!(expected, OldCurrentSize::Present((BLOCK_SIZE_V2 + 123) as i64));
        assert_eq!(put_and_backfill(&set_disks, bucket, object, 64, &opts).await, Some(expected));

        // Versioned writes: a new version over a live latest reports the
        // latest's size, not the incoming version's.
        let versioned = "versioned.bin";
        let first_version_opts = ObjectOptions {
            no_lock: true,
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        };
        assert_eq!(
            put_and_backfill(&set_disks, bucket, versioned, 111, &first_version_opts).await,
            Some(OldCurrentSize::Absent)
        );
        let expected = prelookup_expectation(&set_disks, bucket, versioned).await;
        assert_eq!(expected, OldCurrentSize::Present(111));
        let second_version_opts = ObjectOptions {
            no_lock: true,
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        };
        assert_eq!(
            put_and_backfill(&set_disks, bucket, versioned, 222, &second_version_opts).await,
            Some(expected)
        );

        // Delete-marker latest: today's lookup returns the marker as
        // Ok(size 0) — NOT not-found — and delete-marker creation never
        // decremented objects_count, so the backfill must reproduce
        // Present(0) to keep versioned accounting identical.
        set_disks
            .delete_object(
                bucket,
                versioned,
                ObjectOptions {
                    no_lock: true,
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned delete should create a delete marker");
        let expected = prelookup_expectation(&set_disks, bucket, versioned).await;
        assert_eq!(expected, OldCurrentSize::Present(0));
        let third_version_opts = ObjectOptions {
            no_lock: true,
            versioned: true,
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            ..Default::default()
        };
        assert_eq!(
            put_and_backfill(&set_disks, bucket, versioned, 333, &third_version_opts).await,
            Some(expected)
        );
    }

    /// Sub-quorum divergence through the real set-level fanout: when only 2 of
    /// 4 disks can still decode the destination's xl.meta (write quorum is 3),
    /// the PUT must still commit but the backfill must come back unknown —
    /// never a fabricated `Absent`/`Present` from a below-quorum vote.
    #[tokio::test]
    async fn backfill_is_unknown_below_quorum_but_put_succeeds() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "bb-old-current-subquorum";
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        let object = "object.bin";
        assert_eq!(
            put_and_backfill(&set_disks, bucket, object, 1024, &opts).await,
            Some(OldCurrentSize::Absent)
        );

        // Corrupt the committed xl.meta on two disks: those observations turn
        // unknown, leaving 2 definite votes < write quorum 3.
        {
            let disks = set_disks.disks.read().await;
            for disk in disks.iter().flatten().take(2) {
                let meta_path = disk.path().join(bucket).join(object).join("xl.meta");
                fs::write(&meta_path, b"not-an-xl-meta")
                    .await
                    .expect("xl.meta should be overwritable with garbage");
            }
        }

        let backfill = put_and_backfill(&set_disks, bucket, object, 2048, &opts).await;
        assert_eq!(backfill, None, "a below-quorum vote must surface as unknown");
    }
}
