mod native_abort_closure_tests {
    use super::*;
    use crate::disk::multipart_source::{
        MULTIPART_SOURCE_PROTOCOL_VERSION, MultipartAbortCertificate, MultipartAbortRound, MultipartSourceOperation,
        MultipartSourcePhase, MultipartSourceRequest, MultipartSourceResponse, MultipartSourceSnapshot, ProofVerificationContext,
        SourceGeneration,
    };
    use crate::layout::endpoints::{EndpointServerPools, PoolEndpoints};

    async fn operation(disk: &DiskStore, path: &str, operation: MultipartSourceOperation) -> MultipartSourceResponse {
        disk.recover_multipart_source(MultipartSourceRequest {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            path: path.to_owned(),
            operation,
        })
        .await
        .expect("real native Abort operation")
    }

    async fn source_snapshots(disks: &[DiskStore], path: &str) -> Vec<MultipartSourceSnapshot> {
        let mut snapshots = Vec::with_capacity(disks.len());
        for disk in disks {
            let MultipartSourceResponse::Inspected(inspection) =
                operation(disk, path, MultipartSourceOperation::Inspect { lineage: None }).await
            else {
                panic!("native Inspect response");
            };
            snapshots.push(inspection.snapshot.expect("actual source snapshot"));
        }
        snapshots
    }

    async fn prepare_certificate(disks: &[DiskStore], set: &SetDisks, path: &str) -> MultipartAbortCertificate {
        let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
        let authority = set.multipart_proof_verification_context();
        bind_multipart_proof_snapshot(&physical, &authority).expect("bind actual complete physical cohort");
        let snapshots = source_snapshots(disks, path).await;
        let round = MultipartAbortRound {
            lineage: snapshots[0].lineage.clone(),
            generation: SourceGeneration {
                epoch: snapshots
                    .iter()
                    .map(|snapshot| snapshot.generation.epoch)
                    .max()
                    .expect("nonempty cohort")
                    + 1,
                nonce: Uuid::new_v4(),
            },
            round: Uuid::new_v4(),
        };
        let mut proofs = Vec::with_capacity(disks.len());
        let mut disk_ids = Vec::with_capacity(disks.len());
        for (slot, disk) in disks.iter().enumerate() {
            let MultipartSourceResponse::AbortPrepared(proof) = operation(
                disk,
                path,
                MultipartSourceOperation::PrepareAbort {
                    expected: snapshots[slot].clone(),
                    round: round.clone(),
                },
            )
            .await
            else {
                panic!("native PrepareAbort response");
            };
            assert_eq!(proof.snapshot.physical_slot, slot);
            assert_eq!(proof.snapshot.phase, MultipartSourcePhase::Aborting);
            assert!(proof.snapshot.head_sha256.is_none() && proof.snapshot.capability_sha256.is_none());
            disk_ids.push(disk.get_disk_id().await.expect("actual disk ID").expect("formatted disk"));
            proofs.push(proof);
        }
        let certificate = MultipartAbortCertificate { round, disk_ids, proofs };
        certificate
            .verify_with_authority(&authority)
            .expect("complete N physical closure certificate");
        certificate
    }

    async fn cold_reopen(disks: Vec<DiskStore>, set: Arc<SetDisks>) -> (Vec<DiskStore>, Arc<SetDisks>) {
        let endpoints = disks.iter().map(|disk| disk.endpoint()).collect::<Vec<_>>();
        let format = set.format.clone();
        let old_context = Arc::downgrade(&set.ctx);
        let old_authority = match set.multipart_proof_verification_context() {
            ProofVerificationContext::Local(authority) => Arc::downgrade(&authority),
            ProofVerificationContext::RpcOnly => panic!("isolated native authority required"),
        };
        drop(set);
        drop(disks);
        assert!(old_context.upgrade().is_none(), "all old InstanceContext owners must die");
        assert!(
            old_authority.upgrade().is_none(),
            "no old local proof authority may hydrate cold evidence"
        );
        let mut reopened = Vec::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            reopened.push(
                new_disk(
                    endpoint,
                    &DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .expect("cold formatted disk reopen"),
            );
        }
        let context = Arc::new(crate::runtime::instance::InstanceContext::new());
        context.update_erasure_type(SetupType::Erasure).await;
        context
            .bind_multipart_proof_topology(&EndpointServerPools(vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 4,
                endpoints: endpoints.clone().into(),
                cmd_line: String::new(),
                platform: String::new(),
            }]))
            .expect("fresh complete physical topology");
        let cold = SetDisks::new_with_instance_ctx(
            "native-abort-cold-owner".to_owned(),
            Arc::new(RwLock::new(reopened.iter().cloned().map(Some).collect())),
            4,
            2,
            0,
            0,
            endpoints,
            format,
            Vec::new(),
            context,
        )
        .await;
        // Direct disk inspection needs the same verified instance binding that
        // production coordinator entry points establish before native RPCs.
        bind_multipart_proof_snapshot(
            &reopened.iter().cloned().map(Some).collect::<Vec<_>>(),
            &cold.multipart_proof_verification_context(),
        )
        .expect("bind the actual cold physical cohort to its new native proof authority");
        (reopened, cold)
    }

    async fn rename_fanout_slot_for_physical_source(
        set: &SetDisks,
        disks: &[DiskStore],
        bucket: &str,
        object: &str,
        upload: &str,
        physical_slot: usize,
    ) -> usize {
        let (source, metadata) = set
            .check_upload_id_exists(bucket, object, upload, true)
            .await
            .expect("read the actual physical source layout before fault injection");
        let fanout_slot = metadata[physical_slot]
            .erasure
            .index
            .checked_sub(1)
            .filter(|slot| *slot < disks.len())
            .expect("actual source uses a valid one-based erasure role");
        let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
        let (shuffled, _) = SetDisks::shuffle_disks_and_parts_metadata_by_index(&physical, &metadata, &source);
        assert_eq!(
            shuffled[fanout_slot]
                .as_ref()
                .expect("actual fanout participant")
                .get_disk_id()
                .await
                .expect("fanout disk ID"),
            disks[physical_slot].get_disk_id().await.expect("physical disk ID"),
            "rename fault indices follow erasure order, so select the actual physical producer explicitly"
        );
        fanout_slot
    }

    async fn originals(disks: &[DiskStore], path: &str) -> Vec<Bytes> {
        let mut raw = Vec::with_capacity(disks.len());
        for disk in disks {
            raw.push(
                disk.read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"))
                    .await
                    .expect("snapshot actual initial original bytes"),
            );
        }
        raw
    }

    async fn body(set: &SetDisks, bucket: &str, object: &str, opts: &ObjectOptions) -> Vec<u8> {
        let mut reader = set
            .get_object_reader(bucket, object, None, HeaderMap::new(), opts)
            .await
            .expect("read actual target body");
        let mut bytes = Vec::new();
        reader
            .stream
            .read_to_end(&mut bytes)
            .await
            .expect("read the whole actual target body");
        bytes
    }

    #[tokio::test]
    async fn native_abort_requires_all_originals_and_preserves_every_source_on_missing_witness() {
        for missing in [false, true] {
            let (dirs, disks, set) = hermetic_set_disks(4).await;
            let bucket = "native-abort-all-originals";
            let object = if missing { "missing" } else { "healthy" };
            make_bucket_on_all(&disks, bucket).await;
            let (upload, _) = stage_upload_with_create_opts(&set, bucket, object, &[0x64; 4096], &ObjectOptions::default()).await;
            let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
            let before = originals(&disks, &path).await;
            for disk in &disks {
                let fi = disk
                    .read_version("", RUSTFS_META_MULTIPART_BUCKET, &path, "", &ReadOptions::default())
                    .await
                    .expect("marked initial source");
                assert!(native_source_protocol(&fi).expect("known birth marker"));
            }
            if missing {
                disks[3]
                    .delete(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"), Default::default())
                    .await
                    .expect("model initial publication without one physical original witness");
            }
            let aborted = set
                .abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
                .await;
            if missing {
                assert!(
                    matches!(aborted, Err(StorageError::ErasureWriteQuorum | StorageError::InsufficientWriteQuorum(..))),
                    "marked native source cannot take the legacy read-quorum shortcut: {aborted:?}"
                );
                for (slot, dir) in dirs.iter().enumerate() {
                    assert!(
                        dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists(),
                        "no source root is deleted"
                    );
                    if slot != 3 {
                        assert_eq!(
                            disks[slot]
                                .read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"))
                                .await
                                .expect("unchanged actual source"),
                            before[slot]
                        );
                    }
                }
            } else {
                aborted.expect("healthy full-N native upload can be ordinarily aborted");
                for dir in dirs {
                    assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
                }
            }
        }
    }

    #[tokio::test]
    async fn native_abort_unknown_disk_keeps_original_bytes_and_never_starts_deletion() {
        let (dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-abort-unknown";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let (upload, _) = stage_upload_with_create_opts(&set, bucket, object, &[0x65; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        let before = originals(&disks, &path).await;
        let saved = set.disks.write().await[3].take();
        let failed = set
            .abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
            .await;
        assert!(failed.is_err(), "unknown actual participant cannot certify all-N closure");
        set.disks.write().await[3] = saved;
        assert_eq!(originals(&disks, &path).await, before);
        for dir in &dirs {
            assert!(dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
        }
        set.abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
            .await
            .expect("the same upload can abort once all actual witnesses return");
    }

    #[tokio::test]
    async fn native_abort_partial_prepare_cold_reopen_unifies_without_recreating_a_head() {
        let (dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-abort-partial-prepare";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let (upload, _) = stage_upload_with_create_opts(&set, bucket, object, &[0x66; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        let before = originals(&disks, &path).await;
        {
            let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
            bind_multipart_proof_snapshot(&physical, &set.multipart_proof_verification_context()).expect("real topology");
            let snapshot = source_snapshots(&disks, &path).await.remove(0);
            let round = MultipartAbortRound {
                lineage: snapshot.lineage.clone(),
                generation: SourceGeneration {
                    epoch: snapshot.generation.epoch + 1,
                    nonce: Uuid::new_v4(),
                },
                round: Uuid::new_v4(),
            };
            let response = operation(
                &disks[0],
                &path,
                MultipartSourceOperation::PrepareAbort {
                    expected: snapshot,
                    round,
                },
            )
            .await;
            assert!(matches!(response, MultipartSourceResponse::AbortPrepared(_)));
        }
        assert!(matches!(
            disks[0]
                .read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"))
                .await,
            Err(DiskError::FileNotFound)
        ));
        for (slot, disk) in disks.iter().enumerate() {
            assert_eq!(retained_native_multipart_original(disk, slot, bucket, object, &path).await, before[slot]);
        }
        let (reopened, cold) = cold_reopen(disks, set).await;
        cold.abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
            .await
            .expect("abandoned preparation converges to a new closed round under a new authority");
        for dir in dirs {
            assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
        }
        drop(reopened);
    }

    #[tokio::test]
    async fn native_abort_partial_certificate_commit_cold_reopen_preserves_full_n_authority() {
        let (dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-abort-partial-commit";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let (upload, _) = stage_upload_with_create_opts(&set, bucket, object, &[0x67; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        let before = originals(&disks, &path).await;
        {
            let certificate = prepare_certificate(&disks, &set, &path).await;
            let response = operation(
                &disks[0],
                &path,
                MultipartSourceOperation::CommitAbort {
                    expected: certificate.proofs[0].snapshot.clone(),
                    certificate,
                },
            )
            .await;
            assert!(matches!(response, MultipartSourceResponse::AbortCommitted(_)));
        }
        for (slot, disk) in disks.iter().enumerate() {
            assert_eq!(retained_native_multipart_original(disk, slot, bucket, object, &path).await, before[slot]);
        }
        let (_reopened, cold) = cold_reopen(disks, set).await;
        cold.abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
            .await
            .expect("one persisted complete N certificate resumes the all-copy barrier before cleanup");
        for dir in dirs {
            assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
        }
    }

    #[tokio::test]
    async fn stale_cleanup_reclaims_certified_one_and_two_disk_residuals_after_reopen() {
        for survivors in [1, 2] {
            let (dirs, disks, set) = hermetic_set_disks(4).await;
            let bucket = "native-stale-certified";
            let object = format!("survivors-{survivors}");
            make_bucket_on_all(&disks, bucket).await;
            let opts = ObjectOptions {
                max_parity: true,
                ..Default::default()
            };
            let (upload, _) = stage_upload_with_create_opts(&set, bucket, &object, &[0x61; 4096], &opts).await;
            let path = SetDisks::get_upload_id_dir(bucket, &object, &upload);
            let before = originals(&disks, &path).await;
            {
                let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
                let SourceCleanupAuthority::Native { certificate, snapshots } =
                    prepare_source_cleanup(&physical, bucket, &object, &path, true, &set.multipart_proof_verification_context())
                        .await
                        .expect("close every original and persist the certificate")
                else {
                    panic!("native upload must retain a native certificate")
                };
                for slot in 0..(4 - survivors) {
                    let response = operation(
                        &disks[slot],
                        &path,
                        MultipartSourceOperation::DeleteAborted {
                            expected: snapshots[slot].clone().expect("actual certified source"),
                            certificate: certificate.as_ref().clone(),
                        },
                    )
                    .await;
                    assert!(matches!(response, MultipartSourceResponse::AbortDeleted(_)));
                }
            }
            let (reopened, cold) = cold_reopen(disks, set).await;
            for slot in (4 - survivors)..4 {
                assert_eq!(
                    retained_native_multipart_original(&reopened[slot], slot, bucket, &object, &path).await,
                    before[slot]
                );
            }
            cold.lock_stale_multipart_cleanup(&path)
                .await
                .expect("complete certificate authorizes the background retry")
                .delete(&cold)
                .await
                .expect("background retry removes certified survivors");
            for dir in &dirs {
                assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            }
            assert!(
                cold.lock_stale_multipart_cleanup(&path).await.is_err(),
                "a repeated pass must not recreate the retired upload"
            );
        }
    }

    #[tokio::test]
    #[serial]
    async fn stale_certified_residual_survives_lock_loss_then_converges() {
        let target = Arc::new(std::sync::RwLock::new(None));
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| {
                Arc::new(SelectiveLockLossClient::new(Arc::clone(&target), Arc::clone(&refresh_calls))) as Arc<dyn LockClient>
            })
            .collect();
        let (dirs, disks, set) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "stale-certified-lock-loss";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let opts = ObjectOptions {
            max_parity: true,
            ..Default::default()
        };
        let (upload, _) = stage_upload_with_create_opts(&set, bucket, object, &[0x63; 4096], &opts).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        let (fi, _) = set
            .check_upload_id_exists(bucket, object, &upload, false)
            .await
            .expect("source metadata before closure");
        let part_path = format!("{path}/{}/part.1", fi.data_dir.expect("source data directory"));
        let original = originals(&disks, &path).await;
        let surviving_part = disks[3]
            .read_all(RUSTFS_META_MULTIPART_BUCKET, &part_path)
            .await
            .expect("snapshot surviving shard");
        {
            let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
            let SourceCleanupAuthority::Native { certificate, snapshots } =
                prepare_source_cleanup(&physical, bucket, object, &path, true, &set.multipart_proof_verification_context())
                    .await
                    .expect("all original participants close and persist the certificate")
            else {
                panic!("native certificate required")
            };
            let authority = set.multipart_proof_verification_context();
            let mut ids = certificate.disk_ids.iter().copied().map(Some).collect::<Vec<_>>();
            ids[0] = None;
            assert_eq!(
                crate::set_disk::core::multipart_source::abort_certificate_cohort(&certificate, &ids, &authority),
                Err(DiskError::ErasureWriteQuorum),
                "an unavailable participant is a retryable proof gap"
            );
            ids[1] = Some(Uuid::new_v4());
            assert_eq!(
                crate::set_disk::core::multipart_source::abort_certificate_cohort(&certificate, &ids, &authority),
                Err(DiskError::FileCorrupt),
                "a missing participant cannot hide a known identity conflict"
            );
            ids[0] = Some(Uuid::new_v4());
            ids[1] = Some(certificate.disk_ids[1]);
            assert_eq!(
                crate::set_disk::core::multipart_source::abort_certificate_cohort(&certificate, &ids, &authority),
                Err(DiskError::FileCorrupt),
                "a known different identity remains a certificate conflict"
            );
            for slot in 0..3 {
                let response = operation(
                    &disks[slot],
                    &path,
                    MultipartSourceOperation::DeleteAborted {
                        expected: snapshots[slot].clone().expect("certified source"),
                        certificate: certificate.as_ref().clone(),
                    },
                )
                .await;
                assert!(matches!(response, MultipartSourceResponse::AbortDeleted(_)));
            }
        }
        assert!(dirs[3].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
        let offline = set.disks.write().await[0]
            .take()
            .expect("take an original participant offline");
        let rejected = set.lock_stale_multipart_cleanup(&path).await;
        set.disks.write().await[0] = Some(offline);
        let err = rejected
            .err()
            .expect("a certificate must not substitute for an unavailable participant");
        assert!(
            matches!(err, StorageError::ErasureWriteQuorum | StorageError::InsufficientWriteQuorum(..)),
            "the missing participant must be reported as a quorum failure: {err:?}"
        );
        assert_eq!(retained_native_multipart_original(&disks[3], 3, bucket, object, &path).await, original[3]);
        assert_eq!(
            disks[3]
                .read_all(RUSTFS_META_MULTIPART_BUCKET, &part_path)
                .await
                .expect("surviving shard while a participant was offline"),
            surviving_part
        );
        tokio::time::pause();
        *target.write().expect("lock-loss target") =
            Some(rustfs_lock::ObjectKey::new(RUSTFS_META_MULTIPART_BUCKET, path.clone()));
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let guard = set
            .lock_stale_multipart_cleanup(&path)
            .await
            .expect("certificate admits the one-disk residual");
        tokio::time::advance(Duration::from_secs(11)).await;
        for _ in 0..20 {
            if guard.is_lock_lost() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(refresh_calls.load(Ordering::Acquire) > 0, "the real lock refresh must fail");
        assert!(guard.is_lock_lost(), "the cleanup guard must observe lock loss");
        // Real disk I/O must not run under Tokio's auto-advancing paused clock:
        // a pending blocking operation could otherwise exhaust its timeout.
        tokio::time::resume();
        let err = guard.delete(&set).await.expect_err("lock loss must fence physical deletion");
        assert!(matches!(err, StorageError::NamespaceLockQuorumUnavailable { .. }), "{err:?}");
        assert_eq!(retained_native_multipart_original(&disks[3], 3, bucket, object, &path).await, original[3]);
        assert_eq!(
            disks[3]
                .read_all(RUSTFS_META_MULTIPART_BUCKET, &part_path)
                .await
                .expect("surviving shard after loss"),
            surviving_part
        );

        *target.write().expect("restore lock service") = None;
        set.lock_stale_multipart_cleanup(&path)
            .await
            .expect("reacquire the cleanup lock")
            .delete(&set)
            .await
            .expect("the certified residual converges after lock recovery");
        for dir in &dirs {
            assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
        }
    }

    #[tokio::test]
    async fn native_abort_partial_delete_surviving_producer_confirms_current_and_historical_exact_target() {
        use crate::set_disk::core::io_primitives::{rename_fault_injection, rollback_fault_injection};
        for historical in [false, true] {
            let (dirs, disks, set) = hermetic_set_disks(4).await;
            let bucket = "native-abort-surviving-producer";
            let object = if historical { "historical" } else { "current" };
            let opts = ObjectOptions {
                versioned: historical,
                ..Default::default()
            };
            make_bucket_on_all(&disks, bucket).await;
            let expected_body = vec![0x68; 4096];
            let (upload, parts) = stage_upload_with_create_opts(&set, bucket, object, &expected_body, &opts).await;
            let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
            let before = originals(&disks, &path).await;
            let failed_fanout = rename_fanout_slot_for_physical_source(&set, &disks, bucket, object, &upload, 3).await;
            {
                let rename_fault = rename_fault_injection::fail_rename_on(object, &[failed_fanout]);
                let coordinator_fault =
                    rollback_fault_injection::arm(object, 0, rollback_fault_injection::Fault::CoordinatorPanic);
                set.clone()
                    .complete_multipart_upload(bucket, object, &upload, parts.clone(), &opts)
                    .await
                    .expect_err("crash the coordinator after the actual three producer publications");
                drop(coordinator_fault);
                drop(rename_fault);
            }
            drop(
                set.acquire_write_lock_diag("native_abort_producer_tail", bucket, object)
                    .await
                    .expect("actual object owners drain"),
            );
            let completed = set
                .get_object_info(bucket, object, &opts)
                .await
                .expect("the real three-producer target exists");
            for (slot, disk) in disks.iter().enumerate() {
                let actual = disk.read_version("", bucket, object, "", &ReadOptions::default()).await;
                if slot < 3 {
                    let actual = actual.expect("each of physical slots 0/1/2 really produced C");
                    let publication = publication_identity(&actual)
                        .expect("strict native target tuple")
                        .expect("actual C producer");
                    assert_eq!(publication.lineage.path, path);
                    assert_eq!(actual.erasure.index, actual.erasure.distribution[slot]);
                    assert_eq!(actual.data_dir, completed.data_dir);
                    assert_eq!(
                        actual.version_id.filter(|id| !id.is_nil()),
                        completed.version_id.filter(|id| !id.is_nil())
                    );
                } else {
                    assert!(
                        matches!(actual, Err(DiskError::FileNotFound | DiskError::FileVersionNotFound)),
                        "physical slot3 must have no C and cannot later supply a RootMissing C vote: {actual:?}"
                    );
                }
            }
            let target_opts = ObjectOptions {
                versioned: historical,
                version_id: completed
                    .version_id
                    .filter(|version| !version.is_nil())
                    .map(|version| version.to_string()),
                ..Default::default()
            };
            assert_eq!(body(&set, bucket, object, &target_opts).await, expected_body);
            if historical {
                set.put_object(
                    bucket,
                    object,
                    &mut PutObjReader::from_vec(vec![0x69; 37]),
                    &ObjectOptions {
                        versioned: true,
                        write_completion: crate::object_api::WriteCompletion::TailDrained,
                        ..Default::default()
                    },
                )
                .await
                .expect("real successor PUT makes C a historical exact target");
            }
            {
                let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
                let SourceCleanupAuthority::Native { certificate, snapshots } =
                    prepare_source_cleanup(&physical, bucket, object, &path, true, &set.multipart_proof_verification_context())
                        .await
                        .expect("real all-N closure and all-copy commit")
                else {
                    panic!("marked native closure");
                };
                for slot in [0, 1, 3] {
                    let response = operation(
                        &disks[slot],
                        &path,
                        MultipartSourceOperation::DeleteAborted {
                            expected: snapshots[slot].clone().expect("actual committed source"),
                            certificate: certificate.as_ref().clone(),
                        },
                    )
                    .await;
                    assert!(matches!(response, MultipartSourceResponse::AbortDeleted(_)));
                }
            }
            assert!(!dirs[0].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            assert!(!dirs[1].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            assert!(dirs[2].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            assert!(!dirs[3].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            let (reopened, cold) = cold_reopen(disks, set).await;
            assert_eq!(
                retained_native_multipart_original(&reopened[2], 2, bucket, object, &path).await,
                before[2]
            );
            let replayed = cold
                .replay_completed_multipart_upload(bucket, object, &upload, &parts, None, &opts)
                .await
                .expect("fresh Q must include the surviving Aborting true producer")
                .expect("exact current/historical completed C");
            assert!(replayed.multipart_completion_replayed);
            assert_eq!(replayed.etag, completed.etag);
            assert_eq!(replayed.version_id, completed.version_id);
            assert_eq!(body(&cold, bucket, object, &target_opts).await, expected_body);
            cold.abort_multipart_upload(bucket, object, &upload, &opts)
                .await
                .expect("certified original on one remaining root can finish cold cleanup");
            for dir in &dirs {
                assert!(!dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path).exists());
            }
            assert_eq!(body(&cold, bucket, object, &target_opts).await, expected_body);
            if historical {
                assert_eq!(body(&cold, bucket, object, &opts).await, vec![0x69; 37]);
            }
        }
    }

    #[tokio::test]
    async fn native_birth_upgrade_is_guarded_and_readonly_cleanup_does_not_mark_legacy() {
        let (_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-birth-upgrade";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let (upload, parts) = stage_upload_with_create_opts(&set, bucket, object, &[0x6a; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        pin_historical_unmarked_upload(&disks, bucket, &path).await;
        let before = originals(&disks, &path).await;
        set.check_multipart_upload_path_exists(bucket, object, &upload, &path, MultipartMetadataQuorum::Read, true)
            .await
            .expect("readonly legacy cleanup identity lookup");
        assert_eq!(
            originals(&disks, &path).await,
            before,
            "readonly discovery cannot upgrade or create a private state"
        );
        {
            let _guard = set
                .acquire_multipart_upload_write_lock("native_birth_upgrade", bucket, object, &upload, &ObjectOptions::default())
                .await
                .expect("canonical source upgrade guard");
            let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
            upgrade_legacy_source(&physical, bucket, object, &path, &set.multipart_proof_verification_context())
                .await
                .expect("exact-identity native upgrade through every physical source owner");
            for disk in &disks {
                let fi = disk
                    .read_version("", RUSTFS_META_MULTIPART_BUCKET, &path, "", &ReadOptions::default())
                    .await
                    .expect("marked source");
                assert!(native_source_protocol(&fi).expect("strict dual-family marker"));
            }
        }
        set.clone()
            .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
            .await
            .expect("upgraded legacy source publishes through the real native path");
        for disk in &disks {
            let fi = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("actual final metadata");
            assert!(
                !rustfs_utils::http::contains_key_str(&fi.metadata, crate::disk::multipart_source::NATIVE_SOURCE_PROTOCOL_SUFFIX),
                "the source-only protocol marker cannot leak into target equivalence metadata"
            );
        }
        assert_eq!(body(&set, bucket, object, &ObjectOptions::default()).await, vec![0x6a; 4096]);
    }
    #[tokio::test]
    async fn native_birth_partial_upgrade_cold_reopen_completes_without_a_quorum_dead_end() {
        let (_dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-birth-partial-upgrade";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let (upload, parts) = stage_upload_with_create_opts(&set, bucket, object, &[0x6b; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        pin_historical_unmarked_upload(&disks, bucket, &path).await;
        {
            let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
            bind_multipart_proof_snapshot(&physical, &set.multipart_proof_verification_context()).expect("actual topology");
            let snapshots = source_snapshots(&disks, &path).await;
            for slot in [0, 1] {
                let response = operation(
                    &disks[slot],
                    &path,
                    MultipartSourceOperation::UpgradeLegacy {
                        expected: snapshots[slot].clone(),
                    },
                )
                .await;
                assert!(matches!(response, MultipartSourceResponse::LegacyUpgraded(_)));
            }
            for (slot, disk) in disks.iter().enumerate() {
                let fi = disk
                    .read_version("", RUSTFS_META_MULTIPART_BUCKET, &path, "", &ReadOptions::default())
                    .await
                    .expect("actual partially upgraded source");
                assert_eq!(native_source_protocol(&fi).expect("known protocol aliases"), slot < 2);
            }
        }
        let (reopened, cold) = cold_reopen(disks, set).await;
        cold.clone()
            .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
            .await
            .expect("cold retry resumes a partial upgrade before dispatching native publication");
        assert_eq!(body(&cold, bucket, object, &ObjectOptions::default()).await, vec![0x6b; 4096]);
        for disk in &reopened {
            let final_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .expect("actual completed target");
            assert!(!rustfs_utils::http::contains_key_str(
                &final_info.metadata,
                crate::disk::multipart_source::NATIVE_SOURCE_PROTOCOL_SUFFIX
            ));
        }
    }
    #[tokio::test]
    async fn native_orphan_controls_veto_genuine_legacy_read_quorum_without_deleting_any_source_or_target() {
        use crate::set_disk::core::io_primitives::{rename_fault_injection, rollback_fault_injection};
        use std::{
            collections::BTreeMap,
            path::{Path, PathBuf},
        };

        fn exact_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
            let mut result = BTreeMap::new();
            if !root.exists() {
                return result;
            }
            let mut pending = vec![root.to_path_buf()];
            while let Some(path) = pending.pop() {
                let metadata = std::fs::symlink_metadata(&path).expect("inspect actual fixture tree");
                let relative = path
                    .strip_prefix(root)
                    .expect("fixture entry belongs to its exact root")
                    .to_path_buf();
                if metadata.is_dir() {
                    result.insert(relative, None);
                    pending.extend(
                        std::fs::read_dir(&path)
                            .expect("list actual fixture tree")
                            .map(|entry| entry.expect("actual fixture entry").path()),
                    );
                } else {
                    assert!(
                        metadata.is_file(),
                        "source and target fixtures contain only ordinary files and directories"
                    );
                    result.insert(relative, Some(std::fs::read(&path).expect("snapshot every actual file byte")));
                }
            }
            result
        }

        let (dirs, disks, set) = hermetic_set_disks(4).await;
        let bucket = "native-orphan-legacy-veto";
        let object = "object";
        make_bucket_on_all(&disks, bucket).await;
        let predecessor = vec![0x71; 4096];
        set.put_object(
            bucket,
            object,
            &mut PutObjReader::from_vec(predecessor.clone()),
            &ObjectOptions {
                write_completion: crate::object_api::WriteCompletion::TailDrained,
                ..Default::default()
            },
        )
        .await
        .expect("seed a real predecessor whose whole body remains readable");
        let (upload, parts) = stage_upload_with_create_opts(&set, bucket, object, &[0x72; 4096], &ObjectOptions::default()).await;
        let path = SetDisks::get_upload_id_dir(bucket, object, &upload);
        let initial = originals(&disks, &path).await;
        let producer_fanout = rename_fanout_slot_for_physical_source(&set, &disks, bucket, object, &upload, 0).await;
        let failed_fanout = (0..disks.len()).filter(|slot| *slot != producer_fanout).collect::<Vec<_>>();
        {
            let rename_fault = rename_fault_injection::fail_rename_on(object, &failed_fanout);
            let coordinator_fault = rollback_fault_injection::arm(object, 0, rollback_fault_injection::Fault::CoordinatorPanic);
            set.clone()
                .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
                .await
                .expect_err("stop the coordinator after exactly one actual native publication");
            drop(coordinator_fault);
            drop(rename_fault);
        }
        drop(
            set.acquire_write_lock_diag("orphan_legacy_veto_tail", bucket, object)
                .await
                .expect("actual publication owners drain"),
        );
        let produced = disks[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("read the actual single-producer target metadata");
        let publication = publication_identity(&produced)
            .expect("strict native publication tuple")
            .expect("real producer C");
        assert_eq!(publication.lineage.path, path);
        assert_eq!(produced.erasure.index, produced.erasure.distribution[0]);
        let challenge = Uuid::new_v4();
        let MultipartSourceResponse::TerminalConfirmed(proof) = operation(
            &disks[0],
            &path,
            MultipartSourceOperation::ConfirmTerminal {
                expected: None,
                publication: publication.clone(),
                challenge,
            },
        )
        .await
        else {
            panic!("real physical producer must return a native exact-target proof");
        };
        proof
            .verify_with_authority(&set.multipart_proof_verification_context())
            .expect("real native proof authority");
        assert_eq!(proof.kind, crate::disk::multipart_source::MultipartProofKind::TerminalTarget);
        assert_eq!(proof.challenge, challenge);
        assert_eq!(proof.snapshot.physical_slot, 0);
        assert_eq!(
            Some(proof.snapshot.disk_id),
            disks[0].get_disk_id().await.expect("actual producer disk ID")
        );
        assert_eq!(proof.snapshot.publication.as_ref(), Some(&publication));
        drop(proof);
        assert_eq!(retained_native_multipart_original(&disks[0], 0, bucket, object, &path).await, initial[0]);
        let source0 = dirs[0].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path);
        assert!(
            !source0.join(STORAGE_FORMAT_FILE).exists(),
            "the publisher really consumed its source head"
        );
        assert!(
            source0.join(".mpu-completions").is_dir(),
            "real independently owned native originals remain"
        );

        // Slots 1/2 use their own genuine historical originals. Slot 3 has no
        // authority, so a legacy decision depends on exactly two physical votes.
        pin_historical_unmarked_upload(&disks[1..], bucket, &path).await;
        let mut legacy_originals = Vec::new();
        for slot in [1, 2] {
            legacy_originals.push(
                disks[slot]
                    .read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"))
                    .await
                    .expect("pin the original belonging to this exact legacy physical slot"),
            );
            let source = dirs[slot].path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path);
            std::fs::create_dir(source.join(MULTIPART_RECOVERY_DIR)).expect("create the historical recovery directory");
            std::fs::rename(
                source.join(STORAGE_FORMAT_FILE),
                source.join(MULTIPART_RECOVERY_DIR).join(STORAGE_FORMAT_FILE),
            )
            .expect("retain this slot's genuine authority without duplicating votes");
        }
        disks[3]
            .delete(RUSTFS_META_MULTIPART_BUCKET, &format!("{path}/{STORAGE_FORMAT_FILE}"), Default::default())
            .await
            .expect("remove the fourth historical head without adding an authority vote");
        let physical = disks.iter().cloned().map(Some).collect::<Vec<_>>();
        let recovery = format!("{path}/{MULTIPART_RECOVERY_DIR}");
        let (metadata, errors) =
            SetDisks::read_all_fileinfo(&physical, bucket, RUSTFS_META_MULTIPART_BUCKET, &recovery, "", false, false, false)
                .await
                .expect("observe the actual legacy recovery fanout");
        assert_eq!(errors.iter().filter(|error| error.is_none()).count(), 2);
        assert_eq!(
            SetDisks::object_quorum_from_meta(&metadata, &errors, 2),
            Ok((2, 3)),
            "the genuine legacy files alone reach readQ"
        );
        for (index, slot) in [1, 2].into_iter().enumerate() {
            assert_eq!(metadata[slot].erasure.index, metadata[slot].erasure.distribution[slot]);
            assert!(!recognized_native_source_controls(&metadata[slot]).expect("historical recovery files remain unmarked"));
            assert_eq!(
                disks[slot]
                    .read_all(RUSTFS_META_MULTIPART_BUCKET, &format!("{recovery}/{STORAGE_FORMAT_FILE}"))
                    .await
                    .expect("actual same-slot recovery bytes"),
                legacy_originals[index]
            );
        }
        std::fs::remove_file(source0.join(".mpu-source-state")).expect("inject loss of only the native state record");
        let source_roots = dirs
            .iter()
            .map(|dir| dir.path().join(RUSTFS_META_MULTIPART_BUCKET).join(&path))
            .collect::<Vec<_>>();
        let target_roots = dirs
            .iter()
            .map(|dir| dir.path().join(bucket).join(object))
            .collect::<Vec<_>>();
        let sources_before = source_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>();
        let targets_before = target_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>();
        let inspected = disks[0]
            .recover_multipart_source(MultipartSourceRequest {
                protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
                path: path.clone(),
                operation: MultipartSourceOperation::Inspect { lineage: None },
            })
            .await;
        assert!(
            matches!(inspected, Err(DiskError::ConditionalDeleteConflict)),
            "known orphan native evidence remains unknown: {inspected:?}"
        );
        let readonly = set
            .check_multipart_upload_path_exists(bucket, object, &upload, &path, MultipartMetadataQuorum::Read, true)
            .await;
        assert!(
            matches!(
                readonly,
                Err(StorageError::ErasureReadQuorum
                    | StorageError::ErasureWriteQuorum
                    | StorageError::InsufficientReadQuorum(..)
                    | StorageError::InsufficientWriteQuorum(..))
            ),
            "one orphan native slot vetoes legacy fallback despite a genuine readQ: {readonly:?}"
        );
        assert_eq!(
            source_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>(),
            sources_before,
            "readonly classification changes zero source bytes"
        );
        assert_eq!(target_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>(), targets_before);
        let aborted = set
            .abort_multipart_upload(bucket, object, &upload, &ObjectOptions::default())
            .await;
        assert!(
            matches!(
                aborted,
                Err(StorageError::ErasureReadQuorum
                    | StorageError::ErasureWriteQuorum
                    | StorageError::InsufficientReadQuorum(..)
                    | StorageError::InsufficientWriteQuorum(..))
            ),
            "uncertain native state cannot authorize legacy source deletion: {aborted:?}"
        );
        assert_eq!(
            source_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>(),
            sources_before,
            "ordinary Abort deletes no source and preserves every original/part/control byte"
        );
        assert_eq!(
            target_roots.iter().map(|root| exact_tree(root)).collect::<Vec<_>>(),
            targets_before,
            "Abort never mutates any target metadata, shard or native anchor"
        );
        assert_eq!(body(&set, bucket, object, &ObjectOptions::default()).await, predecessor);
    }
    mod initial_probe_tests {
        include!("multipart_initial_probe_tests.rs");
    }
}
