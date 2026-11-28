// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::table_catalog::tests::prepared_commit::{native_vector_index_fixture, run_native_vector_delete_test};
use crate::table_catalog::*;
use std::sync::atomic::AtomicUsize;
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn observed_task(
    observer: &Arc<VectorMaintenanceCompletionObserver>,
    work: &VectorMaintenanceWork,
) -> VectorMaintenanceTaskObservation {
    observer.begin(
        work.snapshot().job().resource(),
        Some(work.lease().job_id()),
        Some(*work.snapshot().job().kind()),
        VectorMaintenanceObservedOperation::Job,
    )
}

async fn check_native_poll_passthrough(work: &VectorMaintenanceWork, binding: &VectorMaintenanceGcLockBinding) {
    let observer = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let owner = Arc::new(());
    let original_owner = owner.clone();
    scope(Some(observed_task(&observer, work)), async {
        claimed(work);
        let mut polls = 0;
        let inner = std::future::poll_fn(|cx| {
            assert!(cx.waker().will_wake(&waker));
            // This read also requires that the observer mutex is absent during the native poll.
            let current = observer.gc_lock_snapshot();
            assert_eq!(current.acquisitions[0].poll_count, polls);
            if polls == 0 {
                assert!(current.acquisitions[0].first_pending.is_none());
            }
            polls += 1;
            if polls < 3 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok::<_, LockError>(original_owner.clone()))
            }
        });
        let observed =
            observe_native_gc_write_lock(&binding.lock_resource, "original-native-owner", Duration::from_secs(5), inner);
        let scoped = with_gc_acquisition(binding.clone(), observed);
        let mut scoped = std::pin::pin!(scoped);
        let mut cx = Context::from_waker(&waker);
        assert!(scoped.as_mut().poll(&mut cx).is_pending());
        let first = observer.gc_lock_snapshot();
        assert_eq!(first.acquisitions[0].poll_count, 1);
        assert!(first.acquisitions[0].first_pending.is_some());
        assert!(first.acquisitions[0].terminal.is_none());
        let pending_sequence = first.acquisitions[0].first_pending.unwrap().sequence;
        assert!(scoped.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            observer.gc_lock_snapshot().acquisitions[0].first_pending.unwrap().sequence,
            pending_sequence
        );
        let Poll::Ready(Ok(returned)) = scoped.as_mut().poll(&mut cx) else {
            panic!("original future must complete")
        };
        assert!(Arc::ptr_eq(&returned, &owner));
        assert_eq!(wake.0.load(Ordering::Relaxed), 2);
    })
    .await;
    let snapshot = observer.gc_lock_snapshot();
    let record = &snapshot.acquisitions[0];
    assert!(!snapshot.overflow && !snapshot.context_error);
    assert_eq!(record.poll_count, 3);
    assert!(matches!(record.terminal, Some(VectorMaintenanceGcLockTerminal::ReadyOk)));
    assert!(record.first_poll.unwrap().sequence < record.first_pending.unwrap().sequence);
    assert!(record.first_pending.unwrap().sequence < record.terminal_moment.unwrap().sequence);
    assert!(!record.cancelled);
}

async fn check_original_errors_and_cancel(work: &VectorMaintenanceWork, binding: &VectorMaintenanceGcLockBinding) {
    let observer = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
    let source = Box::new(std::io::Error::from_raw_os_error(5));
    let source_address = std::ptr::from_ref(source.as_ref());
    let original = LockError::Network {
        message: "native failure".into(),
        source,
    };
    let result = scope(Some(observed_task(&observer, work)), async {
        claimed(work);
        with_gc_acquisition(
            binding.clone(),
            observe_native_gc_write_lock(
                &binding.lock_resource,
                "native-error-owner",
                Duration::from_secs(5),
                std::future::ready(Err::<(), _>(original)),
            ),
        )
        .await
    })
    .await;
    let LockError::Network { source, .. } = result.unwrap_err() else { panic!("original error kind") };
    assert_eq!(std::ptr::from_ref(source.downcast_ref::<std::io::Error>().unwrap()), source_address);
    let snapshot = observer.gc_lock_snapshot();
    let record = &snapshot.acquisitions[0];
    assert!(record.first_pending.is_none());
    assert!(matches!(record.terminal, Some(VectorMaintenanceGcLockTerminal::ReadyErr)));
    let error = record.error.as_ref().unwrap();
    assert!(matches!(error.kind, VectorMaintenanceGcLockErrorKind::Network));
    assert_eq!(error.sources[0].io_raw_os_error, Some(5));

    let dropped = Arc::new(AtomicUsize::new(0));
    struct PendingOwner(Arc<AtomicUsize>);
    impl Drop for PendingOwner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl Future for PendingOwner {
        type Output = Result<(), LockError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    scope(Some(observed_task(&observer, work)), async {
        claimed(work);
        let waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
        let mut cx = Context::from_waker(&waker);
        {
            let original = PendingOwner(dropped.clone());
            let observed =
                observe_native_gc_write_lock(&binding.lock_resource, "cancelled-owner", Duration::from_secs(5), original);
            let mut scoped = std::pin::pin!(with_gc_acquisition(binding.clone(), observed));
            assert!(scoped.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    })
    .await;
    let snapshot = observer.gc_lock_snapshot();
    assert_eq!(snapshot.acquisitions.len(), 2);
    assert!(snapshot.acquisitions[1].cancelled);
    assert!(matches!(
        snapshot.acquisitions[1].terminal,
        Some(VectorMaintenanceGcLockTerminal::Cancelled)
    ));
    assert_eq!(snapshot.acquisitions[1].poll_count, 1);
}

async fn check_invalid_context_and_capacity(work: &VectorMaintenanceWork, binding: &VectorMaintenanceGcLockBinding) {
    for mismatch in 0..5 {
        let observer = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
        let mut bad = binding.clone();
        match mismatch {
            0 => bad.job_id = Uuid::new_v4(),
            1 => bad.resource.table_id.push_str("-other"),
            2 => bad.kind = VectorMaintenanceKind::AnnBuild,
            _ => (),
        }
        let task = observed_task(&observer, work);
        let result = scope(Some(task), async {
            if mismatch != 3 {
                claimed(work);
            }
            let native_key = if mismatch == 4 {
                ObjectKey::new("other", "key")
            } else {
                binding.lock_resource.clone()
            };
            with_gc_acquisition(
                bad,
                observe_native_gc_write_lock(
                    &native_key,
                    "native-owner",
                    Duration::from_secs(5),
                    std::future::ready(Ok::<_, LockError>(19)),
                ),
            )
            .await
        })
        .await
        .unwrap();
        assert_eq!(result, 19);
        let snapshot = observer.gc_lock_snapshot();
        assert!(snapshot.context_error);
        assert!(snapshot.acquisitions.is_empty());
    }

    let duplicate = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
    scope(Some(observed_task(&duplicate, work)), async {
        claimed(work);
        with_gc_acquisition(binding.clone(), async {
            for value in [31, 32] {
                assert_eq!(
                    observe_native_gc_write_lock(
                        &binding.lock_resource,
                        "same-scope",
                        Duration::from_secs(5),
                        std::future::ready(Ok::<_, LockError>(value))
                    )
                    .await
                    .unwrap(),
                    value
                );
            }
        })
        .await;
    })
    .await;
    assert!(duplicate.gc_lock_snapshot().context_error);
    assert_eq!(duplicate.gc_lock_snapshot().acquisitions.len(), 1);
    assert_eq!(
        observe_native_gc_write_lock(
            &binding.lock_resource,
            "outside-task",
            Duration::from_secs(5),
            std::future::ready(Ok::<_, LockError>(37))
        )
        .await
        .unwrap(),
        37
    );
    assert_eq!(duplicate.gc_lock_snapshot().acquisitions.len(), 1);

    let missing_instance = VectorMaintenanceCompletionObserver::new(|_| None);
    scope(Some(observed_task(&missing_instance, work)), async {
        claimed(work);
        assert!(
            with_gc_acquisition(
                binding.clone(),
                observe_native_gc_write_lock(
                    &binding.lock_resource,
                    "owner",
                    Duration::from_secs(5),
                    std::future::ready(Ok::<_, LockError>(())),
                )
            )
            .await
            .is_ok()
        );
    })
    .await;
    assert!(missing_instance.gc_lock_snapshot().context_error);

    let observer = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
    for _ in 0..=MAX_GC_ACQUISITIONS {
        scope(Some(observed_task(&observer, work)), async {
            claimed(work);
            assert_eq!(
                with_gc_acquisition(
                    binding.clone(),
                    observe_native_gc_write_lock(
                        &binding.lock_resource,
                        "owner",
                        Duration::from_secs(5),
                        std::future::ready(Ok::<_, LockError>(23)),
                    )
                )
                .await
                .unwrap(),
                23
            );
        })
        .await;
    }
    let snapshot = observer.gc_lock_snapshot();
    assert!(snapshot.overflow);
    assert_eq!(snapshot.acquisitions.len(), MAX_GC_ACQUISITIONS);
    assert!(serde_json::to_vec(&snapshot).unwrap().len() < 4 * 1024 * 1024);

    let oversized = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, Uuid::new_v4());
    let mut large = binding.clone();
    large.targets[0].object_key = "x".repeat(MAX_GC_BINDING_BYTES);
    scope(Some(observed_task(&oversized, work)), async {
        claimed(work);
        assert!(
            with_gc_acquisition(large, std::future::ready(Ok::<(), LockError>(())))
                .await
                .is_ok()
        );
    })
    .await;
    assert!(oversized.gc_lock_snapshot().overflow);
    assert!(oversized.gc_lock_snapshot().acquisitions.is_empty());
}

#[test]
#[serial_test::serial]
fn gc_lock_observer_preserves_native_wait_owner_and_bounded_context() {
    run_native_vector_delete_test("gc-lock-observer", || async {
        let (catalog, original, _fixture) = native_vector_index_fixture().await;
        let storage = crate::app::gating_test_env::shared_gating_ecstore().await;
        let entry = VectorCatalogEntry::from_entry(original).unwrap();
        let resource = entry.head().resource_identity().clone();
        let entry = catalog
            .enroll_vector_maintenance(
                &entry,
                vector_maintenance_enroll_commit_id(&resource, &VectorMaintenanceHeadV1::from_entry(&entry).unwrap()).unwrap(),
            )
            .await
            .unwrap();
        let (preparation, _) = catalog
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let key = format!("{}data/gc-observer-target.bin", resource.dataset_prefix());
        let mut reader = PutObjReader::from_vec(b"native-gc-observation".to_vec());
        storage
            .put_object(
                resource.storage_bucket_id(),
                &key,
                &mut reader,
                &ObjectOptions {
                    mod_time: Some(time::OffsetDateTime::now_utc() - time::Duration::seconds(1800)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let metadata = catalog
            .backend
            .object_metadata(resource.storage_bucket_id(), &key)
            .await
            .unwrap()
            .unwrap();
        let target =
            VectorRetirementTargetV1::from_object_metadata(&resource, CatalogObjectKey::try_new(key).unwrap(), &metadata)
                .unwrap();
        let recorded_resource =
            VectorMaintenanceResourceV1::try_new(&resource, entry.head().namespace(), entry.head().name()).unwrap();
        let evidence = VectorRetirementEvidenceV1::try_new(
            recorded_resource.clone(),
            VectorMaintenanceHeadV1::from_entry(&entry).unwrap(),
            None,
            vec![entry.head().metadata().into()],
            vec![entry.head().metadata().into()],
            Vec::new(),
            VectorHistoryRetentionV1::All,
        )
        .unwrap();
        let prepared = catalog
            .prepare_vector_retirement(&preparation, evidence, vec![target.clone()])
            .await
            .unwrap();
        let plan_ref = CatalogMetadataRefRecordV1::from(prepared.plan_ref());
        let spec = VectorMaintenanceJobSpec::from_semantic(
            VectorMaintenanceSemanticSpec::try_new(
                recorded_resource.clone(),
                VectorMaintenanceKind::OrphanGc,
                VectorMaintenanceOrigin::Periodic,
                VectorMaintenanceInputV1::try_new(
                    Some(VectorMaintenanceHeadV1::from_entry(&entry).unwrap()),
                    None,
                    Some(plan_ref.clone()),
                )
                .unwrap(),
                VectorMaintenanceExecutionIdentityV1::try_new("gc-observer-user".into(), [61; 16]).unwrap(),
                None,
                VectorMaintenancePolicyV1::default(),
            )
            .unwrap(),
        )
        .unwrap();
        drop(preparation);
        let queued = catalog.create_vector_maintenance_job(spec).await.unwrap();
        let (selector_guard, _) = catalog
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let selector = catalog
            .read_vector_maintenance_test_gc_lock_with_guard(&selector_guard, *queued.job().job_id())
            .await
            .unwrap();
        assert_eq!(selector.job_object, CatalogMetadataRefRecordV1::from(queued.observed_object()));
        assert_eq!(selector.plan_ref, plan_ref);
        assert_eq!(selector.targets, vec![target.clone()]);
        assert!(
            catalog
                .read_vector_maintenance_test_gc_lock_with_guard(&selector_guard, Uuid::new_v4())
                .await
                .is_err()
        );
        drop(selector_guard);
        let current = catalog
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        let worker = VectorMaintenanceWorker::try_new(
            "gc-observer-worker".into(),
            Uuid::new_v4(),
            queued.job().execution_identity().clone(),
        )
        .unwrap();
        let VectorMaintenanceClaimOutcome::Acquired(work) = catalog
            .claim_vector_maintenance_job(
                &resource,
                *queued.job().job_id(),
                VectorMaintenanceClaimCondition::from_snapshot(&queued, current),
                worker,
                Uuid::new_v4(),
            )
            .await
            .unwrap()
        else {
            panic!("original GC claim")
        };
        let dispatch = catalog.begin_vector_maintenance_dispatch(work.lease()).await.unwrap();
        let (query, _) = catalog
            .begin_vector_operation(resource.vector_bucket().unwrap(), entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        assert!(
            catalog
                .read_vector_maintenance_test_gc_lock_with_guard(&query, work.lease().job_id())
                .await
                .is_err()
        );
        let observer = VectorMaintenanceCompletionObserver::new_for_instance(|_| None, storage.id);
        let job = catalog.read_vector_maintenance_job_with_guard(&dispatch).await.unwrap();
        let task = observed_task(&observer, &work);
        let publication = scope(Some(task), async {
            claimed(&work);
            catalog
                .publish_vector_retirement(
                    &dispatch,
                    prepared,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    CatalogCommitId::try_new(Uuid::new_v4().to_string()).unwrap(),
                )
                .await
        });
        let watch = async {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut query = Some(query);
            loop {
                let snapshot = observer.gc_lock_snapshot();
                assert!(!snapshot.overflow && !snapshot.context_error);
                if let Some(record) = snapshot.acquisitions.first().filter(|record| record.first_pending.is_some()) {
                    assert!(record.terminal.is_none());
                    assert_eq!(record.binding.job_object, CatalogMetadataRefRecordV1::from(job.observed_object()));
                    assert_eq!(record.binding.job_revision, *job.job().revision());
                    assert_eq!(record.binding.lease, *work.lease());
                    assert_eq!(record.binding.resource, recorded_resource);
                    assert_eq!(record.binding.plan_ref, plan_ref);
                    assert_eq!(record.binding.targets, vec![target.clone()]);
                    assert_eq!(record.native_resource, record.binding.lock_resource);
                    assert_eq!(record.native_resource, selector.lock_resource);
                    assert_eq!(record.binding.evidence_ref, selector.evidence_ref);
                    assert!(!record.native_owner.is_empty());
                    assert_eq!(record.acquire_timeout, get_lock_acquire_timeout());
                    assert_eq!(query.as_ref().unwrap().resource_identity(), &resource);
                    query.as_ref().unwrap().check_held().unwrap();
                    drop(query.take());
                    return;
                }
                assert!(Instant::now() < deadline, "original native Pending must be observed");
                tokio::task::yield_now().await;
            }
        };
        let (result, ()) = tokio::join!(publication, watch);
        let authorization = result.unwrap();
        let snapshot = observer.gc_lock_snapshot();
        assert_eq!(snapshot.instance_id, Some(storage.id));
        assert!(!snapshot.overflow && !snapshot.context_error);
        assert_eq!(snapshot.acquisitions.len(), 1);
        let record = &snapshot.acquisitions[0];
        assert!(matches!(record.terminal, Some(VectorMaintenanceGcLockTerminal::ReadyOk)));
        assert!(record.first_pending.unwrap().sequence < record.terminal_moment.unwrap().sequence);
        assert!(!record.cancelled);
        let current = catalog
            .read_vector_maintenance_resource(&resource, entry.head().namespace(), entry.head().name())
            .await
            .unwrap();
        assert_eq!(current.maintenance().unwrap().retirement_tip, Some((&authorization).into()));
        drop(dispatch);
        check_native_poll_passthrough(&work, &record.binding).await;
        check_original_errors_and_cancel(&work, &record.binding).await;
        check_invalid_context_and_capacity(&work, &record.binding).await;
    });
}

#[test]
fn gc_lock_error_projection_bounds_original_error_text() {
    let error = LockError::Timeout {
        resource: "exact-gc-resource".into(),
        timeout: Duration::from_secs(5),
    };
    let (failure, overflow) = gc_failure(&error);
    assert!(!overflow);
    assert!(matches!(failure.unwrap().kind, VectorMaintenanceGcLockErrorKind::Timeout));
    let error = LockError::Internal {
        message: "x".repeat(MAX_MESSAGE_BYTES * 2),
    };
    let (_, overflow) = gc_failure(&error);
    assert!(overflow);
}
