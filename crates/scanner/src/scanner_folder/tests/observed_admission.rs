// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::storage_api::EcstoreHealResultItem as HealItem;
use crate::storage_api::owner::DiskSetSelector;
use crate::storage_api::scanner_io::BucketInfo;
use crate::workload_admission::with_scanner_node_requests;
use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use rustfs_heal::heal::channel::HealChannelProcessor;
use rustfs_heal::heal::manager::{HealConfig, HealManager};
use rustfs_heal::heal::storage::{HealListItem, HealObjectInfo, HealStorageAPI};
use rustfs_heal_contracts::completion::{HealCompletion, publish_observed_heal_completion};
use rustfs_heal_contracts::heal_channel::{
    HealObservationChannelError, HealOpts, HealReceiptCommand, ObservedHealAdmissionReceipt, ObservedHealReceiptCommand,
    ObservedHealReceiptReceiver,
};
use rustfs_heal_contracts::outcome::HealExecutionOutcome;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

const TEST_NAME: &str =
    "scanner_folder::tests::observed_admission::observed_receipts_survive_node_cancellation_and_fast_terminal";
const DROP_TEST_NAME: &str = "scanner_folder::tests::observed_admission::scanner_item_drop_after_send_records_unknown_once";
const UNSENT_TEST_NAME: &str =
    "scanner_folder::tests::observed_admission::scanner_item_closed_channels_are_not_uncertain_admission";
const CHILD_ENV: &str = "RUSTFS_SCANNER_OBSERVED_ADMISSION_TEST_CHILD";
const WAIT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct AdmissionOnlyStorage(AtomicUsize);

impl AdmissionOnlyStorage {
    fn unexpected<T>(&self) -> rustfs_heal::Result<T> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(rustfs_heal::Error::other("admission fixture must not access storage"))
    }
}

#[async_trait::async_trait]
impl HealStorageAPI for AdmissionOnlyStorage {
    async fn get_object_meta(&self, _: &str, _: &str) -> rustfs_heal::Result<Option<HealObjectInfo>> {
        self.unexpected()
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> rustfs_heal::Result<Vec<u8>> {
        self.unexpected()
    }
    async fn get_bucket_info(&self, _: &str) -> rustfs_heal::Result<Option<BucketInfo>> {
        self.unexpected()
    }
    async fn list_buckets(&self) -> rustfs_heal::Result<Vec<BucketInfo>> {
        self.unexpected()
    }
    async fn list_buckets_for_admin_heal(&self, _: Option<DiskSetSelector>) -> rustfs_heal::Result<Vec<BucketInfo>> {
        self.unexpected()
    }
    async fn object_exists(&self, _: &str, _: &str) -> rustfs_heal::Result<bool> {
        self.unexpected()
    }
    async fn heal_object(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: &HealOpts,
    ) -> rustfs_heal::Result<(HealItem, Option<rustfs_heal::Error>)> {
        self.unexpected()
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> rustfs_heal::Result<HealItem> {
        self.unexpected()
    }
    async fn heal_format(&self, _: bool) -> rustfs_heal::Result<(HealItem, Option<rustfs_heal::Error>)> {
        self.unexpected()
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> rustfs_heal::Result<(Vec<HealListItem>, Option<String>, bool)> {
        self.unexpected()
    }
    async fn get_disk_for_resume(&self, _: &str) -> rustfs_heal::Result<crate::DiskStore> {
        self.unexpected()
    }
}

fn node() -> Arc<NodeAdmission> {
    let node = NodeAdmission::new(NonZeroU16::new(1).expect("one node unit"));
    node.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    node
}

fn request(object: &str) -> HealChannelRequest {
    build_object_heal_request(
        "bucket".to_owned(),
        object.to_owned(),
        Some(Uuid::from_u128(42).to_string()),
        HealScanMode::Deep,
        HealChannelPriority::High,
    )
}

type ObservedReply = oneshot::Sender<Result<ObservedHealAdmissionReceipt, HealObservationChannelError>>;

async fn relay_observed(
    incoming: &mut ObservedHealReceiptReceiver,
    processor: &mpsc::UnboundedSender<ObservedHealReceiptCommand>,
) -> (ObservedReply, String, ObservedHealAdmissionReceipt) {
    let command = tokio::time::timeout(WAIT, incoming.recv())
        .await
        .expect("observed command arrives")
        .expect("observed channel remains open");
    let request_id = command.request.id.clone();
    let (response_tx, response_rx) = oneshot::channel();
    processor
        .send(ObservedHealReceiptCommand {
            request: command.request,
            response_tx,
        })
        .expect("processor channel remains open");
    let receipt = tokio::time::timeout(WAIT, response_rx)
        .await
        .expect("real manager decides admission")
        .expect("processor replies")
        .expect("manager admission succeeds");
    (command.response_tx, request_id, receipt)
}

async fn wait_status(
    predicate: impl Fn(&crate::heal_completion::ScannerHealObservationStatus) -> bool,
) -> crate::heal_completion::ScannerHealObservationStatus {
    tokio::time::timeout(WAIT, async {
        loop {
            let status = crate::heal_completion::scanner_heal_observation_status();
            if predicate(&status) {
                return status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observer reaches expected state")
}

fn unknown_count(status: &crate::heal_completion::ScannerHealObservationStatus, reason: &str) -> u64 {
    status
        .unknown_candidates
        .iter()
        .find(|row| row.reason == reason)
        .map_or(0, |row| row.count)
}

fn terminal_count(status: &crate::heal_completion::ScannerHealObservationStatus) -> u64 {
    status.terminal_candidates.iter().map(|row| row.count).sum()
}

fn publish_synthetic_terminal(receipt: &ObservedHealAdmissionReceipt) {
    publish_observed_heal_completion(
        HealCompletion {
            task_id: receipt.task_id.clone(),
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: Some(HealExecutionOutcome::Completed),
            object: None,
        },
        receipt.observation_id,
    );
}

fn run_isolated(test_name: &str) -> bool {
    if std::env::var(CHILD_ENV).ok().as_deref() == Some(test_name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, test_name)
        .output()
        .expect("isolated scanner admission fixture");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success() && stdout.contains("1 passed;"), "{stdout}\n{stderr}");
    true
}

#[tokio::test]
async fn observed_receipts_survive_node_cancellation_and_fast_terminal() {
    // Channel senders are process-global; an exact child keeps this fixture isolated.
    if run_isolated(TEST_NAME) {
        return;
    }

    let (commands, mut legacy, mut observed) =
        rustfs_heal_contracts::heal_channel::init_heal_channels_with_observations().expect("isolated observed channel");
    let storage = Arc::new(AdmissionOnlyStorage::default());
    let manager = Arc::new(HealManager::new(
        storage.clone(),
        Some(HealConfig {
            queue_size: 16,
            low_priority_merge_enable: true,
            event_driven_scheduler_enable: false,
            enable_auto_heal: false,
            ..HealConfig::default()
        }),
    ));
    // The processor admits work; the manager scheduler and recovery are not started.
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let (legacy_tx, legacy_rx) = mpsc::unbounded_channel();
    let mut processor = HealChannelProcessor::new(manager.clone());
    let processor_task =
        tokio::spawn(async move { processor.start_with_observed_receipts(commands, legacy_rx, observed_rx).await });

    let waiting_node = node();
    let occupied = tokio::time::timeout(WAIT, waiting_node.acquire())
        .await
        .expect("first node unit is acquired");
    let (node_tx, mut node_rx) = mpsc::channel(1);
    let before_cancel = CancellationToken::new();
    let mut before = Box::pin(with_scanner_node_requests(
        Some(node_tx),
        run_scanner_node_operation(
            Some(&waiting_node),
            &before_cancel,
            send_scanner_heal_request("object", request("not-admitted")),
        ),
    ));
    assert!(futures::poll!(before.as_mut()).is_pending());
    let queued_node = tokio::time::timeout(WAIT, node_rx.recv())
        .await
        .expect("node request arrives")
        .expect("node coordinator remains open");
    before_cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(WAIT, before).await.expect("waiting operation cancels"),
        Err(ScannerError::Other(message)) if message == "Operation cancelled"
    ));
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 0);
    drop(queued_node);
    occupied.complete();

    let owner_request = request("object");
    let owner_id = owner_request.id.clone();
    let mut owner = Box::pin(send_scanner_heal_request("object", owner_request));
    assert!(futures::poll!(owner.as_mut()).is_pending());
    let (owner_reply, accepted_id, owner_receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(accepted_id, owner_id);
    assert_eq!(owner_receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(owner_receipt.task_id, owner_id);
    assert!(owner_receipt.observation_id.is_some());
    owner_reply.send(Ok(owner_receipt.clone())).expect("owner awaits receipt");
    assert_eq!(
        tokio::time::timeout(WAIT, owner)
            .await
            .expect("owner completes")
            .expect("owner admitted"),
        HealAdmissionResult::Accepted
    );

    let mut sentinel = Box::pin(send_scanner_heal_request("object", request("sentinel")));
    assert!(futures::poll!(sentinel.as_mut()).is_pending());
    let (sentinel_reply, _, sentinel_receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(sentinel_receipt.result, HealAdmissionResult::Accepted);
    sentinel_reply
        .send(Ok(sentinel_receipt.clone()))
        .expect("sentinel awaits receipt");
    assert_eq!(
        tokio::time::timeout(WAIT, sentinel)
            .await
            .expect("sentinel completes")
            .expect("sentinel admitted"),
        HealAdmissionResult::Accepted
    );
    wait_status(|status| status.retained_pending_tasks == Some(2) && status.retained_pending_candidates == Some(2)).await;
    assert_eq!(manager.get_queue_length().await, 2);

    let duplicate = request("object");
    assert_ne!(duplicate.id, owner_id);
    let active_node = node();
    let after_cancel = CancellationToken::new();
    let mut after = Box::pin(run_scanner_node_operation(
        Some(&active_node),
        &after_cancel,
        send_scanner_heal_request("object", duplicate),
    ));
    assert!(futures::poll!(after.as_mut()).is_pending());
    let (merged_reply, merged_request_id, merged) = relay_observed(&mut observed, &observed_tx).await;
    assert_ne!(merged_request_id, owner_id);
    assert_eq!(merged.result, HealAdmissionResult::Merged);
    assert_eq!(merged.task_id, owner_id);
    assert_eq!(merged.observation_id, owner_receipt.observation_id);
    assert_eq!(active_node.snapshot().active, 1);
    after_cancel.cancel();
    assert!(
        futures::poll!(after.as_mut()).is_pending(),
        "accepted receipt stays owned after cancellation"
    );
    assert_eq!(active_node.snapshot().active, 0);
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

    // FIFO broadcast plus the sentinel proves the owner's synthetic terminal
    // was consumed before the delayed merged receipt reaches the observer.
    publish_synthetic_terminal(&owner_receipt);
    publish_synthetic_terminal(&sentinel_receipt);
    let status = wait_status(|status| status.terminal_task_observations_total == Some(2)).await;
    assert_eq!(terminal_count(&status), 2);
    merged_reply
        .send(Ok(merged))
        .expect("cancelled operation still awaits receipt");
    assert!(matches!(
        tokio::time::timeout(WAIT, after).await.expect("cancelled operation joins"),
        Ok(Ok(HealAdmissionResult::Merged))
    ));
    assert_eq!(active_node.completed(), Some(0));
    let status = wait_status(|status| terminal_count(status) == 3).await;
    assert_eq!(status.terminal_task_observations_total, Some(2));
    assert_eq!(unknown_count(&status, "identity_unknown"), 3);
    assert_eq!(status.integrity_state, "unknown");

    let lost_node = node();
    let lost_cancel = CancellationToken::new();
    let mut lost = Box::pin(run_scanner_node_operation(
        Some(&lost_node),
        &lost_cancel,
        send_scanner_heal_request("object", request("lost-reply")),
    ));
    assert!(futures::poll!(lost.as_mut()).is_pending());
    let (lost_reply, _, lost_receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(lost_receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(lost_node.snapshot().active, 1);
    lost_cancel.cancel();
    assert!(futures::poll!(lost.as_mut()).is_pending());
    assert_eq!(lost_node.snapshot().active, 0);
    drop(lost_reply);
    assert!(matches!(
        tokio::time::timeout(WAIT, lost).await.expect("lost reply completes"),
        Ok(Err(_))
    ));
    assert_eq!(lost_node.completed(), Some(0));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    wait_status(|status| unknown_count(status, "admission_unknown") == 1).await;
    assert_eq!(manager.get_queue_length().await, 3);

    // Only a closed observed sender before acceptance may fall back to legacy.
    drop(observed);
    let mut legacy_call = Box::pin(send_scanner_heal_request("object", request("legacy")));
    assert!(futures::poll!(legacy_call.as_mut()).is_pending());
    let command = tokio::time::timeout(WAIT, legacy.recv())
        .await
        .expect("legacy command arrives")
        .expect("legacy channel remains open");
    let (response_tx, response_rx) = oneshot::channel();
    legacy_tx
        .send(HealReceiptCommand {
            request: command.request,
            response_tx,
        })
        .expect("legacy processor remains open");
    let receipt = tokio::time::timeout(WAIT, response_rx)
        .await
        .expect("legacy manager decides admission")
        .expect("legacy processor replies")
        .expect("legacy admission succeeds");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    command.response_tx.send(Ok(receipt)).expect("legacy caller awaits receipt");
    assert_eq!(
        tokio::time::timeout(WAIT, legacy_call)
            .await
            .expect("legacy call completes")
            .expect("legacy admitted"),
        HealAdmissionResult::Accepted
    );
    let status = wait_status(|status| unknown_count(status, "owner_generation_unknown") == 1).await;
    assert_eq!(status.terminal_task_observations_total, Some(2));
    assert_eq!(terminal_count(&status), 3);
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(manager.get_queue_length().await, 4);
    assert_eq!(storage.0.load(Ordering::Relaxed), 0);
    processor_task.abort();
    assert!(processor_task.await.expect_err("processor is stopped").is_cancelled());
}

fn scanner_item(object: &str) -> ScannerItem {
    ScannerItem {
        path: format!("bucket/{object}/xl.meta"),
        bucket: "bucket".to_owned(),
        prefix: String::new(),
        object_name: object.to_owned(),
        file_type: std::fs::metadata(env!("CARGO_MANIFEST_DIR"))
            .expect("scanner crate directory")
            .file_type(),
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: true,
        heal_bitrot: false,
        debug: false,
    }
}

fn object_info(object: &str) -> ObjectInfo {
    ObjectInfo {
        bucket: "bucket".to_owned(),
        name: object.to_owned(),
        version_id: Some(Uuid::from_u128(42)),
        ..Default::default()
    }
}

#[tokio::test]
async fn scanner_item_drop_after_send_records_unknown_once() {
    if run_isolated(DROP_TEST_NAME) {
        return;
    }
    let (commands, mut legacy, mut observed) =
        rustfs_heal_contracts::heal_channel::init_heal_channels_with_observations().expect("isolated observed channel");
    let storage = Arc::new(AdmissionOnlyStorage::default());
    let manager = Arc::new(HealManager::new(
        storage.clone(),
        Some(HealConfig {
            queue_size: 16,
            low_priority_merge_enable: true,
            event_driven_scheduler_enable: false,
            enable_auto_heal: false,
            ..HealConfig::default()
        }),
    ));
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let (legacy_tx, legacy_rx) = mpsc::unbounded_channel();
    let mut processor = HealChannelProcessor::new(manager.clone());
    let processor_task =
        tokio::spawn(async move { processor.start_with_observed_receipts(commands, legacy_rx, observed_rx).await });

    let mut item = scanner_item("not-polled");
    let object = object_info("not-polled");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let never_polled = item.heal_actions(&object, 1, &mut summary, Some(&cancellation));
    drop(never_polled);
    cancellation.cancel();
    assert_eq!(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)).await, 1);
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 0);
    assert_eq!(crate::heal_completion::scanner_heal_observation_status().state, "not_started");

    let mut item = scanner_item("normal-reply");
    let object = object_info("normal-reply");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut normal = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(normal.as_mut()).is_pending());
    let (reply, _, receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    reply.send(Ok(receipt)).expect("normal scanner waits for reply");
    assert_eq!(tokio::time::timeout(WAIT, normal).await.expect("normal call completes"), 1);
    let status = wait_status(|status| status.retained_pending_candidates == Some(1)).await;
    assert_eq!(unknown_count(&status, "admission_unknown"), 0);

    let mut item = scanner_item("observed-drop");
    let object = object_info("observed-drop");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut dropped = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(dropped.as_mut()).is_pending());
    let (reply, _, receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    drop(dropped);
    assert!(reply.send(Ok(receipt)).is_err(), "scanner receiver was dropped");
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(unknown_count(&status, "admission_unknown"), 1, "RED_SCANNER_DROP_UNKNOWN");
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 2);

    let mut item = scanner_item("reply-before-drop");
    let object = object_info("reply-before-drop");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut dropped = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(dropped.as_mut()).is_pending());
    let (reply, _, receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    reply.send(Ok(receipt)).expect("unpolled scanner receiver remains open");
    assert_eq!(
        unknown_count(&crate::heal_completion::scanner_heal_observation_status(), "admission_unknown"),
        1
    );
    drop(dropped);
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(unknown_count(&status, "admission_unknown"), 2, "unobserved reply is unknown once");
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 3);

    let mut item = scanner_item("observed-error");
    let object = object_info("observed-error");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut error = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(error.as_mut()).is_pending());
    let (reply, _, receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    drop(reply);
    assert_eq!(tokio::time::timeout(WAIT, error).await.expect("error call completes"), 1);
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(unknown_count(&status, "admission_unknown"), 3, "error is counted once");
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 4);

    let mut item = scanner_item("observed-typed-error");
    let object = object_info("observed-typed-error");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut error = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(error.as_mut()).is_pending());
    let (reply, _, receipt) = relay_observed(&mut observed, &observed_tx).await;
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    reply
        .send(Err(HealObservationChannelError::LegacyCommandClosed))
        .expect("receiver remains open");
    assert_eq!(tokio::time::timeout(WAIT, error).await.expect("typed error call completes"), 1);
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(
        unknown_count(&status, "admission_unknown"),
        4,
        "accepted send determines error classification"
    );
    assert_eq!(unknown_count(&status, "admission_not_sent"), 0);
    assert!(matches!(observed.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 5);

    drop(observed);
    let mut item = scanner_item("legacy-drop");
    let object = object_info("legacy-drop");
    let mut summary = SizeSummary::default();
    let cancellation = CancellationToken::new();
    let mut dropped = Box::pin(item.heal_actions(&object, 1, &mut summary, Some(&cancellation)));
    assert!(futures::poll!(dropped.as_mut()).is_pending());
    let command = tokio::time::timeout(WAIT, legacy.recv())
        .await
        .expect("legacy command arrives")
        .expect("legacy channel remains open");
    let (response_tx, response_rx) = oneshot::channel();
    legacy_tx
        .send(HealReceiptCommand {
            request: command.request,
            response_tx,
        })
        .expect("legacy processor remains open");
    let receipt = tokio::time::timeout(WAIT, response_rx)
        .await
        .expect("legacy manager decides admission")
        .expect("legacy processor replies")
        .expect("legacy admission succeeds");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    cancellation.cancel();
    drop(dropped);
    assert!(command.response_tx.send(Ok(receipt)).is_err());
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(unknown_count(&status, "admission_unknown"), 5, "legacy drop is counted once");
    assert!(matches!(legacy.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert_eq!(manager.get_queue_length().await, 6);

    drop(legacy);
    assert!(send_scanner_heal_request("object", request("not-sent")).await.is_err());
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(
        unknown_count(&status, "admission_unknown"),
        5,
        "unaccepted send is not uncertain admission"
    );
    assert_eq!(unknown_count(&status, "admission_not_sent"), 1);
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(manager.get_queue_length().await, 6);
    assert_eq!(storage.0.load(Ordering::Relaxed), 0);
    processor_task.abort();
    assert!(processor_task.await.expect_err("processor is stopped").is_cancelled());
}

#[tokio::test]
async fn scanner_item_closed_channels_are_not_uncertain_admission() {
    if run_isolated(UNSENT_TEST_NAME) {
        return;
    }
    let (_commands, legacy, observed) =
        rustfs_heal_contracts::heal_channel::init_heal_channels_with_observations().expect("isolated observed channel");
    drop(observed);
    drop(legacy);
    let mut item = scanner_item("closed-channels");
    let object = object_info("closed-channels");
    let mut summary = SizeSummary::default();
    assert_eq!(item.heal_actions(&object, 1, &mut summary, None).await, 1);
    let status = crate::heal_completion::scanner_heal_observation_status();
    assert_eq!(unknown_count(&status, "admission_unknown"), 0, "RED_SCANNER_UNSENT_UNKNOWN");
    assert_eq!(unknown_count(&status, "admission_not_sent"), 1);
    assert_eq!(status.integrity_state, "unknown");
}
