//! Exercise participant preservation through real multipart staging and commit.

use super::*;
use crate::disk::FileWriter;
use crate::erasure::coding::CustomWriter;
use crate::set_disk::ops::object::hermetic_set_disks_support::hermetic_set_disks_isolated;
use crate::storage_api_contracts::object::ObjectIO as _;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::io::AsyncWrite;

#[derive(Clone, Copy)]
enum Fault {
    DelayedShutdown,
    WriteFailure,
}

struct Round {
    fault: Fault,
    legacy_policy: bool,
    failed_slot: usize,
    completed: AtomicUsize,
    delayed_observed: AtomicBool,
    delayed_waker: Mutex<Option<Waker>>,
    delayed_dropped: AtomicBool,
}

type RegisteredWriter = (Arc<Round>, usize);
static WRITER_ROUNDS: OnceLock<Mutex<HashMap<String, RegisteredWriter>>> = OnceLock::new();

struct WriterRound {
    endpoints: Vec<String>,
    round: Arc<Round>,
}

impl WriterRound {
    fn install(disks: &[DiskStore], failed_slot: usize, fault: Fault, legacy_policy: bool) -> Self {
        let round = Arc::new(Round {
            fault,
            legacy_policy,
            failed_slot,
            completed: AtomicUsize::new(0),
            delayed_observed: AtomicBool::new(false),
            delayed_waker: Mutex::new(None),
            delayed_dropped: AtomicBool::new(false),
        });
        let endpoints = disks.iter().map(|disk| disk.endpoint().to_string()).collect::<Vec<_>>();
        let mut registry = WRITER_ROUNDS
            .get_or_init(Mutex::default)
            .lock()
            .expect("writer hook registry");
        for (slot, endpoint) in endpoints.iter().enumerate() {
            assert!(registry.insert(endpoint.clone(), (round.clone(), slot)).is_none());
        }
        Self { endpoints, round }
    }
}

pub(super) fn write_policy(
    disks: &[Option<DiskStore>],
    quorum: usize,
) -> Option<crate::erasure::coding::encode::ShardWritePolicy> {
    let registry = WRITER_ROUNDS.get()?.lock().expect("writer hook registry");
    disks.iter().flatten().find_map(|disk| {
        registry.get(&disk.endpoint().to_string()).and_then(|(round, _)| {
            round
                .legacy_policy
                .then(|| crate::erasure::coding::encode::ShardWritePolicy::object(quorum))
        })
    })
}

impl Drop for WriterRound {
    fn drop(&mut self) {
        let mut registry = WRITER_ROUNDS
            .get()
            .expect("installed writer registry")
            .lock()
            .expect("writer hook registry");
        for endpoint in &self.endpoints {
            registry.remove(endpoint);
        }
    }
}

pub(super) async fn create_writer(
    disk: &DiskStore,
    path: &str,
    length: i64,
    shard_size: usize,
) -> Option<disk::error::Result<BitrotWriterWrapper>> {
    let registered = WRITER_ROUNDS
        .get()?
        .lock()
        .expect("writer hook registry")
        .get(&disk.endpoint().to_string())
        .cloned();
    let (round, slot) = registered?;
    let length = usize::try_from(length).expect("the fixture writes known-size parts");
    let encoded_length = coding::bitrot_shard_file_size(length, shard_size, HashAlgorithm::HighwayHash256S);
    let inner = match disk
        .create_file(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_TMP_BUCKET, path, encoded_length as i64)
        .await
    {
        Ok(inner) => inner,
        Err(error) => return Some(Err(error)),
    };
    let writer = ControlledWriter {
        inner,
        round,
        slot,
        delay: None,
        completed: false,
    };
    Some(Ok(BitrotWriterWrapper::new(
        CustomWriter::new_tokio_writer(Box::new(writer)),
        shard_size,
        HashAlgorithm::HighwayHash256S,
    )))
}

struct ControlledWriter {
    inner: FileWriter,
    round: Arc<Round>,
    slot: usize,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
    completed: bool,
}

impl Drop for ControlledWriter {
    fn drop(&mut self) {
        if self.slot == self.round.failed_slot && !self.completed {
            self.round.delayed_dropped.store(true, Ordering::Release);
        }
    }
}

impl AsyncWrite for ControlledWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if self.slot == self.round.failed_slot && matches!(self.round.fault, Fault::WriteFailure) {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "injected shard write failure")));
        }
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.slot == self.round.failed_slot && matches!(self.round.fault, Fault::DelayedShutdown) {
            self.round.delayed_observed.store(true, Ordering::Release);
            // Delay only after three real writers finish. This ensures the old
            // post-quorum grace expires regardless of filesystem scheduling.
            {
                let mut waker = self.round.delayed_waker.lock().expect("delayed writer waker");
                *waker = Some(cx.waker().clone());
                if self.round.completed.load(Ordering::Acquire) < 3 {
                    return Poll::Pending;
                }
            }
            let delay = self
                .delay
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_secs(2))));
            if delay.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
        }
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        if matches!(result, Poll::Ready(Ok(()))) && !self.completed {
            self.completed = true;
            self.round.completed.fetch_add(1, Ordering::AcqRel);
            if let Some(waker) = self.round.delayed_waker.lock().expect("delayed writer waker").take() {
                waker.wake();
            }
        }
        result
    }
}

async fn upload_two_parts(
    set: &Arc<SetDisks>,
    disks: &[DiskStore],
    bucket: &str,
    object: &str,
    fault: Fault,
    legacy_policy: bool,
) -> (String, Vec<CompletePart>, Vec<u8>, FileInfo) {
    for disk in disks {
        disk.make_volume(bucket).await.expect("bucket volume");
    }
    let opts = ObjectOptions::default();
    let upload = set
        .new_multipart_upload(bucket, object, &opts)
        .await
        .expect("new multipart upload");
    let mut parts = Vec::new();
    let mut expected = Vec::new();
    for (number, size, slot) in [(1, 5 * 1024 * 1024, 2), (2, 4096, 3)] {
        let payload = vec![number as u8; size];
        expected.extend_from_slice(&payload);
        let hook = WriterRound::install(disks, slot, fault, legacy_policy);
        let mut reader = PutObjReader::from_vec(payload);
        let part = set
            .put_object_part(bucket, object, &upload.upload_id, number, &mut reader, &opts)
            .await
            .expect("three completed shards must admit each part");
        match fault {
            Fault::DelayedShutdown if !legacy_policy => {
                assert!(hook.round.delayed_observed.load(Ordering::Acquire));
                assert!(!hook.round.delayed_dropped.load(Ordering::Acquire), "finite slow shard must finish");
                assert_eq!(hook.round.completed.load(Ordering::Acquire), 4, "all real shard writers finished");
            }
            Fault::DelayedShutdown | Fault::WriteFailure => {
                assert!(hook.round.delayed_dropped.load(Ordering::Acquire), "failed shard must be dropped");
                assert_eq!(hook.round.completed.load(Ordering::Acquire), 3);
            }
        }
        parts.push(CompletePart {
            part_num: number,
            etag: part.etag,
            ..Default::default()
        });
    }
    let (metadata, _) = set
        .check_upload_id_exists(bucket, object, &upload.upload_id, true)
        .await
        .expect("upload metadata");
    (upload.upload_id, parts, expected, metadata)
}

#[tokio::test]
async fn multipart_budget_keeps_different_finite_shutdown_stragglers_and_completes() {
    let (_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
    let bucket = "multipart-finite-shutdown";
    let object = "object";
    let (upload, parts, expected, metadata) = upload_two_parts(&set, &disks, bucket, object, Fault::DelayedShutdown, false).await;
    let data_dir = metadata.data_dir.expect("parts-backed data directory");
    let erasure =
        coding::Erasure::try_new(metadata.erasure.data_blocks, metadata.erasure.parity_blocks, metadata.erasure.block_size)
            .expect("upload erasure layout");
    let stage = SetDisks::get_upload_id_dir(bucket, object, &upload);
    for disk in &disks {
        for (number, size) in [(1, 5 * 1024 * 1024), (2, 4096)] {
            let path = format!("{stage}/{data_dir}/part.{number}");
            let bytes = disk
                .read_all(RUSTFS_META_MULTIPART_BUCKET, &path)
                .await
                .expect("every disk has every part");
            let shard_len = erasure.shard_file_size(size);
            let encoded_len =
                coding::bitrot_shard_file_size(shard_len as usize, erasure.shard_size(), HashAlgorithm::HighwayHash256S);
            assert_eq!(bytes.len(), encoded_len, "complete physical shard: {path}");
        }
    }
    set.clone()
        .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
        .await
        .expect("all parts have a common completed quorum");
    let mut reader = set
        .get_object_reader(bucket, object, None, http::HeaderMap::new(), &ObjectOptions::default())
        .await
        .expect("read committed object");
    let mut actual = Vec::new();
    reader.stream.read_to_end(&mut actual).await.expect("whole object readback");
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn multipart_budget_distinct_failed_shards_do_not_bypass_complete_quorum() {
    let (_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
    let bucket = "multipart-distinct-failures";
    let object = "object";
    let (upload, parts, _, _) = upload_two_parts(&set, &disks, bucket, object, Fault::WriteFailure, false).await;
    let error = set
        .clone()
        .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
        .await
        .expect_err("two common complete disks cannot meet the write quorum of three");
    assert!(error.is_quorum_error(), "expected complete quorum refusal, got {error:?}");
    set.check_upload_id_exists(bucket, object, &upload, false)
        .await
        .expect("failed completion retains retryable upload state");
    assert!(
        set.get_object_info(bucket, object, &ObjectOptions::default()).await.is_err(),
        "failed completion must not publish an object"
    );
}

#[tokio::test]
async fn multipart_budget_old_grace_loses_common_part_quorum() {
    let (_dirs, disks, set) = hermetic_set_disks_isolated(4).await;
    let bucket = "multipart-old-grace";
    let object = "object";
    let (upload, parts, _, _) = upload_two_parts(&set, &disks, bucket, object, Fault::DelayedShutdown, true).await;
    let error = set
        .clone()
        .complete_multipart_upload(bucket, object, &upload, parts, &ObjectOptions::default())
        .await
        .expect_err("the old per-part grace loses the common completion quorum");
    assert!(error.is_quorum_error(), "expected complete quorum refusal, got {error:?}");
}
