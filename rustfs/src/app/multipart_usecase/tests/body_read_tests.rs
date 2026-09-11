// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::app::object::request_body::{BodyReadControl, ObservedBody};
use crate::app::storage_api::test::contract::bucket::{BucketOperations, MakeBucketOptions};
use crate::app::storage_api::test::contract::object::ObjectIO;
use http_body::Frame;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

fn part_request(bucket: &str, upload: &str, body: StreamingBlob, size: i64) -> S3Request<UploadPartInput> {
    build_request(
        UploadPartInput::builder()
            .bucket(bucket.to_owned())
            .key("object".to_owned())
            .upload_id(upload.to_owned())
            .part_number(1)
            .content_length(Some(size))
            .body(Some(body))
            .build()
            .expect("part input"),
        Method::PUT,
    )
}

type BodySender = mpsc::UnboundedSender<Result<Frame<Bytes>, std::io::Error>>;

fn observed_request(
    bucket: &str,
    upload: &str,
    size: usize,
) -> (S3Request<UploadPartInput>, BodySender, oneshot::Receiver<()>, Arc<AtomicUsize>) {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (started, waiting) = oneshot::channel();
    let mut started = Some(started);
    let polls = Arc::new(AtomicUsize::new(0));
    let body_polls = Arc::clone(&polls);
    let stream = futures::stream::poll_fn(move |cx| {
        body_polls.fetch_add(1, Ordering::Relaxed);
        let result = receiver.poll_recv(cx);
        if result.is_pending()
            && let Some(started) = started.take()
        {
            let _ = started.send(());
        }
        result
    });
    let control = BodyReadControl::default();
    let body = ObservedBody::new(http_body_util::StreamBody::new(stream), control.clone());
    let mut request = part_request(bucket, upload, StreamingBlob::from(s3s::Body::http_body_unsync(body)), size as i64);
    request.extensions.insert(control);
    (request, sender, waiting, polls)
}

async fn temporary_entries(disks: &[std::path::PathBuf]) -> std::collections::BTreeSet<std::path::PathBuf> {
    let mut entries = std::collections::BTreeSet::new();
    for disk in disks {
        let path = disk.join(".rustfs.sys/tmp");
        let mut directory = match tokio::fs::read_dir(path).await {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => panic!("temporary directory: {error}"),
        };
        while let Some(entry) = directory.next_entry().await.expect("temporary entry") {
            if entry.file_name() != ".trash" {
                entries.insert(entry.path());
            }
        }
    }
    entries
}

#[test]
#[serial_test::serial]
fn upload_part_body_timeout_cleans_storage_preserves_old_part_and_releases_permits() {
    crate::app::gating_test_env::run_large_stack_test("upload-part-timeout-direct", || async {
        assert_body_timeout_storage_lifecycle(4096, 512).await;
    });
}

#[test]
#[serial_test::serial]
fn upload_part_body_timeout_pipeline_storage_lifecycle() {
    // Fresh processes with the existing ingest and batching settings cover
    // Vec, BytesMut, and batched pipelines independently.
    crate::app::gating_test_env::run_large_stack_test("upload-part-timeout-pipeline", || async {
        assert_body_timeout_storage_lifecycle(3 * 1024 * 1024, 2 * 1024 * 1024 + 512).await;
    });
}

async fn assert_body_timeout_storage_lifecycle(part_size: usize, partial_size: usize) {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    struct RestoreMetrics(bool);
    impl Drop for RestoreMetrics {
        fn drop(&mut self) {
            rustfs_io_metrics::set_put_stage_metrics_enabled(self.0);
        }
    }
    let _restore = RestoreMetrics(rustfs_io_metrics::put_stage_metrics_enabled());
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    rustfs_io_metrics::set_put_stage_metrics_enabled(true);
    let path = if part_size == 4096 {
        "multipart_write_single_block_non_inline"
    } else if part_size >= rustfs_utils::get_env_usize("RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES", 128 * 1024 * 1024) {
        "multipart_write_pipeline_batched_large"
    } else {
        "multipart_write_pipeline"
    };
    let path_count = || {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| {
                if key.key().name() == "rustfs_s3_put_object_path_total"
                    && key.key().labels().any(|label| label.key() == "path" && label.value() == path)
                    && let DebugValue::Counter(count) = value
                {
                    Some(count)
                } else {
                    None
                }
            })
            .sum::<u64>()
    };
    let (disks, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
    let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
    let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
    let manager = Arc::new(ConcurrencyManager::with_large_put_admission_for_test(
        true,
        1,
        rustfs_config::DEFAULT_PUT_LARGE_FOREGROUND_ADMISSION_MIN_SIZE_BYTES,
        Duration::ZERO,
    ));
    let usecase = Arc::new(DefaultMultipartUsecase::with_context_and_concurrency_manager(
        Some(context),
        Arc::clone(&manager),
    ));
    let bucket = format!("body-stall-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket");

    for capped in [false, true] {
        let mut options = ObjectOptions::default();
        if capped {
            insert_str(&mut options.user_defined, SUFFIX_MAX_TOTAL_OBJECT_SIZE, (2 * part_size).to_string());
        }
        let upload = store
            .new_multipart_upload(&bucket, "object", &options)
            .await
            .expect("upload session");
        let mut old_etag = None;
        for (replacement, retry_after_failure) in [(false, true), (true, true), (true, false)] {
            let baseline = temporary_entries(&disks).await;
            let _ = path_count();
            let (request, sender, waiting, _) = observed_request(&bucket, &upload.upload_id, part_size);
            sender
                .send(Ok(Frame::data(Bytes::from(vec![7; partial_size]))))
                .expect("partial body");
            let mut upload_future = Box::pin(usecase.execute_upload_part(request));
            tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut upload_future => panic!("upload finished before storage requested raw input: {:?}", result.err().map(|error| error.code().clone())),
                    result = waiting => result.expect("raw reader"),
                }
            })
                .await
                .expect("storage must request raw input");
            // Advance only after actual storage demand; filesystem setup and
            // cleanup run on a real clock and cannot race auto-advance.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            tokio::time::resume();
            let error = tokio::time::timeout(Duration::from_secs(10), upload_future)
                .await
                .expect("inline cleanup must complete")
                .expect_err("stalled part");
            assert_eq!(error.code(), &S3ErrorCode::RequestTimeout);
            assert_eq!(path_count(), 1, "failure must exercise {path}");
            assert!(sender.is_closed(), "producer must release the failed raw body");
            assert_eq!(
                temporary_entries(&disks).await,
                baseline,
                "failed part must clean temporary shards inline"
            );
            let parts = store
                .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
                .await
                .expect("list parts after error");
            assert_eq!(parts.parts.len(), usize::from(replacement));
            if replacement {
                assert_eq!(parts.parts[0].etag, old_etag, "failed overwrite must preserve the committed part");
            }
            if !retry_after_failure {
                continue;
            }
            let payload = vec![if replacement { 9 } else { 8 }; part_size];
            let request = part_request(&bucket, &upload.upload_id, StreamingBlob::from(Bytes::from(payload)), part_size as i64);
            let retry = tokio::time::timeout(Duration::from_secs(10), usecase.execute_upload_part(request))
                .await
                .expect("foreground and capped staging permits must be released")
                .expect("same-number retry");
            old_etag = retry.output.e_tag.map(|etag| etag.value().to_owned());
        }
        let parts = store
            .list_object_parts(&bucket, "object", &upload.upload_id, None, 1000, &ObjectOptions::default())
            .await
            .expect("successful retry");
        assert_eq!(parts.parts.len(), 1);
        assert_eq!(parts.parts[0].etag, old_etag);
        assert_eq!(parts.parts[0].size, part_size);
        store
            .clone()
            .complete_multipart_upload(
                &bucket,
                "object",
                &upload.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: old_etag,
                    ..CompletePart::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("failed replacement must leave the prior part completable");
        let mut object = store
            .get_object_reader(&bucket, "object", None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed old part remains readable");
        let mut restored = Vec::new();
        object.stream.read_to_end(&mut restored).await.expect("read all old bytes");
        assert_eq!(restored, vec![9; part_size]);
    }
    eprintln!(
        "verified storage lifecycle: path={path}, bytesmut={}",
        rustfs_utils::get_env_bool("RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST", true)
    );
}

#[test]
#[serial_test::serial]
fn upload_part_foreground_queue_does_not_consume_body_timeout() {
    crate::app::gating_test_env::run_large_stack_test("upload-part-timeout-queue", assert_foreground_queue);
}

async fn assert_foreground_queue() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
    let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
    let manager = Arc::new(ConcurrencyManager::with_multipart_admission_queue_for_test(
        1,
        Duration::from_secs(1200),
        1,
    ));
    let held = manager.admit_multipart_part(4096).await.expect("hold foreground permit");
    let usecase = DefaultMultipartUsecase::with_context_and_concurrency_manager(Some(context), Arc::clone(&manager));
    let bucket = format!("body-queue-{}", Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("bucket");
    let upload = store
        .new_multipart_upload(&bucket, "object", &ObjectOptions::default())
        .await
        .expect("upload session");
    let (request, sender, waiting, polls) = observed_request(&bucket, &upload.upload_id, 4096);
    let task = tokio::spawn(async move { usecase.execute_upload_part(request).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while manager.put_object_admission_snapshot().queued != Some(1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request must enter the actual foreground queue");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(600)).await;
    tokio::time::resume();
    assert_eq!(polls.load(Ordering::Relaxed), 0, "queued requests must not poll the raw body");
    assert!(!task.is_finished());
    drop(held);
    waiting.await.expect("read begins after admission");
    sender
        .send(Ok(Frame::data(Bytes::from(vec![7; 4096]))))
        .expect("body after admission");
    drop(sender);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("queued upload completes")
        .expect("upload task")
        .expect("queue time is not client inactivity");
}
