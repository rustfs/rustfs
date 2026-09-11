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
use crate::error::ApiError;
use futures::{StreamExt, poll};
use http_body_util::StreamBody;
use s3s::S3ErrorCode;
use s3s::dto::StreamingBlob;
use std::io;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::io::StreamReader;

mod protocol;

type FrameSender = mpsc::UnboundedSender<Result<Frame<Bytes>, io::Error>>;

fn raw_reader(timeout: Duration) -> (FrameSender, DynReader, BodyReadControl) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let control = BodyReadControl::default();
    control.activate(timeout, "bucket", "object", "request", 65536);
    let body = ObservedBody::new(StreamBody::new(UnboundedReceiverStream::new(receiver)), control.clone());
    let stream = StreamingBlob::from(s3s::Body::http_body_unsync(body));
    let reader = rustfs_rio::wrap_reader(StreamReader::new(stream.map(|item| item.map_err(io::Error::other))));
    (sender, reader, control)
}

#[tokio::test(start_paused = true)]
async fn body_stall_survives_s3s_and_io_wrapping() {
    let (_sender, inner, control) = raw_reader(Duration::from_secs(300));
    let mut reader = DemandReader::new(inner, control);
    let mut output = Vec::new();
    let mut read = Box::pin(reader.read_to_end(&mut output));
    assert!(poll!(read.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(299)).await;
    assert!(poll!(read.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    let error = read.await.expect_err("a body that remains open must time out");
    let api = ApiError::from(error);
    assert_eq!(api.code, S3ErrorCode::RequestTimeout);
    let s3_error = s3s::S3Error::from(api);
    assert_eq!(s3_error.status_code(), Some(http::StatusCode::BAD_REQUEST));
}

#[tokio::test(start_paused = true)]
async fn positive_raw_progress_can_outlast_the_inactivity_timeout() {
    let (sender, inner, control) = raw_reader(Duration::from_secs(300));
    let mut reader = DemandReader::new(inner, control);
    let mut output = Vec::new();
    let mut read = Box::pin(reader.read_to_end(&mut output));
    assert!(poll!(read.as_mut()).is_pending());
    let start = Instant::now();
    for _ in 0..8 {
        tokio::time::advance(Duration::from_secs(60)).await;
        sender
            .send(Ok(Frame::data(Bytes::from(vec![7; 8192]))))
            .expect("body receiver");
        assert!(poll!(read.as_mut()).is_pending());
    }
    drop(sender);
    assert_eq!(read.await.expect("progressing upload"), 65536);
    assert_eq!(start.elapsed(), Duration::from_secs(480));
    assert_eq!(output, vec![7; 65536]);
}

#[tokio::test(start_paused = true)]
async fn empty_frames_do_not_extend_the_inactivity_budget() {
    let (sender, inner, control) = raw_reader(Duration::from_secs(300));
    let mut reader = DemandReader::new(inner, control);
    let mut output = Vec::new();
    let mut read = Box::pin(reader.read_to_end(&mut output));
    assert!(poll!(read.as_mut()).is_pending());
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(60)).await;
        sender.send(Ok(Frame::data(Bytes::new()))).expect("body receiver");
        assert!(poll!(read.as_mut()).is_pending());
    }
    tokio::time::advance(Duration::from_secs(60)).await;
    sender.send(Ok(Frame::data(Bytes::new()))).expect("body receiver");
    assert_eq!(
        ApiError::from(read.await.expect_err("empty frames are not progress")).code,
        S3ErrorCode::RequestTimeout
    );
}

#[tokio::test(start_paused = true)]
async fn disabled_or_not_yet_read_body_has_no_inactivity_deadline() {
    for timeout in [Duration::ZERO, Duration::from_secs(300)] {
        let (sender, inner, control) = raw_reader(timeout);
        // Covers both foreground admission and staging admission, before the
        // owner ever asks its final reader for input.
        tokio::time::advance(Duration::from_secs(1000)).await;
        let mut reader = DemandReader::new(inner, control);
        let mut output = Vec::new();
        let mut read = Box::pin(reader.read_to_end(&mut output));
        assert!(poll!(read.as_mut()).is_pending());
        if timeout.is_zero() {
            tokio::time::advance(Duration::from_secs(1000)).await;
            assert!(poll!(read.as_mut()).is_pending());
        }
        sender
            .send(Ok(Frame::data(Bytes::from_static(b"ok"))))
            .expect("body receiver");
        drop(sender);
        assert_eq!(read.await.expect("queued or disabled body"), 2);
        assert_eq!(output, b"ok");
    }
}

#[tokio::test(start_paused = true)]
async fn compressed_output_pauses_raw_wait_during_storage_backpressure() {
    use rustfs_utils::compress::CompressionAlgorithm;

    let (sender, inner, control) = raw_reader(Duration::from_secs(300));
    let compressed = rustfs_rio::CompressReader::with_block_size(inner, 8192, CompressionAlgorithm::default());
    let mut reader = DemandReader::new(rustfs_rio::boxed_reader(compressed), control);
    sender
        .send(Ok(Frame::data(Bytes::from(vec![3; 1024]))))
        .expect("body receiver");
    let mut buffer = vec![0; 16384];
    // CompressReader sees the partial input and then Pending, yet can return
    // a complete compressed block to the storage writer.
    let first = reader.read(&mut buffer).await.expect("buffered compressed block");
    assert!(first > 0);
    let mut compressed_bytes = buffer[..first].to_vec();
    tokio::time::advance(Duration::from_secs(600)).await;

    let mut read = Box::pin(reader.read(&mut buffer));
    assert!(poll!(read.as_mut()).is_pending(), "storage backpressure is not a client stall");
    tokio::time::advance(Duration::from_secs(299)).await;
    assert!(poll!(read.as_mut()).is_pending());
    sender
        .send(Ok(Frame::data(Bytes::from(vec![4; 1024]))))
        .expect("body receiver");
    drop(sender);
    let next = read.await.expect("input after storage backpressure");
    compressed_bytes.extend_from_slice(&buffer[..next]);
    reader
        .read_to_end(&mut compressed_bytes)
        .await
        .expect("remaining compressed data");

    let mut restored = Vec::new();
    rustfs_rio::DecompressReader::new(std::io::Cursor::new(compressed_bytes), CompressionAlgorithm::default())
        .read_to_end(&mut restored)
        .await
        .expect("roundtrip after backpressure");
    assert_eq!(restored, [vec![3; 1024], vec![4; 1024]].concat());
}

#[tokio::test(start_paused = true)]
async fn read_owner_cancellation_releases_the_raw_body() {
    let (sender, inner, control) = raw_reader(Duration::from_secs(300));
    let mut reader = DemandReader::new(inner, control);
    let owner = tokio::spawn(async move { reader.read_to_end(&mut Vec::new()).await });
    tokio::task::yield_now().await;
    owner.abort();
    assert!(owner.await.expect_err("read owner was canceled").is_cancelled());
    assert!(sender.is_closed(), "the canceled producer must release the body receiver");
}

#[tokio::test(start_paused = true)]
async fn buffered_output_pauses_but_does_not_reset_elapsed_inactivity() {
    struct BufferedOutput {
        inner: DynReader,
        ready: Arc<AtomicBool>,
    }
    impl AsyncRead for BufferedOutput {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.ready.swap(false, Ordering::AcqRel) {
                buf.put_slice(b"x");
                return Poll::Ready(Ok(()));
            }
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }
    let (_sender, inner, control) = raw_reader(Duration::from_secs(300));
    let ready = Arc::new(AtomicBool::new(false));
    let transform = BufferedOutput {
        inner,
        ready: Arc::clone(&ready),
    };
    let mut reader = DemandReader::new(rustfs_rio::wrap_reader(transform), control);
    let mut buffer = [0; 1];
    let mut read = Box::pin(reader.read(&mut buffer));
    assert!(poll!(read.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(200)).await;
    assert!(poll!(read.as_mut()).is_pending());
    ready.store(true, Ordering::Release);
    assert_eq!(read.await.expect("transform releases buffered output"), 1);
    tokio::time::advance(Duration::from_secs(600)).await;
    let mut read = Box::pin(reader.read(&mut buffer));
    assert!(poll!(read.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(99)).await;
    assert!(poll!(read.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    let result = poll!(read.as_mut());
    let Poll::Ready(Err(error)) = result else {
        panic!("remaining inactivity budget must expire immediately at 100 seconds");
    };
    assert_eq!(ApiError::from(error).code, S3ErrorCode::RequestTimeout);
}

#[tokio::test(start_paused = true)]
async fn final_write_reader_preserves_checksums_through_compression_and_sse() {
    use crate::app::storage_api::multipart_usecase::io::{HashReader, WriteEncryption, WritePlan};
    use rustfs_rio::{Checksum, ChecksumType};
    use rustfs_utils::CompressionAlgorithm;

    let payload = vec![7; 65536];
    for plan in [
        WritePlan::new().with_compression(CompressionAlgorithm::default()),
        WritePlan::new().with_encryption(WriteEncryption::multipart([5; 32], [9; 12], 1)),
        WritePlan::new()
            .with_compression(CompressionAlgorithm::default())
            .with_encryption(WriteEncryption::multipart([5; 32], [9; 12], 1)),
    ] {
        let (sender, inner, control) = raw_reader(Duration::from_secs(300));
        let checksum = Checksum::new_from_data(ChecksumType::CRC32, &payload).expect("plaintext checksum");
        let mut plaintext = HashReader::from_reader(inner, 65536, 65536, None, None, false).expect("plaintext reader");
        plaintext
            .add_non_trailing_checksum(Some(checksum.clone()), false)
            .expect("attach checksum");
        let mut reader = plan.apply(plaintext, 65536).expect("write plan");
        let inner = reader.take_inner();
        reader.inner = rustfs_rio::boxed_reader(DemandReader::new(inner, control));
        let mut output = Vec::new();
        let mut read = Box::pin(reader.read_to_end(&mut output));
        assert!(poll!(read.as_mut()).is_pending());
        for chunk in payload.chunks(8192) {
            tokio::time::advance(Duration::from_secs(60)).await;
            sender
                .send(Ok(Frame::data(Bytes::copy_from_slice(chunk))))
                .expect("raw body progress");
            assert!(poll!(read.as_mut()).is_pending());
        }
        drop(sender);
        read.await.expect("transformed reader completes beyond one inactivity period");
        assert!(!output.is_empty());
        assert_eq!(reader.content_crc_type(), Some(ChecksumType::CRC32));
        assert_eq!(reader.content_crc().get("CRC32"), Some(&checksum.encoded));
    }
}

#[tokio::test(start_paused = true)]
async fn native_body_errors_preserve_their_original_source() {
    let (sender, inner, control) = raw_reader(Duration::from_secs(300));
    let mut reader = DemandReader::new(inner, control);
    sender
        .send(Err(io::Error::new(io::ErrorKind::TimedOut, "disk timeout")))
        .expect("body receiver");
    let error = reader.read_to_end(&mut Vec::new()).await.expect_err("native error");
    assert_eq!(ApiError::from(error).code, S3ErrorCode::InternalError);
}
