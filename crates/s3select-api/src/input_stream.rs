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

use crate::{
    MAX_ERROR_SOURCE_DEPTH, SelectError, SelectInputMetrics, metrics::SelectInputMetricsRecorder,
    query::session::QueryExecutionGuard,
};
use async_compression::tokio::bufread::BzDecoder;
use bytes::{Buf as _, Bytes};
use datafusion::object_store::{Error as ObjectStoreError, Result as ObjectStoreResult};
use flate2::bufread::GzDecoder;
use futures::{StreamExt, stream};
use futures_core::stream::BoxStream;
use std::{
    error::Error as StdError,
    io::{self, BufRead as _, Read as _},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};

pub(crate) const MAX_SELECT_RECORD_BYTES: usize = 1024 * 1024;
const MAX_SELECT_PROCESSED_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;
pub(crate) const SELECT_DECODE_CHUNK_BYTES: usize = 64 * 1024;
const DECOMPRESSION_CHANNEL_CAPACITY: usize = 2;

pub(crate) type SelectInputReader = Box<dyn AsyncRead + Unpin + Send>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompressionFormat {
    Gzip,
    Bzip2,
}

impl CompressionFormat {
    fn name(self) -> &'static str {
        match self {
            Self::Gzip => "GZIP",
            Self::Bzip2 => "BZIP2",
        }
    }

    fn invalid_header_error(self) -> SelectError {
        SelectError::InvalidCompressionFormatForObject {
            compression: self.name(),
        }
    }
}

pub(crate) fn processed_bytes_limit() -> u64 {
    // Processed throughput is independent of compression ratio and live
    // memory; use the S3 Select object-size ceiling as the absolute bound.
    MAX_SELECT_PROCESSED_BYTES
}

pub(crate) fn compressed_input_reader(
    reader: SelectInputReader,
    compressed_size: u64,
    format: CompressionFormat,
    input_metrics: Arc<SelectInputMetrics>,
    max_processed_bytes: u64,
    query_guard: Option<QueryExecutionGuard>,
) -> SelectInputReader {
    // rustfs-zip does not expose Select's streaming metrics, member validation,
    // typed errors, or cancellation contract, so the protocol adapter lives here.
    let input_metrics = input_metrics.recorder();
    let reader = ScannedReader::new(reader, compressed_size, input_metrics.clone());
    let reader = CooperativeReader::new(reader);
    let decoder = match format {
        CompressionFormat::Gzip => blocking_gzip_reader(Box::new(reader), query_guard),
        CompressionFormat::Bzip2 => blocking_bzip2_reader(Box::new(Bzip2HeaderValidatingReader::new(reader)), query_guard),
    };
    Box::new(CooperativeReader::new(ProcessedReader::new(decoder, input_metrics, max_processed_bytes)))
}

fn blocking_gzip_reader(reader: SelectInputReader, query_guard: Option<QueryExecutionGuard>) -> SelectInputReader {
    // The bounded bridge keeps RFC 1952 decoding off Tokio workers while
    // preserving member validation, backpressure, and reader cancellation.
    let (compressed_tx, compressed_rx) = mpsc::channel(DECOMPRESSION_CHANNEL_CAPACITY);
    let (decoded_tx, decoded_rx) = mpsc::channel(DECOMPRESSION_CHANNEL_CAPACITY);

    let decoded_closed = decoded_tx.clone();
    drop(tokio::spawn(async move {
        let mut stream = ReaderStream::with_capacity(reader, SELECT_DECODE_CHUNK_BYTES);
        loop {
            let item = tokio::select! {
                biased;
                _ = decoded_closed.closed() => break,
                item = stream.next() => item,
            };
            let Some(item) = item else {
                break;
            };
            let sent = tokio::select! {
                biased;
                _ = decoded_closed.closed() => false,
                result = compressed_tx.send(item) => result.is_ok(),
            };
            if !sent {
                break;
            }
        }
    }));

    let blocking_output = decoded_tx.clone();
    spawn_decoder_thread("s3select-gzip", decoded_tx, query_guard, move || {
        decode_gzip(compressed_rx, blocking_output)
    });

    Box::new(StreamReader::new(ReceiverStream::new(decoded_rx)))
}

fn blocking_bzip2_reader(reader: SelectInputReader, query_guard: Option<QueryExecutionGuard>) -> SelectInputReader {
    let (decoded_tx, decoded_rx) = mpsc::channel(DECOMPRESSION_CHANNEL_CAPACITY);
    let runtime = tokio::runtime::Handle::current();
    let blocking_output = decoded_tx.clone();
    // A dedicated thread avoids occupying Tokio's blocking pool while the
    // decoder waits for asynchronous object reads. Query admission bounds the
    // number of concurrent Select decoder threads.
    spawn_decoder_thread("s3select-bzip2", decoded_tx, query_guard, move || {
        runtime.block_on(decode_bzip2(reader, blocking_output))
    });
    Box::new(StreamReader::new(ReceiverStream::new(decoded_rx)))
}

fn spawn_decoder_thread(
    name: &'static str,
    decoded: mpsc::Sender<io::Result<Bytes>>,
    query_guard: Option<QueryExecutionGuard>,
    task: impl FnOnce() -> io::Result<()> + Send + 'static,
) {
    let (finished_tx, finished_rx) = oneshot::channel();
    let spawn_result = std::thread::Builder::new().name(name.to_string()).spawn(move || {
        let _query_guard = query_guard;
        let _ = finished_tx.send(task());
    });
    if spawn_result.is_err() {
        let _ = decoded.try_send(Err(io::Error::other(SelectError::InternalError)));
        return;
    }

    drop(tokio::spawn(async move {
        let result = finished_rx
            .await
            .unwrap_or_else(|_| Err(io::Error::other(SelectError::InternalError)));
        if let Err(error) = result {
            let _ = decoded.send(Err(error)).await;
        }
    }));
}

async fn decode_bzip2(reader: SelectInputReader, decoded: mpsc::Sender<io::Result<Bytes>>) -> io::Result<()> {
    let reader = BufReader::with_capacity(SELECT_DECODE_CHUNK_BYTES, reader);
    let mut decoder = BzDecoder::new(reader);
    decoder.multiple_members(true);
    let mut buffer = vec![0; SELECT_DECODE_CHUNK_BYTES];
    loop {
        let read = tokio::select! {
            biased;
            _ = decoded.closed() => return Ok(()),
            result = decoder.read(&mut buffer) => result?,
        };
        if read == 0 {
            return Ok(());
        }
        let bytes = Bytes::copy_from_slice(&buffer[..read]);
        tokio::select! {
            biased;
            _ = decoded.closed() => return Ok(()),
            result = decoded.send(Ok(bytes)) => {
                if result.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn decode_gzip(compressed: mpsc::Receiver<io::Result<Bytes>>, decoded: mpsc::Sender<io::Result<Bytes>>) -> io::Result<()> {
    let mut reader = io::BufReader::with_capacity(SELECT_DECODE_CHUNK_BYTES, BlockingChannelReader::new(compressed));
    let mut buffer = vec![0; SELECT_DECODE_CHUNK_BYTES];
    let mut decoded_member = false;
    loop {
        if decoded.is_closed() {
            return Ok(());
        }

        if reader.fill_buf()?.is_empty() {
            return if decoded_member {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, SelectError::TruncatedInput))
            };
        }
        let header = read_gzip_header(&mut reader).map_err(|error| {
            if decoded_member
                && find_error_source::<SelectError>(&error)
                    .is_some_and(|source| matches!(source, SelectError::InvalidCompressionFormatForObject { .. }))
            {
                io::Error::new(error.kind(), SelectError::TruncatedInput)
            } else {
                error
            }
        })?;
        let mut decoder = GzDecoder::new(std::io::Read::chain(io::Cursor::new(header), reader));
        loop {
            let read = decoder.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            if decoded.blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read]))).is_err() {
                return Ok(());
            }
        }
        let (_, remaining) = decoder.into_inner().into_inner();
        reader = remaining;
        decoded_member = true;
    }
}

fn read_gzip_header<R: io::BufRead>(reader: &mut R) -> io::Result<[u8; 10]> {
    let mut fixed = [0; 10];
    read_gzip_exact(reader, &mut fixed)?;
    if fixed[..3] != [0x1f, 0x8b, 0x08] || fixed[3] & 0xe0 != 0 {
        return Err(invalid_gzip_header_error());
    }
    let flags = fixed[3];
    let mut header_crc = (flags & GZIP_FLAG_HEADER_CRC != 0).then(|| crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc));
    update_gzip_header_crc(&mut header_crc, &fixed);

    if flags & GZIP_FLAG_EXTRA != 0 {
        let mut length = [0; 2];
        read_gzip_exact(reader, &mut length)?;
        update_gzip_header_crc(&mut header_crc, &length);
        let extra_len = usize::from(u16::from_le_bytes(length));
        read_gzip_header_bytes(reader, extra_len, &mut header_crc)?;
    }
    if flags & GZIP_FLAG_NAME != 0 {
        read_gzip_text_field(reader, &mut header_crc)?;
    }
    if flags & GZIP_FLAG_COMMENT != 0 {
        read_gzip_text_field(reader, &mut header_crc)?;
    }
    if let Some(digest) = header_crc {
        let expected =
            u16::try_from(digest.finalize() & u64::from(u16::MAX)).map_err(|_| io::Error::other(SelectError::InternalError))?;
        let mut actual = [0; 2];
        read_gzip_exact(reader, &mut actual)?;
        if u16::from_le_bytes(actual) != expected {
            return Err(invalid_gzip_header_error());
        }
    }

    fixed[3] &= GZIP_FLAG_TEXT;
    Ok(fixed)
}

fn read_gzip_exact<R: io::Read>(reader: &mut R, bytes: &mut [u8]) -> io::Result<()> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof && !error_chain_contains::<CompressedSourceReadError>(&error) {
            io::Error::new(io::ErrorKind::UnexpectedEof, SelectError::TruncatedInput)
        } else {
            error
        }
    })
}

fn read_gzip_header_bytes<R: io::BufRead>(
    reader: &mut R,
    mut remaining: usize,
    header_crc: &mut Option<crc_fast::Digest>,
) -> io::Result<()> {
    while remaining > 0 {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, SelectError::TruncatedInput));
        }
        let consumed = remaining.min(available.len());
        update_gzip_header_crc(header_crc, &available[..consumed]);
        reader.consume(consumed);
        remaining -= consumed;
    }
    Ok(())
}

fn read_gzip_text_field<R: io::BufRead>(reader: &mut R, header_crc: &mut Option<crc_fast::Digest>) -> io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, SelectError::TruncatedInput));
        }
        let terminator = available.iter().position(|byte| *byte == 0);
        let consumed = terminator.map_or(available.len(), |position| position + 1);
        update_gzip_header_crc(header_crc, &available[..consumed]);
        reader.consume(consumed);
        if terminator.is_some() {
            return Ok(());
        }
    }
}

fn update_gzip_header_crc(header_crc: &mut Option<crc_fast::Digest>, bytes: &[u8]) {
    if let Some(digest) = header_crc {
        digest.update(bytes);
    }
}

fn invalid_gzip_header_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, CompressionFormat::Gzip.invalid_header_error())
}

struct BlockingChannelReader {
    receiver: mpsc::Receiver<io::Result<Bytes>>,
    current: Bytes,
}

impl BlockingChannelReader {
    fn new(receiver: mpsc::Receiver<io::Result<Bytes>>) -> Self {
        Self {
            receiver,
            current: Bytes::new(),
        }
    }
}

impl io::Read for BlockingChannelReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.current.is_empty() {
            match self.receiver.blocking_recv() {
                Some(Ok(bytes)) => self.current = bytes,
                Some(Err(error)) => return Err(error),
                None => return Ok(0),
            }
        }
        let read = buffer.len().min(self.current.len());
        buffer[..read].copy_from_slice(&self.current[..read]);
        self.current.advance(read);
        Ok(read)
    }
}

pub(crate) fn compressed_input_stream(
    reader: SelectInputReader,
    compressed_size: u64,
    format: CompressionFormat,
    input_metrics: Arc<SelectInputMetrics>,
    record_delimiter: Vec<u8>,
    max_processed_bytes: u64,
    query_guard: Option<QueryExecutionGuard>,
) -> ObjectStoreResult<BoxStream<'static, ObjectStoreResult<Bytes>>> {
    let record_size = RecordSizeTracker::new(record_delimiter).map_err(select_object_store_error)?;
    let reader = compressed_input_reader(reader, compressed_size, format, input_metrics, max_processed_bytes, query_guard);
    let stream = ReaderStream::with_capacity(reader, SELECT_DECODE_CHUNK_BYTES);
    Ok(stream::try_unfold((stream, record_size), |(mut stream, mut record_size)| async move {
        match stream.next().await {
            Some(Ok(bytes)) => {
                record_size.observe(&bytes).map_err(select_object_store_error)?;
                Ok(Some((bytes, (stream, record_size))))
            }
            Some(Err(error)) => Err(input_io_error(error)),
            None => {
                record_size.finish().map_err(select_object_store_error)?;
                Ok(None)
            }
        }
    })
    .boxed())
}

pub(crate) fn input_io_error(source: io::Error) -> ObjectStoreError {
    let source: Box<dyn StdError + Send + Sync> = match find_error_source::<SelectError>(&source) {
        Some(error) => Box::new(error.clone()),
        None => Box::new(source),
    };
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source,
    }
}

fn select_object_store_error(source: SelectError) -> ObjectStoreError {
    ObjectStoreError::Generic {
        store: "EcObjectStore",
        source: Box::new(source),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("compressed object source read failed")]
struct CompressedSourceReadError {
    #[source]
    source: io::Error,
}

struct ScannedReader<R> {
    inner: tokio::io::Take<R>,
    input_metrics: SelectInputMetricsRecorder,
}

impl<R: AsyncRead + Unpin> ScannedReader<R> {
    fn new(reader: R, compressed_size: u64, input_metrics: SelectInputMetricsRecorder) -> Self {
        Self {
            inner: reader.take(compressed_size),
            input_metrics,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ScannedReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(source)) => Poll::Ready(Err(source_read_error(source))),
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                if read == 0 && self.inner.limit() > 0 {
                    let source = io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("compressed object stream ended with {} bytes remaining", self.inner.limit()),
                    );
                    return Poll::Ready(Err(source_read_error(source)));
                }
                self.input_metrics.record_scanned(read);
                Poll::Ready(Ok(()))
            }
        }
    }
}

struct CooperativeReader<R> {
    inner: R,
    bytes_since_yield: usize,
    yield_pending: bool,
}

impl<R> CooperativeReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            bytes_since_yield: 0,
            yield_pending: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CooperativeReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.yield_pending {
            self.yield_pending = false;
            self.bytes_since_yield = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let remaining_budget = SELECT_DECODE_CHUNK_BYTES - self.bytes_since_yield;
        let read_limit = remaining_budget.min(buf.remaining());
        let read = {
            let unfilled = buf.initialize_unfilled_to(read_limit);
            let mut limited = ReadBuf::new(unfilled);
            match Pin::new(&mut self.inner).poll_read(cx, &mut limited) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => limited.filled().len(),
            }
        };
        buf.advance(read);
        self.bytes_since_yield += read;
        self.yield_pending = read > 0 && self.bytes_since_yield == SELECT_DECODE_CHUNK_BYTES;
        Poll::Ready(Ok(()))
    }
}

fn source_read_error(source: io::Error) -> io::Error {
    let kind = source.kind();
    io::Error::new(kind, CompressedSourceReadError { source })
}

struct Bzip2HeaderValidatingReader<R> {
    inner: R,
    position: usize,
    pending_error: Option<SelectError>,
}

impl<R> Bzip2HeaderValidatingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            position: 0,
            pending_error: None,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Bzip2HeaderValidatingReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if let Some(error) = self.pending_error.take() {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error)));
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.position == 4 {
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }

        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                if after == before {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::UnexpectedEof, SelectError::TruncatedInput)));
                }
                if let Err(error) = validate_bzip2_header(&mut self.position, &buf.filled()[before..after]) {
                    buf.set_filled(before + error.offset);
                    if error.offset == 0 {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error.source)));
                    }
                    self.pending_error = Some(error.source);
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

struct HeaderValidationError {
    offset: usize,
    source: SelectError,
}

fn validate_bzip2_header(position: &mut usize, bytes: &[u8]) -> Result<(), HeaderValidationError> {
    for (offset, byte) in bytes.iter().copied().enumerate() {
        let valid = match *position {
            0 => byte == b'B',
            1 => byte == b'Z',
            2 => byte == b'h',
            3 => matches!(byte, b'1'..=b'9'),
            _ => break,
        };
        if !valid {
            return Err(HeaderValidationError {
                offset,
                source: CompressionFormat::Bzip2.invalid_header_error(),
            });
        }
        *position += 1;
    }
    Ok(())
}

const GZIP_FLAG_HEADER_CRC: u8 = 0x02;
const GZIP_FLAG_EXTRA: u8 = 0x04;
const GZIP_FLAG_NAME: u8 = 0x08;
const GZIP_FLAG_COMMENT: u8 = 0x10;
const GZIP_FLAG_TEXT: u8 = 0x01;

struct ProcessedReader<R> {
    inner: R,
    input_metrics: SelectInputMetricsRecorder,
    processed_bytes: u64,
    max_processed_bytes: u64,
}

impl<R> ProcessedReader<R> {
    fn new(inner: R, input_metrics: SelectInputMetricsRecorder, max_processed_bytes: u64) -> Self {
        Self {
            inner,
            input_metrics,
            processed_bytes: 0,
            max_processed_bytes,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ProcessedReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(classify_decoder_error(error))),
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                let read = u64::try_from(read).unwrap_or(u64::MAX);
                let Some(processed_bytes) = self.processed_bytes.checked_add(read) else {
                    return Poll::Ready(Err(processed_bytes_limit_error()));
                };
                if processed_bytes > self.max_processed_bytes {
                    return Poll::Ready(Err(processed_bytes_limit_error()));
                }
                self.processed_bytes = processed_bytes;
                self.input_metrics.record_processed(buf.filled().len() - before);
                Poll::Ready(Ok(()))
            }
        }
    }
}

fn processed_bytes_limit_error() -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, SelectError::ResourceExhausted)
}

fn classify_decoder_error(error: io::Error) -> io::Error {
    if error_chain_contains::<CompressedSourceReadError>(&error) || error_chain_contains::<SelectError>(&error) {
        return error;
    }

    let select_error = if error.kind() == io::ErrorKind::OutOfMemory {
        SelectError::ResourceExhausted
    } else {
        SelectError::TruncatedInput
    };
    io::Error::new(error.kind(), select_error)
}

fn error_chain_contains<T: StdError + 'static>(error: &(dyn StdError + 'static)) -> bool {
    find_error_source::<T>(error).is_some()
}

fn find_error_source<'a, T: StdError + 'static>(error: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut current = Some(error);
    for _ in 0..MAX_ERROR_SOURCE_DEPTH {
        let Some(error) = current else {
            break;
        };
        if let Some(error) = error.downcast_ref::<T>() {
            return Some(error);
        }
        current = error
            .downcast_ref::<io::Error>()
            .and_then(|error| error.get_ref())
            .map(|source| source as &(dyn StdError + 'static))
            .or_else(|| error.source());
    }
    None
}

struct RecordSizeTracker {
    delimiter: Vec<u8>,
    prefix: Vec<usize>,
    record_bytes: usize,
    matched: usize,
}

impl RecordSizeTracker {
    fn new(delimiter: Vec<u8>) -> Result<Self, SelectError> {
        if delimiter.is_empty() {
            return Err(SelectError::InvalidDataSource);
        }

        let mut prefix = vec![0; delimiter.len()];
        let mut matched = 0;
        for index in 1..delimiter.len() {
            while matched > 0 && delimiter[index] != delimiter[matched] {
                matched = prefix[matched - 1];
            }
            if delimiter[index] == delimiter[matched] {
                matched += 1;
            }
            prefix[index] = matched;
        }

        Ok(Self {
            delimiter,
            prefix,
            record_bytes: 0,
            matched: 0,
        })
    }

    fn observe(&mut self, bytes: &[u8]) -> Result<(), SelectError> {
        if self.delimiter.len() == 1 {
            return self.observe_single_byte_delimiter(bytes);
        }

        for &byte in bytes {
            self.record_bytes = self.record_bytes.checked_add(1).ok_or(SelectError::OverMaxRecordSize)?;
            while self.matched > 0 && byte != self.delimiter[self.matched] {
                self.matched = self.prefix[self.matched - 1];
            }
            if byte == self.delimiter[self.matched] {
                self.matched += 1;
            }
            if self.matched == self.delimiter.len() {
                let payload_bytes = self.record_bytes - self.delimiter.len();
                if payload_bytes > MAX_SELECT_RECORD_BYTES {
                    return Err(SelectError::OverMaxRecordSize);
                }
                self.record_bytes = 0;
                self.matched = 0;
            } else if self.record_bytes - self.matched > MAX_SELECT_RECORD_BYTES {
                return Err(SelectError::OverMaxRecordSize);
            }
        }
        Ok(())
    }

    fn observe_single_byte_delimiter(&mut self, mut bytes: &[u8]) -> Result<(), SelectError> {
        let delimiter = self.delimiter[0];
        while let Some(index) = bytes.iter().position(|byte| *byte == delimiter) {
            let payload_bytes = self.record_bytes.checked_add(index).ok_or(SelectError::OverMaxRecordSize)?;
            if payload_bytes > MAX_SELECT_RECORD_BYTES {
                return Err(SelectError::OverMaxRecordSize);
            }
            self.record_bytes = 0;
            bytes = &bytes[index + 1..];
        }
        self.record_bytes = self
            .record_bytes
            .checked_add(bytes.len())
            .ok_or(SelectError::OverMaxRecordSize)?;
        if self.record_bytes > MAX_SELECT_RECORD_BYTES {
            return Err(SelectError::OverMaxRecordSize);
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), SelectError> {
        if self.record_bytes > MAX_SELECT_RECORD_BYTES {
            Err(SelectError::OverMaxRecordSize)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
pub(crate) async fn encode_compressed_fixture(format: CompressionFormat, input: &[u8]) -> Vec<u8> {
    use tokio::io::AsyncWriteExt as _;

    let cursor = std::io::Cursor::new(Vec::new());
    match format {
        CompressionFormat::Gzip => {
            let mut encoder = async_compression::tokio::write::GzipEncoder::new(cursor);
            encoder.write_all(input).await.expect("gzip fixture should encode");
            encoder.shutdown().await.expect("gzip fixture should finish");
            encoder.into_inner().into_inner()
        }
        CompressionFormat::Bzip2 => {
            let mut encoder = async_compression::tokio::write::BzEncoder::new(cursor);
            encoder.write_all(input).await.expect("bzip2 fixture should encode");
            encoder.shutdown().await.expect("bzip2 fixture should finish");
            encoder.into_inner().into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SelectInputMetricsSnapshot;
    use futures::TryStreamExt;
    use std::{io::Cursor, sync::Mutex as StdMutex, thread::ThreadId};
    use tokio::io::{AsyncWriteExt, DuplexStream};

    struct ThreadRecordingReader {
        inner: Cursor<Vec<u8>>,
        thread_id: Arc<StdMutex<Option<ThreadId>>>,
    }

    impl AsyncRead for ThreadRecordingReader {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let mut thread_id = this.thread_id.lock().expect("thread recorder mutex should not be poisoned");
            thread_id.get_or_insert_with(|| std::thread::current().id());
            drop(thread_id);
            Pin::new(&mut this.inner).poll_read(cx, buffer)
        }
    }

    struct ErrorAfterReader {
        inner: Cursor<Vec<u8>>,
        end: u64,
        failed: bool,
    }

    impl AsyncRead for ErrorAfterReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.inner.position() < self.end {
                return Pin::new(&mut self.inner).poll_read(cx, buffer);
            }
            if !self.failed {
                self.failed = true;
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "injected source failure")));
            }
            Poll::Ready(Ok(()))
        }
    }

    async fn decode(format: CompressionFormat, compressed: Vec<u8>) -> (ObjectStoreResult<Vec<u8>>, SelectInputMetricsSnapshot) {
        let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
        let metrics = Arc::new(SelectInputMetrics::default());
        let stream = compressed_input_stream(
            Box::new(Cursor::new(compressed)),
            compressed_len,
            format,
            Arc::clone(&metrics),
            b"\n".to_vec(),
            u64::MAX,
            None,
        )
        .expect("record delimiter should be valid");
        let result = stream.try_collect::<Vec<_>>().await.map(|chunks| chunks.concat());
        (result, metrics.snapshot())
    }

    fn select_error(error: &ObjectStoreError) -> Option<SelectError> {
        find_error_source::<SelectError>(error).cloned()
    }

    #[test]
    fn protocol_limits_match_the_s3_select_contract() {
        assert_eq!(MAX_SELECT_RECORD_BYTES, 1_048_576);
        assert_eq!(MAX_SELECT_PROCESSED_BYTES, 5_497_558_138_880);
    }

    #[tokio::test]
    async fn gzip_and_bzip2_preserve_bytes_and_metric_boundaries() {
        const INPUT: &[u8] = b"name,age\nAlice,30\n";
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let compressed = encode_compressed_fixture(format, INPUT).await;
            let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
            let (decoded, metrics) = decode(format, compressed).await;

            assert_eq!(decoded.expect("valid compressed input should decode"), INPUT);
            assert_eq!(metrics.bytes_scanned, compressed_len);
            assert_eq!(
                metrics.bytes_processed,
                u64::try_from(INPUT.len()).expect("fixture length should fit in u64")
            );
        }
    }

    #[tokio::test]
    async fn source_read_errors_are_not_reclassified_as_truncated_input() {
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let compressed = encode_compressed_fixture(format, b"name\nAlice\n").await;
            let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
            let expected_size = u64::try_from(compressed.len() + 1).expect("fixture length should fit in u64");
            let stream = compressed_input_stream(
                Box::new(ErrorAfterReader {
                    inner: Cursor::new(compressed),
                    end: compressed_len,
                    failed: false,
                }),
                expected_size,
                format,
                Arc::new(SelectInputMetrics::default()),
                b"\n".to_vec(),
                u64::MAX,
                None,
            )
            .expect("record delimiter should be valid");

            let error = stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("a storage read failure must terminate decoding");

            assert_eq!(select_error(&error), None, "{format:?} must not report TruncatedInput: {error:?}");
            assert!(
                find_error_source::<CompressedSourceReadError>(&error).is_some(),
                "{format:?} must preserve the source error class"
            );
        }
    }

    #[tokio::test]
    async fn source_read_error_inside_gzip_header_is_not_truncated_input() {
        let compressed = encode_compressed_fixture(CompressionFormat::Gzip, b"name\nAlice\n").await;
        let partial_header = compressed[..5].to_vec();
        let stream = compressed_input_stream(
            Box::new(Cursor::new(partial_header)),
            10,
            CompressionFormat::Gzip,
            Arc::new(SelectInputMetrics::default()),
            b"\n".to_vec(),
            u64::MAX,
            None,
        )
        .expect("record delimiter should be valid");

        let error = stream
            .try_collect::<Vec<_>>()
            .await
            .expect_err("a source failure inside the GZIP header must terminate decoding");

        assert_eq!(select_error(&error), None, "source failure must not report TruncatedInput: {error:?}");
        assert!(
            find_error_source::<CompressedSourceReadError>(&error).is_some(),
            "the source error class must survive GZIP header validation"
        );
    }

    #[tokio::test]
    async fn concatenated_members_decode_in_order() {
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let mut compressed = encode_compressed_fixture(format, b"a\n").await;
            compressed.extend_from_slice(&encode_compressed_fixture(format, b"b\n").await);
            let (decoded, _) = decode(format, compressed).await;
            assert_eq!(decoded.expect("valid concatenated members should decode"), b"a\nb\n");
        }

        let mut compressed = encode_compressed_fixture(CompressionFormat::Gzip, b"a\n").await;
        let (second, _) = gzip_with_optional_header_crc(b"b\n").await;
        compressed.extend_from_slice(&second);
        let (decoded, _) = decode(CompressionFormat::Gzip, compressed).await;
        assert_eq!(decoded.expect("optional headers must work in later GZIP members"), b"a\nb\n");
    }

    #[tokio::test]
    async fn invalid_initial_headers_are_typed_as_invalid_compression() {
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let (result, _) = decode(format, b"not compressed\n".to_vec()).await;
            let error = result.expect_err("invalid compression header must fail");
            let compression = format.name();
            assert_eq!(select_error(&error), Some(SelectError::InvalidCompressionFormatForObject { compression }));
            assert_eq!(
                select_error(&error).expect("typed compression error").to_string(),
                format!("{compression} is not applicable to the queried object. Please correct the request and try again.")
            );
        }
    }

    #[tokio::test]
    async fn fixed_header_variants_are_validated_before_decoding() {
        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, b"name\nAlice\n").await;
        for (position, invalid) in [(2, 0), (3, 0x20)] {
            let mut mutated = encoded.clone();
            mutated[position] = invalid;
            let (result, _) = decode(CompressionFormat::Gzip, mutated).await;
            assert_eq!(
                select_error(&result.expect_err("invalid GZIP fixed header must fail")),
                Some(CompressionFormat::Gzip.invalid_header_error())
            );
        }

        let (result, _) = decode(CompressionFormat::Bzip2, b"BZh0".to_vec()).await;
        assert_eq!(
            select_error(&result.expect_err("invalid BZIP2 block size must fail")),
            Some(CompressionFormat::Bzip2.invalid_header_error())
        );
    }

    #[tokio::test]
    async fn incomplete_valid_headers_are_typed_as_truncated_input() {
        for (format, prefixes) in [
            (CompressionFormat::Gzip, vec![b"".as_slice(), b"\x1f", b"\x1f\x8b", b"\x1f\x8b\x08"]),
            (CompressionFormat::Bzip2, vec![b"".as_slice(), b"B", b"BZ", b"BZh"]),
        ] {
            for prefix in prefixes {
                let (result, _) = decode(format, prefix.to_vec()).await;
                assert_eq!(
                    select_error(&result.expect_err("incomplete compression header must fail")),
                    Some(SelectError::TruncatedInput)
                );
            }
        }
    }

    async fn gzip_with_optional_header_crc(input: &[u8]) -> (Vec<u8>, usize) {
        const EXTRA: &[u8] = b"s3-select";
        const FILE_NAME: &[u8] = b"select.csv";
        const COMMENT: &[u8] = b"fixture";

        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, input).await;
        let mut header = encoded[..10].to_vec();
        header[3] = GZIP_FLAG_EXTRA | GZIP_FLAG_NAME | GZIP_FLAG_COMMENT | GZIP_FLAG_HEADER_CRC;
        header.extend_from_slice(
            &u16::try_from(EXTRA.len())
                .expect("GZIP extra fixture should fit its length field")
                .to_le_bytes(),
        );
        header.extend_from_slice(EXTRA);
        header.extend_from_slice(FILE_NAME);
        header.push(0);
        header.extend_from_slice(COMMENT);
        header.push(0);
        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        digest.update(&header);
        let crc = digest.finalize();
        let crc = u16::try_from(crc & u64::from(u16::MAX)).expect("masked header CRC should fit in u16");
        let crc_offset = header.len();
        header.extend_from_slice(&crc.to_le_bytes());
        header.extend_from_slice(&encoded[10..]);
        (header, crc_offset)
    }

    async fn gzip_with_text_header(input: &[u8], flag: u8, field_bytes: usize) -> Vec<u8> {
        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, input).await;
        let mut member = encoded[..10].to_vec();
        member[3] = flag;
        member.extend(std::iter::repeat_n(b'x', field_bytes));
        member.push(0);
        member.extend_from_slice(&encoded[10..]);
        member
    }

    async fn gzip_with_text_header_crc(input: &[u8], flag: u8, field_bytes: usize) -> (Vec<u8>, usize) {
        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, input).await;
        let mut member = encoded[..10].to_vec();
        member[3] = flag | GZIP_FLAG_HEADER_CRC;
        member.extend(std::iter::repeat_n(b'x', field_bytes));
        member.push(0);
        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        digest.update(&member);
        let crc = u16::try_from(digest.finalize() & u64::from(u16::MAX)).expect("masked header CRC should fit in u16");
        let crc_offset = member.len();
        member.extend_from_slice(&crc.to_le_bytes());
        member.extend_from_slice(&encoded[10..]);
        (member, crc_offset)
    }

    async fn gzip_with_extra_header_crc(input: &[u8], extra_bytes: usize) -> (Vec<u8>, usize) {
        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, input).await;
        let mut member = encoded[..10].to_vec();
        member[3] = GZIP_FLAG_EXTRA | GZIP_FLAG_HEADER_CRC;
        member.extend_from_slice(
            &u16::try_from(extra_bytes)
                .expect("GZIP extra fixture should fit its length field")
                .to_le_bytes(),
        );
        member.extend(std::iter::repeat_n(b'x', extra_bytes));
        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        digest.update(&member);
        let crc = u16::try_from(digest.finalize() & u64::from(u16::MAX)).expect("masked header CRC should fit in u16");
        let crc_offset = member.len();
        member.extend_from_slice(&crc.to_le_bytes());
        member.extend_from_slice(&encoded[10..]);
        (member, crc_offset)
    }

    #[tokio::test]
    async fn gzip_optional_header_crc_is_validated_without_error_strings() {
        const INPUT: &[u8] = b"name\nAlice\n";

        let (encoded, crc_offset) = gzip_with_optional_header_crc(INPUT).await;
        let (decoded, _) = decode(CompressionFormat::Gzip, encoded.clone()).await;
        assert_eq!(decoded.expect("valid optional GZIP header should decode"), INPUT);

        let mut corrupt = encoded;
        corrupt[crc_offset] ^= 0xff;
        let (result, _) = decode(CompressionFormat::Gzip, corrupt).await;
        assert_eq!(
            select_error(&result.expect_err("invalid GZIP header CRC must fail")),
            Some(SelectError::InvalidCompressionFormatForObject {
                compression: CompressionFormat::Gzip.name(),
            })
        );

        let mut truncated = encode_compressed_fixture(CompressionFormat::Gzip, INPUT).await;
        truncated.truncate(10);
        truncated[3] = GZIP_FLAG_NAME;
        truncated.extend_from_slice(b"unterminated-name");
        let (result, _) = decode(CompressionFormat::Gzip, truncated).await;
        assert_eq!(
            select_error(&result.expect_err("incomplete optional GZIP header must fail")),
            Some(SelectError::TruncatedInput)
        );
    }

    #[tokio::test]
    async fn gzip_optional_header_can_cross_source_chunks() {
        const INPUT: &[u8] = b"name\nAlice\n";
        const LARGE_GZIP_TEXT_FIELD_BYTES: usize = SELECT_DECODE_CHUNK_BYTES * 3 + 17;

        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, INPUT).await;
        let mut with_name = encoded[..10].to_vec();
        with_name[3] = GZIP_FLAG_NAME;
        with_name.extend(std::iter::repeat_n(b'x', LARGE_GZIP_TEXT_FIELD_BYTES));
        with_name.push(0);
        with_name.extend_from_slice(&encoded[10..]);
        let expected_scanned = u64::try_from(with_name.len()).expect("fixture length should fit in u64");
        let (mut writer, reader) = tokio::io::duplex(257);
        let writer = tokio::spawn(async move {
            writer
                .write_all(&with_name)
                .await
                .expect("fixture source should accept bytes");
            writer.shutdown().await.expect("fixture source should close");
        });
        let metrics = Arc::new(SelectInputMetrics::default());
        let stream = compressed_input_stream(
            Box::new(reader),
            expected_scanned,
            CompressionFormat::Gzip,
            Arc::clone(&metrics),
            b"\n".to_vec(),
            u64::MAX,
            None,
        )
        .expect("record delimiter should be valid");
        let decoded = stream
            .try_collect::<Vec<_>>()
            .await
            .map(|chunks| chunks.concat())
            .expect("chunked optional GZIP names should decode");
        writer.await.expect("fixture writer should complete");

        assert_eq!(decoded, INPUT);
        assert_eq!(metrics.snapshot().bytes_scanned, expected_scanned);
    }

    #[tokio::test]
    async fn valid_512_byte_text_fields_decode_in_every_gzip_member() {
        for flag in [GZIP_FLAG_NAME, GZIP_FLAG_COMMENT] {
            let mut encoded = encode_compressed_fixture(CompressionFormat::Gzip, b"a\n").await;
            encoded.extend_from_slice(&gzip_with_text_header(b"b\n", flag, 512).await);
            let (decoded, _) = decode(CompressionFormat::Gzip, encoded).await;
            assert_eq!(decoded.expect("RFC 1952 does not limit zero-terminated text fields"), b"a\nb\n");
        }
    }

    #[tokio::test]
    async fn long_text_field_header_crc_accumulates_across_source_chunks() {
        const FIELD_BYTES: usize = SELECT_DECODE_CHUNK_BYTES * 3 + 17;

        for flag in [GZIP_FLAG_NAME, GZIP_FLAG_COMMENT] {
            let first = encode_compressed_fixture(CompressionFormat::Gzip, b"a\n").await;
            let (second, crc_offset) = gzip_with_text_header_crc(b"b\n", flag, FIELD_BYTES).await;

            let mut valid = first.clone();
            valid.extend_from_slice(&second);
            let (decoded, _) = decode(CompressionFormat::Gzip, valid).await;
            assert_eq!(decoded.expect("multi-chunk GZIP header CRC should validate"), b"a\nb\n");

            let mut corrupt_second = second;
            corrupt_second[crc_offset] ^= 0xff;
            let mut corrupt = first;
            corrupt.extend_from_slice(&corrupt_second);
            let (result, _) = decode(CompressionFormat::Gzip, corrupt).await;
            assert_eq!(
                select_error(&result.expect_err("corrupt multi-chunk GZIP header CRC must fail")),
                Some(SelectError::TruncatedInput)
            );
        }
    }

    #[tokio::test]
    async fn maximum_extra_field_header_crc_accumulates_across_source_chunks() {
        const INPUT: &[u8] = b"name\nAlice\n";
        let (valid, crc_offset) = gzip_with_extra_header_crc(INPUT, usize::from(u16::MAX)).await;

        let (decoded, _) = decode(CompressionFormat::Gzip, valid.clone()).await;
        assert_eq!(decoded.expect("maximum GZIP extra field should decode"), INPUT);

        let mut corrupt = valid;
        corrupt[crc_offset] ^= 0xff;
        let (result, _) = decode(CompressionFormat::Gzip, corrupt).await;
        assert_eq!(
            select_error(&result.expect_err("corrupt multi-chunk GZIP extra-field CRC must fail")),
            Some(CompressionFormat::Gzip.invalid_header_error())
        );
    }

    #[test]
    fn processed_byte_limit_is_independent_of_live_memory_budget() {
        assert_eq!(processed_bytes_limit(), MAX_SELECT_PROCESSED_BYTES);
        assert!(processed_bytes_limit() > 80 * 1024 * 1024);
    }

    #[test]
    fn cooperative_reader_yields_after_bounded_ready_input() {
        let source = Cursor::new(vec![0_u8; SELECT_DECODE_CHUNK_BYTES + 1]);
        let mut reader = CooperativeReader::new(source);
        let mut output = vec![0_u8; SELECT_DECODE_CHUNK_BYTES + 1];
        let mut read_buf = ReadBuf::new(&mut output);
        let waker = futures::task::noop_waker_ref();
        let mut context = Context::from_waker(waker);

        assert!(Pin::new(&mut reader).poll_read(&mut context, &mut read_buf).is_ready());
        assert_eq!(read_buf.filled().len(), SELECT_DECODE_CHUNK_BYTES);
        assert!(Pin::new(&mut reader).poll_read(&mut context, &mut read_buf).is_pending());
        assert!(Pin::new(&mut reader).poll_read(&mut context, &mut read_buf).is_ready());
        assert_eq!(read_buf.filled().len(), SELECT_DECODE_CHUNK_BYTES + 1);
    }

    #[tokio::test]
    async fn bzip2_decoded_output_yields_at_the_cooperative_quantum() {
        let input = vec![b'x'; SELECT_DECODE_CHUNK_BYTES * 2];
        let compressed = encode_compressed_fixture(CompressionFormat::Bzip2, &input).await;
        let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
        let mut reader = compressed_input_reader(
            Box::new(Cursor::new(compressed)),
            compressed_len,
            CompressionFormat::Bzip2,
            Arc::new(SelectInputMetrics::default()),
            processed_bytes_limit(),
            None,
        );
        let mut output = vec![0; SELECT_DECODE_CHUNK_BYTES * 2];
        let mut read_buf = ReadBuf::new(&mut output);
        while read_buf.filled().len() < SELECT_DECODE_CHUNK_BYTES {
            futures::future::poll_fn(|cx| Pin::new(&mut reader).poll_read(cx, &mut read_buf))
                .await
                .expect("valid BZIP2 input should decode");
        }
        assert_eq!(read_buf.filled().len(), SELECT_DECODE_CHUNK_BYTES);

        let waker = futures::task::noop_waker_ref();
        let mut context = Context::from_waker(waker);
        assert!(Pin::new(&mut reader).poll_read(&mut context, &mut read_buf).is_pending());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bzip2_decoder_polls_source_off_runtime_thread() {
        let input = b"name\nAlice\n";
        let compressed = encode_compressed_fixture(CompressionFormat::Bzip2, input).await;
        let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
        let runtime_thread = std::thread::current().id();
        let source_thread = Arc::new(StdMutex::new(None));
        let source = ThreadRecordingReader {
            inner: Cursor::new(compressed),
            thread_id: Arc::clone(&source_thread),
        };
        let mut reader = compressed_input_reader(
            Box::new(source),
            compressed_len,
            CompressionFormat::Bzip2,
            Arc::new(SelectInputMetrics::default()),
            processed_bytes_limit(),
            None,
        );
        let mut decoded = Vec::new();

        reader
            .read_to_end(&mut decoded)
            .await
            .expect("valid BZIP2 input should decode off the runtime thread");

        assert_eq!(decoded, input);
        assert_ne!(
            source_thread
                .lock()
                .expect("thread recorder mutex should not be poisoned")
                .expect("compressed source should be polled"),
            runtime_thread,
            "BZIP2 decoding must not poll codec work on a Tokio runtime worker"
        );
    }

    #[tokio::test]
    async fn decoder_thread_holds_query_admission_until_exit() {
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(
            Arc::clone(&admission)
                .try_acquire_owned()
                .expect("query admission should be available"),
        );
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (decoded_tx, _decoded_rx) = mpsc::channel(1);

        spawn_decoder_thread("s3select-guard-test", decoded_tx, Some(permit), move || {
            let _ = started_tx.send(());
            release_rx.recv().map_err(io::Error::other)
        });

        started_rx.await.expect("decoder thread should start");
        assert!(Arc::clone(&admission).try_acquire_owned().is_err());
        release_tx.send(()).expect("decoder thread should be releasable");
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(1), Arc::clone(&admission).acquire_owned())
            .await
            .expect("decoder thread should release admission promptly")
            .expect("query admission should remain open");
        drop(recovered);
    }

    #[test]
    fn bzip2_decoder_does_not_wait_for_tokio_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("test runtime should build");

        runtime.block_on(async {
            let (blocker_started_tx, blocker_started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = blocker_started_tx.send(());
                let _ = release_rx.recv();
            });
            blocker_started_rx.await.expect("blocking worker should be occupied");

            let input = b"name\nAlice\n";
            let compressed = encode_compressed_fixture(CompressionFormat::Bzip2, input).await;
            let decode_result =
                tokio::time::timeout(std::time::Duration::from_secs(2), decode(CompressionFormat::Bzip2, compressed)).await;

            let _ = release_tx.send(());
            blocker.await.expect("blocking worker should finish");

            let (decoded, _) = decode_result.expect("BZIP2 decoder must not queue behind Tokio blocking work");
            assert_eq!(decoded.expect("valid BZIP2 input should decode"), input);
        });
    }

    #[tokio::test]
    async fn processed_reader_streams_past_the_default_memory_budget() {
        let expected = 64 * 1024 * 1024 + 1;
        let metrics = Arc::new(SelectInputMetrics::default());
        let source = tokio::io::repeat(b'x').take(expected);
        let mut reader = ProcessedReader::new(source, metrics.recorder(), processed_bytes_limit());
        let mut buffer = vec![0; SELECT_DECODE_CHUNK_BYTES];
        let mut total = 0_u64;

        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .expect("streaming input within the expansion budget should pass");
            if read == 0 {
                break;
            }
            total += u64::try_from(read).expect("read buffer length fits in u64");
        }

        assert_eq!(total, expected);
        assert_eq!(metrics.snapshot().bytes_processed, expected);
    }

    #[tokio::test]
    async fn truncated_and_corrupt_gzip_are_typed_as_truncated_input() {
        let encoded = encode_compressed_fixture(CompressionFormat::Gzip, b"name\nAlice\n").await;

        let mut truncated = encoded.clone();
        truncated.truncate(truncated.len() - 1);
        let (result, _) = decode(CompressionFormat::Gzip, truncated).await;
        assert_eq!(
            select_error(&result.expect_err("truncated gzip trailer must fail")),
            Some(SelectError::TruncatedInput)
        );

        let mut corrupt = encoded;
        let checksum_index = corrupt.len() - 8;
        corrupt[checksum_index] ^= 0xff;
        let (result, _) = decode(CompressionFormat::Gzip, corrupt).await;
        assert_eq!(
            select_error(&result.expect_err("corrupt gzip checksum must fail")),
            Some(SelectError::TruncatedInput)
        );

        let mut corrupt_size = encode_compressed_fixture(CompressionFormat::Gzip, b"name\nAlice\n").await;
        let size_index = corrupt_size.len() - 1;
        corrupt_size[size_index] ^= 0xff;
        let (result, _) = decode(CompressionFormat::Gzip, corrupt_size).await;
        assert_eq!(
            select_error(&result.expect_err("corrupt gzip uncompressed size must fail")),
            Some(SelectError::TruncatedInput)
        );
    }

    #[tokio::test]
    async fn truncated_bzip2_is_typed_as_truncated_input() {
        let mut encoded = encode_compressed_fixture(CompressionFormat::Bzip2, b"name\nAlice\n").await;
        encoded.truncate(encoded.len() - 1);
        let (result, _) = decode(CompressionFormat::Bzip2, encoded).await;
        assert_eq!(
            select_error(&result.expect_err("truncated bzip2 trailer must fail")),
            Some(SelectError::TruncatedInput)
        );

        let mut corrupt = encode_compressed_fixture(CompressionFormat::Bzip2, b"name\nAlice\n").await;
        let checksum_index = corrupt.len() - 2;
        corrupt[checksum_index] ^= 0xff;
        let (result, _) = decode(CompressionFormat::Bzip2, corrupt).await;
        assert_eq!(
            select_error(&result.expect_err("corrupt bzip2 checksum must fail")),
            Some(SelectError::TruncatedInput)
        );
    }

    #[tokio::test]
    async fn trailing_non_member_bytes_fail_closed() {
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let mut encoded = encode_compressed_fixture(format, b"name\nAlice\n").await;
            encoded.extend_from_slice(b"trailing garbage");
            let (result, _) = decode(format, encoded).await;
            assert_eq!(
                select_error(&result.expect_err("trailing bytes must not be ignored")),
                Some(SelectError::TruncatedInput)
            );
        }
    }

    #[tokio::test]
    async fn oversized_compressed_record_fails_before_unbounded_buffering() {
        let mut input = vec![b'x'; MAX_SELECT_RECORD_BYTES + 1];
        input.push(b'\n');
        let compressed = encode_compressed_fixture(CompressionFormat::Gzip, &input).await;
        let (result, _) = decode(CompressionFormat::Gzip, compressed).await;
        assert_eq!(
            select_error(&result.expect_err("oversized compressed record must fail")),
            Some(SelectError::OverMaxRecordSize)
        );
    }

    #[tokio::test]
    async fn one_megabyte_record_is_accepted_with_or_without_delimiter() {
        for terminated in [false, true] {
            let mut input = vec![b'x'; MAX_SELECT_RECORD_BYTES];
            if terminated {
                input.push(b'\n');
            }
            let compressed = encode_compressed_fixture(CompressionFormat::Gzip, &input).await;
            let (decoded, _) = decode(CompressionFormat::Gzip, compressed).await;
            assert_eq!(decoded.expect("record at the protocol limit should decode"), input);
        }
    }

    #[tokio::test]
    async fn processed_byte_limit_rejects_many_small_records() {
        let input = b"{}\n".repeat(1024);
        let compressed = encode_compressed_fixture(CompressionFormat::Gzip, &input).await;
        let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
        let exact_stream = compressed_input_stream(
            Box::new(Cursor::new(compressed.clone())),
            compressed_len,
            CompressionFormat::Gzip,
            Arc::new(SelectInputMetrics::default()),
            b"\n".to_vec(),
            u64::try_from(input.len()).expect("fixture length should fit in u64"),
            None,
        )
        .expect("record delimiter should be valid");
        assert_eq!(
            exact_stream
                .try_collect::<Vec<_>>()
                .await
                .expect("decoded bytes at the processed limit should pass")
                .concat(),
            input
        );

        let stream = compressed_input_stream(
            Box::new(Cursor::new(compressed)),
            compressed_len,
            CompressionFormat::Gzip,
            Arc::new(SelectInputMetrics::default()),
            b"\n".to_vec(),
            u64::try_from(input.len() - 1).expect("fixture length should fit in u64"),
            None,
        )
        .expect("record delimiter should be valid");

        let error = stream
            .try_collect::<Vec<_>>()
            .await
            .expect_err("decoded bytes over the decompression budget must fail");
        assert_eq!(select_error(&error), Some(SelectError::ResourceExhausted));
    }

    #[tokio::test]
    async fn oversized_final_record_with_partial_delimiter_fails_at_eof() {
        let mut input = vec![b'x'; MAX_SELECT_RECORD_BYTES + 1];
        input.push(b'\r');
        let compressed = encode_compressed_fixture(CompressionFormat::Gzip, &input).await;
        let compressed_len = u64::try_from(compressed.len()).expect("fixture length should fit in u64");
        let metrics = Arc::new(SelectInputMetrics::default());
        let stream = compressed_input_stream(
            Box::new(Cursor::new(compressed)),
            compressed_len,
            CompressionFormat::Gzip,
            metrics,
            b"\r\n".to_vec(),
            u64::MAX,
            None,
        )
        .expect("record delimiter should be valid");

        let error = stream
            .try_collect::<Vec<_>>()
            .await
            .expect_err("oversized unterminated record must fail");
        assert_eq!(select_error(&error), Some(SelectError::OverMaxRecordSize));
    }

    struct DropObservedReader {
        inner: DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl AsyncRead for DropObservedReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl Drop for DropObservedReader {
        fn drop(&mut self) {
            self.dropped.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    #[tokio::test]
    async fn dropping_decoder_stream_releases_source_without_background_work() {
        let input = b"a\n".repeat(MAX_SELECT_RECORD_BYTES);
        for format in [CompressionFormat::Gzip, CompressionFormat::Bzip2] {
            let compressed = encode_compressed_fixture(format, &input).await;
            let compressed_len = u64::try_from(compressed.len() + 1).expect("fixture length should fit in u64");
            let (source, mut peer) = tokio::io::duplex(compressed.len());
            peer.write_all(&compressed).await.expect("write compressed fixture");
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let admission = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = Arc::new(
                Arc::clone(&admission)
                    .try_acquire_owned()
                    .expect("query admission should be available"),
            );
            let mut stream = compressed_input_stream(
                Box::new(DropObservedReader {
                    inner: source,
                    dropped: Arc::clone(&dropped),
                }),
                compressed_len,
                format,
                Arc::new(SelectInputMetrics::default()),
                b"\n".to_vec(),
                u64::MAX,
                Some(permit),
            )
            .expect("record delimiter should be valid");
            let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
                .await
                .expect("decoder should produce a chunk before source EOF")
                .expect("decoder stream should produce a chunk")
                .expect("valid partial decode should succeed");
            assert!(!first.is_empty());

            drop(stream);

            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("dropping decoded output must cancel the blocked source read");
            let recovered = tokio::time::timeout(std::time::Duration::from_secs(1), Arc::clone(&admission).acquire_owned())
                .await
                .expect("decoder exit should release query admission")
                .expect("query admission should remain open");
            drop(recovered);
        }
    }
}
