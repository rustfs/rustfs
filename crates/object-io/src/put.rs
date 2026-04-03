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

use bytes::Buf;
use futures_util::{Stream, StreamExt};
use http::HeaderMap;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::store_api::ObjectOptions;
use rustfs_rio::{
    BlockReadable, BoxReadBlockFuture, Checksum, EtagResolvable, HashReader, HashReaderDetector, Reader, TryGetIndex, WarpReader,
};
use rustfs_utils::http::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM;
use rustfs_utils::http::headers::{
    AMZ_DECODED_CONTENT_LENGTH, AMZ_MINIO_SNOWBALL_IGNORE_DIRS, AMZ_MINIO_SNOWBALL_IGNORE_ERRORS, AMZ_MINIO_SNOWBALL_PREFIX,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS, AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, AMZ_RUSTFS_SNOWBALL_PREFIX, AMZ_SERVER_SIDE_ENCRYPTION,
    AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, AMZ_SNOWBALL_EXTRACT, AMZ_SNOWBALL_IGNORE_DIRS, AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_PREFIX,
};
use s3s::dto::{ChecksumAlgorithm, PutObjectInput, ServerSideEncryption};
use s3s::{S3Error, s3_error};
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio_tar::Archive;
use tokio_util::io::StreamReader;

pub const AMZ_SNOWBALL_EXTRACT_COMPAT: &str = "X-Amz-Snowball-Auto-Extract";
pub const AMZ_SNOWBALL_PREFIX_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Prefix";
pub const AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Dirs";
pub const AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Errors";

const AMZ_META_PREFIX_LOWER: &str = "x-amz-meta-";
const SNOWBALL_PREFIX_SUFFIX_LOWER: &str = "snowball-prefix";
const SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER: &str = "snowball-ignore-dirs";
const SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER: &str = "snowball-ignore-errors";
const SNOWBALL_PREFIX_HEADER_KEYS: &[&str] = &[AMZ_MINIO_SNOWBALL_PREFIX, AMZ_SNOWBALL_PREFIX, AMZ_RUSTFS_SNOWBALL_PREFIX];
const SNOWBALL_IGNORE_DIRS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_DIRS,
    AMZ_SNOWBALL_IGNORE_DIRS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
];
const SNOWBALL_IGNORE_ERRORS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectChecksums {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub crc64nvme: Option<String>,
}

impl PutObjectChecksums {
    pub fn merge_from_map(&mut self, checksums: &HashMap<String, String>) {
        for (key, checksum) in checksums {
            match rustfs_rio::ChecksumType::from_string(key.as_str()) {
                rustfs_rio::ChecksumType::CRC32 => self.crc32 = Some(checksum.clone()),
                rustfs_rio::ChecksumType::CRC32C => self.crc32c = Some(checksum.clone()),
                rustfs_rio::ChecksumType::SHA1 => self.sha1 = Some(checksum.clone()),
                rustfs_rio::ChecksumType::SHA256 => self.sha256 = Some(checksum.clone()),
                rustfs_rio::ChecksumType::CRC64_NVME => self.crc64nvme = Some(checksum.clone()),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PutObjectTransformStage {
    compression_applied: bool,
    encryption_applied: bool,
}

impl PutObjectTransformStage {
    pub fn mark_compression(&mut self) {
        self.compression_applied = true;
    }

    pub fn mark_encryption(&mut self) {
        self.encryption_applied = true;
    }

    #[must_use]
    pub const fn compression_applied(self) -> bool {
        self.compression_applied
    }

    #[must_use]
    pub const fn encryption_applied(self) -> bool {
        self.encryption_applied
    }

    #[must_use]
    pub fn effective_copy_mode(self) -> rustfs_io_metrics::CopyMode {
        resolve_put_effective_copy_mode(self.compression_applied, self.encryption_applied)
    }

    #[must_use]
    pub fn metric_kind(self) -> Option<&'static str> {
        resolve_put_transform_metric_kind(self.compression_applied, self.encryption_applied)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutObjectCompatIngressKind {
    BufferedStreamCompat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectCompatIngressPlan {
    pub kind: PutObjectCompatIngressKind,
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutObjectIngressKind {
    LegacyCompat,
    ReducedCopyCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectIngressPlan {
    pub kind: PutObjectIngressKind,
    pub compat: PutObjectCompatIngressPlan,
    pub enable_zero_copy: bool,
}

pub type BoxIngressStream<B, E> = Pin<Box<dyn Stream<Item = Result<B, E>> + Send + Sync + 'static>>;
pub type PutObjectCompatIngressStream<S, B, E> = futures_util::stream::Map<S, fn(Result<B, E>) -> std::io::Result<B>>;
pub type PutObjectCompatIngress<S, B, E> = tokio::io::BufReader<StreamReader<PutObjectCompatIngressStream<S, B, E>, B>>;

pub fn box_put_object_ingress_stream<S, B, E>(body: S) -> BoxIngressStream<B, E>
where
    S: Stream<Item = Result<B, E>> + Send + Sync + 'static,
{
    Box::pin(body)
}

pub struct PutObjectReducedCopyIngress<B, E> {
    body: BoxIngressStream<B, E>,
    compat: PutObjectCompatIngressPlan,
}

impl<B, E> PutObjectReducedCopyIngress<B, E> {
    pub const fn new(body: BoxIngressStream<B, E>, compat: PutObjectCompatIngressPlan) -> Self {
        Self { body, compat }
    }

    pub fn from_stream<S>(body: S, compat: PutObjectCompatIngressPlan) -> Self
    where
        S: Stream<Item = Result<B, E>> + Send + Sync + 'static,
    {
        Self::new(box_put_object_ingress_stream(body), compat)
    }

    pub const fn compat_plan(&self) -> PutObjectCompatIngressPlan {
        self.compat
    }

    pub fn into_body(self) -> BoxIngressStream<B, E> {
        self.body
    }
}

impl<B, E> PutObjectReducedCopyIngress<B, E>
where
    B: Buf,
    E: std::fmt::Display,
    PutObjectCompatIngress<BoxIngressStream<B, E>, B, E>: Send + Sync + Unpin + 'static,
{
    pub fn into_compat_reader(self) -> Box<dyn Reader> {
        build_put_object_compat_reader(self.body, self.compat)
    }
}

pub enum PutObjectIngressSource<B, E> {
    LegacyCompat(Box<dyn Reader>),
    ReducedCopyCandidate(PutObjectReducedCopyIngress<B, E>),
}

struct PutObjectReducedCopyReader<S, B> {
    body: S,
    current_chunk: Option<B>,
    pending_error: Option<std::io::Error>,
}

impl<S, B> PutObjectReducedCopyReader<S, B> {
    fn new(body: S) -> Self {
        Self {
            body,
            current_chunk: None,
            pending_error: None,
        }
    }

    fn copy_chunk_into_slice(chunk: &mut B, buf: &mut [u8]) -> usize
    where
        B: Buf,
    {
        let mut copied = 0;
        let to_copy = chunk.remaining().min(buf.len());

        while copied < to_copy {
            let slice = chunk.chunk();
            if !slice.is_empty() {
                let len = (to_copy - copied).min(slice.len());
                buf[copied..copied + len].copy_from_slice(&slice[..len]);
                chunk.advance(len);
                copied += len;
                continue;
            }

            let dest = &mut buf[copied..to_copy];
            chunk.copy_to_slice(dest);
            copied = to_copy;
        }

        copied
    }

    fn copy_chunk_into_read_buf(chunk: &mut B, buf: &mut tokio::io::ReadBuf<'_>) -> usize
    where
        B: Buf,
    {
        let to_copy = chunk.remaining().min(buf.remaining());
        let dest = &mut buf.initialize_unfilled()[..to_copy];
        let copied = Self::copy_chunk_into_slice(chunk, dest);
        buf.advance(copied);
        copied
    }

    async fn read_into_slice<E>(&mut self, buf: &mut [u8]) -> std::io::Result<usize>
    where
        S: Stream<Item = Result<B, E>> + Unpin,
        B: Buf + Unpin,
        E: std::fmt::Display,
    {
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }

        if buf.is_empty() {
            return Ok(0);
        }

        let mut copied = 0;

        loop {
            if copied == buf.len() {
                return Ok(copied);
            }

            if let Some(chunk) = self.current_chunk.as_mut() {
                if !chunk.has_remaining() {
                    self.current_chunk = None;
                    continue;
                }

                copied += Self::copy_chunk_into_slice(chunk, &mut buf[copied..]);
                if chunk.has_remaining() || copied == buf.len() {
                    return Ok(copied);
                }

                self.current_chunk = None;
                continue;
            }

            match self.body.next().await {
                Some(Ok(chunk)) => {
                    if chunk.remaining() == 0 {
                        continue;
                    }
                    self.current_chunk = Some(chunk);
                }
                Some(Err(err)) => {
                    let err = std::io::Error::other(err.to_string());
                    if copied > 0 {
                        self.pending_error = Some(err);
                        return Ok(copied);
                    }
                    return Err(err);
                }
                None => return Ok(copied),
            }
        }
    }
}

impl<S, B, E> AsyncRead for PutObjectReducedCopyReader<S, B>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: Buf + Unpin,
    E: std::fmt::Display,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if let Some(err) = this.pending_error.take() {
            return std::task::Poll::Ready(Err(err));
        }

        let mut read_any = false;

        loop {
            if buf.remaining() == 0 {
                return std::task::Poll::Ready(Ok(()));
            }

            if let Some(chunk) = this.current_chunk.as_mut() {
                if !chunk.has_remaining() {
                    this.current_chunk = None;
                    continue;
                }

                if Self::copy_chunk_into_read_buf(chunk, buf) > 0 {
                    read_any = true;
                }

                if chunk.has_remaining() || buf.remaining() == 0 {
                    return std::task::Poll::Ready(Ok(()));
                }

                this.current_chunk = None;
                continue;
            }

            match std::pin::Pin::new(&mut this.body).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(chunk))) => {
                    if chunk.remaining() == 0 {
                        continue;
                    }
                    this.current_chunk = Some(chunk);
                }
                std::task::Poll::Ready(Some(Err(err))) => {
                    let err = std::io::Error::other(err.to_string());
                    if read_any {
                        this.pending_error = Some(err);
                        return std::task::Poll::Ready(Ok(()));
                    }
                    return std::task::Poll::Ready(Err(err));
                }
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(Ok(())),
                std::task::Poll::Pending => {
                    if read_any {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    return std::task::Poll::Pending;
                }
            }
        }
    }
}

impl<S, B, E> BlockReadable for PutObjectReducedCopyReader<S, B>
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + Sync,
    B: Buf + Unpin + Send + Sync,
    E: std::fmt::Display + Send + Sync,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        Box::pin(async move { self.read_into_slice(buf).await })
    }
}

impl<S, B> EtagResolvable for PutObjectReducedCopyReader<S, B> {}

impl<S, B> HashReaderDetector for PutObjectReducedCopyReader<S, B> {}

impl<S, B> TryGetIndex for PutObjectReducedCopyReader<S, B> {}

impl<S, B, E> Reader for PutObjectReducedCopyReader<S, B>
where
    S: Stream<Item = Result<B, E>> + Unpin + Send + Sync,
    B: Buf + Unpin + Send + Sync,
    E: std::fmt::Display + Send + Sync,
{
}

fn build_put_object_reduced_copy_reader<B, E>(candidate: PutObjectReducedCopyIngress<B, E>) -> Box<dyn Reader>
where
    B: Buf + Send + Sync + Unpin + 'static,
    E: std::fmt::Display + Send + Sync + 'static,
{
    Box::new(PutObjectReducedCopyReader::new(candidate.into_body()))
}

impl<B, E> PutObjectIngressSource<B, E>
where
    B: Buf,
    E: std::fmt::Display,
    PutObjectCompatIngress<BoxIngressStream<B, E>, B, E>: Send + Sync + Unpin + 'static,
{
    pub fn into_compat_reader(self) -> Box<dyn Reader> {
        match self {
            Self::LegacyCompat(reader) => reader,
            Self::ReducedCopyCandidate(candidate) => candidate.into_compat_reader(),
        }
    }

    pub fn into_reader(self) -> Box<dyn Reader>
    where
        B: Buf + Send + Sync + Unpin + 'static,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        match self {
            Self::LegacyCompat(reader) => reader,
            Self::ReducedCopyCandidate(candidate) => build_put_object_reduced_copy_reader(candidate),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutObjectPlainBodyKind {
    LegacyCompat,
    ReducedCopyCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutObjectBodyKind {
    Plain(PutObjectPlainBodyKind),
    Compressed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectBodyPlan {
    pub ingress: PutObjectIngressPlan,
    pub kind: PutObjectBodyKind,
}

impl PutObjectBodyPlan {
    pub const fn should_compress(&self) -> bool {
        matches!(self.kind, PutObjectBodyKind::Compressed)
    }

    pub const fn plain_body_kind(&self) -> Option<PutObjectPlainBodyKind> {
        match self.kind {
            PutObjectBodyKind::Plain(kind) => Some(kind),
            PutObjectBodyKind::Compressed => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectExtractOptions {
    pub prefix: Option<String>,
    pub ignore_dirs: bool,
    pub ignore_errors: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectLegacyHashValues {
    pub md5hex: Option<String>,
    pub sha256hex: Option<String>,
}

impl PutObjectLegacyHashValues {
    pub fn clear_for_transformed_body(&mut self) {
        self.md5hex = None;
        self.sha256hex = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectLegacyHashStagePlan {
    pub size: i64,
    pub actual_size: i64,
    pub apply_s3_checksum: bool,
    pub ignore_s3_checksum_value: bool,
}

pub struct PutObjectHashStage {
    pub reader: HashReader,
    pub want_checksum: Option<Checksum>,
    pub ingress_kind: PutObjectIngressKind,
}

fn build_put_object_hash_stage(
    reader: Box<dyn Reader>,
    ingress_kind: PutObjectIngressKind,
    hash_values: PutObjectLegacyHashValues,
    plan: PutObjectLegacyHashStagePlan,
    headers: &HeaderMap,
    trailing_headers: Option<s3s::TrailingHeaders>,
) -> std::io::Result<PutObjectHashStage> {
    let mut reader = HashReader::new(reader, plan.size, plan.actual_size, hash_values.md5hex, hash_values.sha256hex, false)?;
    let requested_checksum_type = rustfs_rio::ChecksumType::from_header(headers);
    let want_checksum = if plan.apply_s3_checksum {
        reader.add_checksum_from_s3s(headers, trailing_headers, plan.ignore_s3_checksum_value)?;
        if requested_checksum_type.is_set() && reader.checksum().is_none() {
            reader.enable_auto_checksum(requested_checksum_type)?;
        }
        reader.checksum()
    } else {
        None
    };

    Ok(PutObjectHashStage {
        reader,
        want_checksum,
        ingress_kind,
    })
}

pub fn apply_trailing_checksums(
    algorithm: Option<&str>,
    trailing_headers: &Option<s3s::TrailingHeaders>,
    checksums: &mut PutObjectChecksums,
) {
    let Some(alg) = algorithm else { return };
    let Some(checksum_str) = trailing_headers.as_ref().and_then(|trailer| {
        let key = match alg {
            ChecksumAlgorithm::CRC32 => rustfs_rio::ChecksumType::CRC32.key(),
            ChecksumAlgorithm::CRC32C => rustfs_rio::ChecksumType::CRC32C.key(),
            ChecksumAlgorithm::SHA1 => rustfs_rio::ChecksumType::SHA1.key(),
            ChecksumAlgorithm::SHA256 => rustfs_rio::ChecksumType::SHA256.key(),
            ChecksumAlgorithm::CRC64NVME => rustfs_rio::ChecksumType::CRC64_NVME.key(),
            _ => return None,
        };
        trailer.read(|headers| {
            headers
                .get(key.unwrap_or_default())
                .and_then(|value| value.to_str().ok().map(|s| s.to_string()))
        })
    }) else {
        return;
    };

    match alg {
        ChecksumAlgorithm::CRC32 => checksums.crc32 = checksum_str,
        ChecksumAlgorithm::CRC32C => checksums.crc32c = checksum_str,
        ChecksumAlgorithm::SHA1 => checksums.sha1 = checksum_str,
        ChecksumAlgorithm::SHA256 => checksums.sha256 = checksum_str,
        ChecksumAlgorithm::CRC64NVME => checksums.crc64nvme = checksum_str,
        _ => (),
    }
}

pub fn resolve_put_body_size(content_length: Option<i64>, headers: &HeaderMap) -> s3s::S3Result<i64> {
    let size = match content_length {
        Some(c) => c,
        None => {
            if let Some(val) = headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                match atoi::atoi::<i64>(val.as_bytes()) {
                    Some(x) => x,
                    None => return Err(s3_error!(UnexpectedContent)),
                }
            } else {
                return Err(s3_error!(UnexpectedContent));
            }
        }
    };

    if size == -1 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(size)
}

pub fn should_use_zero_copy(size: i64, headers: &HeaderMap) -> bool {
    const ZERO_COPY_MIN_SIZE: i64 = 1024 * 1024;

    if size <= ZERO_COPY_MIN_SIZE {
        return false;
    }

    !has_put_encryption_headers(headers) && !put_request_is_compressible(headers)
}

fn has_put_encryption_headers(headers: &HeaderMap) -> bool {
    headers.get(AMZ_SERVER_SIDE_ENCRYPTION).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID).is_some()
}

fn put_request_is_compressible(headers: &HeaderMap) -> bool {
    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct) = content_type.to_str()
    {
        let compressible_types = [
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/javascript",
            "application/json",
            "application/xml",
            "text/xml",
        ];
        return compressible_types.iter().any(|ty| ct.contains(ty));
    }

    false
}

fn should_use_put_reduced_copy_candidate(
    size: i64,
    headers: &HeaderMap,
    encryption_enabled: bool,
    compression_enabled: bool,
) -> bool {
    const ZERO_COPY_MIN_SIZE: i64 = 1024 * 1024;

    if size <= ZERO_COPY_MIN_SIZE {
        return false;
    }

    if !encryption_enabled && has_put_encryption_headers(headers) {
        return false;
    }

    compression_enabled || !put_request_is_compressible(headers)
}

fn map_put_object_ingress_error<B, E>(result: Result<B, E>) -> std::io::Result<B>
where
    E: std::fmt::Display,
{
    result.map_err(|err| std::io::Error::other(err.to_string()))
}

pub fn build_put_object_compat_ingress<S, B, E>(body: S, plan: PutObjectCompatIngressPlan) -> PutObjectCompatIngress<S, B, E>
where
    S: Stream<Item = Result<B, E>>,
    B: Buf,
    E: std::fmt::Display,
{
    match plan.kind {
        PutObjectCompatIngressKind::BufferedStreamCompat => tokio::io::BufReader::with_capacity(
            plan.buffer_size,
            StreamReader::new(body.map(map_put_object_ingress_error::<B, E> as fn(Result<B, E>) -> std::io::Result<B>)),
        ),
    }
}

pub fn build_put_object_compat_reader<S, B, E>(body: S, plan: PutObjectCompatIngressPlan) -> Box<dyn Reader>
where
    S: Stream<Item = Result<B, E>>,
    B: Buf,
    E: std::fmt::Display,
    PutObjectCompatIngress<S, B, E>: Send + Sync + Unpin + 'static,
{
    Box::new(WarpReader::new(build_put_object_compat_ingress(body, plan)))
}

pub fn build_put_object_ingress_source<S, B, E>(body: S, plan: PutObjectBodyPlan) -> PutObjectIngressSource<B, E>
where
    S: Stream<Item = Result<B, E>> + Send + Sync + 'static,
    B: Buf + 'static,
    E: std::fmt::Display + 'static,
    PutObjectCompatIngress<BoxIngressStream<B, E>, B, E>: Send + Sync + Unpin + 'static,
{
    match (plan.kind, plan.ingress.kind) {
        (PutObjectBodyKind::Compressed, PutObjectIngressKind::ReducedCopyCandidate)
        | (PutObjectBodyKind::Plain(PutObjectPlainBodyKind::ReducedCopyCandidate), PutObjectIngressKind::ReducedCopyCandidate) => {
            PutObjectIngressSource::ReducedCopyCandidate(PutObjectReducedCopyIngress::from_stream(body, plan.ingress.compat))
        }
        (PutObjectBodyKind::Compressed, _)
        | (PutObjectBodyKind::Plain(PutObjectPlainBodyKind::LegacyCompat), _)
        | (PutObjectBodyKind::Plain(PutObjectPlainBodyKind::ReducedCopyCandidate), PutObjectIngressKind::LegacyCompat) => {
            PutObjectIngressSource::LegacyCompat(build_put_object_compat_reader(
                box_put_object_ingress_stream(body),
                plan.ingress.compat,
            ))
        }
    }
}

pub fn build_put_object_legacy_hash_stage(
    reader: Box<dyn Reader>,
    hash_values: PutObjectLegacyHashValues,
    plan: PutObjectLegacyHashStagePlan,
    headers: &HeaderMap,
    trailing_headers: Option<s3s::TrailingHeaders>,
) -> std::io::Result<PutObjectHashStage> {
    build_put_object_hash_stage(reader, PutObjectIngressKind::LegacyCompat, hash_values, plan, headers, trailing_headers)
}

pub fn build_put_object_plain_hash_stage<B, E>(
    ingress: PutObjectIngressSource<B, E>,
    hash_values: PutObjectLegacyHashValues,
    plan: PutObjectLegacyHashStagePlan,
    headers: &HeaderMap,
    trailing_headers: Option<s3s::TrailingHeaders>,
) -> std::io::Result<PutObjectHashStage>
where
    B: Buf + Send + Sync + Unpin + 'static,
    E: std::fmt::Display + Send + Sync + 'static,
{
    match ingress {
        PutObjectIngressSource::LegacyCompat(reader) => {
            build_put_object_hash_stage(reader, PutObjectIngressKind::LegacyCompat, hash_values, plan, headers, trailing_headers)
        }
        PutObjectIngressSource::ReducedCopyCandidate(candidate) => build_put_object_hash_stage(
            build_put_object_reduced_copy_reader(candidate),
            PutObjectIngressKind::ReducedCopyCandidate,
            hash_values,
            plan,
            headers,
            trailing_headers,
        ),
    }
}

pub fn plan_put_object_ingress(size: i64, headers: &HeaderMap, buffer_size: usize) -> PutObjectIngressPlan {
    plan_put_object_ingress_with_transforms(size, headers, buffer_size, false, false)
}

pub fn plan_put_object_ingress_with_transforms(
    size: i64,
    headers: &HeaderMap,
    buffer_size: usize,
    encryption_enabled: bool,
    compression_enabled: bool,
) -> PutObjectIngressPlan {
    let enable_zero_copy = should_use_put_reduced_copy_candidate(size, headers, encryption_enabled, compression_enabled);
    PutObjectIngressPlan {
        kind: if enable_zero_copy {
            PutObjectIngressKind::ReducedCopyCandidate
        } else {
            PutObjectIngressKind::LegacyCompat
        },
        compat: PutObjectCompatIngressPlan {
            kind: PutObjectCompatIngressKind::BufferedStreamCompat,
            buffer_size,
        },
        enable_zero_copy,
    }
}

pub fn plan_put_object_body(size: i64, headers: &HeaderMap, key: &str, buffer_size: usize) -> PutObjectBodyPlan {
    plan_put_object_body_with_transforms(size, headers, key, buffer_size, false)
}

pub fn plan_put_object_body_with_transforms(
    size: i64,
    headers: &HeaderMap,
    key: &str,
    buffer_size: usize,
    encryption_enabled: bool,
) -> PutObjectBodyPlan {
    let compression_enabled = size > MIN_COMPRESSIBLE_SIZE as i64 && is_compressible(headers, key);
    let ingress = plan_put_object_ingress_with_transforms(size, headers, buffer_size, encryption_enabled, compression_enabled);
    let kind = if compression_enabled {
        PutObjectBodyKind::Compressed
    } else {
        PutObjectBodyKind::Plain(match ingress.kind {
            PutObjectIngressKind::LegacyCompat => PutObjectPlainBodyKind::LegacyCompat,
            PutObjectIngressKind::ReducedCopyCandidate => PutObjectPlainBodyKind::ReducedCopyCandidate,
        })
    };

    PutObjectBodyPlan { ingress, kind }
}

pub fn resolve_put_effective_copy_mode(applied_compression: bool, applied_encryption: bool) -> rustfs_io_metrics::CopyMode {
    if applied_compression || applied_encryption {
        rustfs_io_metrics::CopyMode::Transformed
    } else {
        rustfs_io_metrics::CopyMode::SingleCopy
    }
}

pub fn resolve_put_transform_metric_kind(applied_compression: bool, applied_encryption: bool) -> Option<&'static str> {
    match (applied_compression, applied_encryption) {
        (true, true) => Some("compression_encryption"),
        (true, false) => Some("compression"),
        (false, true) => Some("encryption"),
        (false, false) => None,
    }
}

pub fn resolve_put_transformed_fallback_reason(
    ingress_kind: PutObjectIngressKind,
    compressed: bool,
    encryption_enabled: bool,
) -> Option<rustfs_io_metrics::FallbackReason> {
    if ingress_kind == PutObjectIngressKind::ReducedCopyCandidate {
        return None;
    }

    match (compressed, encryption_enabled) {
        (true, true) => Some(rustfs_io_metrics::FallbackReason::TransformCompressionEncryptionLegacy),
        (true, false) => Some(rustfs_io_metrics::FallbackReason::TransformCompressionLegacy),
        (false, true) => Some(rustfs_io_metrics::FallbackReason::TransformEncryptionLegacy),
        (false, false) => None,
    }
}

pub fn header_value_is_true(headers: &HeaderMap, key: &str) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

pub fn is_put_object_extract_requested(headers: &HeaderMap) -> bool {
    header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT) || header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT_COMPAT)
}

fn trimmed_header_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
}

fn is_exact_snowball_meta_key(key: &str, exact_keys: &[&str]) -> bool {
    exact_keys.iter().any(|exact_key| key.eq_ignore_ascii_case(exact_key))
}

fn snowball_meta_value_by_suffix(headers: &HeaderMap, suffix_lower: &str, exact_keys: &[&str]) -> Option<String> {
    for (name, value) in headers {
        let key = name.as_str();
        if key.starts_with(AMZ_META_PREFIX_LOWER)
            && key.ends_with(suffix_lower)
            && !is_exact_snowball_meta_key(key, exact_keys)
            && let Ok(parsed) = value.to_str()
        {
            return Some(parsed.trim().to_string());
        }
    }

    None
}

fn snowball_meta_value(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> Option<String> {
    for key in exact_keys {
        if let Some(value) = trimmed_header_value(headers, key) {
            return Some(value);
        }
    }

    snowball_meta_value_by_suffix(headers, suffix_lower, exact_keys)
}

fn snowball_meta_flag(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> bool {
    snowball_meta_value(headers, exact_keys, suffix_lower).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub fn normalize_snowball_prefix(prefix: &str) -> Option<String> {
    let normalized = prefix.trim().trim_matches('/');
    if normalized.is_empty() {
        return None;
    }

    Some(normalized.to_string())
}

pub fn normalize_extract_entry_key(path: &str, prefix: Option<&str>, is_dir: bool) -> String {
    let path = path.trim_matches('/');
    let mut key = match prefix {
        Some(prefix) if !path.is_empty() => format!("{prefix}/{path}"),
        Some(prefix) => prefix.to_string(),
        None => path.to_string(),
    };

    if is_dir && !key.ends_with('/') {
        key.push('/');
    }

    key
}

pub fn resolve_put_object_extract_options(headers: &HeaderMap) -> PutObjectExtractOptions {
    let prefix = snowball_meta_value(headers, SNOWBALL_PREFIX_HEADER_KEYS, SNOWBALL_PREFIX_SUFFIX_LOWER)
        .and_then(|value| normalize_snowball_prefix(&value));
    let ignore_dirs = snowball_meta_flag(headers, SNOWBALL_IGNORE_DIRS_HEADER_KEYS, SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER);
    let ignore_errors = snowball_meta_flag(headers, SNOWBALL_IGNORE_ERRORS_HEADER_KEYS, SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER);

    PutObjectExtractOptions {
        prefix,
        ignore_dirs,
        ignore_errors,
    }
}

pub fn map_extract_archive_error(err: impl std::fmt::Display) -> S3Error {
    s3_error!(InvalidArgument, "Failed to process archive entry: {}", err)
}

pub async fn apply_extract_entry_pax_extensions<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> s3s::S3Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry.pax_extensions().await.map_err(map_extract_archive_error)? else {
        return Ok(());
    };

    for ext in extensions {
        let ext = ext.map_err(map_extract_archive_error)?;
        let key = ext.key().map_err(map_extract_archive_error)?;
        let value = ext.value().map_err(map_extract_archive_error)?;

        if let Some(meta_key) = key.strip_prefix("minio.metadata.") {
            let meta_key = meta_key.strip_prefix("x-amz-meta-").unwrap_or(meta_key);
            if !meta_key.is_empty() {
                metadata.insert(meta_key.to_string(), value.to_string());
            }
            continue;
        }

        if key == "minio.versionId" && !value.is_empty() {
            opts.version_id = Some(value.to_string());
        }
    }

    Ok(())
}

pub fn is_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    input
        .server_side_encryption
        .as_ref()
        .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || input.ssekms_key_id.is_some()
        || headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
}

pub fn is_post_object_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    is_sse_kms_requested(input, headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[test]
    fn should_use_zero_copy_accepts_large_unencrypted_binary_payload() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_small_payloads() {
        assert!(!should_use_zero_copy(512 * 1024, &HeaderMap::new()));
    }

    #[test]
    fn resolve_put_body_size_uses_content_length_when_present() {
        assert_eq!(resolve_put_body_size(Some(123), &HeaderMap::new()).unwrap(), 123);
    }

    #[test]
    fn resolve_put_body_size_uses_decoded_content_length_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_DECODED_CONTENT_LENGTH, "456".parse().unwrap());
        assert_eq!(resolve_put_body_size(None, &headers).unwrap(), 456);
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption", HeaderValue::from_static("AES256"));
        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_compressible_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_boundary_at_1mb() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_small_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024 - 1, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_one_megabyte() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_sse_customer_algorithm() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_kms_key_id() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_compressible_content_types() {
        let mut headers = HeaderMap::new();
        headers.insert(
            rustfs_utils::http::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_allows_large_unencrypted_binary_objects() {
        let mut headers = HeaderMap::new();
        headers.insert(rustfs_utils::http::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn plan_put_object_ingress_preserves_buffer_size_and_fast_path_decision() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));

        let plan = plan_put_object_ingress(2 * 1024 * 1024, &headers, 256 * 1024);

        assert_eq!(plan.kind, PutObjectIngressKind::ReducedCopyCandidate);
        assert_eq!(plan.compat.kind, PutObjectCompatIngressKind::BufferedStreamCompat);
        assert_eq!(plan.compat.buffer_size, 256 * 1024);
        assert!(plan.enable_zero_copy);
    }

    #[test]
    fn plan_put_object_body_disables_compression_for_small_payloads() {
        let plan = plan_put_object_body(1024, &HeaderMap::new(), "small.bin", 64 * 1024);

        assert_eq!(plan.ingress.kind, PutObjectIngressKind::LegacyCompat);
        assert_eq!(plan.ingress.compat.buffer_size, 64 * 1024);
        assert!(!plan.ingress.enable_zero_copy);
        assert_eq!(plan.kind, PutObjectBodyKind::Plain(PutObjectPlainBodyKind::LegacyCompat));
        assert!(!plan.should_compress());
    }

    #[test]
    fn plan_put_object_body_marks_large_plain_payload_as_reduced_copy_candidate() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));

        let plan = plan_put_object_body(2 * 1024 * 1024, &headers, "large.bin", 256 * 1024);

        assert_eq!(plan.kind, PutObjectBodyKind::Plain(PutObjectPlainBodyKind::ReducedCopyCandidate));
        assert_eq!(plan.plain_body_kind(), Some(PutObjectPlainBodyKind::ReducedCopyCandidate));
        assert!(!plan.should_compress());
    }

    #[test]
    fn plan_put_object_body_with_transforms_allows_encrypted_large_binary_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("AES256"));

        let plan = plan_put_object_body_with_transforms(2 * 1024 * 1024, &headers, "large.bin", 256 * 1024, true);

        assert_eq!(plan.kind, PutObjectBodyKind::Plain(PutObjectPlainBodyKind::ReducedCopyCandidate));
        assert_eq!(plan.plain_body_kind(), Some(PutObjectPlainBodyKind::ReducedCopyCandidate));
        assert!(plan.ingress.enable_zero_copy);
    }

    #[test]
    fn build_put_object_ingress_source_preserves_reduced_copy_candidate_for_compressed_body() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"compressed"))]);
        let plan = PutObjectBodyPlan {
            ingress: PutObjectIngressPlan {
                kind: PutObjectIngressKind::ReducedCopyCandidate,
                compat: PutObjectCompatIngressPlan {
                    kind: PutObjectCompatIngressKind::BufferedStreamCompat,
                    buffer_size: 256 * 1024,
                },
                enable_zero_copy: true,
            },
            kind: PutObjectBodyKind::Compressed,
        };

        let source = build_put_object_ingress_source(stream, plan);
        assert!(matches!(source, PutObjectIngressSource::ReducedCopyCandidate(_)));
    }

    #[test]
    fn put_object_body_plan_reports_compressed_kind() {
        let plan = PutObjectBodyPlan {
            ingress: PutObjectIngressPlan {
                kind: PutObjectIngressKind::LegacyCompat,
                compat: PutObjectCompatIngressPlan {
                    kind: PutObjectCompatIngressKind::BufferedStreamCompat,
                    buffer_size: 256 * 1024,
                },
                enable_zero_copy: false,
            },
            kind: PutObjectBodyKind::Compressed,
        };

        assert_eq!(plan.plain_body_kind(), None);
        assert!(plan.should_compress());
    }

    #[test]
    fn resolve_put_effective_copy_mode_marks_transformed_paths() {
        assert_eq!(resolve_put_effective_copy_mode(false, false), rustfs_io_metrics::CopyMode::SingleCopy);
        assert_eq!(resolve_put_effective_copy_mode(true, false), rustfs_io_metrics::CopyMode::Transformed);
        assert_eq!(resolve_put_effective_copy_mode(false, true), rustfs_io_metrics::CopyMode::Transformed);
    }

    #[test]
    fn put_object_transform_stage_tracks_transform_shape() {
        let mut stage = PutObjectTransformStage::default();
        assert_eq!(stage.effective_copy_mode(), rustfs_io_metrics::CopyMode::SingleCopy);
        assert_eq!(stage.metric_kind(), None);

        stage.mark_compression();
        assert!(stage.compression_applied());
        assert_eq!(stage.effective_copy_mode(), rustfs_io_metrics::CopyMode::Transformed);
        assert_eq!(stage.metric_kind(), Some("compression"));

        stage.mark_encryption();
        assert!(stage.encryption_applied());
        assert_eq!(stage.metric_kind(), Some("compression_encryption"));
    }

    #[test]
    fn resolve_put_transform_metric_kind_reports_transform_shape() {
        assert_eq!(resolve_put_transform_metric_kind(false, false), None);
        assert_eq!(resolve_put_transform_metric_kind(true, false), Some("compression"));
        assert_eq!(resolve_put_transform_metric_kind(false, true), Some("encryption"));
        assert_eq!(resolve_put_transform_metric_kind(true, true), Some("compression_encryption"));
    }

    #[test]
    fn resolve_put_transformed_fallback_reason_isolated_from_plain_path() {
        assert_eq!(
            resolve_put_transformed_fallback_reason(PutObjectIngressKind::LegacyCompat, true, false),
            Some(rustfs_io_metrics::FallbackReason::TransformCompressionLegacy)
        );
        assert_eq!(
            resolve_put_transformed_fallback_reason(PutObjectIngressKind::LegacyCompat, false, true),
            Some(rustfs_io_metrics::FallbackReason::TransformEncryptionLegacy)
        );
        assert_eq!(
            resolve_put_transformed_fallback_reason(PutObjectIngressKind::LegacyCompat, true, true),
            Some(rustfs_io_metrics::FallbackReason::TransformCompressionEncryptionLegacy)
        );
        assert_eq!(
            resolve_put_transformed_fallback_reason(PutObjectIngressKind::ReducedCopyCandidate, true, true),
            None
        );
    }

    #[test]
    fn is_put_object_extract_requested_accepts_meta_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));
        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_accepts_compat_header_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT_COMPAT, HeaderValue::from_static(" TRUE "));
        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_rejects_missing_or_false_value() {
        let mut headers = HeaderMap::new();
        assert!(!is_put_object_extract_requested(&headers));
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("false"));
        assert!(!is_put_object_extract_requested(&headers));
    }

    #[test]
    fn normalize_snowball_prefix_trims_slashes_and_whitespace() {
        assert_eq!(normalize_snowball_prefix(" /batch/incoming/ "), Some("batch/incoming".to_string()));
        assert_eq!(normalize_snowball_prefix("///"), None);
    }

    #[test]
    fn normalize_extract_entry_key_applies_prefix_and_directory_suffix() {
        assert_eq!(
            normalize_extract_entry_key("nested/path.txt", Some("imports"), false),
            "imports/nested/path.txt"
        );
        assert_eq!(normalize_extract_entry_key("nested/dir/", Some("imports"), true), "imports/nested/dir/");
        assert_eq!(normalize_extract_entry_key("top-level", None, false), "top-level");
    }

    #[test]
    fn resolve_put_object_extract_options_defaults_when_headers_missing() {
        let headers = HeaderMap::new();
        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(
            options,
            PutObjectExtractOptions {
                prefix: None,
                ignore_dirs: false,
                ignore_errors: false
            }
        );
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_internal_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX_INTERNAL, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL, HeaderValue::from_static("true"));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("internal/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static(" /standard/prefix/ "));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static(" true "));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("standard/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_suffix_compatible_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-prefix"),
            HeaderValue::from_static(" /partner/import "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-dirs"),
            HeaderValue::from_static(" true "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-errors"),
            HeaderValue::from_static("TRUE"),
        );

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("partner/import"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_prefers_exact_headers_over_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-acme-snowball-prefix", HeaderValue::from_static("/fallback/prefix/"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_PREFIX, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("/standard/prefix/"));
        headers.insert(AMZ_MINIO_SNOWBALL_PREFIX, HeaderValue::from_static("/minio/prefix/"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("minio/prefix"));
    }

    #[test]
    fn resolve_put_object_extract_options_exact_flags_override_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-dirs", HeaderValue::from_static("true"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-errors", HeaderValue::from_static("true"));

        let options = resolve_put_object_extract_options(&headers);
        assert!(!options.ignore_dirs);
        assert!(!options.ignore_errors);
    }

    #[tokio::test]
    async fn build_put_object_compat_ingress_reads_buffered_stream() {
        let stream = futures_util::stream::iter([
            Ok::<Bytes, &'static str>(Bytes::from_static(b"abc")),
            Ok::<Bytes, &'static str>(Bytes::from_static(b"def")),
        ]);
        let plan = PutObjectCompatIngressPlan {
            kind: PutObjectCompatIngressKind::BufferedStreamCompat,
            buffer_size: 8,
        };

        let mut reader = build_put_object_compat_ingress(stream, plan);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"abcdef");
    }

    #[tokio::test]
    async fn build_put_object_compat_ingress_maps_stream_errors() {
        let stream = futures_util::stream::iter([Err::<Bytes, _>("boom")]);
        let plan = PutObjectCompatIngressPlan {
            kind: PutObjectCompatIngressKind::BufferedStreamCompat,
            buffer_size: 8,
        };

        let mut reader = build_put_object_compat_ingress(stream, plan);
        let err = reader.read_to_end(&mut Vec::new()).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn build_put_object_compat_reader_wraps_ingress_as_reader() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"reader"))]);
        let plan = PutObjectCompatIngressPlan {
            kind: PutObjectCompatIngressKind::BufferedStreamCompat,
            buffer_size: 16,
        };

        let mut reader = build_put_object_compat_reader(stream, plan);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"reader");
    }

    #[tokio::test]
    async fn build_put_object_ingress_source_keeps_plain_candidate_as_reduced_copy_source() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"candidate"))]);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        let plan = plan_put_object_body(2 * 1024 * 1024, &headers, "large.bin", 16);

        let source = build_put_object_ingress_source(stream, plan);
        let candidate = match source {
            PutObjectIngressSource::ReducedCopyCandidate(candidate) => candidate,
            PutObjectIngressSource::LegacyCompat(_) => panic!("expected reduced-copy candidate"),
        };

        assert_eq!(candidate.compat_plan().kind, PutObjectCompatIngressKind::BufferedStreamCompat);
        assert_eq!(candidate.compat_plan().buffer_size, 16);

        let mut reader = candidate.into_compat_reader();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"candidate");
    }

    #[tokio::test]
    async fn build_put_object_ingress_source_routes_compressed_body_to_legacy_compat_reader() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"compressed"))]);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let plan = plan_put_object_body(2 * 1024 * 1024, &headers, "large.json", 32);

        let source = build_put_object_ingress_source(stream, plan);
        let mut reader = match source {
            PutObjectIngressSource::LegacyCompat(reader) => reader,
            PutObjectIngressSource::ReducedCopyCandidate(_) => panic!("expected legacy compat reader"),
        };

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"compressed");
    }

    #[tokio::test]
    async fn build_put_object_reduced_copy_reader_reads_across_multiple_chunks() {
        let stream = futures_util::stream::iter([
            Ok::<Bytes, &'static str>(Bytes::from_static(b"ab")),
            Ok::<Bytes, &'static str>(Bytes::from_static(b"cd")),
            Ok::<Bytes, &'static str>(Bytes::from_static(b"ef")),
        ]);

        let mut reader = build_put_object_reduced_copy_reader(PutObjectReducedCopyIngress::from_stream(
            stream,
            PutObjectCompatIngressPlan {
                kind: PutObjectCompatIngressKind::BufferedStreamCompat,
                buffer_size: 16,
            },
        ));
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"abcdef");
    }

    #[tokio::test]
    async fn build_put_object_reduced_copy_reader_defers_stream_error_until_next_read() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"ab")), Err::<Bytes, _>("boom")]);

        let mut reader = build_put_object_reduced_copy_reader(PutObjectReducedCopyIngress::from_stream(
            stream,
            PutObjectCompatIngressPlan {
                kind: PutObjectCompatIngressKind::BufferedStreamCompat,
                buffer_size: 16,
            },
        ));

        let mut buf = [0_u8; 8];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..n], b"ab");

        let err = reader.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn build_put_object_reduced_copy_reader_supports_direct_block_reads() {
        let stream = futures_util::stream::iter([
            Ok::<Bytes, &'static str>(Bytes::from_static(b"ab")),
            Ok::<Bytes, &'static str>(Bytes::from_static(b"cd")),
            Ok::<Bytes, &'static str>(Bytes::from_static(b"ef")),
        ]);

        let mut reader = build_put_object_reduced_copy_reader(PutObjectReducedCopyIngress::from_stream(
            stream,
            PutObjectCompatIngressPlan {
                kind: PutObjectCompatIngressKind::BufferedStreamCompat,
                buffer_size: 16,
            },
        ));

        let mut first = [0_u8; 4];
        let mut second = [0_u8; 4];
        assert_eq!(reader.read_block(&mut first).await.unwrap(), 4);
        assert_eq!(&first, b"abcd");
        assert_eq!(reader.read_block(&mut second).await.unwrap(), 2);
        assert_eq!(&second[..2], b"ef");
        assert_eq!(reader.read_block(&mut second).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn build_put_object_plain_hash_stage_reports_reduced_copy_candidate_boundary() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"plain"))]);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        let plan = plan_put_object_body(2 * 1024 * 1024, &headers, "large.bin", 32);

        let stage = build_put_object_plain_hash_stage(
            build_put_object_ingress_source(stream, plan),
            PutObjectLegacyHashValues::default(),
            PutObjectLegacyHashStagePlan {
                size: 5,
                actual_size: 5,
                apply_s3_checksum: false,
                ignore_s3_checksum_value: false,
            },
            &headers,
            None,
        )
        .unwrap();

        assert_eq!(stage.ingress_kind, PutObjectIngressKind::ReducedCopyCandidate);

        let mut reader = stage.reader;
        let mut buf = [0_u8; 8];
        let n = reader.read_block(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"plain");
        assert_eq!(reader.read_block(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn build_put_object_plain_hash_stage_preserves_legacy_compat_boundary() {
        let stream = futures_util::stream::iter([Ok::<Bytes, &'static str>(Bytes::from_static(b"legacy"))]);
        let plan = plan_put_object_body(1024, &HeaderMap::new(), "small.bin", 32);

        let stage = build_put_object_plain_hash_stage(
            build_put_object_ingress_source(stream, plan),
            PutObjectLegacyHashValues::default(),
            PutObjectLegacyHashStagePlan {
                size: 6,
                actual_size: 6,
                apply_s3_checksum: false,
                ignore_s3_checksum_value: false,
            },
            &HeaderMap::new(),
            None,
        )
        .unwrap();

        assert_eq!(stage.ingress_kind, PutObjectIngressKind::LegacyCompat);

        let mut reader = stage.reader;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, b"legacy");
    }

    #[test]
    fn put_object_legacy_hash_values_clear_for_transformed_body_resets_hashes() {
        let mut hash_values = PutObjectLegacyHashValues {
            md5hex: Some("md5".to_string()),
            sha256hex: Some("sha256".to_string()),
        };

        hash_values.clear_for_transformed_body();

        assert_eq!(hash_values, PutObjectLegacyHashValues::default());
    }

    #[test]
    fn build_put_object_legacy_hash_stage_preserves_legacy_compat_boundary() {
        let reader: Box<dyn Reader> = Box::new(WarpReader::new(Cursor::new(Vec::from(&b"abc"[..]))));
        let stage = build_put_object_legacy_hash_stage(
            reader,
            PutObjectLegacyHashValues::default(),
            PutObjectLegacyHashStagePlan {
                size: 3,
                actual_size: 3,
                apply_s3_checksum: false,
                ignore_s3_checksum_value: false,
            },
            &HeaderMap::new(),
            None,
        )
        .unwrap();

        assert_eq!(stage.reader.size(), 3);
        assert_eq!(stage.reader.actual_size(), 3);
        assert!(stage.want_checksum.is_none());
    }
}
