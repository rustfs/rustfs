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

//! Versioned, encrypted diagnostic metadata archives.
//!
//! `POST /rustfs/admin/v4/inspect/archive` accepts an exact bucket/object
//! target and an RSA public key. The response is a `RFSINSP1` framed stream:
//!
//! - fixed header: magic, format version, chunk size, wrapped-key length,
//!   nonce prefix, RSA-OAEP-SHA256-wrapped AES-256 key;
//! - authenticated AES-256-GCM data records containing a POSIX tar stream;
//! - one authenticated final record containing the SHA-256 digest of the
//!   complete plaintext tar stream.
//!
//! A consumer must reject a stream without the final record or whose final
//! digest does not match. Raw `xl.meta`, object contents, drive paths,
//! endpoints, user metadata, and encryption keys are never archive entries.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::storage_api::access::spawn_traced;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use crate::storage::storage_api::DiskError;
use crate::storage::{StorageDiskRpcExt, all_local_disk};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rand::RngExt;
use rsa::pkcs8::DecodePublicKey;
use rsa::traits::PublicKeyParts;
use rsa::{Oaep, RsaPublicKey};
use rustfs_filemeta::{FileMeta, VersionType};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{Body, S3Request, S3Response, S3Result, StdError, s3_error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

pub const INSPECT_ARCHIVE_ROUTE_SUFFIX: &str = "/v4/inspect/archive";
pub const INSPECT_ARCHIVE_CONTENT_TYPE: &str = "application/vnd.rustfs.inspect-archive.v1";
pub const INSPECT_ARCHIVE_VERSION: u16 = 1;
pub const INSPECT_ARCHIVE_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const INSPECT_ARCHIVE_MAX_DURATION: Duration = Duration::from_secs(30);

const FORMAT_MAGIC: &[u8; 8] = b"RFSINSP1";
const ARCHIVE_CHUNK_SIZE: usize = 64 * 1024;
const MAX_PUBLIC_KEY_PEM_BYTES: usize = 16 * 1024;
const MIN_RSA_BITS: usize = 2048;
const MAX_RSA_BITS: usize = 8192;
const MAX_METADATA_BYTES_PER_DRIVE: usize = 4 * 1024 * 1024;
const STREAM_CHANNEL_CAPACITY: usize = 4;
const DRIVE_READ_CONCURRENCY: usize = 8;
const RECORD_DATA: u8 = 1;
const RECORD_FINAL: u8 = 2;
const COMPLETION_HEADER: &str = "x-rustfs-inspect-completion";
const COMPLETION_CONTRACT: &str = "authenticated-final-record-required";

pub fn register_inspect_archive_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}{INSPECT_ARCHIVE_ROUTE_SUFFIX}").as_str(),
        AdminOperation(&InspectArchiveHandler {}),
    )?;
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectArchiveRequest {
    bucket: String,
    object: String,
    public_key_pem: String,
}

impl InspectArchiveRequest {
    fn validate(&self) -> S3Result<()> {
        crate::storage::ecstore_bucket::utils::check_valid_bucket_name_strict(&self.bucket)
            .map_err(|_| s3_error!(InvalidArgument, "invalid inspect archive bucket"))?;
        crate::storage::ecstore_bucket::utils::check_bucket_and_object_names(&self.bucket, &self.object)
            .map_err(|_| s3_error!(InvalidArgument, "invalid inspect archive object"))?;
        if crate::storage::ecstore_bucket::utils::has_bad_path_component(&self.object) {
            return Err(s3_error!(InvalidArgument, "inspect archive object contains an unsafe path component"));
        }
        if self.public_key_pem.len() > MAX_PUBLIC_KEY_PEM_BYTES {
            return Err(s3_error!(InvalidArgument, "inspect archive public key is too large"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InspectArchiveCapability {
    pub state: crate::admin::storage_api::cluster::CapabilityStatus,
    pub route: &'static str,
    pub archive_version: u16,
    pub content_type: &'static str,
    pub encryption: &'static str,
    pub completion_contract: &'static str,
    pub max_bytes: usize,
    pub max_duration_secs: u64,
    pub max_metadata_bytes_per_drive: usize,
}

impl InspectArchiveCapability {
    pub(crate) fn current(local_drive_count: usize) -> Self {
        Self::from_runtime(local_drive_count, true)
    }

    fn from_runtime(local_drive_count: usize, crypto_available: bool) -> Self {
        use crate::admin::storage_api::cluster::CapabilityStatus;

        let state = if !crypto_available {
            CapabilityStatus::unsupported().with_reason("RSA-OAEP and AES-256-GCM support is unavailable")
        } else if local_drive_count == 0 {
            CapabilityStatus::disabled().with_reason("no local storage drives are initialized")
        } else {
            CapabilityStatus::supported().with_reason("encrypted drive-metadata archive is available")
        };
        Self {
            state,
            route: "/rustfs/admin/v4/inspect/archive",
            archive_version: INSPECT_ARCHIVE_VERSION,
            content_type: INSPECT_ARCHIVE_CONTENT_TYPE,
            encryption: "RSA-OAEP-SHA256+AES-256-GCM-CHUNKED",
            completion_contract: COMPLETION_CONTRACT,
            max_bytes: INSPECT_ARCHIVE_MAX_BYTES,
            max_duration_secs: INSPECT_ARCHIVE_MAX_DURATION.as_secs(),
            max_metadata_bytes_per_drive: MAX_METADATA_BYTES_PER_DRIVE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct InspectArchiveManifest {
    archive_version: u16,
    drive_count: usize,
    artifact_paths: Vec<String>,
    target_identifiers_included: bool,
    raw_object_data_included: bool,
    raw_metadata_included: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DriveMetadataArtifact {
    drive_index: usize,
    status: &'static str,
    metadata_bytes: Option<usize>,
    metadata_sha256: Option<String>,
    xl_format: Option<XlFormatSummary>,
    version_count: Option<usize>,
    object_versions: Option<usize>,
    delete_markers: Option<usize>,
    legacy_versions: Option<usize>,
    invalid_versions: Option<usize>,
    ec_layouts: Vec<EcLayoutSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct EcLayoutSummary {
    data_blocks: usize,
    parity_blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct XlFormatSummary {
    major: u16,
    minor: u16,
    header_version: u8,
    metadata_version: u8,
}

fn sha256_hex(value: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(value), hex_simd::AsciiCase::Lower)
}

fn unavailable_artifact(drive_index: usize, status: &'static str) -> DriveMetadataArtifact {
    DriveMetadataArtifact {
        drive_index,
        status,
        metadata_bytes: None,
        metadata_sha256: None,
        xl_format: None,
        version_count: None,
        object_versions: None,
        delete_markers: None,
        legacy_versions: None,
        invalid_versions: None,
        ec_layouts: Vec::new(),
    }
}

fn summarize_drive_metadata(drive_index: usize, metadata: &[u8]) -> DriveMetadataArtifact {
    if metadata.len() > MAX_METADATA_BYTES_PER_DRIVE {
        return unavailable_artifact(drive_index, "metadata_too_large");
    }

    let metadata_sha256 = sha256_hex(metadata);
    let metadata_bytes = metadata.len();
    let format = FileMeta::read_format_versions(metadata)
        .ok()
        .map(|(major, minor, header_version, metadata_version)| XlFormatSummary {
            major,
            minor,
            header_version,
            metadata_version,
        });
    let Ok(parsed) = FileMeta::load(metadata) else {
        return DriveMetadataArtifact {
            drive_index,
            status: "metadata_corrupt",
            metadata_bytes: Some(metadata_bytes),
            metadata_sha256: Some(metadata_sha256),
            xl_format: format,
            version_count: None,
            object_versions: None,
            delete_markers: None,
            legacy_versions: None,
            invalid_versions: None,
            ec_layouts: Vec::new(),
        };
    };

    let mut object_versions = 0;
    let mut delete_markers = 0;
    let mut legacy_versions = 0;
    let mut invalid_versions = 0;
    let mut ec_layouts = BTreeSet::new();
    for version in &parsed.versions {
        match version.header.version_type {
            VersionType::Object => object_versions += 1,
            VersionType::Delete => delete_markers += 1,
            VersionType::Legacy => legacy_versions += 1,
            VersionType::Invalid => invalid_versions += 1,
        }
        if version.header.has_ec() {
            ec_layouts.insert(EcLayoutSummary {
                data_blocks: usize::from(version.header.ec_m),
                parity_blocks: usize::from(version.header.ec_n),
            });
        } else if let Ok(full_version) = version.parse_version_meta() {
            if let Some(object) = full_version.object {
                ec_layouts.insert(EcLayoutSummary {
                    data_blocks: object.erasure_m,
                    parity_blocks: object.erasure_n,
                });
            } else if let Some(object) = full_version.legacy_object {
                ec_layouts.insert(EcLayoutSummary {
                    data_blocks: object.erasure.data_blocks,
                    parity_blocks: object.erasure.parity_blocks,
                });
            }
        }
    }

    DriveMetadataArtifact {
        drive_index,
        status: "ok",
        metadata_bytes: Some(metadata_bytes),
        metadata_sha256: Some(metadata_sha256),
        xl_format: format,
        version_count: Some(parsed.versions.len()),
        object_versions: Some(object_versions),
        delete_markers: Some(delete_markers),
        legacy_versions: Some(legacy_versions),
        invalid_versions: Some(invalid_versions),
        ec_layouts: ec_layouts.into_iter().collect(),
    }
}

async fn collect_drive_artifacts(
    bucket: &str,
    object: &str,
    deadline: tokio::time::Instant,
) -> S3Result<Vec<DriveMetadataArtifact>> {
    let disks = all_local_disk().await;
    if disks.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "inspect archive is disabled because no local drives are initialized"
        ));
    }

    let reads = futures::stream::iter(disks.into_iter().enumerate())
        .map(|(drive_index, disk)| async move {
            match tokio::time::timeout_at(deadline, disk.read_xl(bucket, object, false)).await {
                Ok(Ok(metadata)) => {
                    let summarize = tokio::task::spawn_blocking(move || summarize_drive_metadata(drive_index, &metadata.buf));
                    let artifact = tokio::time::timeout_at(deadline, summarize)
                        .await
                        .map_err(|_| s3_error!(SlowDown, "inspect archive metadata summarization timed out"))?
                        .map_err(|_| s3_error!(InternalError, "inspect archive metadata summarization failed"))?;
                    Ok((artifact, true, false))
                }
                Ok(Err(error)) => Ok((
                    unavailable_artifact(drive_index, "metadata_unavailable"),
                    false,
                    !DiskError::is_err_object_not_found(&error),
                )),
                Err(_) => Err(s3_error!(SlowDown, "inspect archive metadata collection timed out")),
            }
        })
        .buffer_unordered(DRIVE_READ_CONCURRENCY);
    let collected: Vec<(DriveMetadataArtifact, bool, bool)> = reads.try_collect().await?;
    let observed_target = collected.iter().any(|(_, observed, _)| *observed);
    let unavailable_drive = collected.iter().any(|(_, _, unavailable)| *unavailable);
    let mut artifacts: Vec<_> = collected.into_iter().map(|(artifact, _, _)| artifact).collect();
    artifacts.sort_by_key(|artifact| artifact.drive_index);

    if !observed_target {
        if unavailable_drive {
            return Err(s3_error!(
                SlowDown,
                "inspect archive target could not be verified because local drive metadata is unavailable"
            ));
        }
        return Err(s3_error!(NoSuchKey, "inspect archive target was not found on any local drive"));
    }
    Ok(artifacts)
}

fn parse_public_key(public_key_pem: &str) -> S3Result<RsaPublicKey> {
    let key = RsaPublicKey::from_public_key_pem(public_key_pem)
        .map_err(|_| s3_error!(InvalidArgument, "inspect archive requires a valid PKCS#8 RSA public key"))?;
    let bits = key.size().saturating_mul(8);
    if !(MIN_RSA_BITS..=MAX_RSA_BITS).contains(&bits) {
        return Err(s3_error!(
            InvalidArgument,
            "inspect archive RSA public key must be between {MIN_RSA_BITS} and {MAX_RSA_BITS} bits"
        ));
    }
    Ok(key)
}

fn archive_manifest(artifacts: &[DriveMetadataArtifact]) -> InspectArchiveManifest {
    InspectArchiveManifest {
        archive_version: INSPECT_ARCHIVE_VERSION,
        drive_count: artifacts.len(),
        artifact_paths: (0..artifacts.len()).map(|index| format!("drives/{index:04}.json")).collect(),
        target_identifiers_included: false,
        raw_object_data_included: false,
        raw_metadata_included: false,
    }
}

async fn append_json_entry<W, T>(archive: &mut tokio_tar::Builder<W>, path: &str, value: &T) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: Serialize,
{
    let data = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    let mut header = tokio_tar::Header::new_gnu();
    header.set_mode(0o600);
    header.set_mtime(0);
    header.set_size(u64::try_from(data.len()).map_err(io::Error::other)?);
    header.set_cksum();
    archive.append_data(&mut header, path, data.as_slice()).await
}

async fn write_tar_archive<W>(writer: W, artifacts: Vec<DriveMetadataArtifact>) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let manifest = archive_manifest(&artifacts);
    let mut archive = tokio_tar::Builder::new(writer);
    append_json_entry(&mut archive, "manifest.json", &manifest).await?;
    for artifact in artifacts {
        let path = format!("drives/{:04}.json", artifact.drive_index);
        append_json_entry(&mut archive, &path, &artifact).await?;
    }
    archive.finish().await
}

struct ArchiveByteStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for ArchiveByteStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::into_inner(self).inner.poll_next_unpin(cx)
    }
}

impl ByteStream for ArchiveByteStream {}

fn encryption_aad(record_type: u8, counter: u32) -> [u8; 13] {
    let mut aad = [0_u8; 13];
    aad[..8].copy_from_slice(FORMAT_MAGIC);
    aad[8] = record_type;
    aad[9..].copy_from_slice(&counter.to_be_bytes());
    aad
}

fn encrypt_record(
    cipher: &Aes256Gcm,
    nonce_prefix: &[u8; 8],
    record_type: u8,
    counter: u32,
    plaintext: &[u8],
) -> io::Result<Bytes> {
    let mut nonce = [0_u8; 12];
    nonce[..8].copy_from_slice(nonce_prefix);
    nonce[8..].copy_from_slice(&counter.to_be_bytes());
    let ciphertext = cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: plaintext,
                aad: &encryption_aad(record_type, counter),
            },
        )
        .map_err(|_| io::Error::other("failed to encrypt inspect archive record"))?;
    let ciphertext_len = u32::try_from(ciphertext.len()).map_err(io::Error::other)?;
    let mut framed = Vec::with_capacity(9 + ciphertext.len());
    framed.push(record_type);
    framed.extend_from_slice(&counter.to_be_bytes());
    framed.extend_from_slice(&ciphertext_len.to_be_bytes());
    framed.extend_from_slice(&ciphertext);
    Ok(Bytes::from(framed))
}

async fn send_bounded(
    tx: &mpsc::Sender<Result<Bytes, StdError>>,
    sent: &mut usize,
    bytes: Bytes,
    deadline: tokio::time::Instant,
) -> io::Result<()> {
    let next = sent
        .checked_add(bytes.len())
        .ok_or_else(|| io::Error::other("inspect archive byte accounting overflow"))?;
    if next > INSPECT_ARCHIVE_MAX_BYTES {
        return Err(io::Error::other("inspect archive exceeded the server byte limit"));
    }
    tokio::select! {
        biased;
        _ = tx.closed() => {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "inspect archive client disconnected"));
        }
        result = tokio::time::timeout_at(deadline, tx.send(Ok(bytes))) => {
            result
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "inspect archive streaming timed out"))?
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "inspect archive client disconnected"))?;
        }
    }
    *sent = next;
    Ok(())
}

async fn encrypt_archive_stream<R>(
    mut reader: R,
    completion: oneshot::Receiver<io::Result<()>>,
    public_key: RsaPublicKey,
    deadline: tokio::time::Instant,
    tx: mpsc::Sender<Result<Bytes, StdError>>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut key_bytes = [0_u8; 32];
    rand::rng().fill(&mut key_bytes);
    let key = Key::<Aes256Gcm>::from(key_bytes);
    let cipher = Aes256Gcm::new(&key);
    let (wrapped_key, nonce_prefix) = {
        let mut rng = rand::rng();
        let wrapped_key = public_key
            .encrypt(&mut rng, Oaep::<Sha256>::new(), key.as_slice())
            .map_err(|_| io::Error::other("failed to wrap inspect archive data key"))?;
        let mut nonce_prefix = [0_u8; 8];
        rng.fill(&mut nonce_prefix);
        (wrapped_key, nonce_prefix)
    };
    let wrapped_len = u16::try_from(wrapped_key.len()).map_err(io::Error::other)?;

    let mut header = Vec::with_capacity(25 + wrapped_key.len());
    header.extend_from_slice(FORMAT_MAGIC);
    header.extend_from_slice(&INSPECT_ARCHIVE_VERSION.to_be_bytes());
    header.extend_from_slice(&u32::try_from(ARCHIVE_CHUNK_SIZE).map_err(io::Error::other)?.to_be_bytes());
    header.extend_from_slice(&wrapped_len.to_be_bytes());
    header.extend_from_slice(&nonce_prefix);
    header.extend_from_slice(&wrapped_key);

    let mut sent = 0;
    send_bounded(&tx, &mut sent, Bytes::from(header), deadline).await?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; ARCHIVE_CHUNK_SIZE];
    let mut counter = 0_u32;

    loop {
        let read = tokio::select! {
            biased;
            _ = tx.closed() => return Err(io::Error::new(io::ErrorKind::BrokenPipe, "inspect archive client disconnected")),
            result = tokio::time::timeout_at(deadline, reader.read(&mut buffer)) => {
                result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "inspect archive generation timed out"))??
            }
        };
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        let framed = encrypt_record(&cipher, &nonce_prefix, RECORD_DATA, counter, &buffer[..read])?;
        send_bounded(&tx, &mut sent, framed, deadline).await?;
        counter = counter
            .checked_add(1)
            .ok_or_else(|| io::Error::other("inspect archive record counter exhausted"))?;
    }

    match tokio::time::timeout_at(deadline, completion).await {
        Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "inspect archive generation timed out")),
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => return Err(error),
        Ok(Err(error)) => return Err(io::Error::other(format!("inspect archive producer ended unexpectedly: {error}"))),
    }

    let final_digest = digest.finalize();
    let final_record = encrypt_record(&cipher, &nonce_prefix, RECORD_FINAL, counter, &final_digest)?;
    send_bounded(&tx, &mut sent, final_record, deadline).await
}

fn encrypted_archive_body(
    artifacts: Vec<DriveMetadataArtifact>,
    public_key: RsaPublicKey,
    deadline: tokio::time::Instant,
) -> Body {
    let (reader, writer) = tokio::io::duplex(ARCHIVE_CHUNK_SIZE);
    let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
    let (completion_tx, completion_rx) = oneshot::channel();

    spawn_traced(async move {
        spawn_traced(async move {
            let _ = completion_tx.send(write_tar_archive(writer, artifacts).await);
        });
        let result = encrypt_archive_stream(reader, completion_rx, public_key, deadline, tx.clone()).await;
        if let Err(err) = result {
            let _ = tx.try_send(Err(Box::new(err)));
        }
    });

    let stream: DynByteStream = Box::pin(ArchiveByteStream {
        inner: ReceiverStream::new(rx),
    });
    Body::from(stream)
}

pub struct InspectArchiveHandler {}

fn inspect_archive_gate_actions() -> Vec<Action> {
    vec![Action::AdminAction(AdminAction::InspectDataAction)]
}

#[async_trait::async_trait]
impl Operation for InspectArchiveHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials.as_ref() else {
            return Err(s3_error!(AccessDenied, "Signature is required"));
        };
        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
        let remote_addr = req
            .extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|opt| opt.map(|addr| addr.0));
        validate_admin_request(&req.headers, &cred, owner, false, inspect_archive_gate_actions(), remote_addr).await?;

        let body = req
            .input
            .store_all_limited(rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|_| s3_error!(InvalidRequest, "failed to read inspect archive request"))?;
        let request: InspectArchiveRequest =
            serde_json::from_slice(&body).map_err(|_| s3_error!(InvalidRequest, "inspect archive request must be valid JSON"))?;
        request.validate()?;
        let public_key = parse_public_key(&request.public_key_pem)?;
        let deadline = tokio::time::Instant::now() + INSPECT_ARCHIVE_MAX_DURATION;
        let artifacts = collect_drive_artifacts(&request.bucket, &request.object, deadline).await?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(INSPECT_ARCHIVE_CONTENT_TYPE));
        headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(COMPLETION_HEADER, HeaderValue::from_static(COMPLETION_CONTRACT));
        headers.insert(
            http::header::CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"rustfs-inspect-v1.rfsa\""),
        );
        Ok(S3Response::with_headers(
            (StatusCode::OK, encrypted_archive_body(artifacts, public_key, deadline)),
            headers,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::{EncodePublicKey, LineEnding};
    use s3s::S3ErrorCode;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncWriteExt;

    fn test_keypair() -> (RsaPrivateKey, RsaPublicKey, String) {
        let private = RsaPrivateKey::new(&mut rand::rng(), MIN_RSA_BITS).expect("generate test RSA key");
        let public = RsaPublicKey::from(&private);
        let pem = public.to_public_key_pem(LineEnding::LF).expect("encode test public key");
        (private, public, pem)
    }

    fn valid_request(public_key_pem: String) -> InspectArchiveRequest {
        InspectArchiveRequest {
            bucket: "diagnostics".to_string(),
            object: "objects/report.bin".to_string(),
            public_key_pem,
        }
    }

    fn decode_hex_fixture(value: &str) -> Vec<u8> {
        value
            .split_ascii_whitespace()
            .flat_map(|line| line.as_bytes().chunks_exact(2))
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("fixture contains ASCII hex");
                u8::from_str_radix(pair, 16).expect("fixture contains valid hex")
            })
            .collect()
    }

    fn minio_ec_fixture() -> Vec<u8> {
        decode_hex_fixture(include_str!(
            "../../../../crates/filemeta/tests/fixtures/minio/object_large_bin.xlmeta.hex"
        ))
    }

    fn fixture_artifacts(count: usize) -> Vec<DriveMetadataArtifact> {
        let metadata = minio_ec_fixture();
        (0..count).map(|index| summarize_drive_metadata(index, &metadata)).collect()
    }

    async fn collect_encrypted_stream(
        artifacts: Vec<DriveMetadataArtifact>,
        public_key: RsaPublicKey,
        deadline: tokio::time::Instant,
    ) -> io::Result<Vec<u8>> {
        let (reader, writer) = tokio::io::duplex(ARCHIVE_CHUNK_SIZE);
        let (completion_tx, completion_rx) = oneshot::channel();
        let producer = tokio::spawn(async move {
            let result = write_tar_archive(writer, artifacts).await;
            let rendered = result.as_ref().err().map(ToString::to_string);
            let _ = completion_tx.send(result);
            rendered
        });
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let encryptor = tokio::spawn(encrypt_archive_stream(reader, completion_rx, public_key, deadline, tx));
        let chunks = ReceiverStream::new(rx)
            .map_err(|error| io::Error::other(error.to_string()))
            .try_collect::<Vec<_>>()
            .await?;
        encryptor.await.expect("encryptor task should join")?;
        assert_eq!(producer.await.expect("producer task should join"), None);
        Ok(chunks.into_iter().flatten().collect())
    }

    fn decrypt_stream(private: &RsaPrivateKey, encrypted: &[u8]) -> io::Result<Vec<u8>> {
        if encrypted.len() < 24 || &encrypted[..8] != FORMAT_MAGIC {
            return Err(io::Error::other("invalid inspect archive header"));
        }
        let version = u16::from_be_bytes([encrypted[8], encrypted[9]]);
        if version != INSPECT_ARCHIVE_VERSION {
            return Err(io::Error::other("unsupported inspect archive version"));
        }
        let wrapped_len = usize::from(u16::from_be_bytes([encrypted[14], encrypted[15]]));
        let nonce_prefix: [u8; 8] = encrypted[16..24]
            .try_into()
            .map_err(|_| io::Error::other("invalid nonce prefix"))?;
        let wrapped_end = 24_usize
            .checked_add(wrapped_len)
            .ok_or_else(|| io::Error::other("wrapped key length overflow"))?;
        if wrapped_end > encrypted.len() {
            return Err(io::Error::other("truncated wrapped key"));
        }
        let key = private
            .decrypt(Oaep::<Sha256>::new(), &encrypted[24..wrapped_end])
            .map_err(io::Error::other)?;
        let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| io::Error::other("invalid inspect archive data key length"))?;
        let mut cursor = wrapped_end;
        let mut plaintext = Vec::new();
        let mut digest = Sha256::new();

        while cursor < encrypted.len() {
            if encrypted.len() - cursor < 9 {
                return Err(io::Error::other("truncated inspect archive record header"));
            }
            let record_type = encrypted[cursor];
            let counter = u32::from_be_bytes(
                encrypted[cursor + 1..cursor + 5]
                    .try_into()
                    .map_err(|_| io::Error::other("invalid counter"))?,
            );
            let length = usize::try_from(u32::from_be_bytes(
                encrypted[cursor + 5..cursor + 9]
                    .try_into()
                    .map_err(|_| io::Error::other("invalid ciphertext length"))?,
            ))
            .map_err(io::Error::other)?;
            cursor += 9;
            let end = cursor
                .checked_add(length)
                .ok_or_else(|| io::Error::other("ciphertext length overflow"))?;
            if end > encrypted.len() {
                return Err(io::Error::other("truncated inspect archive record"));
            }
            let mut nonce = [0_u8; 12];
            nonce[..8].copy_from_slice(&nonce_prefix);
            nonce[8..].copy_from_slice(&counter.to_be_bytes());
            let decoded = cipher
                .decrypt(
                    (&nonce).into(),
                    Payload {
                        msg: &encrypted[cursor..end],
                        aad: &encryption_aad(record_type, counter),
                    },
                )
                .map_err(|_| io::Error::other("inspect archive authentication failed"))?;
            cursor = end;
            match record_type {
                RECORD_DATA => {
                    digest.update(&decoded);
                    plaintext.extend_from_slice(&decoded);
                }
                RECORD_FINAL => {
                    if decoded.as_slice() != digest.finalize().as_slice() {
                        return Err(io::Error::other("inspect archive final digest mismatch"));
                    }
                    if cursor != encrypted.len() {
                        return Err(io::Error::other("inspect archive contains records after completion"));
                    }
                    return Ok(plaintext);
                }
                _ => return Err(io::Error::other("invalid inspect archive record order")),
            }
        }

        Err(io::Error::other("inspect archive final record is missing"))
    }

    #[test]
    fn capability_distinguishes_supported_disabled_and_unsupported() {
        assert_eq!(
            InspectArchiveCapability::from_runtime(4, true).state.state,
            crate::admin::storage_api::cluster::CapabilityState::Supported
        );
        assert_eq!(
            InspectArchiveCapability::from_runtime(0, true).state.state,
            crate::admin::storage_api::cluster::CapabilityState::Disabled
        );
        assert_eq!(
            InspectArchiveCapability::from_runtime(4, false).state.state,
            crate::admin::storage_api::cluster::CapabilityState::Unsupported
        );
    }

    #[test]
    fn authorization_gate_requires_only_the_dedicated_inspect_action() {
        assert_eq!(inspect_archive_gate_actions(), vec![Action::AdminAction(AdminAction::InspectDataAction)]);
    }

    #[test]
    fn request_validation_rejects_invalid_targets_and_weak_keys() {
        let (_, _, pem) = test_keypair();
        let mut request = valid_request(pem);
        request.bucket = "../secret".to_string();
        let error = request.validate().expect_err("invalid bucket should fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);

        let weak = RsaPrivateKey::new(&mut rand::rng(), 1024).expect("generate weak RSA key");
        let weak_pem = RsaPublicKey::from(&weak)
            .to_public_key_pem(LineEnding::LF)
            .expect("encode weak public key");
        let error = parse_public_key(&weak_pem).expect_err("weak RSA key should fail");
        assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn multi_drive_ec_fixture_produces_sanitized_artifacts() {
        let raw = minio_ec_fixture();
        let artifacts = fixture_artifacts(4);
        assert_eq!(artifacts.len(), 4);
        assert!(artifacts.iter().all(|artifact| artifact.status == "ok"));
        assert!(artifacts.iter().all(|artifact| !artifact.ec_layouts.is_empty()));

        let encoded = serde_json::to_vec(&artifacts).expect("serialize artifacts");
        assert!(!encoded.windows(raw.len()).any(|window| window == raw));
        assert!(!String::from_utf8_lossy(&encoded).contains("version_id"));
        assert!(!String::from_utf8_lossy(&encoded).contains("meta_sys"));
    }

    #[test]
    fn corrupt_and_oversized_metadata_are_bounded_sanitized_artifacts() {
        let corrupt = summarize_drive_metadata(0, b"not-xl-meta");
        assert_eq!(corrupt.status, "metadata_corrupt");
        assert_eq!(corrupt.metadata_bytes, Some(11));
        assert!(corrupt.metadata_sha256.is_some());
        assert!(corrupt.ec_layouts.is_empty());

        let oversized = summarize_drive_metadata(1, &vec![0_u8; MAX_METADATA_BYTES_PER_DRIVE + 1]);
        assert_eq!(oversized, unavailable_artifact(1, "metadata_too_large"));
    }

    #[tokio::test]
    async fn archive_is_streamed_encrypted_and_requires_authenticated_completion() {
        let (private, public, pem) = test_keypair();
        let request = valid_request(pem);
        let bucket = request.bucket.clone();
        let object = request.object.clone();
        let encrypted =
            collect_encrypted_stream(fixture_artifacts(4), public, tokio::time::Instant::now() + Duration::from_secs(10))
                .await
                .expect("archive should stream");

        assert!(!encrypted.windows(bucket.len()).any(|window| window == bucket.as_bytes()));
        assert!(!encrypted.windows(object.len()).any(|window| window == object.as_bytes()));
        let plaintext = decrypt_stream(&private, &encrypted).expect("complete stream should decrypt");
        assert!(
            plaintext
                .windows(b"manifest.json".len())
                .any(|window| window == b"manifest.json")
        );
        assert!(
            plaintext
                .windows(b"drives/0003.json".len())
                .any(|window| window == b"drives/0003.json")
        );

        let truncated = &encrypted[..encrypted.len() - 1];
        let error = decrypt_stream(&private, truncated).expect_err("truncated stream must not be accepted");
        assert!(error.to_string().contains("truncated") || error.to_string().contains("missing"));

        let mut tampered = encrypted;
        let last = tampered
            .last_mut()
            .expect("encrypted stream contains final authentication tag");
        *last ^= 1;
        let error = decrypt_stream(&private, &tampered).expect_err("tampered stream must not be accepted");
        assert!(error.to_string().contains("authentication failed"));
    }

    #[tokio::test]
    async fn expired_deadline_surfaces_stream_error_without_final_record() {
        let (_, public, _) = test_keypair();
        let (mut writer, reader) = tokio::io::duplex(1);
        let writer_task = tokio::spawn(async move {
            writer.write_all(b"blocked").await.expect("first byte can be written");
            futures::future::pending::<()>().await;
        });
        let (_completion_tx, completion_rx) = oneshot::channel();
        let (tx, mut rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let result = encrypt_archive_stream(reader, completion_rx, public, tokio::time::Instant::now(), tx).await;
        assert_eq!(result.expect_err("expired deadline should fail").kind(), io::ErrorKind::TimedOut);
        assert!(rx.recv().await.expect("header is emitted before timeout").is_ok());
        while let Some(chunk) = rx.recv().await {
            let chunk = chunk.expect("pre-timeout chunks remain authenticated data records");
            assert_ne!(chunk.first().copied(), Some(RECORD_FINAL));
        }
        writer_task.abort();
    }

    #[tokio::test]
    async fn producer_failure_never_emits_a_final_record() {
        let (_, public, _) = test_keypair();
        let (mut writer, reader) = tokio::io::duplex(ARCHIVE_CHUNK_SIZE);
        let (completion_tx, completion_rx) = oneshot::channel();
        let producer = tokio::spawn(async move {
            writer.write_all(b"partial-tar").await.expect("write partial archive");
            drop(writer);
            let _ = completion_tx.send(Err(io::Error::other("fixture producer failed")));
        });
        let (tx, mut rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let result =
            encrypt_archive_stream(reader, completion_rx, public, tokio::time::Instant::now() + Duration::from_secs(10), tx)
                .await;
        assert!(
            result
                .expect_err("producer failure must propagate")
                .to_string()
                .contains("fixture producer failed")
        );
        assert!(rx.recv().await.expect("stream header").is_ok());
        while let Some(chunk) = rx.recv().await {
            let chunk = chunk.expect("partial data records remain authenticated");
            assert_ne!(chunk.first().copied(), Some(RECORD_FINAL));
        }
        producer.await.expect("producer task should join");
    }

    #[tokio::test]
    async fn byte_limit_rejects_record_before_emitting_it() {
        let (tx, mut rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let mut sent = INSPECT_ARCHIVE_MAX_BYTES - 1;
        let error = send_bounded(
            &tx,
            &mut sent,
            Bytes::from_static(b"xx"),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err("record crossing the byte limit must fail");
        assert!(error.to_string().contains("byte limit"));
        assert_eq!(sent, INSPECT_ARCHIVE_MAX_BYTES - 1);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn slow_client_cannot_extend_stream_past_deadline() {
        let (tx, _rx) = mpsc::channel(1);
        tx.send(Ok(Bytes::from_static(b"occupied")))
            .await
            .expect("fill channel before testing deadline");
        let mut sent = 0;
        let error = send_bounded(&tx, &mut sent, Bytes::from_static(b"blocked"), tokio::time::Instant::now())
            .await
            .expect_err("full response queue must honor the deadline");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(sent, 0);
    }

    struct DropObservedReader {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropObservedReader {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    impl AsyncRead for DropObservedReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut tokio::io::ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn dropping_client_cancels_archive_reader() {
        let (_, public, _) = test_keypair();
        let dropped = Arc::new(AtomicBool::new(false));
        let reader = DropObservedReader {
            dropped: Arc::clone(&dropped),
        };
        let (_completion_tx, completion_rx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        drop(rx);
        let error =
            encrypt_archive_stream(reader, completion_rx, public, tokio::time::Instant::now() + Duration::from_secs(10), tx)
                .await
                .expect_err("closed client channel should cancel");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn missing_credentials_are_denied_before_request_processing() {
        let request = S3Request {
            method: Method::POST,
            uri: format!("{ADMIN_PREFIX}{INSPECT_ARCHIVE_ROUTE_SUFFIX}")
                .parse()
                .expect("valid route"),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
            input: Body::from(Bytes::from_static(b"not-json")),
        };
        let error = InspectArchiveHandler {}
            .call(request, Params::new())
            .await
            .expect_err("unsigned request should fail");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    }
}
