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

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZlibDecoder, ZstdDecoder};
use async_compression::tokio::write::{BzEncoder, GzipEncoder, XzEncoder, ZlibEncoder, ZstdEncoder};
use std::collections::HashSet;
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::task::spawn_blocking;
use tokio_stream::StreamExt;
use tokio_tar::Archive;
use zip::{CompressionMethod, ZipArchive, ZipWriter, write::SimpleFileOptions};

pub type Result<T> = std::result::Result<T, ZipError>;

#[derive(Debug, Error)]
pub enum ZipError {
    #[error("unsupported {operation} for format {format:?}")]
    UnsupportedFormat {
        format: CompressionFormat,
        operation: &'static str,
    },
    #[error("invalid compression level {0}: value exceeds i32::MAX")]
    InvalidCompressionLevel(u32),
    #[error("unsafe archive entry path: {0}")]
    UnsafeEntryPath(String),
    #[error("archive entry path length {length} exceeds limit {limit}: {path}")]
    EntryPathTooLong { path: String, length: usize, limit: usize },
    #[error("archive entry count {count} exceeds limit {limit}")]
    EntryCountLimitExceeded { count: usize, limit: usize },
    #[error("archive entry '{path}' size {size} exceeds limit {limit}")]
    EntrySizeLimitExceeded { path: String, size: u64, limit: u64 },
    #[error("archive total unpacked size {size} exceeds limit {limit}")]
    TotalUnpackedSizeLimitExceeded { size: u64, limit: u64 },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompressionCodec {
    Gzip,
    Bzip2,
    Xz,
    Zlib,
    Zstd,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ArchiveKind {
    Tar,
    Zip,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ArchiveFormat {
    Tar,
    TarGzip,
    TarBzip2,
    TarXz,
    TarZlib,
    TarZstd,
    Zip,
    Unknown,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CompressionFormat {
    Gzip,
    Bzip2,
    Zip,
    Xz,
    Zlib,
    Zstd,
    Tar,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionLevel {
    Fastest,
    Best,
    #[default]
    Default,
    Level(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipEntry {
    pub name: String,
    pub size: u64,
    pub compressed_size: u64,
    pub is_dir: bool,
    pub compression_method: String,
    pub archive_kind: ArchiveKind,
    pub format: ArchiveFormat,
    pub unix_mode: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZipExtractSummary {
    pub entry_count: usize,
    pub directory_count: usize,
    pub file_count: usize,
    pub total_unpacked_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveLimits {
    pub max_entries: usize,
    pub max_entry_size: u64,
    pub max_total_unpacked_size: u64,
    pub max_path_length: usize,
    pub validate_entry_paths: bool,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_entry_size: 1_073_741_824,
            max_total_unpacked_size: 10_737_418_240,
            max_path_length: 1024,
            validate_entry_paths: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZipWriteOptions {
    pub compression_level: CompressionLevel,
    pub create_directory_entries: bool,
}

impl Default for ZipWriteOptions {
    fn default() -> Self {
        Self {
            compression_level: CompressionLevel::Default,
            create_directory_entries: false,
        }
    }
}

const SMALL_ZIP_EXTRACT_FAST_PATH_LIMIT: u64 = 8 * 1024;

#[derive(Clone, Default)]
struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn into_vec(self) -> Vec<u8> {
        self.inner.lock().expect("shared in-memory writer lock poisoned").clone()
    }
}

impl AsyncWrite for SharedBuffer {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("shared in-memory writer lock poisoned"))?;
        inner.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl CompressionFormat {
    pub fn from_extension(ext: &str) -> Self {
        Self::from_archive_format(ArchiveFormat::from_extension(ext))
    }

    pub fn from_archive_format(format: ArchiveFormat) -> Self {
        match format {
            ArchiveFormat::TarGzip => CompressionFormat::Gzip,
            ArchiveFormat::TarBzip2 => CompressionFormat::Bzip2,
            ArchiveFormat::TarXz => CompressionFormat::Xz,
            ArchiveFormat::TarZlib => CompressionFormat::Zlib,
            ArchiveFormat::TarZstd => CompressionFormat::Zstd,
            ArchiveFormat::Tar => CompressionFormat::Tar,
            ArchiveFormat::Zip => CompressionFormat::Zip,
            ArchiveFormat::Unknown => CompressionFormat::Unknown,
        }
    }

    pub fn archive_format_from_path<P: AsRef<Path>>(path: P) -> ArchiveFormat {
        ArchiveFormat::from_path(path)
    }

    pub fn archive_kind(&self) -> Option<ArchiveKind> {
        match self {
            CompressionFormat::Tar => Some(ArchiveKind::Tar),
            CompressionFormat::Zip => Some(ArchiveKind::Zip),
            CompressionFormat::Gzip
            | CompressionFormat::Bzip2
            | CompressionFormat::Xz
            | CompressionFormat::Zlib
            | CompressionFormat::Zstd
            | CompressionFormat::Unknown => None,
        }
    }

    pub fn compression_codec(&self) -> Option<CompressionCodec> {
        match self {
            CompressionFormat::Gzip => Some(CompressionCodec::Gzip),
            CompressionFormat::Bzip2 => Some(CompressionCodec::Bzip2),
            CompressionFormat::Xz => Some(CompressionCodec::Xz),
            CompressionFormat::Zlib => Some(CompressionCodec::Zlib),
            CompressionFormat::Zstd => Some(CompressionCodec::Zstd),
            CompressionFormat::Tar | CompressionFormat::Zip | CompressionFormat::Unknown => None,
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        Self::from_archive_format(ArchiveFormat::from_path(path))
    }

    pub fn extension(&self) -> &'static str {
        match self {
            CompressionFormat::Gzip => "gz",
            CompressionFormat::Bzip2 => "bz2",
            CompressionFormat::Zip => "zip",
            CompressionFormat::Xz => "xz",
            CompressionFormat::Zlib => "zlib",
            CompressionFormat::Zstd => "zst",
            CompressionFormat::Tar => "tar",
            CompressionFormat::Unknown => "",
        }
    }

    pub fn is_supported(&self) -> bool {
        !matches!(self, CompressionFormat::Unknown)
    }

    pub fn get_decoder<R>(&self, input: R) -> Result<Box<dyn AsyncRead + Send + Unpin>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let reader = BufReader::new(input);

        let decoder: Box<dyn AsyncRead + Send + Unpin + 'static> = match self {
            CompressionFormat::Gzip => Box::new(GzipDecoder::new(reader)),
            CompressionFormat::Bzip2 => Box::new(BzDecoder::new(reader)),
            CompressionFormat::Zlib => Box::new(ZlibDecoder::new(reader)),
            CompressionFormat::Xz => Box::new(XzDecoder::new(reader)),
            CompressionFormat::Zstd => Box::new(ZstdDecoder::new(reader)),
            CompressionFormat::Tar => Box::new(reader),
            CompressionFormat::Zip => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "stream decoding",
                });
            }
            CompressionFormat::Unknown => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "decoding",
                });
            }
        };

        Ok(decoder)
    }

    fn convert_level(level: CompressionLevel) -> Result<async_compression::Level> {
        match level {
            CompressionLevel::Fastest => Ok(async_compression::Level::Fastest),
            CompressionLevel::Best => Ok(async_compression::Level::Best),
            CompressionLevel::Default => Ok(async_compression::Level::Default),
            CompressionLevel::Level(n) => {
                let level = i32::try_from(n).map_err(|_| ZipError::InvalidCompressionLevel(n))?;
                Ok(async_compression::Level::Precise(level))
            }
        }
    }

    pub fn get_encoder<W>(&self, output: W, level: CompressionLevel) -> Result<Box<dyn AsyncWrite + Send + Unpin>>
    where
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let writer = BufWriter::new(output);

        let encoder: Box<dyn AsyncWrite + Send + Unpin + 'static> = match self {
            CompressionFormat::Gzip => Box::new(GzipEncoder::with_quality(writer, Self::convert_level(level)?)),
            CompressionFormat::Bzip2 => Box::new(BzEncoder::with_quality(writer, Self::convert_level(level)?)),
            CompressionFormat::Zlib => Box::new(ZlibEncoder::with_quality(writer, Self::convert_level(level)?)),
            CompressionFormat::Xz => Box::new(XzEncoder::with_quality(writer, Self::convert_level(level)?)),
            CompressionFormat::Zstd => Box::new(ZstdEncoder::with_quality(writer, Self::convert_level(level)?)),
            CompressionFormat::Tar => Box::new(writer),
            CompressionFormat::Zip => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "stream encoding",
                });
            }
            CompressionFormat::Unknown => {
                return Err(ZipError::UnsupportedFormat {
                    format: *self,
                    operation: "encoding",
                });
            }
        };

        Ok(encoder)
    }
}

impl ArchiveFormat {
    pub fn from_extension(ext: &str) -> Self {
        match ext.to_ascii_lowercase().as_str() {
            "gz" | "gzip" | "tgz" => ArchiveFormat::TarGzip,
            "bz2" | "bzip2" | "tbz" | "tbz2" => ArchiveFormat::TarBzip2,
            "xz" | "txz" => ArchiveFormat::TarXz,
            "zlib" | "zz" => ArchiveFormat::TarZlib,
            "zst" | "zstd" | "tzst" => ArchiveFormat::TarZstd,
            "tar" => ArchiveFormat::Tar,
            "zip" => ArchiveFormat::Zip,
            _ => ArchiveFormat::Unknown,
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref();
        let lower_name = path.file_name().and_then(|name| name.to_str()).map(str::to_ascii_lowercase);

        if let Some(name) = lower_name {
            if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
                return ArchiveFormat::TarGzip;
            }
            if name.ends_with(".tar.bz2") || name.ends_with(".tbz") || name.ends_with(".tbz2") {
                return ArchiveFormat::TarBzip2;
            }
            if name.ends_with(".tar.xz") || name.ends_with(".txz") {
                return ArchiveFormat::TarXz;
            }
            if name.ends_with(".tar.zst") || name.ends_with(".tzst") {
                return ArchiveFormat::TarZstd;
            }
            if name.ends_with(".tar.zlib") {
                return ArchiveFormat::TarZlib;
            }
        }

        path.extension()
            .and_then(|s| s.to_str())
            .map(Self::from_extension)
            .unwrap_or(ArchiveFormat::Unknown)
    }

    pub fn archive_kind(&self) -> Option<ArchiveKind> {
        match self {
            ArchiveFormat::Tar
            | ArchiveFormat::TarGzip
            | ArchiveFormat::TarBzip2
            | ArchiveFormat::TarXz
            | ArchiveFormat::TarZlib
            | ArchiveFormat::TarZstd => Some(ArchiveKind::Tar),
            ArchiveFormat::Zip => Some(ArchiveKind::Zip),
            ArchiveFormat::Unknown => None,
        }
    }

    pub fn compression_codec(&self) -> Option<CompressionCodec> {
        match self {
            ArchiveFormat::TarGzip => Some(CompressionCodec::Gzip),
            ArchiveFormat::TarBzip2 => Some(CompressionCodec::Bzip2),
            ArchiveFormat::TarXz => Some(CompressionCodec::Xz),
            ArchiveFormat::TarZlib => Some(CompressionCodec::Zlib),
            ArchiveFormat::TarZstd => Some(CompressionCodec::Zstd),
            ArchiveFormat::Tar | ArchiveFormat::Zip | ArchiveFormat::Unknown => None,
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            ArchiveFormat::Tar => "tar",
            ArchiveFormat::TarGzip => "tar.gz",
            ArchiveFormat::TarBzip2 => "tar.bz2",
            ArchiveFormat::TarXz => "tar.xz",
            ArchiveFormat::TarZlib => "tar.zlib",
            ArchiveFormat::TarZstd => "tar.zst",
            ArchiveFormat::Zip => "zip",
            ArchiveFormat::Unknown => "",
        }
    }
}

/// Read entries from a tar-family archive stream.
///
/// Supported formats are:
/// - `CompressionFormat::Tar`
/// - `CompressionFormat::Gzip`
/// - `CompressionFormat::Bzip2`
/// - `CompressionFormat::Xz`
/// - `CompressionFormat::Zlib`
/// - `CompressionFormat::Zstd`
///
/// `CompressionFormat::Zip` is intentionally not supported here because ZIP
/// requires central-directory semantics and is handled through file-based
/// helper APIs.
pub async fn read_archive_entries<R, F, Fut>(input: R, format: CompressionFormat, callback: F) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    F: FnMut(tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    read_archive_entries_with_limits(input, format, ArchiveLimits::default(), callback).await
}

pub async fn read_archive_entries_with_limits<R, F, Fut>(
    input: R,
    format: CompressionFormat,
    limits: ArchiveLimits,
    mut callback: F,
) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    F: FnMut(tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    let decoder = format.get_decoder(input)?;
    let mut ar = Archive::new(decoder);
    let mut entries = ar.entries()?;
    let mut entry_count = 0_usize;
    let mut total_unpacked_size = 0_u64;

    while let Some(entry) = entries.next().await {
        let entry = entry?;
        entry_count += 1;
        validate_archive_entry_count(entry_count, limits)?;

        let entry_path = entry.path()?.to_string_lossy().into_owned();
        validate_archive_entry_name(&entry_path, limits)?;

        let entry_size = entry.header().size()?;
        validate_archive_entry_size(&entry_path, entry_size, limits)?;
        total_unpacked_size = total_unpacked_size.saturating_add(entry_size);
        validate_archive_total_size(total_unpacked_size, limits)?;

        callback(entry).await?;
    }

    Ok(())
}

/// Backward-compatible wrapper for archive entry iteration.
pub async fn decompress<R, F, Fut>(input: R, format: CompressionFormat, callback: F) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    F: FnMut(tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    read_archive_entries(input, format, callback).await
}

/// Explicit tar-family alias for callers that want a clearer name than
/// `decompress()`.
pub async fn extract_tar_entries<R, F, Fut>(input: R, format: CompressionFormat, callback: F) -> Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
    F: FnMut(tokio_tar::Entry<Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    read_archive_entries(input, format, callback).await
}

fn normalize_zip_entry_name(name: &str) -> Result<String> {
    let path = Path::new(name);
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ZipError::UnsafeEntryPath(name.to_string()));
            }
        }
    }

    let normalized = normalized.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        return Err(ZipError::UnsafeEntryPath(name.to_string()));
    }

    Ok(normalized)
}

fn validate_archive_entry_name(name: &str, limits: ArchiveLimits) -> Result<()> {
    if !limits.validate_entry_paths {
        return Ok(());
    }

    let normalized = normalize_zip_entry_name(name)?;
    let length = normalized.len();
    if length > limits.max_path_length {
        return Err(ZipError::EntryPathTooLong {
            path: normalized,
            length,
            limit: limits.max_path_length,
        });
    }

    Ok(())
}

fn validate_archive_entry_size(path: &str, size: u64, limits: ArchiveLimits) -> Result<()> {
    if size > limits.max_entry_size {
        return Err(ZipError::EntrySizeLimitExceeded {
            path: path.to_string(),
            size,
            limit: limits.max_entry_size,
        });
    }

    Ok(())
}

fn validate_archive_entry_count(count: usize, limits: ArchiveLimits) -> Result<()> {
    if count > limits.max_entries {
        return Err(ZipError::EntryCountLimitExceeded {
            count,
            limit: limits.max_entries,
        });
    }

    Ok(())
}

fn validate_archive_total_size(total_size: u64, limits: ArchiveLimits) -> Result<()> {
    if total_size > limits.max_total_unpacked_size {
        return Err(ZipError::TotalUnpackedSizeLimitExceeded {
            size: total_size,
            limit: limits.max_total_unpacked_size,
        });
    }

    Ok(())
}

fn zip_method_for_level(level: CompressionLevel) -> CompressionMethod {
    match level {
        CompressionLevel::Fastest => CompressionMethod::Stored,
        CompressionLevel::Best | CompressionLevel::Default | CompressionLevel::Level(_) => CompressionMethod::Deflated,
    }
}

fn parent_directories_for(path: &str) -> Vec<String> {
    let path = Path::new(path);
    let mut current = PathBuf::new();
    let mut directories = Vec::new();

    if let Some(parent) = path.parent() {
        for component in parent.components() {
            if let Component::Normal(part) = component {
                current.push(part);
                directories.push(format!("{}/", current.to_string_lossy().replace('\\', "/")));
            }
        }
    }

    directories
}

fn ensure_directory(path: &Path, created_directories: &mut HashSet<PathBuf>) -> Result<()> {
    let path = path.to_path_buf();
    if created_directories.insert(path.clone()) {
        std::fs::create_dir_all(&path)?;
    }

    Ok(())
}

fn write_small_zip_entry<R: Read>(reader: &mut R, output_path: &Path, size: u64) -> Result<()> {
    let size = usize::try_from(size).map_err(|_| io::Error::other("small zip entry size overflow"))?;
    let mut buffer = [0_u8; SMALL_ZIP_EXTRACT_FAST_PATH_LIMIT as usize];
    reader.read_exact(&mut buffer[..size])?;
    std::fs::write(output_path, &buffer[..size])?;
    Ok(())
}

pub async fn extract_zip_simple<P: AsRef<Path>, Q: AsRef<Path>>(zip_path: P, extract_to: Q) -> Result<Vec<ZipEntry>> {
    extract_zip_with_limits(zip_path, extract_to, ArchiveLimits::default()).await
}

pub async fn extract_zip_to_path_with_limits<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    extract_to: Q,
    limits: ArchiveLimits,
) -> Result<ZipExtractSummary> {
    let zip_path = zip_path.as_ref().to_path_buf();
    let extract_to = extract_to.as_ref().to_path_buf();

    spawn_blocking(move || extract_zip_impl(zip_path, extract_to, limits, false).map(|(_, summary)| summary)).await?
}

pub async fn extract_zip_with_limits<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    extract_to: Q,
    limits: ArchiveLimits,
) -> Result<Vec<ZipEntry>> {
    let zip_path = zip_path.as_ref().to_path_buf();
    let extract_to = extract_to.as_ref().to_path_buf();

    spawn_blocking(move || extract_zip_impl(zip_path, extract_to, limits, true).map(|(entries, _)| entries.unwrap_or_default()))
        .await?
}

fn extract_zip_impl(
    zip_path: PathBuf,
    extract_to: PathBuf,
    limits: ArchiveLimits,
    collect_entries: bool,
) -> Result<(Option<Vec<ZipEntry>>, ZipExtractSummary)> {
    let file = std::fs::File::open(&zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    std::fs::create_dir_all(&extract_to)?;
    let mut created_directories = HashSet::from([extract_to.clone()]);

    let mut entries = collect_entries.then(|| Vec::with_capacity(archive.len()));
    let mut summary = ZipExtractSummary::default();
    for index in 0..archive.len() {
        validate_archive_entry_count(index + 1, limits)?;
        let mut zip_file = archive.by_index(index)?;
        let enclosed_name = zip_file
            .enclosed_name()
            .ok_or_else(|| ZipError::UnsafeEntryPath(zip_file.name().to_string()))?;
        let entry_name = enclosed_name.to_string_lossy().replace('\\', "/");
        let is_dir = zip_file.is_dir();
        let size = zip_file.size();
        validate_archive_entry_name(&entry_name, limits)?;
        validate_archive_entry_size(&entry_name, size, limits)?;
        summary.total_unpacked_size = summary.total_unpacked_size.saturating_add(size);
        validate_archive_total_size(summary.total_unpacked_size, limits)?;
        let output_path = extract_to.join(&enclosed_name);

        if is_dir {
            ensure_directory(&output_path, &mut created_directories)?;
            summary.directory_count += 1;
        } else {
            if let Some(parent) = output_path.parent() {
                ensure_directory(parent, &mut created_directories)?;
            }
            if size <= SMALL_ZIP_EXTRACT_FAST_PATH_LIMIT {
                write_small_zip_entry(&mut zip_file, &output_path, size)?;
            } else {
                let mut output = std::fs::File::create(&output_path)?;
                std::io::copy(&mut zip_file, &mut output)?;
            }
            summary.file_count += 1;
        }
        summary.entry_count += 1;

        if let Some(ref mut entries) = entries {
            entries.push(ZipEntry {
                name: entry_name,
                size,
                compressed_size: zip_file.compressed_size(),
                is_dir,
                compression_method: format!("{:?}", zip_file.compression()),
                archive_kind: ArchiveKind::Zip,
                format: ArchiveFormat::Zip,
                unix_mode: zip_file.unix_mode(),
            });
        }
    }

    Ok((entries, summary))
}

pub async fn create_zip_simple<P: AsRef<Path>>(
    zip_path: P,
    files: Vec<(String, Vec<u8>)>,
    compression_level: CompressionLevel,
) -> Result<()> {
    create_zip_with_options(
        zip_path,
        files,
        ZipWriteOptions {
            compression_level,
            ..ZipWriteOptions::default()
        },
    )
    .await
}

pub async fn create_zip_with_options<P: AsRef<Path>>(
    zip_path: P,
    files: Vec<(String, Vec<u8>)>,
    options: ZipWriteOptions,
) -> Result<()> {
    let zip_path = zip_path.as_ref().to_path_buf();

    spawn_blocking(move || -> Result<()> {
        if let Some(parent) = zip_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::File::create(&zip_path)?;
        let mut writer = ZipWriter::new(file);
        let file_options = SimpleFileOptions::default().compression_method(zip_method_for_level(options.compression_level));
        let explicit_directories = files
            .iter()
            .filter(|(name, _)| name.ends_with('/'))
            .map(|(name, _)| normalize_zip_entry_name(name))
            .collect::<Result<HashSet<_>>>()?;
        let mut written_directories = HashSet::new();

        for (name, contents) in files {
            let entry_name = normalize_zip_entry_name(&name)?;
            if name.ends_with('/') {
                if written_directories.insert(entry_name.clone()) {
                    writer.add_directory(entry_name, file_options)?;
                }
            } else {
                if options.create_directory_entries {
                    for directory in parent_directories_for(&entry_name) {
                        if !explicit_directories.contains(&directory) && written_directories.insert(directory.clone()) {
                            writer.add_directory(directory, file_options)?;
                        }
                    }
                }
                writer.start_file(entry_name, file_options)?;
                writer.write_all(&contents)?;
            }
        }

        writer.finish()?;
        Ok(())
    })
    .await?
}

pub struct Compressor {
    format: CompressionFormat,
    level: CompressionLevel,
}

impl Compressor {
    pub fn new(format: CompressionFormat) -> Self {
        Self {
            format,
            level: CompressionLevel::Default,
        }
    }

    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    pub async fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        let sink = SharedBuffer::default();
        let mut encoder = self.format.get_encoder(sink.clone(), self.level)?;
        let mut reader = input;

        io::copy(&mut reader, &mut encoder).await?;
        encoder.shutdown().await?;
        drop(encoder);

        Ok(sink.into_vec())
    }

    pub async fn decompress(&self, input: Vec<u8>) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        let cursor = std::io::Cursor::new(input);
        let mut decoder = self.format.get_decoder(cursor)?;

        decoder.read_to_end(&mut output).await?;
        Ok(output)
    }
}

pub struct Decompressor {
    format: CompressionFormat,
}

impl Decompressor {
    pub fn new(format: CompressionFormat) -> Self {
        Self { format }
    }

    pub fn auto_detect<P: AsRef<Path>>(path: P) -> Self {
        Self {
            format: CompressionFormat::from_path(path),
        }
    }

    pub async fn decompress_file<P: AsRef<Path>>(&self, input_path: P, output_path: P) -> Result<()> {
        let input_file = File::open(&input_path).await?;
        let output_file = File::create(&output_path).await?;

        let mut decoder = self.format.get_decoder(input_file)?;
        let mut writer = BufWriter::new(output_file);

        io::copy(&mut decoder, &mut writer).await?;
        writer.shutdown().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::io::AsyncReadExt;
    use tokio_tar::{Builder, Header};
    use zip::write::FileOptions;

    async fn build_tar_bytes(files: &[(&str, &[u8])]) -> io::Result<Vec<u8>> {
        let sink = SharedBuffer::default();
        let handle = sink.clone();
        let mut builder = Builder::new(sink);

        for (path, content) in files {
            let mut header = Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, *path, &content[..]).await?;
        }

        builder.finish().await?;
        Ok(handle.into_vec())
    }

    async fn build_compressed_tar_bytes(format: CompressionFormat, files: &[(&str, &[u8])]) -> Result<Vec<u8>> {
        let tar_bytes = build_tar_bytes(files).await?;
        Compressor::new(format).compress(&tar_bytes).await
    }

    async fn build_zip_file_with_entries(path: &Path, files: &[(&str, &[u8])]) -> Result<()> {
        let path = path.to_path_buf();
        let files = files
            .iter()
            .map(|(name, content)| ((*name).to_string(), content.to_vec()))
            .collect::<Vec<_>>();
        spawn_blocking(move || -> Result<()> {
            let file = std::fs::File::create(path)?;
            let mut writer = ZipWriter::new(file);
            let options: FileOptions<'_, ()> = FileOptions::default().compression_method(CompressionMethod::Stored);
            for (name, content) in files {
                writer.start_file(name, options)?;
                writer.write_all(&content)?;
            }
            writer.finish()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn collect_archive_entries(payload: Vec<u8>, format: CompressionFormat) -> Result<Vec<(String, Vec<u8>)>> {
        let seen = Arc::new(Mutex::new(Vec::<(String, Vec<u8>)>::new()));
        let seen_ref = Arc::clone(&seen);
        let cursor = std::io::Cursor::new(payload);

        read_archive_entries(cursor, format, move |mut entry| {
            let seen_ref = Arc::clone(&seen_ref);
            async move {
                let path = entry.path()?.to_string_lossy().into_owned();
                let mut content = Vec::new();
                entry.read_to_end(&mut content).await?;
                seen_ref.lock().expect("seen collection lock poisoned").push((path, content));
                Ok(())
            }
        })
        .await?;

        Ok(seen.lock().expect("seen collection lock poisoned").clone())
    }

    #[test]
    fn test_compression_format_from_extension() {
        assert_eq!(CompressionFormat::from_extension("gz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_extension("ZIP"), CompressionFormat::Zip);
        assert_eq!(CompressionFormat::from_extension("tzst"), CompressionFormat::Zstd);
        assert_eq!(CompressionFormat::from_extension("txt"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_archive_format_from_extension() {
        assert_eq!(ArchiveFormat::from_extension("gz"), ArchiveFormat::TarGzip);
        assert_eq!(ArchiveFormat::from_extension("tbz2"), ArchiveFormat::TarBzip2);
        assert_eq!(ArchiveFormat::from_extension("txz"), ArchiveFormat::TarXz);
        assert_eq!(ArchiveFormat::from_extension("zip"), ArchiveFormat::Zip);
        assert_eq!(ArchiveFormat::from_extension("txt"), ArchiveFormat::Unknown);
    }

    #[test]
    fn test_compression_format_from_path_handles_compound_suffixes() {
        assert_eq!(CompressionFormat::from_path("archive.tar.gz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_path("archive.tgz"), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_path("archive.tar.bz2"), CompressionFormat::Bzip2);
        assert_eq!(CompressionFormat::from_path("archive.zip"), CompressionFormat::Zip);
        assert_eq!(CompressionFormat::from_path("archive"), CompressionFormat::Unknown);
    }

    #[test]
    fn test_archive_format_from_path_handles_compound_suffixes() {
        assert_eq!(ArchiveFormat::from_path("archive.tar.gz"), ArchiveFormat::TarGzip);
        assert_eq!(ArchiveFormat::from_path("archive.tar.bz2"), ArchiveFormat::TarBzip2);
        assert_eq!(ArchiveFormat::from_path("archive.tar.xz"), ArchiveFormat::TarXz);
        assert_eq!(ArchiveFormat::from_path("archive.tar.zst"), ArchiveFormat::TarZstd);
        assert_eq!(ArchiveFormat::from_path("archive.zip"), ArchiveFormat::Zip);
        assert_eq!(ArchiveFormat::from_path("archive"), ArchiveFormat::Unknown);
    }

    #[test]
    fn test_archive_format_and_legacy_compression_format_are_compatible() {
        assert_eq!(CompressionFormat::from_archive_format(ArchiveFormat::TarGzip), CompressionFormat::Gzip);
        assert_eq!(CompressionFormat::from_archive_format(ArchiveFormat::Tar), CompressionFormat::Tar);
        assert_eq!(CompressionFormat::from_archive_format(ArchiveFormat::Zip), CompressionFormat::Zip);
    }

    #[test]
    fn test_archive_format_exposes_archive_kind_and_codec() {
        assert_eq!(ArchiveFormat::TarGzip.archive_kind(), Some(ArchiveKind::Tar));
        assert_eq!(ArchiveFormat::TarGzip.compression_codec(), Some(CompressionCodec::Gzip));
        assert_eq!(ArchiveFormat::Tar.archive_kind(), Some(ArchiveKind::Tar));
        assert_eq!(ArchiveFormat::Tar.compression_codec(), None);
        assert_eq!(ArchiveFormat::Zip.archive_kind(), Some(ArchiveKind::Zip));
        assert_eq!(ArchiveFormat::Zip.compression_codec(), None);
    }

    #[test]
    fn test_legacy_compression_format_exposes_kind_and_codec() {
        assert_eq!(CompressionFormat::Gzip.archive_kind(), None);
        assert_eq!(CompressionFormat::Gzip.compression_codec(), Some(CompressionCodec::Gzip));
        assert_eq!(CompressionFormat::Tar.archive_kind(), Some(ArchiveKind::Tar));
        assert_eq!(CompressionFormat::Tar.compression_codec(), None);
        assert_eq!(CompressionFormat::Zip.archive_kind(), Some(ArchiveKind::Zip));
    }

    #[test]
    fn test_compression_format_size_is_small() {
        assert!(size_of::<CompressionFormat>() <= 8);
        assert!(size_of::<Option<CompressionFormat>>() <= 16);
    }

    #[test]
    fn test_convert_level_rejects_overflow() {
        let err = CompressionFormat::Gzip
            .get_encoder(SharedBuffer::default(), CompressionLevel::Level(u32::MAX))
            .err()
            .expect("overflow level should return an error");
        assert!(matches!(err, ZipError::InvalidCompressionLevel(u32::MAX)));
    }

    #[test]
    fn test_validate_archive_entry_name_rejects_absolute_path() {
        let err = validate_archive_entry_name("/absolute.txt", ArchiveLimits::default())
            .err()
            .expect("absolute path should fail validation");
        assert!(matches!(err, ZipError::UnsafeEntryPath(path) if path == "/absolute.txt"));
    }

    #[tokio::test]
    async fn test_compressor_round_trip_gzip() {
        let input = b"hello rustfs zip ".repeat(64);
        let compressor = Compressor::new(CompressionFormat::Gzip);

        let compressed = compressor.compress(&input).await.expect("gzip compress should succeed");
        assert!(!compressed.is_empty());
        assert_ne!(compressed, input);

        let decompressed = compressor
            .decompress(compressed)
            .await
            .expect("gzip decompress should succeed");
        assert_eq!(decompressed, input);
    }

    #[tokio::test]
    async fn test_compressor_round_trip_zstd() {
        let input = b"zstd payload ".repeat(128);
        let compressor = Compressor::new(CompressionFormat::Zstd).with_level(CompressionLevel::Best);

        let compressed = compressor.compress(&input).await.expect("zstd compress should succeed");
        let decompressed = compressor
            .decompress(compressed)
            .await
            .expect("zstd decompress should succeed");
        assert_eq!(decompressed, input);
    }

    #[tokio::test]
    async fn test_zip_stream_encoder_is_rejected() {
        let err = CompressionFormat::Zip
            .get_encoder(SharedBuffer::default(), CompressionLevel::Default)
            .err()
            .expect("zip stream encoder should be rejected");
        assert!(matches!(
            err,
            ZipError::UnsupportedFormat {
                format: CompressionFormat::Zip,
                operation: "stream encoding",
            }
        ));
    }

    #[tokio::test]
    async fn test_read_archive_entries_iterates_tar_gzip_entries() {
        let gzip_bytes =
            build_compressed_tar_bytes(CompressionFormat::Gzip, &[("nested/hello.txt", b"hello"), ("world.txt", b"world")])
                .await
                .expect("tar.gz build should succeed");

        let seen = collect_archive_entries(gzip_bytes, CompressionFormat::Gzip)
            .await
            .expect("tar.gz archive iteration should succeed");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "nested/hello.txt");
        assert_eq!(seen[0].1, b"hello");
        assert_eq!(seen[1].0, "world.txt");
        assert_eq!(seen[1].1, b"world");
    }

    #[tokio::test]
    async fn test_read_archive_entries_iterates_tar_bzip2_entries() {
        let payload =
            build_compressed_tar_bytes(CompressionFormat::Bzip2, &[("nested/hello.txt", b"hello"), ("world.txt", b"world")])
                .await
                .expect("tar.bz2 build should succeed");

        let seen = collect_archive_entries(payload, CompressionFormat::Bzip2)
            .await
            .expect("tar.bz2 archive iteration should succeed");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "nested/hello.txt");
        assert_eq!(seen[1].0, "world.txt");
    }

    #[tokio::test]
    async fn test_read_archive_entries_iterates_tar_xz_entries() {
        let payload =
            build_compressed_tar_bytes(CompressionFormat::Xz, &[("nested/hello.txt", b"hello"), ("world.txt", b"world")])
                .await
                .expect("tar.xz build should succeed");

        let seen = collect_archive_entries(payload, CompressionFormat::Xz)
            .await
            .expect("tar.xz archive iteration should succeed");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "nested/hello.txt");
        assert_eq!(seen[1].0, "world.txt");
    }

    #[tokio::test]
    async fn test_read_archive_entries_iterates_tar_zstd_entries() {
        let payload =
            build_compressed_tar_bytes(CompressionFormat::Zstd, &[("nested/hello.txt", b"hello"), ("world.txt", b"world")])
                .await
                .expect("tar.zst build should succeed");

        let seen = collect_archive_entries(payload, CompressionFormat::Zstd)
            .await
            .expect("tar.zst archive iteration should succeed");
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, "nested/hello.txt");
        assert_eq!(seen[1].0, "world.txt");
    }

    #[tokio::test]
    async fn test_extract_tar_entries_alias_matches_stream_behavior() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("hello.txt", b"hello")])
            .await
            .expect("tar.gz build should succeed");
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_ref = Arc::clone(&seen);

        extract_tar_entries(std::io::Cursor::new(payload), CompressionFormat::Gzip, move |entry| {
            let seen_ref = Arc::clone(&seen_ref);
            async move {
                seen_ref
                    .lock()
                    .expect("seen collection lock poisoned")
                    .push(entry.path()?.to_string_lossy().into_owned());
                Ok(())
            }
        })
        .await
        .expect("extract_tar_entries alias should succeed");

        assert_eq!(seen.lock().expect("seen collection lock poisoned").as_slice(), ["hello.txt"]);
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_zip_streams() {
        let err = read_archive_entries(std::io::Cursor::new(Vec::<u8>::new()), CompressionFormat::Zip, |_entry| async { Ok(()) })
            .await
            .err()
            .expect("zip stream should be rejected");

        assert!(matches!(
            err,
            ZipError::UnsupportedFormat {
                format: CompressionFormat::Zip,
                operation: "stream decoding",
            }
        ));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_corrupt_tar_gzip_stream() {
        let err = read_archive_entries(
            std::io::Cursor::new(b"not-a-valid-gzip-stream".to_vec()),
            CompressionFormat::Gzip,
            |_entry| async { Ok(()) },
        )
        .await
        .err()
        .expect("corrupt tar.gz stream should fail");

        assert!(matches!(err, ZipError::Io(_)));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_truncated_tar_gzip_stream() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("hello.txt", b"hello world")])
            .await
            .expect("tar.gz build should succeed");
        let truncated = payload[..payload.len() / 2].to_vec();

        let err = read_archive_entries(std::io::Cursor::new(truncated), CompressionFormat::Gzip, |_entry| async { Ok(()) })
            .await
            .err()
            .expect("truncated tar.gz stream should fail");

        assert!(matches!(err, ZipError::Io(_)));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_too_many_entries() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("one.txt", b"1"), ("two.txt", b"2")])
            .await
            .expect("tar.gz build should succeed");

        let err = read_archive_entries_with_limits(
            std::io::Cursor::new(payload),
            CompressionFormat::Gzip,
            ArchiveLimits {
                max_entries: 1,
                ..ArchiveLimits::default()
            },
            |_entry| async { Ok(()) },
        )
        .await
        .err()
        .expect("entry count limit should fail");

        assert!(matches!(err, ZipError::EntryCountLimitExceeded { count: 2, limit: 1 }));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_oversized_entry() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("big.txt", b"hello world")])
            .await
            .expect("tar.gz build should succeed");

        let err = read_archive_entries_with_limits(
            std::io::Cursor::new(payload),
            CompressionFormat::Gzip,
            ArchiveLimits {
                max_entry_size: 4,
                ..ArchiveLimits::default()
            },
            |_entry| async { Ok(()) },
        )
        .await
        .err()
        .expect("entry size limit should fail");

        assert!(matches!(
            err,
            ZipError::EntrySizeLimitExceeded {
                path,
                size: 11,
                limit: 4,
            } if path == "big.txt"
        ));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_total_unpacked_size_limit() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("one.txt", b"12345"), ("two.txt", b"67890")])
            .await
            .expect("tar.gz build should succeed");

        let err = read_archive_entries_with_limits(
            std::io::Cursor::new(payload),
            CompressionFormat::Gzip,
            ArchiveLimits {
                max_total_unpacked_size: 9,
                ..ArchiveLimits::default()
            },
            |_entry| async { Ok(()) },
        )
        .await
        .err()
        .expect("total unpacked size limit should fail");

        assert!(matches!(err, ZipError::TotalUnpackedSizeLimitExceeded { size: 10, limit: 9 }));
    }

    #[tokio::test]
    async fn test_read_archive_entries_rejects_entry_path_length_limit() {
        let payload = build_compressed_tar_bytes(CompressionFormat::Gzip, &[("nested/hello.txt", b"hello")])
            .await
            .expect("tar.gz build should succeed");

        let err = read_archive_entries_with_limits(
            std::io::Cursor::new(payload),
            CompressionFormat::Gzip,
            ArchiveLimits {
                max_path_length: 5,
                ..ArchiveLimits::default()
            },
            |_entry| async { Ok(()) },
        )
        .await
        .err()
        .expect("path length limit should fail");

        assert!(matches!(
            err,
            ZipError::EntryPathTooLong { path, limit: 5, .. } if path == "nested/hello.txt"
        ));
    }

    #[tokio::test]
    async fn test_create_and_extract_zip_round_trip() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("archive.zip");
        let extract_path = temp.path().join("extract");

        create_zip_simple(
            &zip_path,
            vec![
                ("nested/hello.txt".to_string(), b"hello".to_vec()),
                ("world.txt".to_string(), b"world".to_vec()),
            ],
            CompressionLevel::Default,
        )
        .await
        .expect("zip creation should succeed");

        let entries = extract_zip_simple(&zip_path, &extract_path)
            .await
            .expect("zip extraction should succeed");
        assert_eq!(entries.len(), 2);
        assert_eq!(
            fs::read(extract_path.join("nested/hello.txt"))
                .await
                .expect("nested zip entry should be extracted"),
            b"hello"
        );
        assert_eq!(
            fs::read(extract_path.join("world.txt"))
                .await
                .expect("root zip entry should be extracted"),
            b"world"
        );
        assert!(entries.iter().all(|entry| entry.archive_kind == ArchiveKind::Zip));
        assert!(entries.iter().all(|entry| entry.format == ArchiveFormat::Zip));
    }

    #[tokio::test]
    async fn test_create_zip_with_directory_entries_and_extract_directory_scenarios() {
        let temp = tempdir().expect("tempdir should be created");
        let explicit_zip_path = temp.path().join("explicit-directories.zip");
        let explicit_extract_path = temp.path().join("explicit-extract");
        let auto_zip_path = temp.path().join("auto-directories.zip");
        let auto_extract_path = temp.path().join("auto-extract");

        create_zip_with_options(
            &explicit_zip_path,
            vec![
                ("nested/".to_string(), Vec::new()),
                ("nested/deeper/".to_string(), Vec::new()),
            ],
            ZipWriteOptions {
                compression_level: CompressionLevel::Default,
                create_directory_entries: false,
            },
        )
        .await
        .expect("zip creation with explicit directory entries should succeed");

        let explicit_entries = extract_zip_with_limits(&explicit_zip_path, &explicit_extract_path, ArchiveLimits::default())
            .await
            .expect("zip extraction with explicit directory entries should succeed");

        assert!(
            explicit_entries
                .iter()
                .any(|entry| entry.name.trim_end_matches('/') == "nested" && entry.is_dir)
        );
        assert!(
            explicit_entries
                .iter()
                .any(|entry| entry.name.trim_end_matches('/') == "nested/deeper" && entry.is_dir)
        );
        assert!(
            fs::metadata(explicit_extract_path.join("nested"))
                .await
                .expect("nested directory should exist")
                .is_dir()
        );
        assert!(
            fs::metadata(explicit_extract_path.join("nested/deeper"))
                .await
                .expect("nested deeper directory should exist")
                .is_dir()
        );

        create_zip_with_options(
            &auto_zip_path,
            vec![("nested/deeper/file.txt".to_string(), b"hello".to_vec())],
            ZipWriteOptions {
                compression_level: CompressionLevel::Default,
                create_directory_entries: true,
            },
        )
        .await
        .expect("zip creation with automatic directory entries should succeed");

        let entries = extract_zip_with_limits(&auto_zip_path, &auto_extract_path, ArchiveLimits::default())
            .await
            .expect("zip extraction with automatic directory entries should succeed");

        assert!(
            entries
                .iter()
                .any(|entry| entry.name.trim_end_matches('/') == "nested" && entry.is_dir)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.name.trim_end_matches('/') == "nested/deeper" && entry.is_dir)
        );
        assert!(
            fs::metadata(auto_extract_path.join("nested"))
                .await
                .expect("nested directory should exist")
                .is_dir()
        );
        assert!(
            fs::metadata(auto_extract_path.join("nested/deeper"))
                .await
                .expect("nested deeper directory should exist")
                .is_dir()
        );
        assert_eq!(
            fs::read(auto_extract_path.join("nested/deeper/file.txt"))
                .await
                .expect("nested file should be extracted"),
            b"hello"
        );
    }

    #[tokio::test]
    async fn test_create_zip_with_lots_of_small_files_round_trip() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("many-small-files.zip");
        let extract_path = temp.path().join("extract");
        let files = (0..32)
            .map(|index| (format!("batch/file-{index}.txt"), format!("payload-{index}").into_bytes()))
            .collect::<Vec<_>>();

        create_zip_with_options(
            &zip_path,
            files.clone(),
            ZipWriteOptions {
                compression_level: CompressionLevel::Default,
                create_directory_entries: true,
            },
        )
        .await
        .expect("zip creation for many small files should succeed");

        let entries = extract_zip_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
            .await
            .expect("zip extraction for many small files should succeed");

        assert!(entries.len() >= files.len());
        for (path, expected) in files {
            assert_eq!(
                fs::read(extract_path.join(path))
                    .await
                    .expect("small file should be extracted"),
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_extract_zip_to_path_with_limits_returns_summary() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("summary.zip");
        let extract_path = temp.path().join("extract");

        create_zip_with_options(
            &zip_path,
            vec![
                ("nested/".to_string(), Vec::new()),
                ("nested/hello.txt".to_string(), b"hello".to_vec()),
                ("world.txt".to_string(), b"world".to_vec()),
            ],
            ZipWriteOptions {
                compression_level: CompressionLevel::Default,
                create_directory_entries: false,
            },
        )
        .await
        .expect("zip creation for summary should succeed");

        let summary = extract_zip_to_path_with_limits(&zip_path, &extract_path, ArchiveLimits::default())
            .await
            .expect("zip extract summary should succeed");

        assert_eq!(summary.entry_count, 3);
        assert_eq!(summary.directory_count, 1);
        assert_eq!(summary.file_count, 2);
        assert_eq!(summary.total_unpacked_size, 10);
        assert_eq!(
            fs::read(extract_path.join("nested/hello.txt"))
                .await
                .expect("nested file should be extracted"),
            b"hello"
        );
        assert_eq!(
            fs::read(extract_path.join("world.txt"))
                .await
                .expect("world file should be extracted"),
            b"world"
        );
    }

    #[tokio::test]
    async fn test_zip_helper_exposes_stored_vs_deflated_metadata() {
        let temp = tempdir().expect("tempdir should be created");
        let stored_zip_path = temp.path().join("stored.zip");
        let deflated_zip_path = temp.path().join("deflated.zip");
        let stored_extract_path = temp.path().join("stored-extract");
        let deflated_extract_path = temp.path().join("deflated-extract");
        let payload = b"compressible-content-".repeat(64);

        create_zip_with_options(
            &stored_zip_path,
            vec![("payload.txt".to_string(), payload.clone())],
            ZipWriteOptions {
                compression_level: CompressionLevel::Fastest,
                create_directory_entries: false,
            },
        )
        .await
        .expect("stored zip creation should succeed");

        create_zip_with_options(
            &deflated_zip_path,
            vec![("payload.txt".to_string(), payload.clone())],
            ZipWriteOptions {
                compression_level: CompressionLevel::Best,
                create_directory_entries: false,
            },
        )
        .await
        .expect("deflated zip creation should succeed");

        let stored_entries = extract_zip_with_limits(&stored_zip_path, &stored_extract_path, ArchiveLimits::default())
            .await
            .expect("stored zip extraction should succeed");
        let deflated_entries = extract_zip_with_limits(&deflated_zip_path, &deflated_extract_path, ArchiveLimits::default())
            .await
            .expect("deflated zip extraction should succeed");

        assert_eq!(stored_entries[0].compression_method, "Stored");
        assert_eq!(deflated_entries[0].compression_method, "Deflated");
        assert_eq!(
            fs::read(stored_extract_path.join("payload.txt"))
                .await
                .expect("stored payload should be extracted"),
            payload
        );
        assert_eq!(
            fs::read(deflated_extract_path.join("payload.txt"))
                .await
                .expect("deflated payload should be extracted"),
            payload
        );
        assert!(deflated_entries[0].compressed_size <= stored_entries[0].compressed_size);
    }

    #[tokio::test]
    async fn test_create_zip_rejects_unsafe_entry_name() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("archive.zip");

        let err = create_zip_simple(
            &zip_path,
            vec![("../escape.txt".to_string(), b"escape".to_vec())],
            CompressionLevel::Default,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ZipError::UnsafeEntryPath(path) if path == "../escape.txt"));
    }

    #[tokio::test]
    async fn test_extract_zip_rejects_too_many_entries() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("too-many.zip");
        let extract_path = temp.path().join("extract");

        build_zip_file_with_entries(&zip_path, &[("one.txt", b"1"), ("two.txt", b"2")])
            .await
            .expect("zip fixture should be created");

        let err = extract_zip_with_limits(
            &zip_path,
            &extract_path,
            ArchiveLimits {
                max_entries: 1,
                ..ArchiveLimits::default()
            },
        )
        .await
        .err()
        .expect("zip entry count limit should fail");

        assert!(matches!(err, ZipError::EntryCountLimitExceeded { count: 2, limit: 1 }));
    }

    #[tokio::test]
    async fn test_extract_zip_rejects_oversized_entry() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("oversized.zip");
        let extract_path = temp.path().join("extract");

        build_zip_file_with_entries(&zip_path, &[("big.txt", b"hello world")])
            .await
            .expect("zip fixture should be created");

        let err = extract_zip_with_limits(
            &zip_path,
            &extract_path,
            ArchiveLimits {
                max_entry_size: 4,
                ..ArchiveLimits::default()
            },
        )
        .await
        .err()
        .expect("zip entry size limit should fail");

        assert!(matches!(
            err,
            ZipError::EntrySizeLimitExceeded {
                path,
                size: 11,
                limit: 4,
            } if path == "big.txt"
        ));
    }

    #[tokio::test]
    async fn test_extract_zip_rejects_total_unpacked_size_limit() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("too-large-total.zip");
        let extract_path = temp.path().join("extract");

        build_zip_file_with_entries(&zip_path, &[("one.txt", b"12345"), ("two.txt", b"67890")])
            .await
            .expect("zip fixture should be created");

        let err = extract_zip_with_limits(
            &zip_path,
            &extract_path,
            ArchiveLimits {
                max_total_unpacked_size: 9,
                ..ArchiveLimits::default()
            },
        )
        .await
        .err()
        .expect("zip total size limit should fail");

        assert!(matches!(err, ZipError::TotalUnpackedSizeLimitExceeded { size: 10, limit: 9 }));
    }

    #[tokio::test]
    async fn test_extract_zip_rejects_entry_path_length_limit() {
        let temp = tempdir().expect("tempdir should be created");
        let zip_path = temp.path().join("long-path.zip");
        let extract_path = temp.path().join("extract");

        build_zip_file_with_entries(&zip_path, &[("nested/hello.txt", b"hello")])
            .await
            .expect("zip fixture should be created");

        let err = extract_zip_with_limits(
            &zip_path,
            &extract_path,
            ArchiveLimits {
                max_path_length: 5,
                ..ArchiveLimits::default()
            },
        )
        .await
        .err()
        .expect("zip path length limit should fail");

        assert!(matches!(
            err,
            ZipError::EntryPathTooLong { path, limit: 5, .. } if path == "nested/hello.txt"
        ));
    }

    #[tokio::test]
    async fn test_decompress_file_round_trip() {
        let temp = tempdir().expect("tempdir should be created");
        let input_path = temp.path().join("payload.txt.gz");
        let output_path = temp.path().join("payload.txt");
        let compressed = Compressor::new(CompressionFormat::Gzip)
            .compress(b"payload")
            .await
            .expect("gzip compress should succeed");
        fs::write(&input_path, compressed)
            .await
            .expect("compressed input file should be written");

        Decompressor::auto_detect(&input_path)
            .decompress_file(&input_path, &output_path)
            .await
            .expect("gzip file decompress should succeed");

        assert_eq!(
            fs::read(&output_path)
                .await
                .expect("decompressed output file should be readable"),
            b"payload"
        );
    }
}
