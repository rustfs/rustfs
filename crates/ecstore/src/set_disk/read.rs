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
use crate::bitrot::create_bitrot_chunk_stream;
use crate::store_api::{GetObjectChunkCopyMode, GetObjectChunkPath, GetObjectChunkResult};
use bytes::BytesMut;
use futures_util::{Stream, StreamExt, stream};
use rustfs_config::{DEFAULT_OBJECT_ZERO_COPY_ENABLE, ENV_OBJECT_ZERO_COPY_ENABLE};
use rustfs_io_core::{BoxChunkStream, IoChunk};
use std::io;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::io::ReaderStream;

struct ChannelChunkStream {
    receiver: Mutex<UnboundedReceiver<io::Result<IoChunk>>>,
}

impl ChannelChunkStream {
    fn new(receiver: UnboundedReceiver<io::Result<IoChunk>>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
        }
    }
}

impl Stream for ChannelChunkStream {
    type Item = io::Result<IoChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut receiver = self.receiver.lock().unwrap();
        receiver.poll_recv(cx)
    }
}

struct ChannelChunkWriter {
    sender: UnboundedSender<io::Result<IoChunk>>,
    buffer: BytesMut,
    chunk_size: usize,
}

impl ChannelChunkWriter {
    fn new(sender: UnboundedSender<io::Result<IoChunk>>, chunk_size: usize) -> Self {
        Self {
            sender,
            buffer: BytesMut::with_capacity(chunk_size.max(1)),
            chunk_size: chunk_size.max(1),
        }
    }

    fn push_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let bytes = self.buffer.split().freeze();
        self.sender
            .send(Ok(IoChunk::Shared(bytes)))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "chunk stream receiver dropped"))
    }

    fn finish(&mut self) -> io::Result<()> {
        self.push_buffer()
    }

    fn send_error(&self, err: io::Error) {
        let _ = self.sender.send(Err(err));
    }
}

impl AsyncWrite for ChannelChunkWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::result::Result<usize, io::Error>> {
        self.buffer.extend_from_slice(buf);
        let chunk_size = self.chunk_size;
        while self.buffer.len() >= chunk_size {
            let chunk = self.buffer.split_to(chunk_size).freeze();
            if self.sender.send(Ok(IoChunk::Shared(chunk))).is_err() {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "chunk stream receiver dropped")));
            }
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), io::Error>> {
        Poll::Ready(self.push_buffer())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), io::Error>> {
        Poll::Ready(self.finish())
    }
}

fn merge_chunk_copy_mode(current: GetObjectChunkCopyMode, next: GetObjectChunkCopyMode) -> GetObjectChunkCopyMode {
    use GetObjectChunkCopyMode::{SharedBytes, SingleCopy, TrueZeroCopy};

    match (current, next) {
        (SingleCopy, _) | (_, SingleCopy) => SingleCopy,
        (SharedBytes, _) | (_, SharedBytes) => SharedBytes,
        (TrueZeroCopy, TrueZeroCopy) => TrueZeroCopy,
    }
}

fn block_window(
    offset: usize,
    length: usize,
    block_size: usize,
    block_index: usize,
    start_block: usize,
    end_block: usize,
) -> (usize, usize) {
    if start_block == end_block {
        (offset % block_size, length)
    } else if block_index == start_block {
        (offset % block_size, block_size - (offset % block_size))
    } else if block_index == end_block {
        (0, (offset + length) % block_size)
    } else {
        (0, block_size)
    }
}

async fn send_direct_data_shard_chunks(
    sender: UnboundedSender<io::Result<IoChunk>>,
    mut shard_streams: Vec<BoxChunkStream>,
    data_shards: usize,
    block_size: usize,
    offset: usize,
    length: usize,
) {
    if length == 0 {
        return;
    }

    let start_block = offset / block_size;
    let end_block = offset.saturating_add(length - 1) / block_size;

    for block_index in start_block..=end_block {
        let (block_offset, block_length) = block_window(offset, length, block_size, block_index, start_block, end_block);
        if block_length == 0 {
            break;
        }

        let mut write_left = block_length;
        let mut skip = block_offset;

        for (shard_index, shard_stream) in shard_streams.iter_mut().enumerate().take(data_shards) {
            let Some(chunk) = shard_stream.next().await else {
                let _ = sender.send(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("missing chunk for data shard {shard_index}"),
                )));
                return;
            };

            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    let _ = sender.send(Err(err));
                    return;
                }
            };

            let chunk_len = chunk.len();
            if skip >= chunk_len {
                skip -= chunk_len;
                continue;
            }

            let start = skip;
            skip = 0;
            let take = (chunk_len - start).min(write_left);
            let chunk = if start == 0 && take == chunk_len {
                chunk
            } else {
                match chunk.slice(start, take) {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        let _ = sender.send(Err(err));
                        return;
                    }
                }
            };

            if sender.send(Ok(chunk)).is_err() {
                return;
            }
            write_left -= take;

            if write_left == 0 {
                break;
            }
        }

        if write_left != 0 {
            let _ = sender.send(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "not enough decoded shard data for requested block",
            )));
            return;
        }
    }
}

#[doc(hidden)]
pub async fn collect_direct_data_shard_chunks_for_benchmark(
    shard_streams: Vec<BoxChunkStream>,
    data_shards: usize,
    block_size: usize,
    offset: usize,
    length: usize,
) -> io::Result<Vec<IoChunk>> {
    let (tx, rx) = unbounded_channel();
    send_direct_data_shard_chunks(tx, shard_streams, data_shards, block_size, offset, length).await;

    let mut stream = ChannelChunkStream::new(rx);
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk?);
    }

    Ok(chunks)
}

impl SetDisks {
    #[tracing::instrument(level = "debug", skip(self, h, opts))]
    pub(crate) async fn get_object_chunks(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectChunkResult> {
        let lock_optimization_enabled = is_lock_optimization_enabled();

        let read_lock_guard = if !opts.no_lock {
            let acquire_start = Instant::now();

            if is_deadlock_detection_enabled() {
                debug!(
                    lock_id = format!("{}:{}", bucket, object),
                    lock_type = "read",
                    resource = format!("{}/{}", bucket, object),
                    "Waiting for read lock"
                );
            }

            let guard = self
                .new_ns_lock(bucket, object)
                .await?
                .get_read_lock(get_lock_acquire_timeout())
                .await
                .map_err(|e| {
                    Error::other(format!(
                        "Failed to acquire read lock: {}",
                        self.format_lock_error_from_error(bucket, object, "read", &e)
                    ))
                })?;

            let _lock_id = record_lock_acquire(bucket, object, "read");
            metrics::counter!("rustfs.lock.acquire.total", "type" => "read").increment(1);
            metrics::histogram!("rustfs.lock.acquire.duration.seconds").record(acquire_start.elapsed().as_secs_f64());

            Some(guard)
        } else {
            None
        };

        let (fi, files, disks) = self
            .get_object_fileinfo(bucket, object, opts, true)
            .await
            .map_err(|err| to_object_err(err, vec![bucket, object]))?;
        let object_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        if object_info.delete_marker {
            if opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        if object_info.size == 0 {
            return Ok(GetObjectChunkResult {
                stream: Box::pin(stream::iter(Vec::<io::Result<IoChunk>>::new())),
                path: GetObjectChunkPath::Direct,
                copy_mode: GetObjectChunkCopyMode::SharedBytes,
            });
        }

        if object_info.is_remote() {
            let mut opts = opts.clone();
            if object_info.parts.len() == 1 {
                opts.part_number = Some(1);
            }
            let gr = get_transitioned_object_reader(bucket, object, &range, &h, &object_info, &opts).await?;
            let stream = ReaderStream::new(gr.stream).map(|result| result.map(IoChunk::Shared));
            return Ok(GetObjectChunkResult {
                stream: Box::pin(stream),
                path: GetObjectChunkPath::Bridge,
                copy_mode: GetObjectChunkCopyMode::SingleCopy,
            });
        }

        if fi.erasure.data_blocks > 0 {
            let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files, &fi);
            let total_size = fi.size as usize;
            let requested_length = if let Some(range) = &range {
                let (offset, length) = range
                    .get_offset_length(fi.size)
                    .map_err(|err| to_object_err(err, vec![bucket, object]))?;
                (offset, length as usize)
            } else {
                (0, total_size)
            };

            let (part_index, mut part_offset) = fi.to_part_offset(requested_length.0)?;
            let mut end_offset = requested_length.0;
            if requested_length.1 > 0 {
                end_offset += requested_length.1 - 1;
            }
            let (last_part_index, _) = fi.to_part_offset(end_offset)?;

            let use_zero_copy = rustfs_utils::get_env_bool(ENV_OBJECT_ZERO_COPY_ENABLE, DEFAULT_OBJECT_ZERO_COPY_ENABLE);
            let mut part_streams = Vec::new();
            let mut part_total_read = 0usize;
            let mut merged_copy_mode = GetObjectChunkCopyMode::TrueZeroCopy;

            for current_part in part_index..=last_part_index {
                let part_number = fi.parts[current_part].number;
                let part_size = fi.parts[current_part].size;
                let mut part_length = part_size - part_offset;
                if part_length > (requested_length.1 - part_total_read) {
                    part_length = requested_length.1 - part_total_read;
                }

                let checksum_info = fi.erasure.get_checksum_info(part_number);
                let checksum_algo =
                    if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                        rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                    } else {
                        checksum_info.algorithm.clone()
                    };

                if fi.erasure.data_blocks == 1 {
                    let data_path =
                        format!("{}/{}/part.{}", object, files[current_part].data_dir.unwrap_or_default(), part_number);
                    let chunk_result = create_bitrot_chunk_stream(
                        files[current_part].data.as_deref(),
                        disks[current_part].as_ref(),
                        bucket,
                        &data_path,
                        part_offset,
                        part_length,
                        part_size,
                        fi.erasure.shard_size(),
                        checksum_algo,
                        opts.skip_verify_bitrot,
                        use_zero_copy,
                    )
                    .await?;

                    let Some(chunk_result) = chunk_result else {
                        part_streams.clear();
                        break;
                    };
                    merged_copy_mode = merge_chunk_copy_mode(merged_copy_mode, chunk_result.copy_mode);
                    part_streams.push(chunk_result.stream);
                } else {
                    let erasure = erasure_coding::Erasure::new_with_options(
                        fi.erasure.data_blocks,
                        fi.erasure.parity_blocks,
                        fi.erasure.block_size,
                        fi.uses_legacy_checksum,
                    );
                    let read_offset = (part_offset / erasure.block_size) * erasure.shard_size();
                    let till_offset = erasure.shard_file_offset(part_offset, part_length, part_size);
                    let shard_length = till_offset.saturating_sub(read_offset);
                    let shard_total_size = erasure.shard_file_size(part_size as i64) as usize;
                    let mut shard_streams = Vec::with_capacity(erasure.data_shards);
                    let mut part_copy_mode = GetObjectChunkCopyMode::TrueZeroCopy;

                    for shard_index in 0..erasure.data_shards {
                        let data_path =
                            format!("{}/{}/part.{}", object, files[shard_index].data_dir.unwrap_or_default(), part_number);
                        let chunk_result = match create_bitrot_chunk_stream(
                            files[shard_index].data.as_deref(),
                            disks[shard_index].as_ref(),
                            bucket,
                            &data_path,
                            read_offset,
                            shard_length,
                            shard_total_size,
                            erasure.shard_size(),
                            checksum_algo.clone(),
                            opts.skip_verify_bitrot,
                            use_zero_copy,
                        )
                        .await
                        {
                            Ok(Some(chunk_result)) => chunk_result,
                            Ok(None) => {
                                shard_streams.clear();
                                break;
                            }
                            Err(err) => {
                                debug!(
                                    bucket,
                                    object,
                                    part_number,
                                    shard_index,
                                    error = %err,
                                    "multi-shard direct chunk path unavailable, falling back to decoded read path"
                                );
                                shard_streams.clear();
                                break;
                            }
                        };
                        part_copy_mode = merge_chunk_copy_mode(part_copy_mode, chunk_result.copy_mode);
                        shard_streams.push(chunk_result.stream);
                    }

                    if shard_streams.len() != erasure.data_shards {
                        part_streams.clear();
                        break;
                    }

                    let (tx, rx) = unbounded_channel();
                    tokio::spawn(send_direct_data_shard_chunks(
                        tx,
                        shard_streams,
                        erasure.data_shards,
                        erasure.block_size,
                        part_offset,
                        part_length,
                    ));

                    merged_copy_mode = merge_chunk_copy_mode(merged_copy_mode, part_copy_mode);
                    part_streams.push(Box::pin(ChannelChunkStream::new(rx)));
                }
                part_total_read += part_length;
                part_offset = 0;
            }

            if !part_streams.is_empty() {
                return Ok(GetObjectChunkResult {
                    stream: Box::pin(stream::iter(part_streams).flatten()),
                    path: GetObjectChunkPath::Direct,
                    copy_mode: merged_copy_mode,
                });
            }
        }

        let read_lock_guard = if lock_optimization_enabled {
            if read_lock_guard.is_some() {
                let lock_id = format!("{}:{}", bucket, object);
                record_lock_release(bucket, object, &lock_id, "read");
                metrics::counter!("rustfs.lock.release.early.total", "type" => "read").increment(1);
            }
            drop(read_lock_guard);
            debug!(bucket, object, "Lock optimization: released read lock after metadata read");
            None
        } else {
            read_lock_guard
        };

        let chunk_size = get_duplex_buffer_size();
        let bucket = bucket.to_owned();
        let object = object.to_owned();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        let (tx, rx) = unbounded_channel();

        tokio::spawn(async move {
            let _guard = read_lock_guard;
            let mut writer = ChannelChunkWriter::new(tx, chunk_size);
            if let Err(err) = Self::get_object_with_fileinfo(
                &bucket,
                &object,
                0,
                fi.size,
                &mut writer,
                fi,
                files,
                &disks,
                set_index,
                pool_index,
                skip_verify,
            )
            .await
            {
                error!("get_object_with_fileinfo {bucket}/{object} err {:?}", err);
                writer.send_error(io::Error::other(err.to_string()));
            }

            if let Err(err) = writer.finish() {
                debug!(bucket, object, error = %err, "failed to flush chunk writer");
            }
        });

        Ok(GetObjectChunkResult {
            stream: Box::pin(ChannelChunkStream::new(rx)),
            path: GetObjectChunkPath::Bridge,
            copy_mode: GetObjectChunkCopyMode::SingleCopy,
        })
    }

    pub(super) async fn read_parts(
        disks: &[Option<DiskStore>],
        bucket: &str,
        part_meta_paths: &[String],
        part_numbers: &[usize],
        read_quorum: usize,
    ) -> disk::error::Result<Vec<ObjectPartInfo>> {
        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        // Use batch processor for better performance
        let processor = get_global_processors().read_processor();
        let bucket = bucket.to_string();
        let part_meta_paths = part_meta_paths.to_vec();

        let tasks: Vec<_> = disks
            .iter()
            .map(|disk| {
                let disk = disk.clone();
                let bucket = bucket.clone();
                let part_meta_paths = part_meta_paths.clone();

                async move {
                    if let Some(disk) = disk {
                        disk.read_parts(&bucket, &part_meta_paths).await
                    } else {
                        Err(DiskError::DiskNotFound)
                    }
                }
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        for result in results {
            match result {
                Ok(res) => {
                    errs.push(None);
                    object_parts.push(res);
                }
                Err(e) => {
                    errs.push(Some(e));
                    object_parts.push(vec![]);
                }
            }
        }

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        let mut ret = vec![ObjectPartInfo::default(); part_meta_paths.len()];

        for (part_idx, part_info) in part_meta_paths.iter().enumerate() {
            let mut part_meta_quorum = HashMap::new();
            let mut part_infos = Vec::new();
            for (j, parts) in object_parts.iter().enumerate() {
                if parts.len() != part_meta_paths.len() {
                    *part_meta_quorum.entry(part_info.clone()).or_insert(0) += 1;
                    continue;
                }

                if !parts[part_idx].etag.is_empty() {
                    *part_meta_quorum.entry(parts[part_idx].etag.clone()).or_insert(0) += 1;
                    part_infos.push(parts[part_idx].clone());
                    continue;
                }

                *part_meta_quorum.entry(part_info.clone()).or_insert(0) += 1;
            }

            let mut max_quorum = 0;
            let mut max_etag = None;
            let mut max_part_meta = None;
            for (etag, quorum) in part_meta_quorum.iter() {
                if quorum > &max_quorum {
                    max_quorum = *quorum;
                    max_etag = Some(etag);
                    max_part_meta = Some(etag);
                }
            }

            let mut found = None;
            for info in part_infos.iter() {
                if let Some(etag) = max_etag
                    && info.etag == *etag
                {
                    found = Some(info.clone());
                    break;
                }

                if let Some(part_meta) = max_part_meta
                    && info.etag.is_empty()
                    && part_meta.ends_with(format!("part.{0}.meta", info.number).as_str())
                {
                    found = Some(info.clone());
                    break;
                }
            }

            if let (Some(found), Some(max_etag)) = (found, max_etag)
                && !found.etag.is_empty()
                && part_meta_quorum.get(max_etag).unwrap_or(&0) >= &read_quorum
            {
                ret[part_idx] = found.clone();
            } else {
                ret[part_idx] = ObjectPartInfo {
                    number: part_numbers[part_idx],
                    error: Some(format!("part.{} not found", part_numbers[part_idx])),
                    ..Default::default()
                };
            }
        }

        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip(disks))]
    pub(super) async fn read_all_fileinfo(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>)> {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        let opts = Arc::new(ReadOptions {
            incl_free_versions,
            read_data,
            healing,
        });
        let org_bucket = Arc::new(org_bucket.to_string());
        let bucket = Arc::new(bucket.to_string());
        let object = Arc::new(object.to_string());
        let version_id = Arc::new(version_id.to_string());
        let futures = disks.iter().map(|disk| {
            let disk = disk.clone();
            let opts = opts.clone();
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    disk.read_version(&org_bucket, &bucket, &object, &version_id, &opts).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
        });

        // Wait for all tasks to complete
        let results = join_all(futures).await;

        for result in results {
            match result {
                Ok(res) => match res {
                    Ok(file_info) => {
                        ress.push(file_info);
                        errors.push(None);
                    }
                    Err(e) => {
                        ress.push(FileInfo::default());
                        errors.push(Some(e));
                    }
                },
                Err(_) => {
                    ress.push(FileInfo::default());
                    errors.push(Some(DiskError::Unexpected));
                }
            }
        }
        Ok((ress, errors))
    }

    pub async fn read_version_optimized(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<Vec<FileInfo>> {
        // Use existing disk selection logic
        let disks = self.disks.read().await;
        let required_reads = self.format.erasure.sets.len();

        // Clone parameters outside the closure to avoid lifetime issues
        let bucket = bucket.to_string();
        let object = object.to_string();
        let version_id = version_id.to_string();
        let opts = opts.clone();

        let processor = get_global_processors().read_processor();
        let tasks: Vec<_> = disks
            .iter()
            .take(required_reads + 2) // Read a few extra for reliability
            .filter_map(|disk| {
                disk.as_ref().map(|d| {
                    let disk = d.clone();
                    let bucket = bucket.clone();
                    let object = object.clone();
                    let version_id = version_id.clone();
                    let opts = opts.clone();

                    async move { disk.read_version(&bucket, &bucket, &object, &version_id, &opts).await }
                })
            })
            .collect();

        match processor.execute_batch_with_quorum(tasks, required_reads).await {
            Ok(results) => Ok(results),
            Err(_) => Err(DiskError::FileNotFound.into()), // Use existing error type
        }
    }

    pub(super) async fn read_all_xl(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let (fileinfos, errs) = Self::read_all_raw_file_info(disks, bucket, object, read_data).await;

        Self::pick_latest_quorum_files_info(fileinfos, errs, bucket, object, read_data, incl_free_vers).await
    }

    pub(super) async fn read_all_raw_file_info(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
    ) -> (Vec<Option<RawFileInfo>>, Vec<Option<DiskError>>) {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        let mut futures = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_xl(bucket, object, read_data).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        (ress, errors)
    }

    pub(super) async fn pick_latest_quorum_files_info(
        fileinfos: Vec<Option<RawFileInfo>>,
        errs: Vec<Option<DiskError>>,
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let mut metadata_array = vec![None; fileinfos.len()];
        let mut meta_file_infos = vec![FileInfo::default(); fileinfos.len()];
        let mut metadata_shallow_versions = vec![None; fileinfos.len()];

        let mut v2_bufs = {
            if !read_data {
                vec![Vec::new(); fileinfos.len()]
            } else {
                Vec::new()
            }
        };

        let mut errs = errs;

        for (idx, info_op) in fileinfos.iter().enumerate() {
            if let Some(info) = info_op {
                if !read_data {
                    v2_bufs[idx] = info.buf.clone();
                }

                let xlmeta = match FileMeta::load(&info.buf) {
                    Ok(res) => res,
                    Err(err) => {
                        errs[idx] = Some(err.into());
                        continue;
                    }
                };

                metadata_array[idx] = Some(xlmeta);
                meta_file_infos[idx] = FileInfo::default();
            }
        }

        for (idx, info_op) in metadata_array.iter().enumerate() {
            if let Some(info) = info_op {
                metadata_shallow_versions[idx] = Some(info.versions.clone());
            }
        }

        let shallow_versions: Vec<Vec<FileMetaShallowVersion>> = metadata_shallow_versions.iter().flatten().cloned().collect();

        let read_quorum = fileinfos.len().div_ceil(2);
        let versions = merge_file_meta_versions(read_quorum, false, 1, &shallow_versions);
        let meta = FileMeta {
            versions,
            ..Default::default()
        };

        let finfo = match meta.into_fileinfo(bucket, object, "", true, incl_free_vers, true) {
            Ok(res) => res,
            Err(err) => {
                for item in errs.iter_mut() {
                    if item.is_none() {
                        *item = Some(err.clone().into());
                    }
                }

                return (meta_file_infos, errs);
            }
        };

        if !finfo.is_valid() {
            for item in errs.iter_mut() {
                if item.is_none() {
                    *item = Some(DiskError::FileCorrupt);
                }
            }

            return (meta_file_infos, errs);
        }

        let vid = finfo.version_id.unwrap_or(Uuid::nil());

        for (idx, meta_op) in metadata_array.iter().enumerate() {
            if let Some(meta) = meta_op {
                match meta.into_fileinfo(bucket, object, vid.to_string().as_str(), read_data, incl_free_vers, true) {
                    Ok(res) => meta_file_infos[idx] = res,
                    Err(err) => errs[idx] = Some(err.into()),
                }
            }
        }

        (meta_file_infos, errs)
    }

    pub(super) async fn read_multiple_files(
        disks: &[Option<DiskStore>],
        req: ReadMultipleReq,
        read_quorum: usize,
    ) -> Vec<ReadMultipleResp> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let req = req.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_multiple(req).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        // debug!("ReadMultipleResp ress {:?}", ress);
        // debug!("ReadMultipleResp errors {:?}", errors);

        let mut ret = Vec::with_capacity(req.files.len());

        for want in req.files.iter() {
            let mut quorum = 0;

            let mut get_res = ReadMultipleResp::default();

            for res in ress.iter() {
                if res.is_none() {
                    continue;
                }

                let disk_res = res.as_ref().unwrap();

                for resp in disk_res.iter() {
                    if !resp.error.is_empty() || !resp.exists {
                        continue;
                    }

                    if &resp.file != want || resp.bucket != req.bucket || resp.prefix != req.prefix {
                        continue;
                    }
                    quorum += 1;

                    if get_res.mod_time > resp.mod_time || get_res.data.len() > resp.data.len() {
                        continue;
                    }

                    get_res = resp.clone();
                }
            }

            if quorum < read_quorum {
                // debug!("quorum < read_quorum: {} < {}", quorum, read_quorum);
                get_res.exists = false;
                get_res.error = Error::ErasureReadQuorum.to_string();
                get_res.data = Vec::new();
            }

            ret.push(get_res);
        }

        // log err

        ret
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn get_object_fileinfo(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        read_data: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>, Vec<Option<DiskStore>>)> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let vid = opts.version_id.clone().unwrap_or_default();

        // TODO: optimize concurrency and break once enough slots are available
        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, "", bucket, object, vid.as_str(), read_data, false, opts.incl_free_versions).await?;
        // warn!("get_object_fileinfo parts_metadata {:?}", &parts_metadata);
        // warn!("get_object_fileinfo {}/{} errs {:?}", bucket, object, &errs);

        let _min_disks = self.set_drive_count - self.default_parity_count;

        let (read_quorum, _) = match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))
        {
            Ok(v) => v,
            Err(e) => {
                // error!("Self::object_quorum_from_meta: {:?}, bucket: {}, object: {}", &e, bucket, object);
                return Err(e);
            }
        };

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum as usize) {
            error!("reduce_read_quorum_errs: {:?}, bucket: {}, object: {}", &err, bucket, object);
            return Err(to_object_err(err.into(), vec![bucket, object]));
        }

        let (op_online_disks, mot_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mot_time, etag, read_quorum as usize)?;
        if errs.iter().any(|err| err.is_some()) {
            let _ =
                rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                    fi.volume.to_string(),             // bucket
                    Some(fi.name.to_string()),         // object_prefix
                    false,                             // force_start
                    Some(HealChannelPriority::Normal), // priority
                    Some(self.pool_index),             // pool_index
                    Some(self.set_index),              // set_index
                ))
                .await;
        }
        // debug!("get_object_fileinfo pick fi {:?}", &fi);

        // let online_disks: Vec<Option<DiskStore>> = op_online_disks.iter().filter(|v| v.is_some()).cloned().collect();

        Ok((fi, parts_metadata, op_online_disks))
    }

    pub(super) async fn get_object_info_and_quorum(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> (ObjectInfo, usize, Option<StorageError>) {
        let fi = match self.get_object_fileinfo(bucket, object, opts, false).await {
            Ok((fi, _, _)) => fi,
            Err(e) => return (ObjectInfo::default(), 0, Some(e)),
        };

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        if !fi.version_purge_status().is_empty() && opts.version_id.is_some() {
            return (
                oi,
                write_quorum,
                Some(to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])),
            );
        }

        if fi.deleted {
            if opts.incl_free_versions && fi.tier_free_version() && opts.version_id.is_some() {
                return (oi, write_quorum, None);
            }
            return if opts.version_id.is_none() || opts.delete_marker {
                (oi, write_quorum, Some(to_object_err(StorageError::FileNotFound, vec![bucket, object])))
            } else {
                (
                    oi,
                    write_quorum,
                    Some(to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])),
                )
            };
        }

        (oi, write_quorum, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn get_object_with_fileinfo<W>(
        // &self,
        bucket: &str,
        object: &str,
        offset: usize,
        length: i64,
        writer: &mut W,
        fi: FileInfo,
        files: Vec<FileInfo>,
        disks: &[Option<DiskStore>],
        set_index: usize,
        pool_index: usize,
        skip_verify_bitrot: bool,
    ) -> Result<()>
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        debug!(bucket, object, requested_length = length, offset, "get_object_with_fileinfo start");
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, &files, &fi);

        let total_size = fi.size as usize;

        let length = if length < 0 {
            fi.size as usize - offset
        } else {
            length as usize
        };

        if offset > total_size || offset + length > total_size {
            error!("get_object_with_fileinfo offset out of range: {}, total_size: {}", offset, total_size);
            return Err(Error::other("offset out of range"));
        }

        let (part_index, mut part_offset) = fi.to_part_offset(offset)?;

        let mut end_offset = offset;
        if length > 0 {
            end_offset += length - 1
        }

        let (last_part_index, last_part_relative_offset) = fi.to_part_offset(end_offset)?;

        debug!(
            bucket,
            object, offset, length, end_offset, part_index, last_part_index, last_part_relative_offset, "Multipart read bounds"
        );

        let erasure = erasure_coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );

        let part_indices: Vec<usize> = (part_index..=last_part_index).collect();
        debug!(bucket, object, ?part_indices, "Multipart part indices to stream");

        let mut total_read = 0;
        for current_part in part_indices {
            if total_read == length {
                debug!(
                    bucket,
                    object,
                    total_read,
                    requested_length = length,
                    part_index = current_part,
                    "Stopping multipart stream early because accumulated bytes match request"
                );
                break;
            }

            let part_number = fi.parts[current_part].number;
            let part_size = fi.parts[current_part].size;
            let mut part_length = part_size - part_offset;
            if part_length > (length - total_read) {
                part_length = length - total_read
            }

            let till_offset = erasure.shard_file_offset(part_offset, part_length, part_size);

            let read_offset = (part_offset / erasure.block_size) * erasure.shard_size();

            debug!(
                bucket,
                object,
                part_index = current_part,
                part_number,
                part_offset,
                part_size,
                part_length,
                read_offset,
                till_offset,
                total_read_before = total_read,
                requested_length = length,
                "Streaming multipart part"
            );

            let checksum_info = fi.erasure.get_checksum_info(part_number);
            let checksum_algo =
                if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum_info.algorithm
                };

            // Read zero-copy configuration from environment variable
            // Default: enabled (true) for performance
            let use_zero_copy = rustfs_utils::get_env_bool(ENV_OBJECT_ZERO_COPY_ENABLE, DEFAULT_OBJECT_ZERO_COPY_ENABLE);

            let mut readers = Vec::with_capacity(disks.len());
            let mut errors = Vec::with_capacity(disks.len());
            for (idx, disk_op) in disks.iter().enumerate() {
                match create_bitrot_reader(
                    files[idx].data.as_deref(),
                    disk_op.as_ref(),
                    bucket,
                    &format!("{}/{}/part.{}", object, files[idx].data_dir.unwrap_or_default(), part_number),
                    read_offset,
                    till_offset,
                    erasure.shard_size(),
                    checksum_algo.clone(),
                    skip_verify_bitrot,
                    use_zero_copy,
                )
                .await
                {
                    Ok(Some(reader)) => {
                        readers.push(Some(reader));
                        errors.push(None);
                    }
                    Ok(None) => {
                        readers.push(None);
                        errors.push(Some(DiskError::DiskNotFound));
                    }
                    Err(e) => {
                        readers.push(None);
                        errors.push(Some(e));
                    }
                }
            }

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < erasure.data_shards {
                if let Some(read_err) = reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards) {
                    error!("create_bitrot_reader reduce_read_quorum_errs {:?}", &errors);
                    return Err(to_object_err(read_err.into(), vec![bucket, object]));
                }
                error!("create_bitrot_reader not enough disks to read: {:?}", &errors);
                return Err(Error::other(format!("not enough disks to read: {errors:?}")));
            }

            // Check if we have missing shards even though we can read successfully
            // This happens when a node was offline during write and comes back online
            let total_shards = erasure.data_shards + erasure.parity_shards;
            let available_shards = nil_count;
            let missing_shards = total_shards - available_shards;

            info!(
                bucket,
                object,
                part_number,
                total_shards,
                available_shards,
                missing_shards,
                data_shards = erasure.data_shards,
                parity_shards = erasure.parity_shards,
                "Shard availability check"
            );

            if missing_shards > 0 && available_shards >= erasure.data_shards {
                // We have missing shards but enough to read - trigger background heal
                info!(
                    bucket,
                    object,
                    part_number,
                    missing_shards,
                    available_shards,
                    pool_index,
                    set_index,
                    "Detected missing shards during read, triggering background heal"
                );
                if let Err(e) =
                    rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                        bucket.to_string(),
                        Some(object.to_string()),
                        false,
                        Some(HealChannelPriority::Normal),
                        Some(pool_index),
                        Some(set_index),
                    ))
                    .await
                {
                    warn!(
                        bucket,
                        object,
                        part_number,
                        error = %e,
                        "Failed to enqueue heal request for missing shards"
                    );
                } else {
                    warn!(bucket, object, part_number, "Successfully enqueued heal request for missing shards");
                }
            }

            // debug!(
            //     "read part {} part_offset {},part_length {},part_size {}  ",
            //     part_number, part_offset, part_length, part_size
            // );
            let (written, err) = erasure.decode(writer, readers, part_offset, part_length, part_size).await;
            debug!(
                bucket,
                object,
                part_index = current_part,
                part_number,
                part_length,
                bytes_written = written,
                "Finished decoding multipart part"
            );
            if let Some(e) = err {
                let de_err: DiskError = e.into();
                let mut has_err = true;
                if written == part_length {
                    match de_err {
                        DiskError::FileNotFound | DiskError::FileCorrupt => {
                            error!("erasure.decode err 111 {:?}", &de_err);
                            if let Err(e) = rustfs_common::heal_channel::send_heal_request(
                                rustfs_common::heal_channel::create_heal_request_with_options(
                                    bucket.to_string(),
                                    Some(object.to_string()),
                                    false,
                                    Some(HealChannelPriority::Normal),
                                    Some(pool_index),
                                    Some(set_index),
                                ),
                            )
                            .await
                            {
                                warn!(
                                    bucket,
                                    object,
                                    part_number,
                                    error = %e,
                                    "Failed to enqueue heal request after decode error"
                                );
                            }
                            has_err = false;
                        }
                        _ => {}
                    }
                }

                if has_err {
                    error!("erasure.decode err {} {:?}", written, &de_err);
                    return Err(de_err.into());
                }
            }

            // debug!("ec decode {} written size {}", part_number, n);

            total_read += part_length;
            part_offset = 0;
        }

        // debug!("read end");

        debug!(bucket, object, total_read, expected_length = length, "Multipart read finished");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn send_direct_data_shard_chunks_reassembles_multi_block_range() {
        let data_shards = 4;
        let block_size = 16;
        let shard_streams: Vec<BoxChunkStream> = vec![
            Box::pin(stream::iter(vec![
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[0, 1, 2, 3]))),
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[16, 17, 18, 19]))),
            ])),
            Box::pin(stream::iter(vec![
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[4, 5, 6, 7]))),
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[20, 21, 22, 23]))),
            ])),
            Box::pin(stream::iter(vec![
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[8, 9, 10, 11]))),
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[24, 25, 26, 27]))),
            ])),
            Box::pin(stream::iter(vec![
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[12, 13, 14, 15]))),
                Ok(IoChunk::Shared(Bytes::copy_from_slice(&[28, 29, 30, 31]))),
            ])),
        ];
        let (tx, rx) = unbounded_channel();

        send_direct_data_shard_chunks(tx, shard_streams, data_shards, block_size, 3, 18).await;

        let mut stream = ChannelChunkStream::new(rx);
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk.unwrap().as_bytes());
        }

        assert_eq!(collected, (3u8..21).collect::<Vec<_>>());
    }
}
