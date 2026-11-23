// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

mod restore;
mod save;
mod staging;

use super::*;
use futures::future::try_join_all;
use sha2::Digest as _;
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use tokio::io::AsyncReadExt;

const PAGE_MIN: usize = 8 * 1024;
const PAGE_MAX: usize = 64 * 1024;
const FANOUT: usize = 128;
const IO_CONCURRENCY: usize = 4;
const PREFIX: &[u8] = b"\x83\xa7pagesV1";
type Digest = [u8; 32];
pub(super) type RetiredBundles = [Option<uuid::Uuid>; 2];

/// Older readers see the committed compact bucket or an incomplete empty tree.
#[derive(Deserialize)]
struct PageEnvelope {
    #[serde(rename = "pagesV1")]
    header: Header,
    info: DataUsageCacheInfo,
    #[serde(rename = "cache")]
    _cache: serde::de::IgnoredAny,
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    body: Payload,
    retired: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    abandoned: Option<uuid::Uuid>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Payload {
    Inline,
    Pages(Manifest),
}

impl Header {
    fn manifest(&self) -> Option<&Manifest> {
        match &self.body {
            Payload::Pages(manifest) => Some(manifest),
            Payload::Inline => None,
        }
    }

    fn retired_bundles(&self) -> RetiredBundles {
        [self.retired, self.abandoned]
    }

    fn is_valid(&self) -> bool {
        let live = self.manifest().map(|manifest| manifest.bundle);
        self.manifest()
            .is_none_or(|manifest| !manifest.bundle.is_nil() && manifest.length != 0 && manifest.level <= 9)
            && (self.abandoned.is_none() || self.abandoned != self.retired)
            && self
                .retired_bundles()
                .into_iter()
                .flatten()
                .all(|id| !id.is_nil() && Some(id) != live)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Manifest {
    bundle: uuid::Uuid,
    finished: bool,
    cycle: u64,
    length: u64,
    level: u8,
    root: Digest,
    #[serde(default)]
    bundle_bytes: u64,
    #[serde(default)]
    bundle_commits: u16,
}

#[derive(Debug, Default)]
struct Archive {
    blocks: BTreeMap<Digest, Bytes>,
    leaves: Vec<Digest>,
    buffer: Vec<u8>,
    rolling: u64,
    length: u64,
}

fn invalid(message: &str) -> Error {
    Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_owned()))
}

pub(super) fn unreadable_page(error: &StorageError) -> bool {
    matches!(error, StorageError::FileCorrupt)
        || matches!(error, StorageError::Io(io) if matches!(io.kind(), std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof))
}

fn digest(data: &[u8]) -> Digest {
    Sha256::digest(data).into()
}

fn bundle_prefix(name: &str, bundle: uuid::Uuid) -> String {
    format!("{BUCKET_META_PREFIX}/{name}.pages/{bundle}/")
}

fn block_path(name: &str, bundle: uuid::Uuid, block: &Digest) -> String {
    let hash = hex_simd::encode_to_string(block, hex_simd::AsciiCase::Lower);
    format!("{}{hash}", bundle_prefix(name, bundle))
}

fn bucket_cache_name(name: &str) -> Option<&str> {
    let (bucket, leaf) = name.split_once('/')?;
    (!crate::is_reserved_or_invalid_bucket(bucket, true) && leaf == DATA_USAGE_CACHE_NAME).then_some(name)
}

// The high bits of a multiplicative hash of each byte give a rolling
// content-defined boundary. Insertion before a sealed run need not shift the
// offsets of every following page. Page length is bounded for every input.
const fn byte_gear(byte: u8) -> u64 {
    let mut x = (byte as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const GEAR: [u64; 256] = {
    let mut table = [0; 256];
    let mut byte = 0u8;
    loop {
        table[byte as usize] = byte_gear(byte);
        if byte == u8::MAX {
            break;
        }
        byte += 1;
    }
    table
};

impl Archive {
    fn intern(&mut self, bytes: Vec<u8>) -> Digest {
        let key = digest(&bytes);
        self.blocks.entry(key).or_insert_with(|| Bytes::from(bytes));
        key
    }

    fn seal(&mut self) {
        if !self.buffer.is_empty() {
            let bytes = std::mem::take(&mut self.buffer);
            let key = self.intern(bytes);
            self.leaves.push(key);
            self.rolling = 0;
        }
    }

    fn finish(&mut self, bundle: uuid::Uuid) -> StorageResult<Manifest> {
        self.seal();
        let mut children = std::mem::take(&mut self.leaves);
        let mut level = 0;
        while children.len() > 1 {
            children = children
                .chunks(FANOUT)
                .map(|chunk| {
                    let bytes = chunk.iter().flatten().copied().collect();
                    self.intern(bytes)
                })
                .collect();
            level += 1;
        }
        Ok(Manifest {
            bundle,
            finished: false,
            cycle: 0,
            length: self.length,
            level,
            root: children.pop().ok_or_else(|| invalid("empty scanner checkpoint"))?,
            bundle_bytes: 0,
            bundle_commits: 0,
        })
    }
}

impl Write for Archive {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.length = self
            .length
            .checked_add(u64::try_from(bytes.len()).map_err(std::io::Error::other)?)
            .ok_or_else(|| std::io::Error::other("scanner checkpoint is too large"))?;
        let mut start = 0;
        for (offset, byte) in bytes.iter().enumerate() {
            self.rolling = self.rolling.wrapping_shl(1).wrapping_add(GEAR[usize::from(*byte)]);
            let end = offset + 1;
            let len = self.buffer.len() + (end - start);
            if len == PAGE_MAX || (len >= PAGE_MIN && self.rolling & 0x3fff == 0) {
                self.buffer.extend_from_slice(&bytes[start..end]);
                self.seal();
                start = end;
            }
        }
        self.buffer.extend_from_slice(&bytes[start..]);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) struct EncodedPages {
    archive: Archive,
    root: Manifest,
    inline: Option<Bytes>,
}

impl EncodedPages {
    pub(super) fn length(&self) -> u64 {
        self.archive.length
    }

    /// Share the original MessagePack stream without copying page payloads.
    pub(super) async fn progress_input(&self) -> Option<Vec<Bytes>> {
        let mut pending = vec![(self.root.root, self.root.level)];
        let mut chunks = Vec::new();
        let mut visits = 0_usize;
        while let Some((key, level)) = pending.pop() {
            visits += 1;
            if visits.is_multiple_of(256) {
                tokio::task::yield_now().await;
            }
            let bytes = self.archive.blocks.get(&key)?;
            if level == 0 {
                chunks.push(bytes.clone());
            } else {
                let (children, remainder) = bytes.as_chunks::<32>();
                if !remainder.is_empty() {
                    return None;
                }
                for child in children.iter().rev() {
                    pending.push((*child, level - 1));
                }
            }
        }
        Some(chunks)
    }
}

pub(super) fn encode(cache: &DataUsageCache) -> StorageResult<EncodedPages> {
    let mut archive = Archive::default();
    archive.write_all(&[0x92])?;
    cache.info.serialize(&mut rmp_serde::Serializer::new(&mut archive))?;
    archive.seal();
    let count = u32::try_from(cache.cache.len()).map_err(|_| invalid("too many scanner checkpoint entries"))?;
    // A wide root's growing child set must not shift every subtree page.
    let mut entries: Vec<_> = cache.cache.iter().collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let mut header = Vec::new();
    if count < 16 {
        header.push(0x80 | u8::try_from(count).map_err(|_| invalid("invalid fixed map length"))?);
    } else if let Ok(n) = u16::try_from(count) {
        header.push(0xde);
        header.extend_from_slice(&n.to_be_bytes());
    } else {
        header.push(0xdf);
        header.extend_from_slice(&count.to_be_bytes());
    }
    archive.write_all(&header)?;
    for (name, entry) in entries {
        let mut serializer = rmp_serde::Serializer::new(&mut archive);
        name.serialize(&mut serializer)?;
        entry.ordered_encoding().serialize(&mut serializer)?;
        if name == &cache.info.name {
            archive.seal();
        }
    }
    archive.seal();
    let inline = if archive.length <= PAGE_MIN as u64 {
        let mut buf = Vec::new();
        for key in &archive.leaves {
            buf.extend_from_slice(
                archive
                    .blocks
                    .get(key)
                    .ok_or_else(|| invalid("missing encoded scanner page"))?,
            );
        }
        Some(Bytes::from(buf))
    } else {
        None
    };
    let root = archive.finish(uuid::Uuid::nil())?;
    Ok(EncodedPages { archive, root, inline })
}

pub(super) fn is_manifest(bytes: &[u8]) -> bool {
    bytes.starts_with(PREFIX)
}

fn read_envelope(bytes: &[u8], bucket: &str) -> StorageResult<PageEnvelope> {
    let head: PageEnvelope = rmp_serde::from_slice(bytes).map_err(|_| invalid("invalid scanner checkpoint manifest"))?;
    if head.info.name != bucket || !head.header.is_valid() {
        return Err(invalid("invalid scanner checkpoint identity or size"));
    }
    Ok(head)
}

async fn read_block<S: ScannerObjectIO>(store: Arc<S>, name: &str, bundle: uuid::Uuid, key: Digest) -> StorageResult<Bytes> {
    let path = block_path(name, bundle, &key);
    let mut reader = store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            &path,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await?;
    if reader.object_info.size < 1 || usize::try_from(reader.object_info.size).map_or(true, |size| size > PAGE_MAX) {
        return Err(invalid("invalid scanner checkpoint page length"));
    }
    let bytes = Bytes::from(reader.read_all().await?);
    if bytes.len() > PAGE_MAX || digest(&bytes) != key {
        return Err(invalid("scanner checkpoint page checksum mismatch"));
    }
    Ok(bytes)
}

async fn committed_keys<S: ScannerObjectIO>(
    store: Arc<S>,
    name: &str,
    manifest: &Manifest,
    candidate: &Archive,
) -> StorageResult<HashSet<Digest>> {
    let mut keys = vec![manifest.root];
    let mut known = HashSet::from([manifest.root]);
    for _ in 0..manifest.level {
        let parents = std::mem::take(&mut keys);
        for batch in parents.chunks(IO_CONCURRENCY) {
            let mut reads = Vec::new();
            let mut nodes = Vec::with_capacity(batch.len());
            for &key in batch {
                // The committed parent authenticates the table digest.
                // Unchanged tables are already available in the candidate.
                if let Some(node) = candidate.blocks.get(&key) {
                    nodes.push(node.clone());
                } else {
                    reads.push(read_block(store.clone(), name, manifest.bundle, key));
                }
            }
            nodes.extend(try_join_all(reads).await?);
            for node in nodes {
                if node.is_empty() || node.len() > FANOUT * 32 || node.len() % 32 != 0 {
                    return Err(invalid("invalid scanner checkpoint page table"));
                }
                for &key in node.as_chunks::<32>().0 {
                    keys.push(key);
                    known.insert(key);
                }
                if u64::try_from(keys.len()).unwrap_or(u64::MAX) > manifest.length.div_ceil(PAGE_MIN as u64).saturating_add(3) {
                    return Err(invalid("scanner checkpoint contains excess page references"));
                }
            }
        }
    }
    Ok(known)
}

pub(super) async fn decode<S: ScannerObjectIO>(store: Arc<S>, load_name: &str, bytes: &[u8]) -> StorageResult<DataUsageCache> {
    if !is_manifest(bytes) {
        return DataUsageCache::unmarshal(bytes).map_err(|_| invalid("invalid scanner cache"));
    }
    let name = load_name.strip_suffix(".bkp").unwrap_or(load_name);
    let bucket = bucket_cache_name(name)
        .and_then(|name| name.split_once('/'))
        .ok_or_else(|| invalid("misplaced scanner checkpoint"))?
        .0;
    let head = read_envelope(bytes, bucket)?;
    let Some(manifest) = head.header.manifest() else {
        return DataUsageCache::unmarshal(bytes).map_err(|_| invalid("invalid inline scanner checkpoint"));
    };
    let cache = restore::restore(store, load_name, manifest).await?;
    if cache.info.name != bucket
        || cache.info.snapshot_complete != manifest.finished
        || cache.info.next_cycle != manifest.cycle
        || cache.info.scan_identity != head.info.scan_identity
        || cache.info.source != head.info.source
        || cache.info.leader_epoch != head.info.leader_epoch
        || cache.info.next_cycle != head.info.next_cycle
    {
        return Err(invalid("scanner checkpoint scope changed"));
    }
    Ok(cache)
}

async fn previous_header<S: ScannerObjectIO>(
    store: Arc<S>,
    name: &str,
    revision: &DataUsageCacheRevision,
) -> StorageResult<Option<Header>> {
    if *revision == DataUsageCacheRevision::Missing {
        return Ok(None);
    }
    let path = path_join_buf(&[BUCKET_META_PREFIX, name]);
    let mut reader = store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            &path,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await?;
    if reader
        .object_info
        .etag
        .as_ref()
        .map(|etag| DataUsageCacheRevision::Etag(etag.clone()))
        .as_ref()
        != Some(revision)
    {
        return Err(StorageError::PreconditionFailed);
    }
    // Header is the first value in the envelope. A large compatibility
    // projection or legacy cache does not require reading its body here.
    let mut prefix = Vec::new();
    (&mut reader).take(1024).read_to_end(&mut prefix).await?;
    if !is_manifest(&prefix) {
        return Ok(None);
    }
    let mut deserializer = rmp_serde::Deserializer::new(&prefix[PREFIX.len()..]);
    // A damaged old header cannot participate in reuse or reclamation.
    // Rebuilding uses the CAS revision obtained by the normal loader.
    Ok(Header::deserialize(&mut deserializer).ok().filter(Header::is_valid))
}

fn build_bundle(name: &str, revision: &DataUsageCacheRevision, cache: &DataUsageCache) -> uuid::Uuid {
    // Before the first root CAS, retries and competing builders populate the
    // same staging namespace. Individual page names are immutable digests.
    let mut hash = Sha256::new();
    hash.update(b"rustfs-scanner-checkpoint-v1\0");
    hash.update(name.as_bytes());
    if let Some(identity) = cache.info.scan_identity {
        hash.update(identity.bucket_incarnation.as_bytes());
    }
    if let DataUsageCacheRevision::Etag(etag) = revision {
        hash.update(etag.as_bytes());
    }
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize()[..16]);
    uuid::Uuid::from_bytes(id)
}

pub(super) fn eligible(cache: &DataUsageCache, name: &str) -> bool {
    bucket_cache_name(name).is_some()
        && cache.info.scan_identity.is_some()
        && cache.info.name == name.split('/').next().unwrap_or("")
}

pub(super) async fn prepare<S: ScannerObjectIO + ScannerConfigObjectDelete>(
    cache: &DataUsageCache,
    store: Arc<S>,
    name: &str,
    revision: &DataUsageCacheRevision,
    encoded: &EncodedPages,
    epoch: u64,
    damaged: bool,
) -> StorageResult<(Bytes, RetiredBundles)> {
    let previous_header = previous_header(store.clone(), name, revision).await?;
    let previous = previous_header.as_ref().and_then(Header::manifest);
    let [mut retired, mut abandoned] = previous_header.as_ref().map_or([None, None], Header::retired_bundles);
    for pending in [&mut retired, &mut abandoned] {
        if let Some(id) = *pending
            && reclaim(store.clone(), name, id, epoch).await.is_ok()
        {
            *pending = None;
        }
    }

    // A marker lives beside a new bundle's pages and commits before any of
    // them. We can rediscover that bundle even if the writer never committed
    // its manifest or retained its in-memory write receipts.
    let candidate = build_bundle(name, revision, cache);
    let marker = format!("{}staging", bundle_prefix(name, candidate));
    let has_marker = match read_config_revision(store.clone(), &marker).await {
        Ok(revision) => revision != DataUsageCacheRevision::Missing,
        Err(error) if unreadable_page(&error) => true,
        Err(error) => return Err(error),
    };
    let allow_rotation = retired.is_none() && abandoned.is_none();
    let inline = encoded.inline.as_ref().filter(|_| previous.is_none() || allow_rotation);
    let live_bytes = save::block_bytes(encoded.archive.blocks.values());
    let reuse = match previous {
        Some(head) if inline.is_none() && !damaged && !(head.finished && head.cycle != cache.info.next_cycle) => {
            save::reuse(store.clone(), name, head, &encoded.archive, live_bytes).await?
        }
        _ => None,
    };
    if (inline.is_some() || reuse.is_some()) && has_marker && previous.is_none_or(|head| head.bundle != candidate) {
        if abandoned.is_some_and(|pending| pending != candidate) {
            return Err(Error::other(
                "cannot abandon scanner checkpoint before reclaiming its previous staging bundle",
            ));
        }
        abandoned = Some(candidate);
    }

    if let Some(inline) = inline {
        retired = retired.or_else(|| previous.map(|head| head.bundle));
        let header = Header {
            body: Payload::Inline,
            retired,
            abandoned,
        };
        let cleanup = header.retired_bundles();
        if cleanup == [None, None] {
            return Ok((inline.clone(), cleanup));
        }
        let bytes = rmp_serde::to_vec_named(&CompatibilityEnvelope {
            header: &header,
            info: &cache.info,
            cache: &cache.cache,
        })
        .map_err(|_| invalid("failed to encode inline scanner checkpoint"))?;
        return Ok((Bytes::from(bytes), cleanup));
    }

    let verify_backup = name.ends_with(".bkp");
    let (bundle, known, bundle_bytes, bundle_commits) = if let Some((head, known, total)) = reuse {
        (head.bundle, known, total, head.bundle_commits + 1)
    } else {
        if !allow_rotation {
            // Keep the readable generations and their cleanup obligations.
            return Err(Error::other("cannot rotate scanner checkpoint before reclaiming its retired bundle"));
        }
        retired = previous.map(|head| head.bundle);
        (candidate, HashSet::new(), live_bytes, 1)
    };
    let reserve_candidate = || async {
        if !has_marker {
            DataUsageCache::save_path_with_retry(
                store.clone(),
                &marker,
                &Bytes::from_static(b"scanner checkpoint staging"),
                DataUsageCache::cache_save_timeout(),
                DATA_USAGE_CACHE_SAVE_RETRIES,
                Some(DataUsageCacheRevision::Missing),
                Some(epoch),
            )
            .await?;
        }
        Ok::<(), StorageError>(())
    };
    if bundle == candidate {
        reserve_candidate().await?;
    }
    let mut manifest = encoded.root.clone();
    manifest.bundle = bundle;
    manifest.finished = cache.info.snapshot_complete;
    manifest.cycle = cache.info.next_cycle;
    manifest.bundle_bytes = bundle_bytes;
    manifest.bundle_commits = bundle_commits;

    if let Err(error) = save::stage(store.clone(), name, revision, epoch, &manifest, &encoded.archive, &known).await {
        // A corrupt XL header need not yield an ETag for a conditional repair.
        // Rebuild this backup under a fresh namespace; its old root stays visible
        // until the manifest CAS. Other I/O failures remain retryable in place.
        if !(verify_backup && manifest.bundle_commits > 1 && allow_rotation && unreadable_page(&error)) {
            return Err(error);
        }
        retired = Some(manifest.bundle);
        abandoned = None;
        manifest.bundle = candidate;
        reserve_candidate().await?;
        manifest.bundle_bytes = live_bytes;
        manifest.bundle_commits = 1;
        save::stage(store.clone(), name, revision, epoch, &manifest, &encoded.archive, &HashSet::new()).await?;
    }
    let header = Header {
        body: Payload::Pages(manifest),
        retired,
        abandoned,
    };
    let cleanup = header.retired_bundles();
    let projection = |info: &DataUsageCacheInfo, cache: &BTreeMap<String, DataUsageEntry>| {
        rmp_serde::to_vec_named(&CompatibilityEnvelope {
            header: &header,
            info,
            cache,
        })
        .map_err(|_| invalid("failed to encode scanner checkpoint root"))
    };
    if cache.info.snapshot_complete {
        // Older readers can serve compact totals when the compatibility
        // projection fits. The manifest remains bounded even with many
        // pending repairs or a large lifecycle configuration.
        let mut root = cache
            .checked_flatten_complete_scope(&cache.info.name)
            .ok_or_else(|| invalid("disconnected complete bucket checkpoint"))?;
        root.compacted = true;
        let bytes = projection(&cache.info, &BTreeMap::from([(cache.info.name.clone(), root)]))?;
        if bytes.len() <= PAGE_MIN {
            return Ok((Bytes::from(bytes), cleanup));
        }
    }
    let info = DataUsageCacheInfo {
        name: cache.info.name.clone(),
        source: cache.info.source,
        scan_identity: cache.info.scan_identity,
        leader_epoch: cache.info.leader_epoch,
        next_cycle: cache.info.next_cycle,
        ..Default::default()
    };
    let bytes = projection(&info, &BTreeMap::new())?;
    Ok((Bytes::from(bytes), cleanup))
}

#[derive(Serialize)]
struct CompatibilityEnvelope<'a, T> {
    #[serde(rename = "pagesV1")]
    header: &'a Header,
    info: &'a DataUsageCacheInfo,
    cache: &'a T,
}

pub(super) async fn reclaim_retired<S: ScannerObjectIO + ScannerConfigObjectDelete>(
    store: Arc<S>,
    name: &str,
    bundles: RetiredBundles,
    epoch: Option<u64>,
) {
    if let Some(epoch) = epoch {
        for bundle in bundles.into_iter().flatten() {
            let _ = reclaim(store.clone(), name, bundle, epoch).await;
        }
    }
}

async fn reclaim<S: ScannerObjectIO + ScannerConfigObjectDelete>(
    store: Arc<S>,
    name: &str,
    bundle: uuid::Uuid,
    epoch: u64,
) -> StorageResult<()> {
    let _admission = scanner_publication_admission_for_epoch(store.clone(), epoch)
        .await
        .ok_or_else(cache_publication_epoch_changed)?;
    let prefix = bundle_prefix(name, bundle);
    let opts = ObjectOptions {
        delete_prefix: true,
        ..Default::default()
    };
    match timeout(
        DataUsageCache::cache_save_timeout(),
        store.delete_config_object(RUSTFS_META_BUCKET, &prefix, opts),
    )
    .await
    {
        Ok(Ok(_)) | Ok(Err(StorageError::FileNotFound | StorageError::ObjectNotFound(..))) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(Error::other("retired scanner checkpoint cleanup timed out")),
    }
}

#[cfg(test)]
mod progress_input_tests {
    use super::*;

    #[tokio::test]
    async fn display_progress_input_reassembles_multilevel_checkpoint_pages() {
        for count in [0, 3, 16_384] {
            let mut cache = DataUsageCache::default();
            cache.info.name = "bucket".into();
            cache.replace("bucket", "", DataUsageEntry::default());
            for index in 0..count {
                let path = format!("bucket/object-{index:05}-{}", "x".repeat(512));
                cache.replace(
                    &path,
                    "bucket",
                    DataUsageEntry {
                        objects: 1,
                        size: 11,
                        ..Default::default()
                    },
                );
            }
            let encoded = encode(&cache).expect("encode checkpoint pages");
            if count == 16_384 {
                assert!(encoded.root.level >= 2, "the fixture must cover multiple manifest levels");
            }
            let chunks = encoded.progress_input().await.expect("borrow immutable serialized chunks");
            assert_eq!(
                chunks.iter().map(Bytes::len).sum::<usize>(),
                usize::try_from(encoded.length()).expect("fixture length")
            );
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            let recovered = DataUsageCache::unmarshal(&bytes).expect("decode the original MessagePack stream");
            let totals = recovered
                .checked_flatten_complete_scope("bucket")
                .expect("connected checkpoint");
            assert_eq!((totals.objects, totals.size), (count, count * 11));
            assert_eq!(recovered.cache.len(), cache.cache.len());
        }
    }
}
