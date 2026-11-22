// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

// A losing writer can stage blocks without publishing its accounting. Even
// tiny or identical successful checkpoints periodically retire these orphans.
const MAX_BUNDLE_COMMITS: u16 = 256;

pub(super) fn block_bytes<'a>(blocks: impl Iterator<Item = &'a Bytes>) -> u64 {
    blocks.fold(0u64, |sum, bytes| sum.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX)))
}

pub(super) async fn reuse<'a, S: ScannerObjectIO>(
    store: Arc<S>,
    name: &str,
    head: &'a Manifest,
    candidate: &Archive,
    live: u64,
) -> StorageResult<Option<(&'a Manifest, HashSet<Digest>, u64)>> {
    // Legacy manifests did not account for unreachable blocks in this bundle.
    if head.bundle_bytes == 0 || head.bundle_commits == 0 || head.bundle_commits >= MAX_BUNDLE_COMMITS {
        return Ok(None);
    }
    let known = match committed_keys(store, name, head, candidate).await {
        Ok(known) => known,
        Err(StorageError::FileNotFound | StorageError::ObjectNotFound(..)) => return Ok(None),
        Err(error) if unreadable_page(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    // Count reachable allocation once at bundle creation and newly staged
    // bytes thereafter. A small delta stays incremental even across cycles.
    let added = block_bytes(
        candidate
            .blocks
            .iter()
            .filter_map(|(key, bytes)| (!known.contains(key)).then_some(bytes)),
    );
    let total = head.bundle_bytes.checked_add(added);
    Ok(total
        .filter(|&total| total <= live.saturating_mul(2))
        .map(|total| (head, known, total)))
}

/// Returns the conditional write necessary to restore this content-addressed
/// backup page, or None when the bytes in storage already match the candidate.
async fn backup_revision<S: ScannerObjectIO>(
    store: &S,
    path: &str,
    expected: &Bytes,
) -> StorageResult<Option<DataUsageCacheRevision>> {
    let mut reader = match store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            path,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(reader) => reader,
        Err(StorageError::FileNotFound | StorageError::ObjectNotFound(..)) => return Ok(Some(DataUsageCacheRevision::Missing)),
        Err(error) => return Err(error),
    };
    if usize::try_from(reader.object_info.size).ok() == Some(expected.len()) {
        let mut bytes = Vec::with_capacity(expected.len());
        (&mut reader).take(PAGE_MAX as u64 + 1).read_to_end(&mut bytes).await?;
        if bytes == expected.as_ref() {
            return Ok(None);
        }
    }
    let etag = reader
        .object_info
        .etag
        .ok_or_else(|| invalid("damaged scanner backup page has no revision"))?;
    Ok(Some(DataUsageCacheRevision::Etag(etag)))
}

pub(super) async fn stage<S: ScannerObjectIO + ScannerConfigObjectDelete>(
    store: Arc<S>,
    name: &str,
    revision: &DataUsageCacheRevision,
    epoch: u64,
    manifest: &Manifest,
    archive: &Archive,
    known: &HashSet<Digest>,
) -> StorageResult<()> {
    let verify_backup = name.ends_with(".bkp");
    let staging = staging::Staging::open(&store, name, manifest.bundle, epoch, revision);
    let mut writes = Vec::with_capacity(IO_CONCURRENCY);
    for (key, bytes) in &archive.blocks {
        let staged = staging.contains(key);
        if !verify_backup && (known.contains(key) || staged) {
            continue;
        }
        let path = block_path(name, manifest.bundle, key);
        let store = store.clone();
        let bytes = bytes.clone();
        let key = *key;
        let receipts = &staging;
        writes.push(async move {
            let needed = if verify_backup && (manifest.bundle_commits > 1 || staged) {
                // A healthy primary load does not establish the integrity of its
                // independent backup. Check reused bytes and repair only damaged pages.
                match timeout(DataUsageCache::cache_save_timeout(), backup_revision(store.as_ref(), &path, &bytes))
                    .await
                    .map_err(|_| Error::other("scanner backup page validation timed out"))??
                {
                    Some(revision) => revision,
                    None => return Ok(()),
                }
            } else {
                DataUsageCacheRevision::Missing
            };
            DataUsageCache::save_path_with_retry(
                store,
                &path,
                &bytes,
                DataUsageCache::cache_save_timeout(),
                DATA_USAGE_CACHE_SAVE_RETRIES,
                Some(needed),
                Some(epoch),
            )
            .await?;
            receipts.record(key);
            Ok::<(), Error>(())
        });
        if writes.len() == IO_CONCURRENCY {
            try_join_all(std::mem::take(&mut writes)).await?;
        }
    }
    try_join_all(writes).await?;
    Ok(())
}
