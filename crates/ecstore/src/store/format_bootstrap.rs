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

use super::init_format::{
    check_format_erasure_values, formats_match_reference_slots, legacy_format_max_bytes, read_legacy_format_bytes,
    retain_format_quorum_members, write_format_file,
};
use crate::disk::{DiskAPI, DiskStore, RUSTFS_META_BUCKET, error::DiskError, format::FormatV3};
use crate::error::{Error, Result};
use crate::runtime::instance::InstanceContext;
use futures::{StreamExt, future::join_all, stream};
use std::sync::Arc;
use uuid::Uuid;

use crate::disk::FORMAT_BOOTSTRAP_DONE_FILE;
pub(super) use crate::disk::FORMAT_BOOTSTRAP_FILE;
const FORMAT_BOOTSTRAP_READ_CONCURRENCY: usize = 16;

/// Establishes restartable creator authority before any format is published.
/// A failed or cancelled attempt may leave a partial set of intents, which
/// grants no authority to resume a partially formatted pool.
pub(super) async fn write_format_bootstrap_intents(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> Result<()> {
    if disks.is_empty() || disks.len() != formats.len() {
        return Err(Error::CorruptedFormat);
    }
    let writes = disks.iter().zip(formats).map(|(disk, format)| async move {
        let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
        let format = format.as_ref().ok_or(DiskError::CorruptedFormat)?;
        disk.write_format_control(FORMAT_BOOTSTRAP_FILE, format.to_json()?.into_bytes().into())
            .await
    });
    for result in join_all(writes).await {
        result?;
    }
    Ok(())
}

/// Restores only explicitly unformatted slots from a complete durable intent.
/// Missing, unreadable, or invalid intents withhold creator authority without
/// interfering with ordinary startup from committed pool metadata.
pub(super) async fn resume_format_bootstrap(
    instance_ctx: &Arc<InstanceContext>,
    disks: &mut [Option<DiskStore>],
    formats: &[Option<FormatV3>],
    errors: &[Option<DiskError>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<Option<FormatV3>> {
    if disks.is_empty()
        || disks.len() != formats.len()
        || disks.len() != errors.len()
        || set_count.checked_mul(set_drive_count) != Some(disks.len())
    {
        return Ok(None);
    }

    let max_bytes = legacy_format_max_bytes(disks.len())?;
    let Some(first) = disks.first().and_then(Option::as_ref) else {
        return Ok(None);
    };
    let first_intent = async {
        let reader = first.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await?;
        let data = read_legacy_format_bytes(reader, max_bytes).await?;
        FormatV3::try_from(data.as_slice()).map_err(DiskError::from)
    }
    .await;
    let Ok(first_intent) = first_intent else {
        return Ok(None);
    };
    let intents = {
        let reads = stream::iter(disks.iter().cloned().enumerate().skip(1).map(|(index, disk)| async move {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let reader = disk.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await?;
            let data = read_legacy_format_bytes(reader, max_bytes).await?;
            let format = FormatV3::try_from(data.as_slice())?;
            Ok::<_, DiskError>((index, format))
        }))
        .buffer_unordered(FORMAT_BOOTSTRAP_READ_CONCURRENCY);
        tokio::pin!(reads);
        let mut intents = vec![None; disks.len()];
        intents[0] = Some(first_intent);
        while let Some(result) = reads.next().await {
            let Ok((index, format)) = result else {
                return Ok(None);
            };
            intents[index] = Some(format);
        }
        intents
    };
    let Some(reference) = intents.first().and_then(Option::as_ref) else {
        return Ok(None);
    };
    if reference.erasure.sets.len() != set_count
        || deployment_id.is_some_and(|id| id != reference.id)
        || check_format_erasure_values(&intents, set_drive_count).is_err()
        || !formats_match_reference_slots(&intents, reference, 0)
    {
        return Ok(None);
    }

    if !formats_match_reference_slots(formats, reference, 0) {
        return Err(Error::CorruptedFormat);
    }
    if formats.iter().zip(errors).any(|(format, error)| {
        if format.is_some() {
            error.is_some()
        } else {
            !matches!(error, Some(DiskError::UnformattedDisk))
        }
    }) {
        return Ok(None);
    }

    // Every intent and existing format has been checked before the first
    // mutation. A retry always publishes the same deployment and slot IDs.
    let writes = disks
        .iter()
        .zip(&intents)
        .zip(formats)
        .filter_map(|((disk, intent), existing)| {
            if existing.is_some() {
                return None;
            }
            Some(write_format_file(disk.as_ref()?, intent.as_ref()?))
        });
    for result in join_all(writes).await {
        result?;
    }
    let mut format = reference.clone();
    format.erasure.this = Uuid::nil();
    format.disk_info = None;
    let members = vec![true; disks.len()];
    retain_format_quorum_members(instance_ctx, disks, &format, &members, None, set_drive_count).await?;
    Ok(Some(format))
}

/// Consumes this pool's creator authority before initialized identity publication.
/// Replay requires every intent, so one durably absent slot is sufficient. Each
/// attempt finishes before another begins, including its directory-sync barrier.
pub(super) async fn retire_format_bootstrap_authority(disks: &[Option<DiskStore>]) -> Result<()> {
    let first = disks.first().and_then(Option::as_ref).into_iter();
    let local = disks.iter().skip(1).flatten().filter(|disk| disk.is_local());
    let remote = disks.iter().skip(1).flatten().filter(|disk| !disk.is_local());
    let mut failure = DiskError::DiskNotFound;
    for disk in first.chain(local).chain(remote) {
        match disk
            .rename_file_durable(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE, RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_DONE_FILE)
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => failure = error,
        }
    }
    Err(failure.into())
}
