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
    check_format_erasure_values, formats_match_reference_slots, read_legacy_format_bytes, retain_format_quorum_members,
    write_format_file,
};
use crate::disk::{DiskAPI, DiskStore, RUSTFS_META_BUCKET, error::DiskError, format::FormatV3};
use crate::error::{Error, Result};
use crate::layout::format::legacy_format_max_bytes;
use crate::runtime::instance::InstanceContext;
use futures::{StreamExt, future::join_all, stream};
use std::sync::Arc;
use uuid::Uuid;

use crate::disk::FORMAT_BOOTSTRAP_DONE_FILE;
pub(super) use crate::disk::FORMAT_BOOTSTRAP_FILE;
const FORMAT_BOOTSTRAP_READ_CONCURRENCY: usize = 16;

pub(super) fn project_format_slots(reference: &FormatV3) -> Vec<Option<FormatV3>> {
    reference
        .erasure
        .sets
        .iter()
        .flatten()
        .map(|disk_id| {
            let mut format = reference.clone();
            format.erasure.this = *disk_id;
            Some(format)
        })
        .collect()
}

/// Establishes restartable creator authority before any format is published.
/// The first slot durably selects the plan before fanout. A retry completes
/// that immutable plan while every configured disk remains unformatted.
pub(super) async fn write_format_bootstrap_intents(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> Result<()> {
    if disks.is_empty() || disks.len() != formats.len() || formats.iter().any(Option::is_none) {
        return Err(Error::CorruptedFormat);
    }
    if disks.iter().any(Option::is_none) {
        return Err(DiskError::DiskNotFound.into());
    }
    let first = disks[0].as_ref().ok_or(DiskError::DiskNotFound)?;
    let anchor = formats[0].as_ref().ok_or(DiskError::CorruptedFormat)?;
    let set_drive_count = anchor.erasure.sets.first().map_or(0, Vec::len);
    check_format_erasure_values(formats, set_drive_count)?;
    if !formats_match_reference_slots(formats, anchor, 0) {
        return Err(Error::CorruptedFormat);
    }
    let max_bytes = legacy_format_max_bytes(disks.len())?;
    match first.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await {
        Ok(reader) => {
            let data = read_legacy_format_bytes(reader, max_bytes).await?;
            let existing = FormatV3::try_from(data.as_slice())?;
            if existing.shared_identity() != anchor.shared_identity() || existing.erasure.this != anchor.erasure.this {
                return Err(Error::CorruptedFormat);
            }
        }
        Err(DiskError::FileNotFound) => {
            // A missing anchor cannot authorize replacing another slot's plan.
            // This check is confined to fresh creation, so healthy restarts
            // retain the single-anchor absence fast path.
            let reads = stream::iter(disks.iter().skip(1).cloned().map(|disk| async move {
                let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
                match disk.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await {
                    Err(DiskError::FileNotFound) => Ok(()),
                    Ok(_) => Err(DiskError::CorruptedFormat),
                    Err(error) => Err(error),
                }
            }))
            .buffer_unordered(FORMAT_BOOTSTRAP_READ_CONCURRENCY);
            tokio::pin!(reads);
            while let Some(result) = reads.next().await {
                result?;
            }
        }
        Err(error) => return Err(error.into()),
    }
    first
        .write_format_control(FORMAT_BOOTSTRAP_FILE, anchor.to_json()?.into_bytes().into())
        .await?;
    let writes = disks.iter().zip(formats).skip(1).map(|(disk, format)| async move {
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

/// Completes an anchored plan before its first format publication, or restores
/// explicitly unformatted slots from a complete durable intent set. Partial
/// intent sets grant no authority once any format has been published.
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
    if first_intent.erasure.sets.len() != set_count
        || first_intent.erasure.sets.iter().any(|set| set.len() != set_drive_count)
        || deployment_id.is_some_and(|id| id != first_intent.id)
    {
        return Ok(None);
    }
    let projected = project_format_slots(&first_intent);
    if check_format_erasure_values(&projected, set_drive_count).is_err()
        || !formats_match_reference_slots(&[Some(first_intent.clone())], &first_intent, 0)
    {
        return Ok(None);
    }
    let all_unformatted =
        formats.iter().all(Option::is_none) && errors.iter().all(|error| matches!(error, Some(DiskError::UnformattedDisk)));
    let intents = {
        let reads = stream::iter(disks.iter().cloned().enumerate().skip(1).map(|(index, disk)| async move {
            let disk = disk.as_ref().ok_or(DiskError::DiskNotFound)?;
            let reader = match disk.read_file(RUSTFS_META_BUCKET, FORMAT_BOOTSTRAP_FILE).await {
                Ok(reader) => reader,
                Err(DiskError::FileNotFound) => return Ok((index, None)),
                Err(error) => return Err(error),
            };
            let data = read_legacy_format_bytes(reader, max_bytes).await?;
            let format = FormatV3::try_from(data.as_slice())?;
            Ok::<_, DiskError>((index, Some(format)))
        }))
        .buffer_unordered(FORMAT_BOOTSTRAP_READ_CONCURRENCY);
        tokio::pin!(reads);
        let mut intents = vec![None; disks.len()];
        intents[0] = Some(first_intent);
        while let Some(result) = reads.next().await {
            let Ok((index, format)) = result else {
                return Ok(None);
            };
            intents[index] = format;
        }
        intents
    };
    let Some(reference) = intents.first().and_then(Option::as_ref) else {
        return Ok(None);
    };
    if !formats_match_reference_slots(&intents, reference, 0) {
        return Err(Error::CorruptedFormat);
    }
    if intents.iter().any(Option::is_none) && !all_unformatted {
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
    if all_unformatted {
        // Re-acknowledge every slot under native immutable-plan admission.
        // Merely observing a visible, unacknowledged intent supplies no barrier.
        write_format_bootstrap_intents(disks, &projected).await?;
    }

    // Every intent and existing format has been checked before the first
    // mutation. A retry always publishes the same deployment and slot IDs.
    let writes = disks
        .iter()
        .zip(&projected)
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
