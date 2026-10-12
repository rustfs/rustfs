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

//! Derives image and video variants for an object that was just written.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustfs_media_processing::plan::{is_conversion, is_useful_variant};
use rustfs_media_processing::{Engine, RenderRequest};

use crate::app::object::{DefaultObjectUsecase, InternalPutContext};
use crate::app::storage_api::object_usecase::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::app::storage_api::object_usecase::{ECStore, StorageObjectOptions as ObjectOptions};

use super::{MediaTask, profile};

/// Carries the provenance every derived object records so operators can trace a
/// rendition back to its source. The dual-key invariant requires both
/// `x-rustfs-internal-*` and `x-minio-internal-*`, which
/// [`metadata_compat::insert_str`] writes together.
const SOURCE_SUFFIX: &str = "media-source";

const PRINCIPAL_ID: &str = "rustfs-media-processing";

/// Runs one task end to end: read, probe, plan, render, store.
///
/// Any failure is logged and swallowed. A rendition that cannot be produced
/// must never turn a successful client write into a client-visible error.
pub(crate) async fn process(store: &Arc<ECStore>, engine: &Engine, task: &MediaTask) -> Result<(), String> {
    let outcome = run(store, engine, task).await;
    if let Err(error) = &outcome {
        tracing::warn!(
            event = "media_processing_failed",
            component = "rustfs",
            subsystem = "media_processing",
            bucket = %task.bucket,
            key = %task.key,
            error = %error,
            "Failed to derive media variants"
        );
    }
    outcome
}

async fn run(store: &Arc<ECStore>, engine: &Engine, task: &MediaTask) -> Result<(), String> {
    let Some(mut profile) = profile::load_profile(store, &task.bucket).await else {
        return Ok(());
    };
    // Cheap gates first: most uploads are not media at all.
    if !profile.accepts(&task.key, task.size) {
        return Ok(());
    }
    // A derived object must never derive its own renditions.
    if is_derived_key(&profile, &task.key) {
        return Ok(());
    }
    profile = profile::repair_profile(engine, &profile).await;

    let scratch = ScratchDir::new(&task.bucket, &task.key)?;
    let source = scratch.path().join(source_file_name(&task.key));
    let usecase = DefaultObjectUsecase::from_global();

    match read_source(store, &task.bucket, &task.key).await {
        Ok(bytes) => {
            if let Err(error) = std::fs::write(&source, &bytes) {
                return Err(format!("staging the source failed: {error}"));
            }
        }
        Err(error) => return Err(format!("reading the source failed: {error}")),
    }

    let info = match engine.probe(&source).await {
        Ok(info) => info,
        Err(error) => {
            tracing::debug!(
                event = "media_processing_probe_skipped",
                component = "rustfs",
                subsystem = "media_processing",
                bucket = %task.bucket,
                key = %task.key,
                error = %error,
                "Object is not decodable media; nothing to derive"
            );
            return Ok(());
        }
    };

    let planned = engine.plan(&info, &profile, &task.key);
    if planned.is_empty() {
        return Ok(());
    }

    let mut stored = 0usize;
    for variant in &planned {
        let output = scratch.path().join(output_file_name(&variant.key));
        // The probe result goes along so the video pipeline can remux a source
        // that already matches instead of paying for a full re-encode.
        let request = RenderRequest {
            kind: &variant.kind,
            info: Some(&info),
        };
        if let Err(error) = engine.render(&source, &output, &request).await {
            tracing::warn!(
                event = "media_processing_variant_failed",
                component = "rustfs",
                subsystem = "media_processing",
                bucket = %task.bucket,
                key = %task.key,
                variant = %variant.key,
                error = %error,
                "Failed to render a media variant"
            );
            continue;
        }
        let bytes = std::fs::read(&output).map_err(|error| format!("reading a rendered variant failed: {error}"))?;

        // A rendition that came out bigger than the source is not a rendition;
        // storing it would cost storage and buy nothing. The exception is a
        // rendition whose entire purpose is to be a different thing from the
        // source — an unplayable clip needs a playable copy even when that copy
        // is larger, which it nearly always is.
        // The comparison is against the *source* object's format, not the
        // rendition's: a JPEG of a PNG is a conversion even though the stored
        // key already ends in `.jpg`.
        let source_extension = extension_of(&task.key);
        let forced = is_conversion(&variant.kind, &info, &source_extension);
        if !is_useful_variant(task.size, bytes.len() as u64, profile.min_variant_ratio, forced) {
            tracing::debug!(
                event = "media_processing_variant_not_useful",
                component = "rustfs",
                subsystem = "media_processing",
                bucket = %task.bucket,
                variant = %variant.key,
                source_bytes = task.size,
                variant_bytes = bytes.len(),
                "Skipping a variant that did not save space"
            );
            continue;
        }

        store_variant(&usecase, &task, &variant.key, &bytes).await?;
        stored += 1;
    }

    if stored > 0 && profile.rule_for(&task.key).is_some_and(|rule| rule.delete_source) {
        tracing::info!(
            event = "media_processing_source_deleted",
            component = "rustfs",
            subsystem = "media_processing",
            bucket = %task.bucket,
            key = %task.key,
            "Removing the source object after its variants were stored"
        );
        (**store)
            .delete_object(&task.bucket, &task.key, ObjectOptions::default())
            .await
            .map_err(|error| format!("deleting the source failed: {error}"))?;
    }

    Ok(())
}

/// True when `key` already lives under the profile's variant prefix.
fn is_derived_key(profile: &rustfs_media_processing::plan::MediaProfile, key: &str) -> bool {
    let prefix = profile.variant_prefix.trim_matches('/');
    !prefix.is_empty() && key.starts_with(&format!("{prefix}/"))
}

async fn store_variant(usecase: &DefaultObjectUsecase, task: &MediaTask, key: &str, bytes: &[u8]) -> Result<(), String> {
    // The body stream must be `'static`, so the bytes are copied once here rather
    // than borrowed out of the caller's buffer.
    let owned = bytes::Bytes::copy_from_slice(bytes);
    let mut internal_metadata = HashMap::new();
    rustfs_utils::http::metadata_compat::insert_str(&mut internal_metadata, SOURCE_SUFFIX, task.key.clone());

    usecase
        .internal_put_object(
            InternalPutContext {
                bucket: task.bucket.clone(),
                expected_bucket_incarnation_id: None,
                key: key.to_owned(),
                size: Some(owned.len() as u64),
                expected_md5_hex: None,
                preserve_etag: None,
                if_absent: false,
                preserve_delete_marker: false,
                content_headers: content_type_for(key),
                user_metadata: HashMap::new(),
                tags: None,
                internal_metadata,
                emit_events: false,
                principal_id: PRINCIPAL_ID,
            },
            futures::stream::once(async move { Ok(owned) }),
        )
        .await
        .map_err(|error| format!("storing a variant failed: {error}"))?;
    Ok(())
}

fn content_type_for(key: &str) -> HashMap<String, String> {
    let mime = match key.rsplit('.').next().unwrap_or_default().to_ascii_lowercase().as_str() {
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        "webp" => "image/webp",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        _ => "application/octet-stream",
    };
    HashMap::from([("Content-Type".to_owned(), mime.to_owned())])
}

async fn read_source(store: &Arc<ECStore>, bucket: &str, key: &str) -> Result<Vec<u8>, String> {
    use tokio::io::AsyncReadExt;

    let mut reader = (**store)
        .get_object_reader(bucket, key, None, http::HeaderMap::new(), &ObjectOptions::default())
        .await
        .map_err(|error| error.to_string())?;

    let mut bytes = Vec::new();
    reader
        .stream
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| format!("draining the source reader failed: {error}"))?;
    Ok(bytes)
}

/// A temporary directory removed when the task finishes, however it finishes.
struct ScratchDir(PathBuf);

impl ScratchDir {
    fn new(bucket: &str, key: &str) -> Result<Self, String> {
        // Keys can be long and contain characters that are illegal in a path,
        // so the directory name is derived rather than taken verbatim.
        let dir = std::env::temp_dir().join(format!("rustfs-media-{}", md5_hex(format!("{bucket}/{key}").as_bytes())));
        std::fs::create_dir_all(&dir).map_err(|error| format!("creating a scratch directory failed: {error}"))?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn source_file_name(key: &str) -> String {
    let name = key.rsplit('/').next().unwrap_or(key);
    let extension = name.rsplit_once('.').map(|(_, extension)| extension.to_owned());
    match extension {
        Some(extension) if extension.chars().all(|character| character.is_ascii_alphanumeric()) => format!("source.{extension}"),
        _ => "source.bin".to_owned(),
    }
}

/// Staging name for a rendered variant.
///
/// The extension is kept so the scratch directory stays readable, but it is
/// not what tells ffmpeg the container: the render passes an explicit `-f`
/// because a name like `output-<hash>` carries nothing ffmpeg can infer from.
fn output_file_name(variant_key: &str) -> String {
    let extension = extension_of(variant_key);
    if extension.is_empty() {
        format!("output-{}", md5_hex(variant_key.as_bytes()))
    } else {
        format!("output-{}.{extension}", md5_hex(variant_key.as_bytes()))
    }
}

fn extension_of(key: &str) -> String {
    key.rsplit('/')
        .next()
        .unwrap_or(key)
        .rsplit_once('.')
        .map_or_else(String::new, |(_, extension)| extension.to_ascii_lowercase())
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::Digest as _;
    let mut hasher = md5::Md5::new();
    hasher.update(bytes);
    hasher.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Only used to keep the dedup set honest about derived keys.
#[allow(dead_code)]
fn derived_keys(profile: &rustfs_media_processing::plan::MediaProfile, keys: &HashSet<String>) -> usize {
    keys.iter().filter(|key| is_derived_key(profile, key)).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_media_processing::plan::MediaProfile;

    #[test]
    fn staged_names_are_derived_so_a_hostile_key_cannot_escape_the_scratch_directory() {
        assert_eq!(source_file_name("a/b/c.mkv"), "source.mkv");
        assert_eq!(source_file_name("a/b/c"), "source.bin");
        // A path traversal attempt in the key must not survive into a filename.
        assert_eq!(source_file_name("../../etc/passwd"), "source.bin");
        assert_eq!(source_file_name("a/b/../x.mp4"), "source.mp4");
        assert!(!output_file_name("../../etc/passwd").contains(".."));
    }

    #[test]
    fn variants_get_a_content_type_clients_can_dispatch_on() {
        assert_eq!(
            content_type_for("variants/a/720p.mp4")
                .get("Content-Type")
                .map(String::as_str),
            Some("video/mp4")
        );
        assert_eq!(
            content_type_for("variants/a/thumb.webp")
                .get("Content-Type")
                .map(String::as_str),
            Some("image/webp")
        );
        assert_eq!(
            content_type_for("variants/a/x.xyz").get("Content-Type").map(String::as_str),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn md5_hex_is_stable_and_short_enough_for_a_path() {
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(md5_hex(b"abc").len(), 32);
    }

    #[test]
    fn variants_are_never_derived_from_variants() {
        let profile = MediaProfile::default();
        assert!(is_derived_key(&profile, "variants/a/b/720p.mp4"));
        assert!(!is_derived_key(&profile, "media/a.mp4"));
    }

    #[test]
    fn a_profile_without_a_prefix_derives_nothing_twice() {
        let profile = MediaProfile {
            variant_prefix: String::new(),
            ..MediaProfile::default()
        };
        assert!(!is_derived_key(&profile, "anything/at/all.mp4"));
    }
}
