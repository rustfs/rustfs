// Copyright 2026 RustFS Team
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

use crate::disk::{
    RUSTFS_META_BUCKET,
    error::{DiskError, Result},
};
use rustfs_utils::path::path_join_buf;
use uuid::Uuid;

const NATIVE_STREAM_PREFIX: &str = "native-preparations/";
const NATIVE_STREAM_ROOT: &str = "native-preparations";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeStreamFile {
    Part(usize),
    Staging(Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeStreamPath {
    pub(crate) preparation: Uuid,
    pub(crate) data_dir: Uuid,
    pub(crate) file: NativeStreamFile,
}

fn targets_native_stream_namespace(volume: &str, path: &str) -> bool {
    if !volume.contains(RUSTFS_META_BUCKET) && !path.contains(NATIVE_STREAM_ROOT) {
        return false;
    }
    let normalized = path_join_buf(&[volume, path]);
    let prefix = format!("{RUSTFS_META_BUCKET}/{NATIVE_STREAM_PREFIX}");
    normalized == format!("{RUSTFS_META_BUCKET}/{NATIVE_STREAM_ROOT}") || normalized.starts_with(&prefix)
}

/// Recognize only the reserved per-preparation shard path used by the native
/// stream writer. Malformed paths under that namespace must not fall through to
/// the general disk writer.
pub(crate) fn parse_native_stream_path(volume: &str, path: &str) -> Result<Option<NativeStreamPath>> {
    if !targets_native_stream_namespace(volume, path) {
        return Ok(None);
    }
    if volume != RUSTFS_META_BUCKET {
        return Err(DiskError::InvalidPath);
    }
    if !path.starts_with(NATIVE_STREAM_PREFIX) {
        return Err(DiskError::InvalidPath);
    }

    let mut components = path.split('/');
    if components.next() != Some("native-preparations") {
        return Err(DiskError::InvalidPath);
    }
    let preparation = components
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or(DiskError::InvalidPath)?;
    let data_dir = components
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or(DiskError::InvalidPath)?;
    let file_name = components.next().ok_or(DiskError::InvalidPath)?;
    let file = file_name
        .strip_prefix("part.")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .filter(|part| file_name == format!("part.{part}"))
        .map(NativeStreamFile::Part)
        .or_else(|| {
            file_name
                .strip_prefix(".rustfs-put-")
                .and_then(|value| Uuid::parse_str(value).ok())
                .filter(|value| !value.is_nil())
                .filter(|id| file_name == format!(".rustfs-put-{id}"))
                .map(NativeStreamFile::Staging)
        });

    if components.next().is_some()
        || file.is_none()
        || path != format!("native-preparations/{preparation}/{data_dir}/{file_name}")
    {
        return Err(DiskError::InvalidPath);
    }
    Ok(Some(NativeStreamPath {
        preparation,
        data_dir,
        file: file.expect("validated native stream filename"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_stream_path_requires_canonical_identity_and_part() {
        let preparation = Uuid::from_u128(1);
        let data_dir = Uuid::from_u128(2);
        let valid = format!("native-preparations/{preparation}/{data_dir}/part.1");
        assert!(
            parse_native_stream_path(RUSTFS_META_BUCKET, &valid)
                .expect("valid path")
                .is_some()
        );
        let staging = format!("native-preparations/{preparation}/{data_dir}/.rustfs-put-{data_dir}");
        assert!(
            parse_native_stream_path(RUSTFS_META_BUCKET, &staging)
                .expect("valid staging path")
                .is_some()
        );
        assert!(parse_native_stream_path("bucket", &valid).expect("other volume").is_none());
        assert!(
            parse_native_stream_path(RUSTFS_META_BUCKET, "ordinary/object")
                .expect("ordinary path")
                .is_none()
        );
        for invalid in [
            "native-preparations/00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000002/part.1".to_owned(),
            format!("native-preparations/{preparation}/{data_dir}/part.0"),
            format!("native-preparations/{preparation}/{data_dir}/part.01"),
            format!("native-preparations/{preparation}/{data_dir}/.rustfs-put-{}", Uuid::nil()),
            format!("native-preparations/{preparation}/{data_dir}/.rustfs-put-not-a-uuid"),
            format!("native-preparations/{preparation}/{data_dir}/part.1/extra"),
            format!("native-preparations/{preparation}/../part.1"),
        ] {
            assert!(matches!(
                parse_native_stream_path(RUSTFS_META_BUCKET, &invalid),
                Err(DiskError::InvalidPath)
            ));
        }
    }
}
