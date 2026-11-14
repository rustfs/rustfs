// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Read-only upload ownership evidence for completion callers, not publication authority.
//! ECStore supplies validated records; completion still rechecks its locked storage state.

use crate::storage_api_contracts::multipart::MultipartInfo;

/// A storage observation of the namespace belonging to an upload.
///
/// This does not reserve an upload or authorize publication. Abort, completion,
/// bucket recreation, and replacement can invalidate it before completion runs.
#[derive(Clone)]
pub struct MultipartCompletionBinding {
    bucket: String,
    object: String,
    active_info: Option<MultipartInfo>,
}

impl MultipartCompletionBinding {
    pub(crate) fn active(info: MultipartInfo) -> Self {
        Self {
            bucket: info.bucket.clone(),
            object: info.object.clone(),
            active_info: Some(info),
        }
    }

    pub(crate) fn recorded(bucket: &str, object: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            object: rustfs_utils::path::decode_dir_object(object),
            active_info: None,
        }
    }

    /// The bucket identified by the storage observation.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The decoded object key identified by the storage observation.
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Metadata from an ordinary active lookup, when that lookup succeeded.
    /// Completed and retained records never supply active preflight metadata.
    pub fn active_info(&self) -> Option<&MultipartInfo> {
        self.active_info.as_ref()
    }
}

impl std::fmt::Debug for MultipartCompletionBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MultipartCompletionBinding")
            .field("bucket", &self.bucket)
            .field("object", &self.object)
            .field("has_active_info", &self.active_info.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_binding_debug_excludes_preflight_metadata() {
        let binding = MultipartCompletionBinding::active(MultipartInfo {
            user_defined: [("test-marker".to_owned(), "private-preflight-sentinel".to_owned())].into(),
            ..Default::default()
        });
        let debug = format!("{binding:?}");
        assert!(!debug.contains("private-preflight-sentinel"));
        assert!(debug.contains("has_active_info: true"));
    }
}
