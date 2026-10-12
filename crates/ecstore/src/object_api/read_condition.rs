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

use rustfs_filemeta::{FileInfo, ObjectPartInfo};
use std::future::Future;
use std::sync::Arc;
use time::OffsetDateTime;

/// Borrowed fields needed to decide whether an object read needs a body.
pub struct GetObjectReadMetadata<'a> {
    pub bucket: &'a str,
    pub object: &'a str,
    pub etag: Option<&'a str>,
    pub mod_time: Option<OffsetDateTime>,
    pub parts: &'a [ObjectPartInfo],
    pub delete_marker: bool,
}

impl<'a> GetObjectReadMetadata<'a> {
    pub(crate) fn from_file_info(bucket: &'a str, object: &'a str, info: &'a FileInfo) -> Self {
        Self {
            bucket,
            object,
            etag: info.metadata.get("etag").map(String::as_str),
            mod_time: info.mod_time,
            parts: &info.parts,
            delete_marker: info.deleted,
        }
    }
}

/// A pure decision on quorum-validated metadata. Authorization belongs to the caller.
pub trait GetObjectReadCondition: Send + Sync {
    fn is_terminal(&self, metadata: GetObjectReadMetadata<'_>) -> bool;
}

tokio::task_local! {
    static READ_CONDITION: Arc<dyn GetObjectReadCondition>;
}

/// Applies a condition to this reader call, without inheriting it into shared producers.
pub async fn with_get_object_read_condition<F: Future>(condition: Arc<dyn GetObjectReadCondition>, read: F) -> F::Output {
    READ_CONDITION.scope(condition, read).await
}

pub(crate) fn get_object_read_condition_is_terminal(metadata: GetObjectReadMetadata<'_>) -> bool {
    READ_CONDITION
        .try_with(|condition| condition.is_terminal(metadata))
        .unwrap_or(false)
}

pub(crate) fn get_object_read_condition_is_active() -> bool {
    READ_CONDITION.try_with(|_| ()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Stop;
    impl GetObjectReadCondition for Stop {
        fn is_terminal(&self, _: GetObjectReadMetadata<'_>) -> bool {
            true
        }
    }
    fn metadata() -> GetObjectReadMetadata<'static> {
        GetObjectReadMetadata {
            bucket: "bucket",
            object: "object",
            etag: Some("etag"),
            mod_time: None,
            parts: &[],
            delete_marker: false,
        }
    }

    #[tokio::test]
    async fn read_condition_scope_does_not_escape_or_enter_shared_producers() {
        assert!(!get_object_read_condition_is_terminal(metadata()));
        with_get_object_read_condition(Arc::new(Stop), async {
            assert!(get_object_read_condition_is_terminal(metadata()));
            assert!(
                !tokio::spawn(async { get_object_read_condition_is_terminal(metadata()) })
                    .await
                    .expect("shared producer")
            );
        })
        .await;
        assert!(!get_object_read_condition_is_terminal(metadata()));
    }

    #[tokio::test]
    async fn dropping_a_pending_read_removes_its_condition_scope() {
        let mut read = Box::pin(with_get_object_read_condition(Arc::new(Stop), async {
            assert!(get_object_read_condition_is_terminal(metadata()));
            std::future::pending::<()>().await;
        }));
        assert!(futures::poll!(&mut read).is_pending());
        drop(read);
        assert!(!get_object_read_condition_is_terminal(metadata()));
    }
}
