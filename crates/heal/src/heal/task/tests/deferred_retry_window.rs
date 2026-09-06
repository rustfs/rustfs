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

fn item(name: String, version_id: Option<String>) -> DeferredObject {
    DeferredObject::new(
        HealListItem {
            name,
            version_id,
            mod_time_unix_nanos: None,
            lifecycle_object_info: None,
            is_delete_marker: false,
        },
        1,
    )
}

#[tokio::test(start_paused = true)]
async fn count_cap_and_next_item_preserve_ownership() {
    let mut window = DeferredWindow::default();
    for _ in 0..MAX_DEFERRED_OBJECTS {
        assert!(window.push(item("key".to_string(), None)).is_ok());
    }
    let rejected = window
        .push(item("next".to_string(), Some("version".to_string())))
        .expect_err("count cap");
    assert_eq!(rejected.name, "next");
    assert_eq!(rejected.version_id.as_deref(), Some("version"));
    assert_eq!(window.objects.len(), MAX_DEFERRED_OBJECTS);
    assert!(window.bytes <= MAX_DEFERRED_BYTES);
    assert!(!window.can_advance(1));
    assert!(window.pop_due().is_some());
    assert!(window.push(rejected).is_ok());
}

#[tokio::test(start_paused = true)]
async fn byte_cap_counts_key_version_and_reserved_slots() {
    let mut window = DeferredWindow::default();
    let available = MAX_DEFERRED_BYTES - window.bytes;
    let key = "k".repeat(available / 2);
    let version = "v".repeat(available - key.capacity());
    assert_eq!(key.capacity() + version.capacity(), available);
    assert!(window.push(item(key, Some(version))).is_ok());
    assert_eq!(window.bytes, MAX_DEFERRED_BYTES);
    assert!(!window.can_advance(1));
    assert!(window.push(item("x".to_string(), None)).is_err());
    assert!(window.pop_due().is_some());
    assert_eq!(window.bytes, MAX_DEFERRED_OBJECTS * size_of::<DeferredObject>());
    assert!(window.push(item("x".to_string(), None)).is_ok());
}

#[tokio::test(start_paused = true)]
async fn retry_age_caps_due_time_and_is_not_reset_by_rescheduling() {
    let mut entry = item("a".to_string(), None);
    entry.defer(Duration::from_secs(2));
    let first = entry.first_failure.expect("first failure");
    tokio::time::advance(Duration::from_secs(29)).await;
    entry.defer(Duration::from_secs(8));
    assert_eq!(entry.first_failure, Some(first));
    assert_eq!(entry.due, first + MAX_DEFERRED_AGE);
    assert!(!entry.expired());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(entry.expired());
}
