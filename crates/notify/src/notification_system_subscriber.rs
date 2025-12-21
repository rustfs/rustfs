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

use crate::BucketNotificationConfig;
use crate::rules::{BucketRulesSnapshot, DynRulesContainer, SubscriberIndex};
use rustfs_targets::EventName;

/// NotificationSystemSubscriberView - Provides an interface to manage and query
/// the subscription status of buckets in the notification system.
#[derive(Debug)]
pub struct NotificationSystemSubscriberView {
    index: SubscriberIndex,
}

impl NotificationSystemSubscriberView {
    /// Creates a new NotificationSystemSubscriberView with an empty SubscriberIndex.
    ///
    /// Returns a new instance of NotificationSystemSubscriberView.
    pub fn new() -> Self {
        Self {
            index: SubscriberIndex::default(),
        }
    }

    /// Checks if a bucket has any subscribers for a specific event.
    /// This is a quick check using the event mask in the snapshot.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to check.
    /// * `event` - The event name to check for subscriptions.
    ///
    /// Returns `true` if there are subscribers for the event, `false` otherwise.
    #[inline]
    pub fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        self.index.has_subscriber(bucket, event)
    }

    /// Builds and atomically replaces a bucket's subscription snapshot from the configuration.
    ///
    /// Core principle: masks and rules are calculated and stored together in the same update.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket to update.
    /// * `cfg` - The bucket notification configuration to compile into a snapshot.
    pub fn apply_bucket_config(&self, bucket: &str, cfg: &BucketNotificationConfig) {
        // *It is recommended to merge compile into one function to ensure the same origin.
        let snapshot: BucketRulesSnapshot<DynRulesContainer> = cfg.compile_snapshot();

        // *debug to prevent inconsistencies from being introduced when modifying the compile logic in the future.
        snapshot.debug_assert_mask_consistent();

        self.index.store_snapshot(bucket, snapshot);
    }

    /// Clears a bucket's subscription snapshot.
    ///
    /// #Arguments
    /// * `bucket` - The name of the bucket to clear.
    #[inline]
    pub fn clear_bucket(&self, bucket: &str) {
        self.index.clear_bucket(bucket);
    }
}
