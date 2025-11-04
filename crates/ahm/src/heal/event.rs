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

use crate::error::Error;
use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};
use rustfs_ecstore::disk::endpoint::Endpoint;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Corruption type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CorruptionType {
    /// Data corruption
    DataCorruption,
    /// Metadata corruption
    MetadataCorruption,
    /// Partial corruption
    PartialCorruption,
    /// Complete corruption
    CompleteCorruption,
}

/// Severity level
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Severity {
    /// Low severity
    Low = 0,
    /// Medium severity
    Medium = 1,
    /// High severity
    High = 2,
    /// Critical severity
    Critical = 3,
}

/// Heal event
#[derive(Debug, Clone)]
pub enum HealEvent {
    /// Object corruption event
    ObjectCorruption {
        bucket: String,
        object: String,
        version_id: Option<String>,
        corruption_type: CorruptionType,
        severity: Severity,
    },
    /// Object missing event
    ObjectMissing {
        bucket: String,
        object: String,
        version_id: Option<String>,
        expected_locations: Vec<usize>,
        available_locations: Vec<usize>,
    },
    /// Metadata corruption event
    MetadataCorruption {
        bucket: String,
        object: String,
        corruption_type: CorruptionType,
    },
    /// Disk status change event
    DiskStatusChange {
        endpoint: Endpoint,
        old_status: String,
        new_status: String,
    },
    /// EC decode failure event
    ECDecodeFailure {
        bucket: String,
        object: String,
        version_id: Option<String>,
        missing_shards: Vec<usize>,
        available_shards: Vec<usize>,
    },
    /// Checksum mismatch event
    ChecksumMismatch {
        bucket: String,
        object: String,
        version_id: Option<String>,
        expected_checksum: String,
        actual_checksum: String,
    },
    /// Bucket metadata corruption event
    BucketMetadataCorruption {
        bucket: String,
        corruption_type: CorruptionType,
    },
    /// MRF metadata corruption event
    MRFMetadataCorruption {
        meta_path: String,
        corruption_type: CorruptionType,
    },
}

impl HealEvent {
    /// Convert HealEvent to HealRequest
    pub fn to_heal_request(&self) -> HealRequest {
        match self {
            HealEvent::ObjectCorruption {
                bucket,
                object,
                version_id,
                severity,
                ..
            } => HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                Self::severity_to_priority(severity),
            ),
            HealEvent::ObjectMissing {
                bucket,
                object,
                version_id,
                ..
            } => HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            ),
            HealEvent::MetadataCorruption { bucket, object, .. } => HealRequest::new(
                HealType::Metadata {
                    bucket: bucket.clone(),
                    object: object.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            ),
            HealEvent::DiskStatusChange { endpoint, .. } => {
                // Convert disk status change to erasure set heal
                // Note: This requires access to storage to get bucket list, which is not available here
                // The actual bucket list will need to be provided by the caller or retrieved differently
                let set_disk_id = crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx)
                    .unwrap_or_else(|| {
                        panic!(
                            "Invalid pool/set indices for disk status change event: pool={}, set={}",
                            endpoint.pool_idx, endpoint.set_idx
                        );
                    });
                HealRequest::new(
                    HealType::ErasureSet {
                        buckets: vec![], // Empty bucket list - caller should populate this
                        set_disk_id,
                    },
                    HealOptions::default(),
                    HealPriority::High,
                )
            }
            HealEvent::ECDecodeFailure {
                bucket,
                object,
                version_id,
                ..
            } => HealRequest::new(
                HealType::ECDecode {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::Urgent,
            ),
            HealEvent::ChecksumMismatch {
                bucket,
                object,
                version_id,
                ..
            } => HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            ),
            HealEvent::BucketMetadataCorruption { bucket, .. } => {
                HealRequest::new(HealType::Bucket { bucket: bucket.clone() }, HealOptions::default(), HealPriority::High)
            }
            HealEvent::MRFMetadataCorruption { meta_path, .. } => HealRequest::new(
                HealType::MRF {
                    meta_path: meta_path.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            ),
        }
    }

    /// Convert severity to priority
    fn severity_to_priority(severity: &Severity) -> HealPriority {
        match severity {
            Severity::Low => HealPriority::Low,
            Severity::Medium => HealPriority::Normal,
            Severity::High => HealPriority::High,
            Severity::Critical => HealPriority::Urgent,
        }
    }

    /// Get event description
    pub fn description(&self) -> String {
        match self {
            HealEvent::ObjectCorruption {
                bucket,
                object,
                corruption_type,
                ..
            } => {
                format!("Object corruption detected: {bucket}/{object} - {corruption_type:?}")
            }
            HealEvent::ObjectMissing { bucket, object, .. } => {
                format!("Object missing: {bucket}/{object}")
            }
            HealEvent::MetadataCorruption {
                bucket,
                object,
                corruption_type,
                ..
            } => {
                format!("Metadata corruption: {bucket}/{object} - {corruption_type:?}")
            }
            HealEvent::DiskStatusChange {
                endpoint,
                old_status,
                new_status,
                ..
            } => {
                format!("Disk status changed: {endpoint:?} {old_status} -> {new_status}")
            }
            HealEvent::ECDecodeFailure {
                bucket,
                object,
                missing_shards,
                ..
            } => {
                format!("EC decode failure: {bucket}/{object} - missing shards: {missing_shards:?}")
            }
            HealEvent::ChecksumMismatch {
                bucket,
                object,
                expected_checksum,
                actual_checksum,
                ..
            } => {
                format!("Checksum mismatch: {bucket}/{object} - expected: {expected_checksum}, actual: {actual_checksum}")
            }
            HealEvent::BucketMetadataCorruption {
                bucket, corruption_type, ..
            } => {
                format!("Bucket metadata corruption: {bucket} - {corruption_type:?}")
            }
            HealEvent::MRFMetadataCorruption {
                meta_path,
                corruption_type,
                ..
            } => {
                format!("MRF metadata corruption: {meta_path} - {corruption_type:?}")
            }
        }
    }

    /// Get event severity
    pub fn severity(&self) -> Severity {
        match self {
            HealEvent::ObjectCorruption { severity, .. } => severity.clone(),
            HealEvent::ObjectMissing { .. } => Severity::High,
            HealEvent::MetadataCorruption { .. } => Severity::High,
            HealEvent::DiskStatusChange { .. } => Severity::High,
            HealEvent::ECDecodeFailure { .. } => Severity::Critical,
            HealEvent::ChecksumMismatch { .. } => Severity::High,
            HealEvent::BucketMetadataCorruption { .. } => Severity::High,
            HealEvent::MRFMetadataCorruption { .. } => Severity::High,
        }
    }

    /// Get event timestamp
    pub fn timestamp(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Heal event handler
pub struct HealEventHandler {
    /// Event queue
    events: Vec<HealEvent>,
    /// Maximum number of events
    max_events: usize,
}

impl HealEventHandler {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: Vec::new(),
            max_events,
        }
    }

    /// Add event
    pub fn add_event(&mut self, event: HealEvent) {
        if self.events.len() >= self.max_events {
            // Remove oldest event
            self.events.remove(0);
        }
        self.events.push(event);
    }

    /// Get all events
    pub fn get_events(&self) -> &[HealEvent] {
        &self.events
    }

    /// Clear events
    pub fn clear_events(&mut self) {
        self.events.clear();
    }

    /// Get event count
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Filter events by severity
    pub fn filter_by_severity(&self, min_severity: Severity) -> Vec<&HealEvent> {
        self.events.iter().filter(|event| event.severity() >= min_severity).collect()
    }

    /// Filter events by type
    pub fn filter_by_type(&self, event_type: &str) -> Vec<&HealEvent> {
        self.events
            .iter()
            .filter(|event| match event {
                HealEvent::ObjectCorruption { .. } => event_type == "ObjectCorruption",
                HealEvent::ObjectMissing { .. } => event_type == "ObjectMissing",
                HealEvent::MetadataCorruption { .. } => event_type == "MetadataCorruption",
                HealEvent::DiskStatusChange { .. } => event_type == "DiskStatusChange",
                HealEvent::ECDecodeFailure { .. } => event_type == "ECDecodeFailure",
                HealEvent::ChecksumMismatch { .. } => event_type == "ChecksumMismatch",
                HealEvent::BucketMetadataCorruption { .. } => event_type == "BucketMetadataCorruption",
                HealEvent::MRFMetadataCorruption { .. } => event_type == "MRFMetadataCorruption",
            })
            .collect()
    }
}

impl Default for HealEventHandler {
    fn default() -> Self {
        Self::new(1000)
    }
}
