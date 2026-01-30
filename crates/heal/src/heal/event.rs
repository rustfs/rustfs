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

use crate::heal::{HealOptions, HealPriority, HealRequest, HealType};
use crate::{Error, Result};
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
    pub fn to_heal_request(&self) -> Result<HealRequest> {
        match self {
            HealEvent::ObjectCorruption {
                bucket,
                object,
                version_id,
                severity,
                ..
            } => Ok(HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                Self::severity_to_priority(severity),
            )),
            HealEvent::ObjectMissing {
                bucket,
                object,
                version_id,
                ..
            } => Ok(HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
            HealEvent::MetadataCorruption { bucket, object, .. } => Ok(HealRequest::new(
                HealType::Metadata {
                    bucket: bucket.clone(),
                    object: object.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
            HealEvent::DiskStatusChange { endpoint, .. } => {
                // Convert disk status change to erasure set heal
                // Note: This requires access to storage to get bucket list, which is not available here
                // The actual bucket list will need to be provided by the caller or retrieved differently
                let set_disk_id = crate::heal::utils::format_set_disk_id_from_i32(endpoint.pool_idx, endpoint.set_idx)
                    .ok_or_else(|| Error::InvalidHealType {
                        heal_type: format!("erasure-set(pool={}, set={})", endpoint.pool_idx, endpoint.set_idx),
                    })?;
                Ok(HealRequest::new(
                    HealType::ErasureSet {
                        buckets: vec![], // Empty bucket list - caller should populate this
                        set_disk_id,
                    },
                    HealOptions::default(),
                    HealPriority::High,
                ))
            }
            HealEvent::ECDecodeFailure {
                bucket,
                object,
                version_id,
                ..
            } => Ok(HealRequest::new(
                HealType::ECDecode {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::Urgent,
            )),
            HealEvent::ChecksumMismatch {
                bucket,
                object,
                version_id,
                ..
            } => Ok(HealRequest::new(
                HealType::Object {
                    bucket: bucket.clone(),
                    object: object.clone(),
                    version_id: version_id.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
            HealEvent::BucketMetadataCorruption { bucket, .. } => Ok(HealRequest::new(
                HealType::Bucket { bucket: bucket.clone() },
                HealOptions::default(),
                HealPriority::High,
            )),
            HealEvent::MRFMetadataCorruption { meta_path, .. } => Ok(HealRequest::new(
                HealType::MRF {
                    meta_path: meta_path.clone(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::task::{HealPriority, HealType};

    #[test]
    fn test_heal_event_object_corruption_to_request() {
        let event = HealEvent::ObjectCorruption {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::Object { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_object_missing_to_request() {
        let event = HealEvent::ObjectMissing {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: Some("v1".to_string()),
            expected_locations: vec![0, 1],
            available_locations: vec![2, 3],
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::Object { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_metadata_corruption_to_request() {
        let event = HealEvent::MetadataCorruption {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            corruption_type: CorruptionType::MetadataCorruption,
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::Metadata { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_ec_decode_failure_to_request() {
        let event = HealEvent::ECDecodeFailure {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: None,
            missing_shards: vec![0, 1],
            available_shards: vec![2, 3, 4],
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::ECDecode { .. }));
        assert_eq!(request.priority, HealPriority::Urgent);
    }

    #[test]
    fn test_heal_event_checksum_mismatch_to_request() {
        let event = HealEvent::ChecksumMismatch {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: None,
            expected_checksum: "abc123".to_string(),
            actual_checksum: "def456".to_string(),
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::Object { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_bucket_metadata_corruption_to_request() {
        let event = HealEvent::BucketMetadataCorruption {
            bucket: "test-bucket".to_string(),
            corruption_type: CorruptionType::MetadataCorruption,
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::Bucket { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_mrf_metadata_corruption_to_request() {
        let event = HealEvent::MRFMetadataCorruption {
            meta_path: "test-bucket/test-object".to_string(),
            corruption_type: CorruptionType::MetadataCorruption,
        };

        let request = event.to_heal_request().unwrap();
        assert!(matches!(request.heal_type, HealType::MRF { .. }));
        assert_eq!(request.priority, HealPriority::High);
    }

    #[test]
    fn test_heal_event_severity_to_priority() {
        let event_low = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::Low,
        };
        let request = event_low.to_heal_request().unwrap();
        assert_eq!(request.priority, HealPriority::Low);

        let event_medium = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::Medium,
        };
        let request = event_medium.to_heal_request().unwrap();
        assert_eq!(request.priority, HealPriority::Normal);

        let event_high = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };
        let request = event_high.to_heal_request().unwrap();
        assert_eq!(request.priority, HealPriority::High);

        let event_critical = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::Critical,
        };
        let request = event_critical.to_heal_request().unwrap();
        assert_eq!(request.priority, HealPriority::Urgent);
    }

    #[test]
    fn test_heal_event_description() {
        let event = HealEvent::ObjectCorruption {
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        let desc = event.description();
        assert!(desc.contains("Object corruption detected"));
        assert!(desc.contains("test-bucket/test-object"));
        assert!(desc.contains("DataCorruption"));
    }

    #[test]
    fn test_heal_event_severity() {
        let event = HealEvent::ECDecodeFailure {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            missing_shards: vec![],
            available_shards: vec![],
        };
        assert_eq!(event.severity(), Severity::Critical);

        let event = HealEvent::ObjectMissing {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            expected_locations: vec![],
            available_locations: vec![],
        };
        assert_eq!(event.severity(), Severity::High);
    }

    #[test]
    fn test_heal_event_handler_new() {
        let handler = HealEventHandler::new(10);
        assert_eq!(handler.event_count(), 0);
        assert_eq!(handler.max_events, 10);
    }

    #[test]
    fn test_heal_event_handler_default() {
        let handler = HealEventHandler::default();
        assert_eq!(handler.max_events, 1000);
    }

    #[test]
    fn test_heal_event_handler_add_event() {
        let mut handler = HealEventHandler::new(3);
        let event = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        handler.add_event(event.clone());
        assert_eq!(handler.event_count(), 1);

        handler.add_event(event.clone());
        handler.add_event(event.clone());
        assert_eq!(handler.event_count(), 3);
    }

    #[test]
    fn test_heal_event_handler_max_events() {
        let mut handler = HealEventHandler::new(2);
        let event = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        handler.add_event(event.clone());
        handler.add_event(event.clone());
        handler.add_event(event.clone()); // Should remove oldest

        assert_eq!(handler.event_count(), 2);
    }

    #[test]
    fn test_heal_event_handler_get_events() {
        let mut handler = HealEventHandler::new(10);
        let event = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        handler.add_event(event.clone());
        handler.add_event(event.clone());

        let events = handler.get_events();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_heal_event_handler_clear_events() {
        let mut handler = HealEventHandler::new(10);
        let event = HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        };

        handler.add_event(event);
        assert_eq!(handler.event_count(), 1);

        handler.clear_events();
        assert_eq!(handler.event_count(), 0);
    }

    #[test]
    fn test_heal_event_handler_filter_by_severity() {
        let mut handler = HealEventHandler::new(10);
        handler.add_event(HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::Low,
        });
        handler.add_event(HealEvent::ECDecodeFailure {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            missing_shards: vec![],
            available_shards: vec![],
        });

        let high_severity = handler.filter_by_severity(Severity::High);
        assert_eq!(high_severity.len(), 1); // Only ECDecodeFailure is Critical >= High
    }

    #[test]
    fn test_heal_event_handler_filter_by_type() {
        let mut handler = HealEventHandler::new(10);
        handler.add_event(HealEvent::ObjectCorruption {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            corruption_type: CorruptionType::DataCorruption,
            severity: Severity::High,
        });
        handler.add_event(HealEvent::ObjectMissing {
            bucket: "test".to_string(),
            object: "test".to_string(),
            version_id: None,
            expected_locations: vec![],
            available_locations: vec![],
        });

        let corruption_events = handler.filter_by_type("ObjectCorruption");
        assert_eq!(corruption_events.len(), 1);

        let missing_events = handler.filter_by_type("ObjectMissing");
        assert_eq!(missing_events.len(), 1);
    }
}
