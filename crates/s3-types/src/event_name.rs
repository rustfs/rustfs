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

use std::fmt;

/// Error returned when parsing event name string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEventNameError(String);

impl fmt::Display for ParseEventNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid event name:{}", self.0)
    }
}

impl std::error::Error for ParseEventNameError {}

/// Represents the type of event that occurs on the object.
/// Based on AWS S3 event type and includes RustFS extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EventName {
    // Single event type (values are sequential for compatible mask logic)
    ObjectAccessedGet = 1,
    ObjectAccessedGetRetention = 2,
    ObjectAccessedGetLegalHold = 3,
    ObjectAccessedHead = 4,
    ObjectAccessedAttributes = 5,
    ObjectCreatedCompleteMultipartUpload = 6,
    ObjectCreatedCopy = 7,
    ObjectCreatedPost = 8,
    ObjectCreatedPut = 9,
    ObjectCreatedPutRetention = 10,
    ObjectCreatedPutLegalHold = 11,
    ObjectTaggingPut = 12,
    ObjectTaggingDelete = 13,
    ObjectRemovedDelete = 14,
    ObjectRemovedDeleteMarkerCreated = 15,
    ObjectRemovedDeleteAllVersions = 16,
    ObjectRemovedNoOP = 17,
    BucketCreated = 18,
    BucketRemoved = 19,
    ObjectReplicationFailed = 20,
    ObjectReplicationComplete = 21,
    ObjectReplicationMissedThreshold = 22,
    ObjectReplicationReplicatedAfterThreshold = 23,
    ObjectReplicationNotTracked = 24,
    ObjectRestorePost = 25,
    ObjectRestoreCompleted = 26,
    ObjectTransitionFailed = 27,
    ObjectTransitionComplete = 28,
    ScannerManyVersions = 29,                // ObjectManyVersions corresponding to Go
    ScannerLargeVersions = 30,               // ObjectLargeVersions corresponding to Go
    ScannerBigPrefix = 31,                   // PrefixManyFolders corresponding to Go
    LifecycleDelMarkerExpirationDelete = 32, // ILMDelMarkerExpirationDelete corresponding to Go
    ObjectAclPut = 33,
    LifecycleExpirationDelete = 34,
    LifecycleExpirationDeleteMarkerCreated = 35,
    LifecycleTransition = 36,
    IntelligentTiering = 37,

    // Compound "All" event type (no sequential value for mask)
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTaggingAll,
    LifecycleExpirationAll,
    ObjectTransitionAll,
    ObjectScannerAll, // New, from Go
    #[default]
    Everything, // New, from Go

    // Internal events for metrics (not exposed to S3 notifications)
    ObjectRemovedAbortMultipartUpload,
    ObjectCreatedCreateMultipartUpload,
    ObjectRemovedDeleteObjects,

    // KMS management-plane events, covering both key operations and the
    // service-control endpoints that change which backend holds the keys. They
    // travel to the audit sink only and are never produced by the bucket
    // notification path, so no compound `s3:` event expands to them. New
    // variants must keep being appended here: the discriminant of every
    // preceding variant is a `mask()` bit position, and inserting in the middle
    // would silently renumber existing bits.
    KmsKeyCreated,
    KmsKeyRotated,
    KmsKeyEnabled,
    KmsKeyDisabled,
    KmsKeyDeletionScheduled,
    KmsKeyDeletionCancelled,
    KmsKeyDeleted,
    KmsKeyAccessed,
    KmsServiceConfigured,
    KmsServiceStarted,
    KmsServiceStopped,
}

// Single event type sequential array for Everything.expand()
const SINGLE_EVENT_NAMES_IN_ORDER: [EventName; 32] = [
    EventName::ObjectAccessedGet,
    EventName::ObjectAccessedGetRetention,
    EventName::ObjectAccessedGetLegalHold,
    EventName::ObjectAccessedHead,
    EventName::ObjectAccessedAttributes,
    EventName::ObjectCreatedCompleteMultipartUpload,
    EventName::ObjectCreatedCopy,
    EventName::ObjectCreatedPost,
    EventName::ObjectCreatedPut,
    EventName::ObjectCreatedPutRetention,
    EventName::ObjectCreatedPutLegalHold,
    EventName::ObjectTaggingPut,
    EventName::ObjectTaggingDelete,
    EventName::ObjectRemovedDelete,
    EventName::ObjectRemovedDeleteMarkerCreated,
    EventName::ObjectRemovedDeleteAllVersions,
    EventName::ObjectRemovedNoOP,
    EventName::BucketCreated,
    EventName::BucketRemoved,
    EventName::ObjectReplicationFailed,
    EventName::ObjectReplicationComplete,
    EventName::ObjectReplicationMissedThreshold,
    EventName::ObjectReplicationReplicatedAfterThreshold,
    EventName::ObjectReplicationNotTracked,
    EventName::ObjectRestorePost,
    EventName::ObjectRestoreCompleted,
    EventName::ObjectTransitionFailed,
    EventName::ObjectTransitionComplete,
    EventName::ScannerManyVersions,
    EventName::ScannerLargeVersions,
    EventName::ScannerBigPrefix,
    EventName::LifecycleDelMarkerExpirationDelete,
];

const SINGLE_AWS_AND_EXTENSION_EVENTS_AFTER_COMPAT: [EventName; 5] = [
    EventName::ObjectAclPut,
    EventName::LifecycleExpirationDelete,
    EventName::LifecycleExpirationDeleteMarkerCreated,
    EventName::LifecycleTransition,
    EventName::IntelligentTiering,
];

const LAST_SINGLE_TYPE_VALUE: u32 = EventName::IntelligentTiering as u32;

/// The discriminant of the last `EventName` variant in declaration order.
///
/// Anchoring this on the final variant is what makes the budget assertion below
/// meaningful: `mask()` turns a leaf variant's discriminant `v` into the bit
/// `1 << (v - 1)`, so the highest discriminant is also the highest bit index in
/// use. Keep this pointing at whatever variant is declared last.
const LAST_EVENT_NAME_VALUE: u32 = EventName::KmsServiceStopped as u32;

/// `mask()` returns a `u64`, so discriminants may run from 1 to 64 inclusive.
///
/// Past that, `1u64 << (v - 1)` shifts by 64 or more: a debug build panics with
/// "attempt to shift left with overflow", and a release build silently masks the
/// shift amount down and hands back a bit that already belongs to another event.
/// Either way the failure is far away from the line that added the variant, so
/// fail the build right here instead.
///
/// To widen the budget, change `mask()`'s return type to a wider integer or a
/// bitset and update every caller that stores or compares an event mask.
const _: () = assert!(
    LAST_EVENT_NAME_VALUE <= u64::BITS,
    "EventName has outgrown the 64-bit mask budget: the last variant's discriminant exceeds 64, \
     so EventName::mask() can no longer give every event its own bit. Widen the mask type \
     (see the note on EventName::mask) instead of adding more variants."
);

impl EventName {
    /// The parsed string is EventName.
    pub fn parse(s: &str) -> Result<Self, ParseEventNameError> {
        match s {
            "s3:BucketCreated:*" => Ok(EventName::BucketCreated),
            "s3:BucketRemoved:*" => Ok(EventName::BucketRemoved),
            "s3:ObjectAccessed:*" => Ok(EventName::ObjectAccessedAll),
            "s3:ObjectAccessed:Get" => Ok(EventName::ObjectAccessedGet),
            "s3:ObjectAccessed:GetRetention" => Ok(EventName::ObjectAccessedGetRetention),
            "s3:ObjectAccessed:GetLegalHold" => Ok(EventName::ObjectAccessedGetLegalHold),
            "s3:ObjectAccessed:Head" => Ok(EventName::ObjectAccessedHead),
            "s3:ObjectAccessed:Attributes" => Ok(EventName::ObjectAccessedAttributes),
            "s3:ObjectCreated:*" => Ok(EventName::ObjectCreatedAll),
            "s3:ObjectCreated:CompleteMultipartUpload" => Ok(EventName::ObjectCreatedCompleteMultipartUpload),
            "s3:ObjectCreated:Copy" => Ok(EventName::ObjectCreatedCopy),
            "s3:ObjectCreated:Post" => Ok(EventName::ObjectCreatedPost),
            "s3:ObjectCreated:Put" => Ok(EventName::ObjectCreatedPut),
            "s3:ObjectCreated:PutRetention" => Ok(EventName::ObjectCreatedPutRetention),
            "s3:ObjectCreated:PutLegalHold" => Ok(EventName::ObjectCreatedPutLegalHold),
            "s3:ObjectCreated:PutTagging" => Ok(EventName::ObjectTaggingPut),
            "s3:ObjectCreated:DeleteTagging" => Ok(EventName::ObjectTaggingDelete),
            "s3:ObjectTagging:*" => Ok(EventName::ObjectTaggingAll),
            "s3:ObjectTagging:Put" => Ok(EventName::ObjectTaggingPut),
            "s3:ObjectTagging:Delete" => Ok(EventName::ObjectTaggingDelete),
            "s3:ObjectAcl:Put" => Ok(EventName::ObjectAclPut),
            "s3:ObjectRemoved:*" => Ok(EventName::ObjectRemovedAll),
            "s3:ObjectRemoved:Delete" => Ok(EventName::ObjectRemovedDelete),
            "s3:ObjectRemoved:DeleteMarkerCreated" => Ok(EventName::ObjectRemovedDeleteMarkerCreated),
            "s3:ObjectRemoved:NoOP" => Ok(EventName::ObjectRemovedNoOP),
            "s3:ObjectRemoved:DeleteAllVersions" => Ok(EventName::ObjectRemovedDeleteAllVersions),
            "s3:LifecycleDelMarkerExpiration:Delete" => Ok(EventName::LifecycleDelMarkerExpirationDelete),
            "s3:LifecycleExpiration:*" => Ok(EventName::LifecycleExpirationAll),
            "s3:LifecycleExpiration:Delete" => Ok(EventName::LifecycleExpirationDelete),
            "s3:LifecycleExpiration:DeleteMarkerCreated" => Ok(EventName::LifecycleExpirationDeleteMarkerCreated),
            "s3:Replication:*" => Ok(EventName::ObjectReplicationAll),
            "s3:Replication:OperationFailedReplication" => Ok(EventName::ObjectReplicationFailed),
            "s3:Replication:OperationCompletedReplication" => Ok(EventName::ObjectReplicationComplete),
            "s3:Replication:OperationMissedThreshold" => Ok(EventName::ObjectReplicationMissedThreshold),
            "s3:Replication:OperationReplicatedAfterThreshold" => Ok(EventName::ObjectReplicationReplicatedAfterThreshold),
            "s3:Replication:OperationNotTracked" => Ok(EventName::ObjectReplicationNotTracked),
            "s3:ObjectRestore:*" => Ok(EventName::ObjectRestoreAll),
            "s3:ObjectRestore:Post" => Ok(EventName::ObjectRestorePost),
            "s3:ObjectRestore:Completed" => Ok(EventName::ObjectRestoreCompleted),
            "s3:ObjectTransition:Failed" => Ok(EventName::ObjectTransitionFailed),
            "s3:ObjectTransition:Complete" => Ok(EventName::ObjectTransitionComplete),
            "s3:ObjectTransition:*" => Ok(EventName::ObjectTransitionAll),
            "s3:LifecycleTransition" => Ok(EventName::LifecycleTransition),
            "s3:IntelligentTiering" => Ok(EventName::IntelligentTiering),
            "s3:Scanner:ManyVersions" => Ok(EventName::ScannerManyVersions),
            "s3:Scanner:LargeVersions" => Ok(EventName::ScannerLargeVersions),
            "s3:Scanner:BigPrefix" => Ok(EventName::ScannerBigPrefix),
            "s3:Scanner:*" => Ok(EventName::ObjectScannerAll),
            // KMS events use their own namespace so a `s3:` wildcard in a bucket
            // notification config can never select them. They still round-trip
            // because audit entries are persisted and replayed by the store targets.
            "kms:Key:Created" => Ok(EventName::KmsKeyCreated),
            "kms:Key:Rotated" => Ok(EventName::KmsKeyRotated),
            "kms:Key:Enabled" => Ok(EventName::KmsKeyEnabled),
            "kms:Key:Disabled" => Ok(EventName::KmsKeyDisabled),
            "kms:Key:DeletionScheduled" => Ok(EventName::KmsKeyDeletionScheduled),
            "kms:Key:DeletionCancelled" => Ok(EventName::KmsKeyDeletionCancelled),
            "kms:Key:Deleted" => Ok(EventName::KmsKeyDeleted),
            "kms:Key:Accessed" => Ok(EventName::KmsKeyAccessed),
            "kms:Service:Configured" => Ok(EventName::KmsServiceConfigured),
            "kms:Service:Started" => Ok(EventName::KmsServiceStarted),
            "kms:Service:Stopped" => Ok(EventName::KmsServiceStopped),
            // `Everything` has no string representation (`as_str` yields ""), so it
            // cannot be parsed back from a string. Every other variant round-trips.
            _ => Err(ParseEventNameError(s.to_string())),
        }
    }

    /// Parses an event string into an EventName with explicit error handling.
    #[inline]
    pub fn try_from_event_str(s: &str) -> Result<Self, ParseEventNameError> {
        Self::parse(s)
    }

    /// Returns a string representation of the event type.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventName::BucketCreated => "s3:BucketCreated:*",
            EventName::BucketRemoved => "s3:BucketRemoved:*",
            EventName::ObjectAccessedAll => "s3:ObjectAccessed:*",
            EventName::ObjectAccessedGet => "s3:ObjectAccessed:Get",
            EventName::ObjectAccessedGetRetention => "s3:ObjectAccessed:GetRetention",
            EventName::ObjectAccessedGetLegalHold => "s3:ObjectAccessed:GetLegalHold",
            EventName::ObjectAccessedHead => "s3:ObjectAccessed:Head",
            EventName::ObjectAccessedAttributes => "s3:ObjectAccessed:Attributes",
            EventName::ObjectCreatedAll => "s3:ObjectCreated:*",
            EventName::ObjectCreatedCompleteMultipartUpload => "s3:ObjectCreated:CompleteMultipartUpload",
            EventName::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            EventName::ObjectCreatedPost => "s3:ObjectCreated:Post",
            EventName::ObjectCreatedPut => "s3:ObjectCreated:Put",
            EventName::ObjectCreatedPutRetention => "s3:ObjectCreated:PutRetention",
            EventName::ObjectCreatedPutLegalHold => "s3:ObjectCreated:PutLegalHold",
            EventName::ObjectTaggingAll => "s3:ObjectTagging:*",
            EventName::ObjectTaggingPut => "s3:ObjectTagging:Put",
            EventName::ObjectTaggingDelete => "s3:ObjectTagging:Delete",
            EventName::ObjectAclPut => "s3:ObjectAcl:Put",
            EventName::ObjectRemovedAll => "s3:ObjectRemoved:*",
            EventName::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventName::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            EventName::ObjectRemovedNoOP => "s3:ObjectRemoved:NoOP",
            EventName::ObjectRemovedDeleteAllVersions => "s3:ObjectRemoved:DeleteAllVersions",
            EventName::LifecycleDelMarkerExpirationDelete => "s3:LifecycleDelMarkerExpiration:Delete",
            EventName::LifecycleExpirationAll => "s3:LifecycleExpiration:*",
            EventName::LifecycleExpirationDelete => "s3:LifecycleExpiration:Delete",
            EventName::LifecycleExpirationDeleteMarkerCreated => "s3:LifecycleExpiration:DeleteMarkerCreated",
            EventName::ObjectReplicationAll => "s3:Replication:*",
            EventName::ObjectReplicationFailed => "s3:Replication:OperationFailedReplication",
            EventName::ObjectReplicationComplete => "s3:Replication:OperationCompletedReplication",
            EventName::ObjectReplicationNotTracked => "s3:Replication:OperationNotTracked",
            EventName::ObjectReplicationMissedThreshold => "s3:Replication:OperationMissedThreshold",
            EventName::ObjectReplicationReplicatedAfterThreshold => "s3:Replication:OperationReplicatedAfterThreshold",
            EventName::ObjectRestoreAll => "s3:ObjectRestore:*",
            EventName::ObjectRestorePost => "s3:ObjectRestore:Post",
            EventName::ObjectRestoreCompleted => "s3:ObjectRestore:Completed",
            EventName::ObjectTransitionAll => "s3:ObjectTransition:*",
            EventName::ObjectTransitionFailed => "s3:ObjectTransition:Failed",
            EventName::ObjectTransitionComplete => "s3:ObjectTransition:Complete",
            EventName::LifecycleTransition => "s3:LifecycleTransition",
            EventName::IntelligentTiering => "s3:IntelligentTiering",
            EventName::ScannerManyVersions => "s3:Scanner:ManyVersions",
            EventName::ScannerLargeVersions => "s3:Scanner:LargeVersions",
            EventName::ScannerBigPrefix => "s3:Scanner:BigPrefix",
            EventName::ObjectScannerAll => "s3:Scanner:*", // round-trips via `parse`
            EventName::Everything => "",                   // no string form; cannot be parsed back
            EventName::ObjectRemovedAbortMultipartUpload => "s3:ObjectRemoved:AbortMultipartUpload",
            EventName::ObjectCreatedCreateMultipartUpload => "s3:ObjectCreated:CreateMultipartUpload",
            EventName::ObjectRemovedDeleteObjects => "s3:ObjectRemoved:DeleteObjects",
            EventName::KmsKeyCreated => "kms:Key:Created",
            EventName::KmsKeyRotated => "kms:Key:Rotated",
            EventName::KmsKeyEnabled => "kms:Key:Enabled",
            EventName::KmsKeyDisabled => "kms:Key:Disabled",
            EventName::KmsKeyDeletionScheduled => "kms:Key:DeletionScheduled",
            EventName::KmsKeyDeletionCancelled => "kms:Key:DeletionCancelled",
            EventName::KmsKeyDeleted => "kms:Key:Deleted",
            EventName::KmsKeyAccessed => "kms:Key:Accessed",
            EventName::KmsServiceConfigured => "kms:Service:Configured",
            EventName::KmsServiceStarted => "kms:Service:Started",
            EventName::KmsServiceStopped => "kms:Service:Stopped",
        }
    }

    /// Returns the extended value of the abbreviation event type.
    pub fn expand(&self) -> Vec<Self> {
        match self {
            EventName::ObjectAccessedAll => vec![
                EventName::ObjectAccessedGet,
                EventName::ObjectAccessedHead,
                EventName::ObjectAccessedGetRetention,
                EventName::ObjectAccessedGetLegalHold,
                EventName::ObjectAccessedAttributes,
            ],
            EventName::ObjectCreatedAll => vec![
                EventName::ObjectCreatedCompleteMultipartUpload,
                EventName::ObjectCreatedCopy,
                EventName::ObjectCreatedPost,
                EventName::ObjectCreatedPut,
            ],
            EventName::ObjectTaggingAll => vec![EventName::ObjectTaggingPut, EventName::ObjectTaggingDelete],
            EventName::ObjectRemovedAll => vec![
                EventName::ObjectRemovedDelete,
                EventName::ObjectRemovedDeleteMarkerCreated,
                EventName::ObjectRemovedNoOP,
            ],
            EventName::ObjectReplicationAll => vec![
                EventName::ObjectReplicationFailed,
                EventName::ObjectReplicationComplete,
                EventName::ObjectReplicationNotTracked,
                EventName::ObjectReplicationMissedThreshold,
                EventName::ObjectReplicationReplicatedAfterThreshold,
            ],
            EventName::ObjectRestoreAll => vec![EventName::ObjectRestorePost, EventName::ObjectRestoreCompleted],
            EventName::LifecycleExpirationAll => vec![
                EventName::LifecycleExpirationDelete,
                EventName::LifecycleExpirationDeleteMarkerCreated,
            ],
            EventName::ObjectTransitionAll => vec![
                EventName::ObjectTransitionFailed,
                EventName::ObjectTransitionComplete,
                EventName::LifecycleTransition,
            ],
            EventName::ObjectScannerAll => vec![
                // New
                EventName::ScannerManyVersions,
                EventName::ScannerLargeVersions,
                EventName::ScannerBigPrefix,
            ],
            EventName::Everything => {
                // New
                let mut all = SINGLE_EVENT_NAMES_IN_ORDER.to_vec();
                all.extend(SINGLE_AWS_AND_EXTENSION_EVENTS_AFTER_COMPAT);
                all
            }
            // A single type returns to itself directly
            _ => vec![*self],
        }
    }

    /// Returns the mask of type.
    /// The compound "All" type will be expanded.
    ///
    /// # Bit budget
    ///
    /// A leaf event's mask is `1 << (discriminant - 1)`, so the enum can hold at
    /// most **64 variants** in total — compound "All" variants included, because
    /// they consume discriminants even though they own no bit of their own and
    /// so push every later leaf further up the range.
    ///
    /// The last variant is currently `KmsServiceStopped` at discriminant
    /// `LAST_EVENT_NAME_VALUE`, leaving `64 - LAST_EVENT_NAME_VALUE` free slots.
    /// Before adding an event, check that number; if the budget is gone, widen
    /// the mask beyond `u64` rather than reusing a bit.
    ///
    /// Three guards back this up: the `const` assertion next to
    /// `LAST_EVENT_NAME_VALUE` fails the build once that discriminant passes 64;
    /// `test_event_name_discriminants_are_dense_and_fully_listed` fails if the
    /// anchor is left stale or a variant goes unlisted; and
    /// `test_every_event_mask_is_nonzero_and_leaf_bits_are_unique` fails on the
    /// bit collision a wrapped shift produces in release builds.
    pub fn mask(&self) -> u64 {
        let value = *self as u32;
        if value > 0 && value <= LAST_SINGLE_TYPE_VALUE {
            // It's a single type in the sequential range: one dedicated bit.
            return 1u64 << (value - 1);
        }

        // Everything past the sequential range is either a compound "All" type
        // or an internal leaf event. Compound types expand into their component
        // single types; internal leaf events (e.g. multipart upload
        // create/abort, batch delete) are placed after the compound range and
        // expand to themselves. Recursing on a self-expanding leaf would loop
        // forever (backlog#965), so give each such leaf its own dedicated bit
        // derived from its discriminant. These bits sit above the single-type
        // bits, so they never collide with each other or with any "All" mask.
        let expanded = self.expand();
        if matches!(expanded.as_slice(), [only] if *only == *self) {
            return 1u64 << (value - 1);
        }

        // It's a compound type: OR together its component masks.
        let mut mask = 0u64;
        for n in expanded {
            mask |= n.mask();
        }
        mask
    }

    /// Returns `true` for every object-removal event variant.
    ///
    /// Covers all `ObjectRemoved*` leaf events, including the internal
    /// (metrics-only) ones, so callers can categorize removals without
    /// enumerating each variant by hand.
    #[inline]
    pub fn is_removed(&self) -> bool {
        matches!(
            self,
            EventName::ObjectRemovedDelete
                | EventName::ObjectRemovedDeleteMarkerCreated
                | EventName::ObjectRemovedDeleteAllVersions
                | EventName::ObjectRemovedNoOP
                | EventName::ObjectRemovedAbortMultipartUpload
                | EventName::ObjectRemovedDeleteObjects
        )
    }

    /// Returns `true` for KMS management-plane events.
    ///
    /// These are audit-only: they are never emitted through the bucket
    /// notification pipeline, and no `s3:` event selector expands to them.
    #[inline]
    pub fn is_kms(&self) -> bool {
        matches!(
            self,
            EventName::KmsKeyCreated
                | EventName::KmsKeyRotated
                | EventName::KmsKeyEnabled
                | EventName::KmsKeyDisabled
                | EventName::KmsKeyDeletionScheduled
                | EventName::KmsKeyDeletionCancelled
                | EventName::KmsKeyDeleted
                | EventName::KmsKeyAccessed
                | EventName::KmsServiceConfigured
                | EventName::KmsServiceStarted
                | EventName::KmsServiceStopped
        )
    }
}

/// Returns the S3 notification event schema version for a given event.
#[inline]
pub fn event_schema_version(event_name: EventName) -> &'static str {
    match event_name {
        EventName::ObjectReplicationFailed
        | EventName::ObjectReplicationComplete
        | EventName::ObjectReplicationMissedThreshold
        | EventName::ObjectReplicationReplicatedAfterThreshold
        | EventName::ObjectReplicationNotTracked => "2.2",
        EventName::ObjectRestoreCompleted
        | EventName::ObjectAclPut
        | EventName::ObjectTaggingPut
        | EventName::ObjectTaggingDelete
        | EventName::LifecycleExpirationDelete
        | EventName::LifecycleExpirationDeleteMarkerCreated
        | EventName::LifecycleTransition
        | EventName::IntelligentTiering => "2.3",
        _ => "2.1",
    }
}

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Convert to `EventName` according to string
impl From<&str> for EventName {
    fn from(event_str: &str) -> Self {
        EventName::parse(event_str).unwrap_or_else(|e| panic!("{}", e))
    }
}

impl serde::ser::Serialize for EventName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::de::Deserialize<'de> for EventName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let s = Self::parse(&s).map_err(serde::de::Error::custom)?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // test serialization
    #[test]
    fn test_event_name_serialization_and_deserialization() {
        struct TestCase {
            event: EventName,
            serialized_str: &'static str,
        }

        let test_cases = vec![
            TestCase {
                event: EventName::BucketCreated,
                serialized_str: "\"s3:BucketCreated:*\"",
            },
            TestCase {
                event: EventName::ObjectCreatedAll,
                serialized_str: "\"s3:ObjectCreated:*\"",
            },
            TestCase {
                event: EventName::ObjectCreatedPut,
                serialized_str: "\"s3:ObjectCreated:Put\"",
            },
            TestCase {
                event: EventName::ObjectTaggingPut,
                serialized_str: "\"s3:ObjectTagging:Put\"",
            },
        ];

        for case in &test_cases {
            let serialized = serde_json::to_string(&case.event);
            assert!(serialized.is_ok(), "Serialization failed for `{}`", case.serialized_str);
            assert_eq!(serialized.unwrap(), case.serialized_str);

            let deserialized = serde_json::from_str::<EventName>(case.serialized_str);
            assert!(deserialized.is_ok(), "Deserialization failed for `{}`", case.serialized_str);
            assert_eq!(deserialized.unwrap(), case.event);
        }
    }

    #[test]
    fn test_invalid_event_name_deserialization() {
        let invalid_str = "\"s3:InvalidEvent:Test\"";
        let deserialized = serde_json::from_str::<EventName>(invalid_str);
        assert!(deserialized.is_err(), "Deserialization should fail for invalid event name");

        // Serializing EventName::Everything produces an empty string, but deserializing an empty string should fail.
        let event_name = EventName::Everything;
        let serialized_str = "\"\"";
        let serialized = serde_json::to_string(&event_name);
        assert!(serialized.is_ok(), "Serialization failed for `{serialized_str}`");
        assert_eq!(serialized.unwrap(), serialized_str);

        let deserialized = serde_json::from_str::<EventName>(serialized_str);
        assert!(deserialized.is_err(), "Deserialization should fail for empty string");
    }

    #[test]
    fn test_event_name_aliases_parse_to_aws_compatible_variants() {
        assert_eq!(EventName::parse("s3:ObjectCreated:PutTagging").unwrap(), EventName::ObjectTaggingPut);
        assert_eq!(
            EventName::parse("s3:ObjectCreated:DeleteTagging").unwrap(),
            EventName::ObjectTaggingDelete
        );
        assert_eq!(
            EventName::parse("s3:ObjectTransition:Complete").unwrap(),
            EventName::ObjectTransitionComplete
        );
        assert_eq!(
            EventName::parse("s3:LifecycleDelMarkerExpiration:Delete").unwrap(),
            EventName::LifecycleDelMarkerExpirationDelete
        );
    }

    #[test]
    fn test_object_created_all_expansion_matches_aws_scope() {
        let expanded = EventName::ObjectCreatedAll.expand();
        assert_eq!(
            expanded,
            vec![
                EventName::ObjectCreatedCompleteMultipartUpload,
                EventName::ObjectCreatedCopy,
                EventName::ObjectCreatedPost,
                EventName::ObjectCreatedPut,
            ]
        );
    }

    #[test]
    fn test_event_schema_version_mapping() {
        assert_eq!(event_schema_version(EventName::ObjectCreatedPut), "2.1");
        assert_eq!(event_schema_version(EventName::ObjectReplicationFailed), "2.2");
        assert_eq!(event_schema_version(EventName::LifecycleTransition), "2.3");
    }

    #[test]
    fn test_try_from_event_str_matches_parse() {
        let parsed = EventName::try_from_event_str("s3:ObjectCreated:Put").unwrap();
        assert_eq!(parsed, EventName::ObjectCreatedPut);
        assert!(EventName::try_from_event_str("s3:Invalid").is_err());
    }

    /// Compile-time tripwire for `ALL_EVENT_NAMES`.
    ///
    /// The match below is exhaustive on purpose and does nothing at runtime.
    /// Adding a variant to `EventName` makes it non-exhaustive, so the build
    /// stops here rather than silently leaving the new variant out of
    /// `ALL_EVENT_NAMES` — which would let every mask test below pass while the
    /// new event goes unchecked. When the compiler stops you here:
    ///
    /// 1. Re-read the bit budget documented on `EventName::mask`; the enum has
    ///    room for 64 variants total and only a few slots are still free.
    /// 2. Move `LAST_EVENT_NAME_VALUE` if the new variant is now the last one.
    /// 3. Add the variant to `ALL_EVENT_NAMES`, and to `KMS_EVENT_NAMES` if it
    ///    is a KMS management-plane event.
    fn assert_every_variant_is_listed(ev: EventName) {
        match ev {
            EventName::ObjectAccessedGet
            | EventName::ObjectAccessedGetRetention
            | EventName::ObjectAccessedGetLegalHold
            | EventName::ObjectAccessedHead
            | EventName::ObjectAccessedAttributes
            | EventName::ObjectCreatedCompleteMultipartUpload
            | EventName::ObjectCreatedCopy
            | EventName::ObjectCreatedPost
            | EventName::ObjectCreatedPut
            | EventName::ObjectCreatedPutRetention
            | EventName::ObjectCreatedPutLegalHold
            | EventName::ObjectTaggingPut
            | EventName::ObjectTaggingDelete
            | EventName::ObjectRemovedDelete
            | EventName::ObjectRemovedDeleteMarkerCreated
            | EventName::ObjectRemovedDeleteAllVersions
            | EventName::ObjectRemovedNoOP
            | EventName::BucketCreated
            | EventName::BucketRemoved
            | EventName::ObjectReplicationFailed
            | EventName::ObjectReplicationComplete
            | EventName::ObjectReplicationMissedThreshold
            | EventName::ObjectReplicationReplicatedAfterThreshold
            | EventName::ObjectReplicationNotTracked
            | EventName::ObjectRestorePost
            | EventName::ObjectRestoreCompleted
            | EventName::ObjectTransitionFailed
            | EventName::ObjectTransitionComplete
            | EventName::ScannerManyVersions
            | EventName::ScannerLargeVersions
            | EventName::ScannerBigPrefix
            | EventName::LifecycleDelMarkerExpirationDelete
            | EventName::ObjectAclPut
            | EventName::LifecycleExpirationDelete
            | EventName::LifecycleExpirationDeleteMarkerCreated
            | EventName::LifecycleTransition
            | EventName::IntelligentTiering
            | EventName::ObjectAccessedAll
            | EventName::ObjectCreatedAll
            | EventName::ObjectRemovedAll
            | EventName::ObjectReplicationAll
            | EventName::ObjectRestoreAll
            | EventName::ObjectTaggingAll
            | EventName::LifecycleExpirationAll
            | EventName::ObjectTransitionAll
            | EventName::ObjectScannerAll
            | EventName::Everything
            | EventName::ObjectRemovedAbortMultipartUpload
            | EventName::ObjectCreatedCreateMultipartUpload
            | EventName::ObjectRemovedDeleteObjects
            | EventName::KmsKeyCreated
            | EventName::KmsKeyRotated
            | EventName::KmsKeyEnabled
            | EventName::KmsKeyDisabled
            | EventName::KmsKeyDeletionScheduled
            | EventName::KmsKeyDeletionCancelled
            | EventName::KmsKeyDeleted
            | EventName::KmsKeyAccessed
            | EventName::KmsServiceConfigured
            | EventName::KmsServiceStarted
            | EventName::KmsServiceStopped => {}
        }
    }

    /// Every `EventName` variant in declaration order. Kept exhaustive so the
    /// `mask()` regressions below cover single, compound, and internal events.
    const ALL_EVENT_NAMES: &[EventName] = &[
        EventName::ObjectAccessedGet,
        EventName::ObjectAccessedGetRetention,
        EventName::ObjectAccessedGetLegalHold,
        EventName::ObjectAccessedHead,
        EventName::ObjectAccessedAttributes,
        EventName::ObjectCreatedCompleteMultipartUpload,
        EventName::ObjectCreatedCopy,
        EventName::ObjectCreatedPost,
        EventName::ObjectCreatedPut,
        EventName::ObjectCreatedPutRetention,
        EventName::ObjectCreatedPutLegalHold,
        EventName::ObjectTaggingPut,
        EventName::ObjectTaggingDelete,
        EventName::ObjectRemovedDelete,
        EventName::ObjectRemovedDeleteMarkerCreated,
        EventName::ObjectRemovedDeleteAllVersions,
        EventName::ObjectRemovedNoOP,
        EventName::BucketCreated,
        EventName::BucketRemoved,
        EventName::ObjectReplicationFailed,
        EventName::ObjectReplicationComplete,
        EventName::ObjectReplicationMissedThreshold,
        EventName::ObjectReplicationReplicatedAfterThreshold,
        EventName::ObjectReplicationNotTracked,
        EventName::ObjectRestorePost,
        EventName::ObjectRestoreCompleted,
        EventName::ObjectTransitionFailed,
        EventName::ObjectTransitionComplete,
        EventName::ScannerManyVersions,
        EventName::ScannerLargeVersions,
        EventName::ScannerBigPrefix,
        EventName::LifecycleDelMarkerExpirationDelete,
        EventName::ObjectAclPut,
        EventName::LifecycleExpirationDelete,
        EventName::LifecycleExpirationDeleteMarkerCreated,
        EventName::LifecycleTransition,
        EventName::IntelligentTiering,
        EventName::ObjectAccessedAll,
        EventName::ObjectCreatedAll,
        EventName::ObjectRemovedAll,
        EventName::ObjectReplicationAll,
        EventName::ObjectRestoreAll,
        EventName::ObjectTaggingAll,
        EventName::LifecycleExpirationAll,
        EventName::ObjectTransitionAll,
        EventName::ObjectScannerAll,
        EventName::Everything,
        EventName::ObjectRemovedAbortMultipartUpload,
        EventName::ObjectCreatedCreateMultipartUpload,
        EventName::ObjectRemovedDeleteObjects,
        EventName::KmsKeyCreated,
        EventName::KmsKeyRotated,
        EventName::KmsKeyEnabled,
        EventName::KmsKeyDisabled,
        EventName::KmsKeyDeletionScheduled,
        EventName::KmsKeyDeletionCancelled,
        EventName::KmsKeyDeleted,
        EventName::KmsKeyAccessed,
        EventName::KmsServiceConfigured,
        EventName::KmsServiceStarted,
        EventName::KmsServiceStopped,
    ];

    /// Every KMS management-plane event.
    const KMS_EVENT_NAMES: &[EventName] = &[
        EventName::KmsKeyCreated,
        EventName::KmsKeyRotated,
        EventName::KmsKeyEnabled,
        EventName::KmsKeyDisabled,
        EventName::KmsKeyDeletionScheduled,
        EventName::KmsKeyDeletionCancelled,
        EventName::KmsKeyDeleted,
        EventName::KmsKeyAccessed,
        EventName::KmsServiceConfigured,
        EventName::KmsServiceStarted,
        EventName::KmsServiceStopped,
    ];

    /// `LAST_EVENT_NAME_VALUE` must really be the highest discriminant, and
    /// `ALL_EVENT_NAMES` must really cover every variant.
    ///
    /// The `const` assertion in the parent module only bounds whatever
    /// `LAST_EVENT_NAME_VALUE` points at, so a stale anchor would let the enum
    /// grow past 64 unnoticed. Discriminants are dense (1..=N, no gaps), so
    /// checking that `ALL_EVENT_NAMES` holds each value in `1..=LAST` exactly
    /// once pins both facts at the same time.
    #[test]
    fn test_event_name_discriminants_are_dense_and_fully_listed() {
        assert_every_variant_is_listed(EventName::Everything);

        let mut seen = vec![false; LAST_EVENT_NAME_VALUE as usize + 1];
        for ev in ALL_EVENT_NAMES {
            let value = *ev as u32;
            assert!(
                (1..=LAST_EVENT_NAME_VALUE).contains(&value),
                "{ev:?} has discriminant {value}, outside 1..={LAST_EVENT_NAME_VALUE}; \
                 LAST_EVENT_NAME_VALUE must point at the last variant in declaration order"
            );
            assert!(!seen[value as usize], "discriminant {value} listed twice in ALL_EVENT_NAMES");
            seen[value as usize] = true;
        }

        for (value, present) in seen.iter().enumerate().skip(1) {
            assert!(*present, "discriminant {value} is missing from ALL_EVENT_NAMES");
        }
        assert_eq!(
            ALL_EVENT_NAMES.len(),
            LAST_EVENT_NAME_VALUE as usize,
            "ALL_EVENT_NAMES must list every variant exactly once"
        );
    }

    /// The mask space must not be exhausted: every variant needs a non-zero
    /// mask, and every leaf must own a bit no other leaf uses.
    ///
    /// Once a discriminant passes 64, `1 << (v - 1)` wraps in release builds and
    /// hands back a bit that already belongs to an earlier event, so this fails
    /// on the collision even in the release profile where the shift does not
    /// panic.
    #[test]
    fn test_every_event_mask_is_nonzero_and_leaf_bits_are_unique() {
        let mut leaf_bits = 0u64;
        for ev in ALL_EVENT_NAMES {
            let mask = ev.mask();
            assert_ne!(mask, 0, "{ev:?} has an empty mask — the mask bit budget is exhausted");

            // A leaf is exactly what `mask()` treats as one: it expands to itself.
            let expanded = ev.expand();
            if matches!(expanded.as_slice(), [only] if only == ev) {
                assert_eq!(mask.count_ones(), 1, "leaf {ev:?} must own exactly one bit, got mask {mask:#x}");
                assert_eq!(
                    leaf_bits & mask,
                    0,
                    "leaf {ev:?} reuses a bit already taken by another event (mask {mask:#x}) — \
                     the 64-bit mask budget has been exceeded"
                );
                leaf_bits |= mask;
            }
        }
    }

    /// The highest bit in use must still land inside the `u64`.
    ///
    /// `test_mask_bit_budget_is_not_exhausted` below bounds every listed
    /// discriminant; this one pins the top of the range specifically, so the
    /// variant most likely to fall off the edge is asserted by name rather than
    /// only as one element of an array someone may forget to extend.
    #[test]
    fn test_last_variant_still_gets_its_own_bit() {
        let last = EventName::KmsServiceStopped;
        assert_ne!(last.mask(), 0, "the last variant's mask overflowed to zero");
        assert_eq!(
            last.mask(),
            1u64 << (LAST_EVENT_NAME_VALUE - 1),
            "the last variant must own the top bit of the range"
        );
        assert_eq!(last.mask().count_ones(), 1, "the last variant is a leaf and must own exactly one bit");
    }

    /// Regression for backlog#965: `mask()` used to recurse forever for the
    /// three internal leaf events, overflowing the stack. Every variant must
    /// now return a finite, non-panicking mask.
    #[test]
    fn test_mask_never_recurses_for_any_variant() {
        // Terminating is the point — a regression here overflows the stack rather
        // than failing an assertion — but the masks are collected and checked so
        // the loop cannot be optimised into nothing and so a variant that starts
        // returning an empty mask is caught too (rustfs/backlog#1836).
        let masks: Vec<u64> = ALL_EVENT_NAMES.iter().map(|ev| ev.mask()).collect();

        assert_eq!(masks.len(), ALL_EVENT_NAMES.len());
        for (ev, mask) in ALL_EVENT_NAMES.iter().zip(&masks) {
            assert_ne!(*mask, 0, "{ev:?} must carry at least one bit");
            assert_eq!(ev.mask(), *mask, "{ev:?} must return the same mask every call");
        }
    }

    /// The three internal events (backlog#965) must each carry a non-zero mask
    /// that collides neither with each other nor with any S3-facing bit.
    #[test]
    fn test_internal_event_masks_are_nonzero_and_distinct() {
        let internal = [
            EventName::ObjectRemovedAbortMultipartUpload,
            EventName::ObjectCreatedCreateMultipartUpload,
            EventName::ObjectRemovedDeleteObjects,
        ];
        let everything = EventName::Everything.mask();

        let mut seen = 0u64;
        for ev in internal {
            let m = ev.mask();
            assert_ne!(m, 0, "internal event {ev} must have a non-zero mask");
            assert_eq!(seen & m, 0, "internal event {ev} mask overlaps another internal event");
            assert_eq!(everything & m, 0, "internal event {ev} mask collides with a single-type bit");
            seen |= m;
        }
    }

    /// Every S3-notification variant must round-trip through `as_str` ->
    /// `parse`. Regression for the missing `ObjectScannerAll` parse arm
    /// ("s3:Scanner:*").
    ///
    /// Two groups are deliberately excluded:
    /// - `Everything` has no string form (`as_str` yields "").
    /// - The internal metrics-only events are not exposed to S3 notifications
    ///   and intentionally have no `parse` arm.
    #[test]
    fn test_as_str_parse_round_trip_for_notification_variants() {
        // Internal, metrics-only events: serialized but never parsed back.
        let internal = [
            EventName::ObjectRemovedAbortMultipartUpload,
            EventName::ObjectCreatedCreateMultipartUpload,
            EventName::ObjectRemovedDeleteObjects,
        ];

        for ev in ALL_EVENT_NAMES {
            if *ev == EventName::Everything {
                // `Everything` intentionally serializes to "" and cannot be parsed back.
                assert_eq!(ev.as_str(), "");
                assert!(EventName::parse(ev.as_str()).is_err());
                continue;
            }
            if internal.contains(ev) {
                continue;
            }
            let parsed = EventName::parse(ev.as_str());
            assert_eq!(parsed.as_ref(), Ok(ev), "round-trip failed for {ev} (as_str = {:?})", ev.as_str());
        }
    }

    /// `ObjectScannerAll` specifically must round-trip via "s3:Scanner:*".
    #[test]
    fn test_object_scanner_all_round_trips() {
        assert_eq!(EventName::ObjectScannerAll.as_str(), "s3:Scanner:*");
        assert_eq!(EventName::parse("s3:Scanner:*").unwrap(), EventName::ObjectScannerAll);
    }

    #[test]
    fn test_object_removed_all_includes_noop_extension() {
        let expanded = EventName::ObjectRemovedAll.expand();

        assert!(expanded.contains(&EventName::ObjectRemovedDelete));
        assert!(expanded.contains(&EventName::ObjectRemovedDeleteMarkerCreated));
        assert!(expanded.contains(&EventName::ObjectRemovedNoOP));
    }

    /// `is_removed` must be true for every `ObjectRemoved*` variant and false
    /// for everything else.
    #[test]
    fn test_is_removed_covers_all_object_removed_variants() {
        let removed = [
            EventName::ObjectRemovedDelete,
            EventName::ObjectRemovedDeleteMarkerCreated,
            EventName::ObjectRemovedDeleteAllVersions,
            EventName::ObjectRemovedNoOP,
            EventName::ObjectRemovedAbortMultipartUpload,
            EventName::ObjectRemovedDeleteObjects,
        ];
        for ev in removed {
            assert!(ev.is_removed(), "{ev} should be classified as a removal event");
        }

        let not_removed = [
            EventName::ObjectCreatedPut,
            EventName::ObjectCreatedPost,
            EventName::ObjectCreatedAll,
            EventName::ObjectTaggingPut,
            EventName::ObjectTaggingDelete,
            EventName::ObjectAclPut,
            EventName::ObjectAccessedGet,
        ];
        for ev in not_removed {
            assert!(!ev.is_removed(), "{ev} should not be classified as a removal event");
        }
    }

    /// KMS events are audit-plane only. No `s3:` selector — including the
    /// catch-all `Everything` and every compound "All" type — may share a bit
    /// with them, otherwise a bucket notification rule would silently start
    /// matching KMS activity.
    #[test]
    fn test_kms_event_masks_are_disjoint_from_every_s3_selector() {
        let s3_selectors: Vec<EventName> = ALL_EVENT_NAMES.iter().copied().filter(|ev| !ev.is_kms()).collect();

        let mut seen = 0u64;
        for kms in KMS_EVENT_NAMES {
            let mask = kms.mask();
            assert_ne!(mask, 0, "KMS event {kms} must have a non-zero mask");
            assert_eq!(seen & mask, 0, "KMS event {kms} mask overlaps another KMS event");
            seen |= mask;

            for s3 in &s3_selectors {
                assert_eq!(s3.mask() & mask, 0, "KMS event {kms} mask collides with S3 selector {s3}");
            }
        }
    }

    /// KMS event names must live in their own namespace so that neither a
    /// `s3:` prefix filter nor an `s3:...:*` wildcard can select them.
    #[test]
    fn test_kms_event_names_are_outside_the_s3_namespace() {
        for ev in KMS_EVENT_NAMES {
            assert!(ev.is_kms(), "{ev} should be classified as a KMS event");
            assert!(ev.as_str().starts_with("kms:"), "unexpected KMS event name {:?}", ev.as_str());
            assert_eq!(EventName::parse(ev.as_str()).as_ref(), Ok(ev), "KMS event {ev} must round-trip");
            assert_eq!(ev.expand(), vec![*ev], "KMS event {ev} must expand to itself only");
        }

        for ev in ALL_EVENT_NAMES.iter().filter(|ev| !ev.is_kms()) {
            assert!(!ev.as_str().starts_with("kms:"), "{ev} must not claim the KMS namespace");
        }
    }

    /// `mask()` shifts by `discriminant - 1`, so the enum may hold at most 64
    /// mask-bearing variants. Appending past that overflows the shift instead
    /// of failing loudly, so keep the budget check next to the variants.
    #[test]
    fn test_mask_bit_budget_is_not_exhausted() {
        for ev in ALL_EVENT_NAMES {
            let value = *ev as u32;
            assert!(value <= 64, "{ev} has discriminant {value}; mask() only has 64 bits to hand out");
        }
    }

    /// `Everything` must cover every sequential single-type bit.
    #[test]
    fn test_everything_mask_covers_all_single_types() {
        let everything = EventName::Everything.mask();
        for ev in ALL_EVENT_NAMES {
            let value = *ev as u32;
            if value > 0 && value <= LAST_SINGLE_TYPE_VALUE {
                assert_eq!(everything & ev.mask(), ev.mask(), "Everything mask should cover {ev}");
            }
        }
    }
}
