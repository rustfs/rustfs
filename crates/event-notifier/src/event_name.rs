use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u64)]
pub enum EventName {
    // 对象操作事件
    ObjectCreatedAll = 1 << 0,
    ObjectCreatedPut = 1 << 1,
    ObjectCreatedPost = 1 << 2,
    ObjectCreatedCopy = 1 << 3,
    ObjectCreatedCompleteMultipartUpload = 1 << 4,

    ObjectRemovedAll = 1 << 5,
    ObjectRemovedDelete = 1 << 6,
    ObjectRemovedDeleteMarkerCreated = 1 << 7,

    ObjectAccessedAll = 1 << 8,
    ObjectAccessedGet = 1 << 9,
    ObjectAccessedHead = 1 << 10,

    ObjectRestoredAll = 1 << 11,
    ObjectRestoredPost = 1 << 12,
    ObjectRestoredCompleted = 1 << 13,

    ReplicationAll = 1 << 14,
    ReplicationFailed = 1 << 15,
    ReplicationComplete = 1 << 16,
}

impl EventName {
    pub fn mask(&self) -> u64 {
        *self as u64
    }

    pub fn expand(&self) -> Vec<EventName> {
        match self {
            EventName::ObjectCreatedAll => vec![
                EventName::ObjectCreatedPut,
                EventName::ObjectCreatedPost,
                EventName::ObjectCreatedCopy,
                EventName::ObjectCreatedCompleteMultipartUpload,
            ],
            EventName::ObjectRemovedAll => vec![EventName::ObjectRemovedDelete, EventName::ObjectRemovedDeleteMarkerCreated],
            EventName::ObjectAccessedAll => vec![EventName::ObjectAccessedGet, EventName::ObjectAccessedHead],
            EventName::ObjectRestoredAll => vec![EventName::ObjectRestoredPost, EventName::ObjectRestoredCompleted],
            EventName::ReplicationAll => vec![EventName::ReplicationFailed, EventName::ReplicationComplete],
            _ => vec![*self],
        }
    }
}

impl FromStr for EventName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3:ObjectCreated:*" => Ok(EventName::ObjectCreatedAll),
            "s3:ObjectCreated:Put" => Ok(EventName::ObjectCreatedPut),
            "s3:ObjectCreated:Post" => Ok(EventName::ObjectCreatedPost),
            "s3:ObjectCreated:Copy" => Ok(EventName::ObjectCreatedCopy),
            "s3:ObjectCreated:CompleteMultipartUpload" => Ok(EventName::ObjectCreatedCompleteMultipartUpload),
            "s3:ObjectRemoved:*" => Ok(EventName::ObjectRemovedAll),
            "s3:ObjectRemoved:Delete" => Ok(EventName::ObjectRemovedDelete),
            "s3:ObjectRemoved:DeleteMarkerCreated" => Ok(EventName::ObjectRemovedDeleteMarkerCreated),
            "s3:ObjectAccessed:*" => Ok(EventName::ObjectAccessedAll),
            "s3:ObjectAccessed:Get" => Ok(EventName::ObjectAccessedGet),
            "s3:ObjectAccessed:Head" => Ok(EventName::ObjectAccessedHead),
            "s3:ObjectRestored:*" => Ok(EventName::ObjectRestoredAll),
            "s3:ObjectRestored:Post" => Ok(EventName::ObjectRestoredPost),
            "s3:ObjectRestored:Completed" => Ok(EventName::ObjectRestoredCompleted),
            "s3:Replication:*" => Ok(EventName::ReplicationAll),
            "s3:Replication:Failed" => Ok(EventName::ReplicationFailed),
            "s3:Replication:Complete" => Ok(EventName::ReplicationComplete),
            _ => Err(Error::InvalidEventName(format!("Unrecognized event name: {}", s))),
        }
    }
}
