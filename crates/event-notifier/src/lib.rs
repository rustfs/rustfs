/// RustFS Event Notifier
/// This crate provides a simple event notification system for RustFS.
/// It allows for the registration of event handlers and the triggering of events.
///
mod error;
mod event;
mod event_name;
mod notifier;
mod rules;
mod stats;
mod target;
mod target_entry;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Bucket, Event, Identity, Metadata, Object, Source};
    use crate::target::{TargetID, TargetList};
    use std::collections::HashMap;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[tokio::main]
    async fn main() {
        let target_list = TargetList::new();
        let event = Event {
            event_version: "1.0".to_string(),
            event_source: "aws:s3".to_string(),
            aws_region: "us-west-2".to_string(),
            event_time: "2023-10-01T12:00:00Z".to_string(),
            event_name: "PutObject".to_string(),
            user_identity: Identity {
                principal_id: "user123".to_string(),
            },
            request_parameters: HashMap::new(),
            response_elements: HashMap::new(),
            s3: Metadata {
                schema_version: "1.0".to_string(),
                configuration_id: "config123".to_string(),
                bucket: Bucket {
                    name: "my-bucket".to_string(),
                    owner_identity: Identity {
                        principal_id: "owner123".to_string(),
                    },
                    arn: "arn:aws:s3:::my-bucket".to_string(),
                },
                object: Object {
                    key: "my-object.txt".to_string(),
                    version_id: Some("version123".to_string()),
                    sequencer: "seq123".to_string(),
                    size: Some(1024),
                    etag: Some("etag123".to_string()),
                    content_type: Some("text/plain".to_string()),
                    user_metadata: HashMap::new(),
                },
            },
            source: Source {
                host: "localhost".to_string(),
                user_agent: "RustFS/1.0".to_string(),
            },
        };
        let target_ids: &[TargetID] = &["".to_string()];
        // 发送事件
        let results = target_list.send(event, &target_ids).await;
        println!("result len:{:?}", results.len());
        // 获取统计信息
        let stats = target_list.get_stats();
        for (id, stat) in stats {
            println!("Target {}: {} events, {} failed", id, stat.total_events, stat.failed_events);
        }
    }
}
