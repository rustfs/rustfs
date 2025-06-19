use crate::{Event, EventName};
use std::collections::HashMap;

/// 事件参数
#[derive(Debug, Clone)]
pub struct EventArgs {
    pub event_name: EventName,
    pub bucket_name: String,
    pub object_name: String,
    pub object_size: Option<i64>,
    pub object_etag: Option<String>,
    pub object_version_id: Option<String>,
    pub object_content_type: Option<String>,
    pub object_user_metadata: Option<HashMap<String, String>>,
    pub req_params: HashMap<String, String>,
    pub resp_elements: HashMap<String, String>,
    pub host: String,
    pub user_agent: String,
}

impl EventArgs {
    /// 转换为通知事件
    pub fn to_event(&self) -> Event {
        let event_time = chrono::Utc::now();
        let unique_id = format!("{:X}", event_time.timestamp_nanos_opt().unwrap_or(0));

        let mut resp_elements = HashMap::new();
        if let Some(request_id) = self.resp_elements.get("requestId") {
            resp_elements.insert("x-amz-request-id".to_string(), request_id.clone());
        }
        if let Some(node_id) = self.resp_elements.get("nodeId") {
            resp_elements.insert("x-amz-id-2".to_string(), node_id.clone());
        }

        // RustFS 特定的自定义元素
        // 注意：这里需要获取 endpoint 的逻辑在 Rust 中可能需要单独实现
        resp_elements.insert("x-rustfs-origin-endpoint".to_string(), "".to_string());

        // 添加 deployment ID
        resp_elements.insert("x-rustfs-deployment-id".to_string(), "".to_string());

        if let Some(content_length) = self.resp_elements.get("content-length") {
            resp_elements.insert("content-length".to_string(), content_length.clone());
        }

        let key_name = &self.object_name;
        // 注意：这里可能需要根据 escape 参数进行 URL 编码

        let mut event = Event {
            event_version: "2.0".to_string(),
            event_source: "rustfs:s3".to_string(),
            aws_region: self.req_params.get("region").cloned().unwrap_or_default(),
            event_time,
            event_name: self.event_name,
            user_identity: crate::event::Identity {
                principal_id: self
                    .req_params
                    .get("principalId")
                    .cloned()
                    .unwrap_or_default(),
            },
            request_parameters: self.req_params.clone(),
            response_elements: resp_elements,
            s3: crate::event::Metadata {
                schema_version: "1.0".to_string(),
                configuration_id: "Config".to_string(),
                bucket: crate::event::Bucket {
                    name: self.bucket_name.clone(),
                    owner_identity: crate::event::Identity {
                        principal_id: self
                            .req_params
                            .get("principalId")
                            .cloned()
                            .unwrap_or_default(),
                    },
                    arn: format!("arn:aws:s3:::{}", self.bucket_name),
                },
                object: crate::event::Object {
                    key: key_name.clone(),
                    version_id: self.object_version_id.clone(),
                    sequencer: unique_id,
                    size: self.object_size,
                    etag: self.object_etag.clone(),
                    content_type: self.object_content_type.clone(),
                    user_metadata: Some(self.object_user_metadata.clone().unwrap_or_default()),
                },
            },
            source: crate::event::Source {
                host: self.host.clone(),
                port: "".to_string(),
                user_agent: self.user_agent.clone(),
            },
        };

        // 检查是否为删除事件，如果是删除事件，某些字段应当为空
        let is_removed_event = matches!(
            self.event_name,
            EventName::ObjectRemovedDelete | EventName::ObjectRemovedDeleteMarkerCreated
        );

        if is_removed_event {
            event.s3.object.etag = None;
            event.s3.object.size = None;
            event.s3.object.content_type = None;
            event.s3.object.user_metadata = None;
        }

        event
    }
}
