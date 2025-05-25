use rustfs_event::{AdapterCommon, AdapterConfig, ChannelAdapterType, NotifierSystem, WebhookConfig};
use rustfs_event::{Bucket, Event, EventBuilder, Identity, Metadata, Name, Object, Source};
use rustfs_event::{ChannelAdapter, WebhookAdapter};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_webhook_adapter() {
    let adapter = WebhookAdapter::new(WebhookConfig {
        common: AdapterCommon {
            identifier: "webhook".to_string(),
            comment: "webhook".to_string(),
            enable: true,
            queue_dir: "./deploy/logs/event_queue".to_string(),
            queue_limit: 100,
        },
        endpoint: "http://localhost:8080/webhook".to_string(),
        auth_token: None,
        custom_headers: None,
        max_retries: 1,
        timeout: Some(5),
        retry_interval: Some(5),
        client_cert: None,
        client_key: None,
    });

    // create an s3 metadata object
    let metadata = Metadata {
        schema_version: "1.0".to_string(),
        configuration_id: "test-config".to_string(),
        bucket: Bucket {
            name: "my-bucket".to_string(),
            owner_identity: Identity {
                principal_id: "owner123".to_string(),
            },
            arn: "arn:aws:s3:::my-bucket".to_string(),
        },
        object: Object {
            key: "test.txt".to_string(),
            size: Some(1024),
            etag: Some("abc123".to_string()),
            content_type: Some("text/plain".to_string()),
            user_metadata: None,
            version_id: None,
            sequencer: "1234567890".to_string(),
        },
    };

    // create source object
    let source = Source {
        host: "localhost".to_string(),
        port: "80".to_string(),
        user_agent: "curl/7.68.0".to_string(),
    };

    // Create events using builder mode
    let event = Event::builder()
        .event_version("2.0")
        .event_source("aws:s3")
        .aws_region("us-east-1")
        .event_time("2023-10-01T12:00:00.000Z")
        .event_name(Name::ObjectCreatedPut)
        .user_identity(Identity {
            principal_id: "user123".to_string(),
        })
        .request_parameters(HashMap::new())
        .response_elements(HashMap::new())
        .s3(metadata)
        .source(source)
        .channels(vec![ChannelAdapterType::Webhook.to_string()])
        .build()
        .expect("failed to create event");

    let result = adapter.send(&event).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_notification_system() {
    let config = rustfs_event::NotifierConfig {
        store_path: "./test_events".to_string(),
        channel_capacity: 100,
        adapters: vec![AdapterConfig::Webhook(WebhookConfig {
            common: Default::default(),
            endpoint: "http://localhost:8080/webhook".to_string(),
            auth_token: None,
            custom_headers: None,
            max_retries: 1,
            timeout: Some(5),
            retry_interval: Some(5),
            client_cert: None,
            client_key: None,
        })],
    };
    let system = Arc::new(tokio::sync::Mutex::new(NotifierSystem::new(config.clone()).await.unwrap()));
    let adapters: Vec<Arc<dyn ChannelAdapter>> = vec![Arc::new(WebhookAdapter::new(WebhookConfig {
        common: Default::default(),
        endpoint: "http://localhost:8080/webhook".to_string(),
        auth_token: None,
        custom_headers: None,
        max_retries: 1,
        timeout: Some(5),
        retry_interval: Some(5),
        client_cert: None,
        client_key: None,
    }))];

    // create an s3 metadata object
    let metadata = Metadata {
        schema_version: "1.0".to_string(),
        configuration_id: "test-config".to_string(),
        bucket: Bucket {
            name: "my-bucket".to_string(),
            owner_identity: Identity {
                principal_id: "owner123".to_string(),
            },
            arn: "arn:aws:s3:::my-bucket".to_string(),
        },
        object: Object {
            key: "test.txt".to_string(),
            size: Some(1024),
            etag: Some("abc123".to_string()),
            content_type: Some("text/plain".to_string()),
            user_metadata: None,
            version_id: None,
            sequencer: "1234567890".to_string(),
        },
    };

    // create source object
    let source = Source {
        host: "localhost".to_string(),
        port: "80".to_string(),
        user_agent: "curl/7.68.0".to_string(),
    };

    // create a preconfigured builder with objects
    let event = EventBuilder::for_object_creation(metadata, source)
        .user_identity(Identity {
            principal_id: "user123".to_string(),
        })
        .event_time("2023-10-01T12:00:00.000Z")
        .channels(vec![ChannelAdapterType::Webhook.to_string()])
        .build()
        .expect("failed to create event");

    {
        let system_lock = system.lock().await;
        system_lock.send_event(event).await.unwrap();
    }

    let system_clone = Arc::clone(&system);
    let system_handle = tokio::spawn(async move {
        let mut system = system_clone.lock().await;
        system.start(adapters).await
    });

    // set 10 seconds timeout
    match tokio::time::timeout(std::time::Duration::from_secs(10), system_handle).await {
        Ok(result) => {
            println!("System started successfully");
            assert!(result.is_ok());
        }
        Err(_) => {
            println!("System operation timed out, forcing shutdown");
            // create a new task to handle the timeout
            let system = Arc::clone(&system);
            tokio::spawn(async move {
                if let Ok(mut guard) = system.try_lock() {
                    guard.shutdown().await.unwrap();
                }
            });
            // give the system some time to clean up resources
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
