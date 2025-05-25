use rustfs_event::NotifierSystem;
use rustfs_event::{create_adapters, ChannelAdapterType};
use rustfs_event::{AdapterConfig, NotifierConfig, WebhookConfig};
use rustfs_event::{Bucket, Event, Identity, Metadata, Name, Object, Source};
use std::collections::HashMap;
use std::error;
use std::sync::Arc;
use tokio::signal;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG) // set to debug or lower level
        .with_target(false) // simplify output
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set up log subscriber");

    let config = NotifierConfig {
        store_path: "./events".to_string(),
        channel_capacity: 100,
        adapters: vec![AdapterConfig::Webhook(WebhookConfig {
            endpoint: "http://127.0.0.1:3020/webhook".to_string(),
            auth_token: Some("secret-token".to_string()),
            custom_headers: Some(HashMap::from([("X-Custom".to_string(), "value".to_string())])),
            max_retries: 3,
            timeout: Some(30),
            retry_interval: Some(5),
            client_cert: None,
            client_key: None,
            common: rustfs_event::AdapterCommon {
                identifier: "webhook".to_string(),
                comment: "webhook".to_string(),
                enable: true,
                queue_dir: "./deploy/logs/event_queue".to_string(),
                queue_limit: 100,
            },
        })],
    };

    // event_load_config
    // loading configuration from environment variables
    let _config = NotifierConfig::event_load_config(Some("./crates/event/examples/event.toml".to_string()));
    tracing::info!("event_load_config config: {:?} \n", _config);
    dotenvy::dotenv()?;
    let _config = NotifierConfig::event_load_config(None);
    tracing::info!("event_load_config config: {:?} \n", _config);
    let system = Arc::new(tokio::sync::Mutex::new(NotifierSystem::new(config.clone()).await?));
    let adapters = create_adapters(&config.adapters)?;

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

    // create events using builder mode
    let event = Event::builder()
        .event_time("2023-10-01T12:00:00.000Z")
        .event_name(Name::ObjectCreatedPut)
        .user_identity(Identity {
            principal_id: "user123".to_string(),
        })
        .s3(metadata)
        .source(source)
        .channels(vec![ChannelAdapterType::Webhook.to_string()])
        .build()
        .expect("failed to create event");

    {
        let system = system.lock().await;
        system.send_event(event).await?;
    }

    let system_clone = Arc::clone(&system);
    let system_handle = tokio::spawn(async move {
        let mut system = system_clone.lock().await;
        system.start(adapters).await
    });

    signal::ctrl_c().await?;
    tracing::info!("Received shutdown signal");
    let result = {
        let mut system = system.lock().await;
        system.shutdown().await
    };

    if let Err(e) = result {
        tracing::error!("Failed to shut down the notification system: {}", e);
    } else {
        tracing::info!("Notification system shut down successfully");
    }

    system_handle.await??;
    Ok(())
}
