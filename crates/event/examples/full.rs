use rustfs_event::{
    AdapterConfig, Bucket, ChannelAdapterType, Error as NotifierError, Event, Identity, Metadata, Name, NotifierConfig, Object,
    Source, WebhookConfig,
};
use std::collections::HashMap;
use tokio::signal;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

async fn setup_notification_system() -> Result<(), NotifierError> {
    let config = NotifierConfig {
        store_path: "./deploy/logs/event_store".into(),
        channel_capacity: 100,
        adapters: vec![AdapterConfig::Webhook(WebhookConfig {
            endpoint: "http://127.0.0.1:3020/webhook".into(),
            auth_token: Some("your-auth-token".into()),
            custom_headers: Some(HashMap::new()),
            max_retries: 3,
            timeout: Some(30),
            retry_interval: Some(5),
            client_cert: None,
            client_key: None,
            common: rustfs_event::AdapterCommon {
                identifier: "webhook".into(),
                comment: "webhook".into(),
                enable: true,
                queue_dir: "./deploy/logs/event_queue".into(),
                queue_limit: 100,
            },
        })],
    };

    rustfs_event::initialize(&config).await?;

    // wait for the system to be ready
    for _ in 0..50 {
        // wait up to 5 seconds
        if rustfs_event::is_ready() {
            return Ok(());
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Err(NotifierError::custom("notify the system of initialization timeout"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initialization log
    // tracing_subscriber::fmt::init();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG) // set to debug or lower level
        .with_target(false) // simplify output
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set up log subscriber");

    // set up notification system
    if let Err(e) = setup_notification_system().await {
        eprintln!("unable to initialize notification system:{}", e);
        return Err(e.into());
    }

    // create a shutdown signal processing
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    // start signal processing task
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        println!("Received the shutdown signal and prepared to exit...");
        let _ = shutdown_tx.send(());
    });

    // main application logic
    tokio::select! {
        _ = async {
            loop {
                // application logic
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

                if let Err(e) = rustfs_event::send_event(event).await {
                    eprintln!("send event failed:{}", e);
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        } => {},

        _ = &mut shutdown_rx => {
            println!("close the app");
        }
    }

    // 优雅关闭通知系统
    println!("turn off the notification system");
    if let Err(e) = rustfs_event::shutdown().await {
        eprintln!("An error occurred while shutting down the notification system:{}", e);
    } else {
        println!("the notification system has been closed safely");
    }

    println!("the application has been closed safely");
    Ok(())
}
