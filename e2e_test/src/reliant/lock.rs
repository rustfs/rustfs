#![cfg(test)]

use std::{error::Error, sync::Arc, time::Duration, vec};

use lock::{
    drwmutex::Options,
    lock_args::LockArgs,
    namespace_lock::{new_nslock, NsLockMap},
    new_lock_api,
};
use protos::proto_gen::node_service::{node_service_client::NodeServiceClient, GenerallyLockRequest};
use tokio::sync::RwLock;
use tonic::Request;

async fn get_client() -> Result<NodeServiceClient<tonic::transport::Channel>, Box<dyn Error>> {
    // Ok(NodeServiceClient::connect("http://220.181.1.138:9000").await?)
    Ok(NodeServiceClient::connect("http://localhost:9000").await?)
}

#[tokio::test]
async fn test_lock_unlock_rpc() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "1111".to_string(),
        resources: vec!["dandan".to_string()],
        owner: "dd".to_string(),
        source: "".to_string(),
        quorum: 3,
    };
    let args = serde_json::to_string(&args)?;

    let mut client = get_client().await?;
    let request = Request::new(GenerallyLockRequest { args });

    let response = client.lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        assert!(false, "can not get lock: {}", error_info);
    }
    Ok(())
}

#[tokio::test]
async fn test_lock_unlock_ns_lock() -> Result<(), Box<dyn Error>> {
    let url = url::Url::parse("http://127.0.0.1:9000/data")?;
    let locker = new_lock_api(false, Some(url));
    let ns_mutex = Arc::new(RwLock::new(NsLockMap::new(true)));
    let mut ns = new_nslock(
        Arc::clone(&ns_mutex),
        "local".to_string(),
        "dandan".to_string(),
        vec!["foo".to_string()],
        vec![locker],
    )
    .await;
    assert_eq!(
        ns.get_lock(&Options {
            timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(1),
        })
        .await
        .unwrap(),
        true
    );

    ns.un_lock().await.unwrap();
    Ok(())
}
