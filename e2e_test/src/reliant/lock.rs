#![cfg(test)]

use std::{error::Error, sync::Arc, time::Duration};

use lock::{
    drwmutex::Options,
    lock_args::LockArgs,
    namespace_lock::{new_nslock, NsLockMap},
    new_lock_api,
};
use protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
use tokio::sync::RwLock;
use tonic::Request;

const CLUSTER_ADDR: &str = "http://localhost:9000";

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

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    println!("got client");
    let request = Request::new(GenerallyLockRequest { args: args.clone() });

    println!("start request");
    let response = client.lock(request).await?.into_inner();
    println!("request ended");
    if let Some(error_info) = response.error_info {
        assert!(false, "can not get lock: {}", error_info);
    }

    let request = Request::new(GenerallyLockRequest { args });
    let response = client.un_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        assert!(false, "can not get un_lock: {}", error_info);
    }

    Ok(())
}

#[tokio::test]
async fn test_lock_unlock_ns_lock() -> Result<(), Box<dyn Error>> {
    let url = url::Url::parse("http://127.0.0.1:9000/data")?;
    let locker = new_lock_api(false, Some(url));
    let ns_mutex = Arc::new(RwLock::new(NsLockMap::new(true)));
    let ns = new_nslock(
        Arc::clone(&ns_mutex),
        "local".to_string(),
        "dandan".to_string(),
        vec!["foo".to_string()],
        vec![locker],
    )
    .await;
    assert_eq!(
        ns.0.write().await.get_lock(&Options {
            timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(1),
        })
        .await
        .unwrap(),
        true
    );

    ns.0.write().await.un_lock().await.unwrap();
    Ok(())
}
