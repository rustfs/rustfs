#![cfg(test)]

use std::{collections::HashMap, error::Error, sync::Arc, time::Duration, vec};

use lazy_static::lazy_static;
use lock::{
    drwmutex::Options,
    lock_args::LockArgs,
    namespace_lock::{new_nslock, NsLockMap},
    new_lock_api,
};
use protos::proto_gen::node_service::{node_service_client::NodeServiceClient, GenerallyLockRequest};
use tokio::sync::RwLock;
use tonic::{
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
    Request, Status,
};

lazy_static! {
    pub static ref GLOBAL_Conn_Map: RwLock<HashMap<String, Channel>> = RwLock::new(HashMap::new());
}

async fn get_client() -> Result<
    NodeServiceClient<
        InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
    >,
    Box<dyn Error>,
> {
    let token: MetadataValue<_> = "rustfs rpc".parse()?;
    let channel = match GLOBAL_Conn_Map.read().await.get("local") {
        Some(channel) => channel.clone(),
        None => {
            println!("get channel start");
            let connector = Endpoint::from_static("http://localhost:9000").connect_timeout(Duration::from_secs(60));
            let channel = connector.connect().await?;
            // let channel = Channel::from_static("http://localhost:9000").connect().await?;
            channel
        }
    };
    GLOBAL_Conn_Map.write().await.insert("local".to_owned(), channel.clone());

    // let timeout_channel = Timeout::new(channel, Duration::from_secs(60));
    Ok(NodeServiceClient::with_interceptor(
        channel,
        Box::new(move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        }),
    ))
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
