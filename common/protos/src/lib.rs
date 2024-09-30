mod generated;
use std::{error::Error, time::Duration};

use common::globals::GLOBAL_Conn_Map;
pub use generated::*;
use proto_gen::node_service::node_service_client::NodeServiceClient;
use tonic::{
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
    Request, Status,
};

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

pub async fn node_service_time_out_client(
    addr: &String,
) -> Result<
    NodeServiceClient<
        InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static>>,
    >,
    Box<dyn Error>,
> {
    let token: MetadataValue<_> = "rustfs rpc".parse()?;
    let channel = match GLOBAL_Conn_Map.read().await.get(addr) {
        Some(channel) => channel.clone(),
        None => {
            let connector = Endpoint::from_shared(addr.to_string())?.connect_timeout(Duration::from_secs(60));
            let channel = connector.connect().await?;
            channel
        }
    };
    GLOBAL_Conn_Map.write().await.insert(addr.to_string(), channel.clone());

    // let timeout_channel = Timeout::new(channel, Duration::from_secs(60));
    Ok(NodeServiceClient::with_interceptor(
        channel,
        Box::new(move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        }),
    ))
}
