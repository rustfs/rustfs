// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[allow(unsafe_code)]
mod generated;

use std::{error::Error, time::Duration};

pub use generated::*;
use proto_gen::node_service::node_service_client::NodeServiceClient;
use rustfs_common::globals::GLOBAL_Conn_Map;
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
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

    let channel = { GLOBAL_Conn_Map.read().await.get(addr).cloned() };

    let channel = match channel {
        Some(channel) => channel,
        None => {
            let connector = Endpoint::from_shared(addr.to_string())?
                .connect_timeout(Duration::from_secs(5))
                .tcp_keepalive(Some(Duration::from_secs(10)))
                .http2_keep_alive_interval(Duration::from_secs(5))
                .keep_alive_timeout(Duration::from_secs(3))
                .keep_alive_while_idle(true)
                .timeout(Duration::from_secs(60));
            let channel = connector.connect().await?;

            {
                GLOBAL_Conn_Map.write().await.insert(addr.to_string(), channel.clone());
            }
            channel
        }
    };

    Ok(NodeServiceClient::with_interceptor(
        channel,
        Box::new(move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        }),
    ))
}
