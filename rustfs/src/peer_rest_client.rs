use std::io::Cursor;

use common::error::{Error, Result};
use ecstore::{admin_server_info::ServerProperties, store_api::StorageInfo};
use protos::{node_service_time_out_client, proto_gen::node_service::LocalStorageInfoRequest};
use rmp_serde::Deserializer;
use serde::Deserialize;
use tonic::Request;

struct PeerRestClient {
    addr: String,
}

impl PeerRestClient {
    pub fn new(url: url::Url) -> Self {
        Self {
            addr: format!("{}://{}:{}", url.scheme(), url.host_str().unwrap(), url.port().unwrap()),
        }
    }
}

impl PeerRestClient {
    pub async fn local_storage_info(&self) -> Result<StorageInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LocalStorageInfoRequest { metrics: true });

        let response = client.local_storage_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let info = response.storage_info;

        let mut buf = Deserializer::new(Cursor::new(info));
        let storage_info: StorageInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(storage_info)
    }

    pub async fn server_info(&self) -> Request<ServerProperties> {
        todo!()
    }
}
