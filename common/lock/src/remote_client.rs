use async_trait::async_trait;
use common::error::{Error, Result};
use protos::proto_gen::node_service::{node_service_client::NodeServiceClient, GenerallyLockRequest};
use tonic::Request;
use tracing::info;

use crate::{lock_args::LockArgs, Locker};

#[derive(Debug, Clone)]
pub struct RemoteClient {
    url: url::Url,
}

impl RemoteClient {
    pub fn new(url: url::Url) -> Self {
        Self { url }
    }

    async fn get_client_v2(&self) -> Result<NodeServiceClient<tonic::transport::Channel>> {
        // Ok(NodeServiceClient::connect("http://220.181.1.138:9000").await?)
        let addr = format!("{}://{}:{}", self.url.scheme(), self.url.host_str().unwrap(), self.url.port().unwrap());
        Ok(NodeServiceClient::connect(addr).await?)
    }
}

#[async_trait]
impl Locker for RemoteClient {
    async fn lock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote lock");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.lock(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote unlock");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.un_lock(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote rlock");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.r_lock(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote runlock");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.r_un_lock(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote force_unlock");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.force_un_lock(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn refresh(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote refresh");
        let args = serde_json::to_string(args)?;
        let mut client = self.get_client_v2().await?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.refresh(request).await?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::from_string(error_info));
        }

        Ok(response.success)
    }

    async fn is_local(&self) -> bool {
        false
    }

    async fn close(&self) {}

    async fn is_online(&self) -> bool {
        true
    }
}
