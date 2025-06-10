use async_trait::async_trait;
use protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
use std::io::{Error, Result};
use tonic::Request;
use tracing::info;

use crate::{Locker, lock_args::LockArgs};

#[derive(Debug, Clone)]
pub struct RemoteClient {
    addr: String,
}

impl RemoteClient {
    pub fn new(url: url::Url) -> Self {
        let addr = format!("{}://{}:{}", url.scheme(), url.host_str().unwrap(), url.port().unwrap());
        Self { addr }
    }
}

#[async_trait]
impl Locker for RemoteClient {
    async fn lock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote lock");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.lock(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote unlock");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.un_lock(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote rlock");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.r_lock(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote runlock");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.r_un_lock(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn refresh(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote refresh");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.refresh(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
        info!("remote force_unlock");
        let args = serde_json::to_string(args)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
        let request = Request::new(GenerallyLockRequest { args });

        let response = client.force_un_lock(request).await.map_err(Error::other)?.into_inner();

        if let Some(error_info) = response.error_info {
            return Err(Error::other(error_info));
        }

        Ok(response.success)
    }

    async fn close(&self) {}

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        false
    }
}
