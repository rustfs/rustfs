use std::sync::OnceLock;

use crate::endpoints::EndpointServerPools;
use crate::error::{Error, Result};
use crate::peer_rest_client::PeerRestClient;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_NotificationSys: OnceLock<NotificationSys> = OnceLock::new();
}

pub async fn new_global_notification_sys(eps: EndpointServerPools) -> Result<()> {
    let _ = GLOBAL_NotificationSys
        .set(NotificationSys::new(eps).await)
        .map_err(|_| Error::msg("init notification_sys fail"));
    Ok(())
}

pub struct NotificationSys {
    pub peer_clients: Vec<PeerRestClient>,
    pub all_peer_clients: Vec<PeerRestClient>,
}

impl NotificationSys {
    pub async fn new(eps: EndpointServerPools) -> Self {
        let (peer_clients, all_peer_clients) = PeerRestClient::new_clients(eps).await;
        Self {
            peer_clients,
            all_peer_clients,
        }
    }
}

pub struct NotificationPeerErr {
    pub host: String,
    pub err: Option<Error>,
}

impl NotificationSys {
    pub async fn delete_policy(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }
    pub async fn load_policy(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }

    pub async fn load_policy_mapping(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }
    pub async fn delete_user(&self) -> Vec<NotificationPeerErr> {
        unimplemented!()
    }
}
