use s3s::service::S3Service;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use std::io::Result;
use std::net::SocketAddr;

pub(crate) fn spawn(
    server_config: quinn::ServerConfig,
    bind_addr: SocketAddr,
    service: S3Service,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(SocketAddr, JoinHandle<()>)> {
    let endpoint = s3s_http3::Endpoint::server(server_config, bind_addr)?;
    let local_addr = endpoint.local_addr()?;

    let task = tokio::spawn(async move {
        s3s_http3::serve(endpoint, service, async move {
            let _ = shutdown.recv().await;
        })
        .await;
    });

    Ok((local_addr, task))
}
