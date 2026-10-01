use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use std::io::Result;
use std::net::SocketAddr;

pub(crate) fn spawn<M, S, B, E>(
    server_config: quinn::ServerConfig,
    bind_addr: SocketAddr,
    make_service: M,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(SocketAddr, JoinHandle<()>)>
where
    M: Fn(SocketAddr) -> S + Clone + Send + 'static,
    S: tower::Service<http::Request<s3s_http3::RequestBody>, Response = http::Response<B>, Error = E> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: std::fmt::Debug + Send + 'static,
    E: std::fmt::Debug + Send + 'static,
{
    let endpoint = s3s_http3::Endpoint::server(server_config, bind_addr)?;
    let local_addr = endpoint.local_addr()?;

    let task = tokio::spawn(async move {
        s3s_http3::serve_with(endpoint, make_service, async move {
            let _ = shutdown.recv().await;
        })
        .await;
    });

    Ok((local_addr, task))
}
