use ecstore::{peer_rest_client::PeerRestClient, GLOBAL_Endpoints};
use http::StatusCode;
use hyper::Uri;
use madmin::service_commands::ServiceTraceOpts;
use matchit::Params;
use s3s::{s3_error, Body, S3Request, S3Response, S3Result};
use tokio::sync::mpsc;
use tracing::warn;

use crate::admin::router::Operation;

fn extract_trace_options(uri: &Uri) -> S3Result<ServiceTraceOpts> {
    let mut st_opts = ServiceTraceOpts::default();
    st_opts
        .parse_params(uri)
        .map_err(|_| s3_error!(InvalidRequest, "invalid params"))?;

    Ok(st_opts)
}

pub struct Trace {}

#[async_trait::async_trait]
impl Operation for Trace {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle Trace");

        let trace_opts = extract_trace_options(&req.uri)?;

        // let (tx, rx) = mpsc::channel(10000);
        let perrs = match GLOBAL_Endpoints.get() {
            Some(ep) => PeerRestClient::new_clients(ep).await,
            None => (Vec::new(), Vec::new()),
        };
        return Err(s3_error!(NotImplemented));
    }
}
