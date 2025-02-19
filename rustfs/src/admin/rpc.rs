use super::router::AdminOperation;
use super::router::Operation;
use super::router::S3Router;
use crate::storage::ecfs::bytes_stream;
use common::error::Result;
use ecstore::disk::DiskAPI;
use ecstore::disk::FileReader;
use ecstore::store::find_local_disk;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use s3s::dto::StreamingBlob;
use s3s::s3_error;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use serde_urlencoded::from_bytes;
use tokio_util::io::ReaderStream;
use tracing::warn;

pub const RPC_PREFIX: &str = "/rustfs/rpc";

pub fn regist_rpc_route(r: &mut S3Router<AdminOperation>) -> Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", RPC_PREFIX, "/read_file_stream").as_str(),
        AdminOperation(&ReadFile {}),
    )?;

    Ok(())
}

// /rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}"
#[derive(Debug, Default, serde::Deserialize)]
pub struct ReadFileQuery {
    disk: String,
    volume: String,
    path: String,
    offset: usize,
    length: usize,
}
pub struct ReadFile {}
#[async_trait::async_trait]
impl Operation for ReadFile {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ReadFile");

        let query = {
            if let Some(query) = req.uri.query() {
                let input: ReadFileQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed1"))?;
                input
            } else {
                ReadFileQuery::default()
            }
        };

        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let file: FileReader = disk
            .read_file_stream(&query.volume, &query.path, query.offset, query.length)
            .await
            .map_err(|e| s3_error!(InternalError, "read file err {}", e))?;

        let s = bytes_stream(ReaderStream::new(file), query.length);

        Ok(S3Response::new((StatusCode::OK, Body::from(StreamingBlob::wrap(s)))))

        // let querys = req.uri.query().map(|q| {
        //     let mut querys = HashMap::new();
        //     for (k, v) in url::form_urlencoded::parse(q.as_bytes()) {
        //         println!("{}={}", k, v);
        //         querys.insert(k.to_string(), v.to_string());
        //     }
        //     querys
        // });

        // // TODO: file_path from root

        // if let Some(file_path) = querys.and_then(|q| q.get("file_path").cloned()) {
        //     let file = fs::OpenOptions::new()
        //         .read(true)
        //         .open(file_path)
        //         .await
        //         .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("open file err {}", e)))?;

        //     let s = bytes_stream(ReaderStream::new(file), 0);

        //     return Ok(S3Response::new((StatusCode::OK, Body::from(StreamingBlob::wrap(s)))));
        // }

        // Ok(S3Response::new((StatusCode::BAD_REQUEST, Body::empty())))
    }
}
