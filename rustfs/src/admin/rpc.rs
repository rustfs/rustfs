use super::router::AdminOperation;
use super::router::Operation;
use super::router::S3Router;
use crate::storage::ecfs::bytes_stream;
use ecstore::disk::DiskAPI;
use ecstore::io::READ_BUFFER_SIZE;
use ecstore::store::find_local_disk;
use futures::TryStreamExt;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::StreamingBlob;
use s3s::s3_error;
use serde_urlencoded::from_bytes;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;

pub const RPC_PREFIX: &str = "/rustfs/rpc";

pub fn regist_rpc_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", RPC_PREFIX, "/read_file_stream").as_str(),
        AdminOperation(&ReadFile {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", RPC_PREFIX, "/put_file_stream").as_str(),
        AdminOperation(&PutFile {}),
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
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ReadFileQuery =
                    from_bytes(query.as_bytes()).map_err(|e| s3_error!(InvalidArgument, "get query failed1 {:?}", e))?;
                input
            } else {
                ReadFileQuery::default()
            }
        };

        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let file = disk
            .read_file_stream(&query.volume, &query.path, query.offset, query.length)
            .await
            .map_err(|e| s3_error!(InternalError, "read file err {}", e))?;

        Ok(S3Response::new((
            StatusCode::OK,
            Body::from(StreamingBlob::wrap(bytes_stream(
                ReaderStream::with_capacity(file, READ_BUFFER_SIZE),
                query.length,
            ))),
        )))
    }
}

// /rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}"
#[derive(Debug, Default, serde::Deserialize)]
pub struct PutFileQuery {
    disk: String,
    volume: String,
    path: String,
    append: bool,
    size: usize,
}
pub struct PutFile {}
#[async_trait::async_trait]
impl Operation for PutFile {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: PutFileQuery =
                    from_bytes(query.as_bytes()).map_err(|e| s3_error!(InvalidArgument, "get query failed1 {:?}", e))?;
                input
            } else {
                PutFileQuery::default()
            }
        };

        let Some(disk) = find_local_disk(&query.disk).await else {
            return Err(s3_error!(InvalidArgument, "disk not found"));
        };

        let mut file = if query.append {
            disk.append_file(&query.volume, &query.path)
                .await
                .map_err(|e| s3_error!(InternalError, "append file err {}", e))?
        } else {
            disk.create_file("", &query.volume, &query.path, query.size)
                .await
                .map_err(|e| s3_error!(InternalError, "read file err {}", e))?
        };

        let mut body = StreamReader::new(req.input.into_stream().map_err(std::io::Error::other));

        tokio::io::copy(&mut body, &mut file)
            .await
            .map_err(|e| s3_error!(InternalError, "copy err {}", e))?;

        Ok(S3Response::new((StatusCode::OK, Body::empty())))
    }
}
