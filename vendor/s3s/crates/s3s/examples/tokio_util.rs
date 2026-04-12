use futures::TryStreamExt;
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

pub fn convert_body(body: s3s::Body) -> impl AsyncBufRead + Send + Sync + 'static {
    StreamReader::new(body.into_stream().map_err(std::io::Error::other))
}

pub fn convert_streaming_blob(blob: s3s::dto::StreamingBlob) -> impl AsyncBufRead + Send + Sync + 'static {
    StreamReader::new(blob.into_stream().map_err(std::io::Error::other))
}

fn main() {}
