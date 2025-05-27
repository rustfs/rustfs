use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use common::DEFAULT_DELIMITER;
use ecstore::io::READ_BUFFER_SIZE;
use ecstore::new_object_layer_fn;
use ecstore::store::ECStore;
use ecstore::store_api::ObjectIO;
use ecstore::store_api::ObjectOptions;
use ecstore::StorageAPI;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use futures_core::stream::BoxStream;
use http::HeaderMap;
use object_store::path::Path;
use object_store::Attributes;
use object_store::GetOptions;
use object_store::GetResult;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMultipartOpts;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::{Error as o_Error, Result};
use pin_project_lite::pin_project;
use s3s::dto::SelectObjectContentInput;
use s3s::s3_error;
use s3s::S3Result;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Poll;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use tracing::info;
use transform_stream::AsyncTryStream;

#[derive(Debug)]
pub struct EcObjectStore {
    input: Arc<SelectObjectContentInput>,
    need_convert: bool,
    delimiter: String,

    store: Arc<ECStore>,
}
impl EcObjectStore {
    pub fn new(input: Arc<SelectObjectContentInput>) -> S3Result<Self> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "ec store not inited"));
        };

        let (need_convert, delimiter) = if let Some(csv) = input.request.input_serialization.csv.as_ref() {
            if let Some(delimiter) = csv.field_delimiter.as_ref() {
                if delimiter.len() > 1 {
                    (true, delimiter.to_owned())
                } else {
                    (false, String::new())
                }
            } else {
                (false, String::new())
            }
        } else {
            (false, String::new())
        };

        Ok(Self {
            input,
            need_convert,
            delimiter,
            store,
        })
    }
}

impl std::fmt::Display for EcObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EcObjectStore")
    }
}

#[async_trait]
impl ObjectStore for EcObjectStore {
    async fn put_opts(&self, _location: &Path, _payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(&self, _location: &Path, _opts: PutMultipartOpts) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(&self, location: &Path, _options: GetOptions) -> Result<GetResult> {
        info!("{:?}", location);
        let opts = ObjectOptions::default();
        let h = HeaderMap::new();
        let reader = self
            .store
            .get_object_reader(&self.input.bucket, &self.input.key, None, h, &opts)
            .await
            .map_err(|_| o_Error::NotFound {
                path: format!("{}/{}", self.input.bucket, self.input.key),
                source: "can not get object info".into(),
            })?;

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: reader.object_info.size,
            e_tag: reader.object_info.etag,
            version: None,
        };
        let attributes = Attributes::default();

        let payload = if self.need_convert {
            object_store::GetResultPayload::Stream(
                bytes_stream(
                    ReaderStream::with_capacity(ConvertStream::new(reader.stream, self.delimiter.clone()), READ_BUFFER_SIZE),
                    reader.object_info.size,
                )
                .boxed(),
            )
        } else {
            object_store::GetResultPayload::Stream(
                bytes_stream(ReaderStream::with_capacity(reader.stream, READ_BUFFER_SIZE), reader.object_info.size).boxed(),
            )
        };
        Ok(GetResult {
            payload,
            meta,
            range: 0..reader.object_info.size,
            attributes,
        })
    }

    async fn get_ranges(&self, _location: &Path, _ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        unimplemented!()
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        info!("{:?}", location);
        let opts = ObjectOptions::default();
        let info = self
            .store
            .get_object_info(&self.input.bucket, &self.input.key, &opts)
            .await
            .map_err(|_| o_Error::NotFound {
                path: format!("{}/{}", self.input.bucket, self.input.key),
                source: "can not get object info".into(),
            })?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: info.size,
            e_tag: info.etag,
            version: None,
        })
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        unimplemented!()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _too: &Path) -> Result<()> {
        unimplemented!()
    }
}

pin_project! {
    struct ConvertStream<R> {
        inner: R,
        delimiter: Vec<u8>,
    }
}

impl<R> ConvertStream<R> {
    fn new(inner: R, delimiter: String) -> Self {
        ConvertStream {
            inner,
            delimiter: delimiter.as_bytes().to_vec(),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ConvertStream<R> {
    #[tracing::instrument(level = "debug", skip_all)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = self.project();
        ready!(Pin::new(&mut *me.inner).poll_read(cx, buf))?;
        let bytes = buf.filled();
        let replaced = replace_symbol(me.delimiter, bytes);
        buf.clear();
        buf.put_slice(&replaced);
        Poll::Ready(Ok(()))
    }
}

fn replace_symbol(delimiter: &[u8], slice: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(slice.len());
    let mut i = 0;
    while i < slice.len() {
        if slice[i..].starts_with(delimiter) {
            result.push(DEFAULT_DELIMITER);
            i += delimiter.len();
        } else {
            result.push(slice[i]);
            i += 1;
        }
    }
    result
}

pub fn bytes_stream<S>(stream: S, content_length: usize) -> impl Stream<Item = Result<Bytes>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    AsyncTryStream::<Bytes, o_Error, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while let Some(result) = stream.next().await {
            let mut bytes = result.map_err(|e| o_Error::Generic {
                store: "",
                source: Box::new(e),
            })?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use super::replace_symbol;

    #[test]
    fn test_replace() {
        let ss = String::from("dandan&&is&&best");
        let slice = ss.as_bytes();
        let delimiter = b"&&";
        println!("len: {}", "╦".len());
        let result = replace_symbol(delimiter, slice);
        match String::from_utf8(result) {
            Ok(s) => println!("slice: {}", s),
            Err(e) => eprintln!("Error converting to string: {}", e),
        }
    }
}
