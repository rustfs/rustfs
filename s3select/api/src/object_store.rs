use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
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
use s3s::dto::SelectObjectContentInput;
use s3s::s3_error;
use s3s::S3Result;
use std::ops::Range;
use std::sync::Arc;
use tokio_util::io::ReaderStream;
use tracing::info;
use transform_stream::AsyncTryStream;

#[derive(Debug)]
pub struct EcObjectStore {
    input: SelectObjectContentInput,

    store: Arc<ECStore>,
}

impl EcObjectStore {
    pub fn new(input: SelectObjectContentInput) -> S3Result<Self> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "ec store not inited"));
        };

        Ok(Self { input, store })
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

        // let stream = stream::unfold(reader.stream, |mut blob| async move {
        //     match blob.next().await {
        //         Some(Ok(chunk)) => {
        //             let bytes = chunk;
        //             Some((Ok(bytes), blob))
        //         }
        //         _ => None,
        //     }
        // })
        // .boxed();
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: reader.object_info.size,
            e_tag: reader.object_info.etag,
            version: None,
        };
        let attributes = Attributes::default();

        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                bytes_stream(ReaderStream::with_capacity(reader.stream, READ_BUFFER_SIZE), reader.object_info.size).boxed(),
            ),
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
