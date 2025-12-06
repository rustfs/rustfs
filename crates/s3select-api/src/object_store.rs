// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::pin_mut;
use futures::future::join_all;
use futures::{Stream, StreamExt};
use futures_core::stream::BoxStream;
use http::HeaderMap;
use object_store::{
    Attributes, Error as o_Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use pin_project_lite::pin_project;
use rustfs_common::DEFAULT_DELIMITER;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::store_api::{HTTPRangeSpec, ObjectIO};
use rustfs_ecstore::store_api::ObjectOptions;
use s3s::S3Result;
use s3s::dto::SelectObjectContentInput;
use s3s::s3_error;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::ready;
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
    /// Put operations are not supported for S3 Select API.
    /// S3 Select is a read-only query interface for retrieving subsets of object data.
    async fn put_opts(&self, location: &Path, _payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support put operations. Location: {}",
                location
            )
            .into(),
        })
    }

    /// Multipart upload is not supported for S3 Select API.
    /// S3 Select is a read-only query interface.
    async fn put_multipart_opts(&self, location: &Path, _opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support multipart upload operations. Location: {}",
                location
            )
            .into(),
        })
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
            size: reader.object_info.size as u64,
            e_tag: reader.object_info.etag,
            version: None,
        };
        let attributes = Attributes::default();

        let payload = if self.need_convert {
            object_store::GetResultPayload::Stream(
                bytes_stream(
                    ReaderStream::with_capacity(
                        ConvertStream::new(reader.stream, self.delimiter.clone()),
                        DEFAULT_READ_BUFFER_SIZE,
                    ),
                    reader.object_info.size as usize,
                )
                .boxed(),
            )
        } else {
            object_store::GetResultPayload::Stream(
                bytes_stream(
                    ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE),
                    reader.object_info.size as usize,
                )
                .boxed(),
            )
        };
        Ok(GetResult {
            payload,
            meta,
            range: 0..reader.object_info.size as u64,
            attributes,
        })
    }

    /// Get multiple byte ranges from an object.
    /// This is useful for efficiently reading specific portions of large objects.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        info!("get_ranges: {:?} with {} ranges", location, ranges.len());

        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        // Explicit type annotation for futures to ensure homogeneity
        let mut futures: Vec<Pin<Box<dyn std::future::Future<Output = Result<Bytes>> + Send>>> = Vec::with_capacity(ranges.len());

        for range in ranges {
            // Check for empty range
            if range.start >= range.end {
                futures.push(Box::pin(async { Ok(Bytes::new()) }));
                continue;
            }

            let bucket = self.input.bucket.clone();
            let key = self.input.key.clone();
            let store = self.store.clone();
            
            // Convert u64 range to i64 range for HTTPRangeSpec
            let start = range.start as i64;
            let end = (range.end - 1) as i64;

            futures.push(Box::pin(async move {
                let opts = ObjectOptions::default();
                let h = HeaderMap::new();
                let range_spec = HTTPRangeSpec {
                    is_suffix_length: false,
                    start,
                    end,
                };
                
                let mut reader = store
                    .get_object_reader(&bucket, &key, Some(range_spec), h, &opts)
                    .await
                    .map_err(|e| o_Error::NotFound {
                        path: format!("{}/{}", bucket, key),
                        source: format!("Failed to get object range: {}", e).into(),
                    })?;
                    
                let mut buf = Vec::new();
                tokio::io::copy(&mut reader.stream, &mut buf)
                    .await
                    .map_err(|e| o_Error::Generic {
                         store: "EcObjectStore",
                         source: Box::new(e),
                    })?;
                    
                Ok(Bytes::from(buf))
            }));
        }

        let results = join_all(futures).await;
        results.into_iter().collect()
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
            size: info.size as u64,
            e_tag: info.etag,
            version: None,
        })
    }

    /// Delete operations are not supported for S3 Select API.
    /// S3 Select is a read-only query interface.
    async fn delete(&self, location: &Path) -> Result<()> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support delete operations. Location: {}",
                location
            )
            .into(),
        })
    }

    /// List operations are not supported for S3 Select API.
    /// S3 Select operates on a single, specific object.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let error = o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support list operations. Prefix: {}",
                prefix_str
            )
            .into(),
        };
        futures::stream::once(async move { Err(error) }).boxed()
    }

    /// List with delimiter is not supported for S3 Select API.
    /// S3 Select operates on a single, specific object.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support list operations. Prefix: {:?}",
                prefix
            )
            .into(),
        })
    }

    /// Copy operations are not supported for S3 Select API.
    /// S3 Select is a read-only query interface.
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support copy operations. From: {}, To: {}",
                from, to
            )
            .into(),
        })
    }

    /// Copy if not exists is not supported for S3 Select API.
    /// S3 Select is a read-only query interface.
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Err(o_Error::NotSupported {
            source: format!(
                "S3 Select API does not support copy operations. From: {}, To: {}",
                from, to
            )
            .into(),
        })
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
            Ok(s) => println!("slice: {s}"),
            Err(e) => eprintln!("Error converting to string: {e}"),
        }
    }
}
