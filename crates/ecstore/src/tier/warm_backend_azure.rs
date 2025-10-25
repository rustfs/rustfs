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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use std::collections::HashMap;
use std::sync::Arc;

use azure_core::http::{Body, ClientOptions, RequestContent};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;

use crate::client::{
    admin_handler_utils::AdminError,
    api_put_object::PutObjectOptions,
    transition_api::{Options, ReadCloser, ReaderImpl},
};
use crate::tier::{
    tier_config::TierAzure,
    warm_backend::{WarmBackend, WarmBackendGetOpts},
};
use tracing::warn;

const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
const MAX_PARTS_COUNT: i64 = 10000;
const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

pub struct WarmBackendAzure {
    pub client: Arc<BlobServiceClient>,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

impl WarmBackendAzure {
    pub async fn new(conf: &TierAzure, tier: &str) -> Result<Self, std::io::Error> {
        if conf.access_key == "" || conf.secret_key == "" {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let creds = StorageCredentials::access_key(conf.access_key.clone(), conf.secret_key.clone());
        let client = ClientBuilder::new(conf.access_key.clone(), creds)
            //.endpoint(conf.endpoint)
            .blob_service_client();
        let client = Arc::new(client);
        Ok(Self {
            client,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix("/").unwrap_or(&conf.prefix).to_owned(),
            storage_class: "".to_string(),
        })
    }

    /*pub fn tier(&self) -> *blob.AccessTier {
        if self.storage_class == "" {
            return None;
        }
        for t in blob.PossibleAccessTierValues() {
            if strings.EqualFold(self.storage_class, t) {
                return &t
            }
        }
        None
    }*/

    pub fn get_dest(&self, object: &str) -> String {
        let mut dest_obj = object.to_string();
        if self.prefix != "" {
            dest_obj = format!("{}/{}", &self.prefix, object);
        }
        return dest_obj;
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendAzure {
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let part_size = length;
        let client = self.client.clone();
        let container_client = client.container_client(self.bucket.clone());
        let blob_client = container_client.blob_client(self.get_dest(object));
        /*let res = blob_client
            .upload(
                RequestContent::from(match r {
                    ReaderImpl::Body(content_body) => content_body.to_vec(),
                    ReaderImpl::ObjectBody(mut content_body) => content_body.read_all().await?,
                }),
                false,
                length as u64,
                None,
            )
            .await
        else {
            return Err(std::io::Error::other("upload error"));
        };*/

        let Ok(res) = blob_client
            .put_block_blob(match r {
                ReaderImpl::Body(content_body) => content_body.to_vec(),
                ReaderImpl::ObjectBody(mut content_body) => content_body.read_all().await?,
            })
            .content_type("text/plain")
            .into_future()
            .await
        else {
            return Err(std::io::Error::other("put_block_blob error"));
        };

        //self.ToObjectError(err, object)
        Ok(res.request_id.to_string())
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let client = self.client.clone();
        let container_client = client.container_client(self.bucket.clone());
        let blob_client = container_client.blob_client(self.get_dest(object));
        blob_client.get();
        todo!();
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        let client = self.client.clone();
        let container_client = client.container_client(self.bucket.clone());
        let blob_client = container_client.blob_client(self.get_dest(object));
        blob_client.delete();
        todo!();
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        /*let result = self.client
            .list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1)
            .await?;

        Ok(result.common_prefixes.len() > 0 || result.contents.len() > 0)*/
        Ok(false)
    }
}

/*fn azure_to_object_error(err: Error, params: Vec<String>) -> Option<error> {
    if err == nil {
      return nil
    }

    bucket := ""
    object := ""
    if len(params) >= 1 {
      bucket = params[0]
    }
    if len(params) == 2 {
      object = params[1]
    }

    azureErr, ok := err.(*azcore.ResponseError)
    if !ok {
      // We don't interpret non Azure errors. As azure errors will
      // have StatusCode to help to convert to object errors.
      return err
    }

    serviceCode := azureErr.ErrorCode
    statusCode := azureErr.StatusCode

    azureCodesToObjectError(err, serviceCode, statusCode, bucket, object)
}*/

/*fn azure_codes_to_object_error(err: Error, service_code: String, status_code: i32, bucket: String, object: String) -> Option<Error> {
  switch serviceCode {
  case "ContainerNotFound", "ContainerBeingDeleted":
    err = BucketNotFound{Bucket: bucket}
  case "ContainerAlreadyExists":
    err = BucketExists{Bucket: bucket}
  case "InvalidResourceName":
    err = BucketNameInvalid{Bucket: bucket}
  case "RequestBodyTooLarge":
    err = PartTooBig{}
  case "InvalidMetadata":
    err = UnsupportedMetadata{}
  case "BlobAccessTierNotSupportedForAccountType":
    err = NotImplemented{}
  case "OutOfRangeInput":
    err = ObjectNameInvalid{
      Bucket: bucket,
      Object: object,
    }
  default:
    switch statusCode {
    case http.StatusNotFound:
      if object != "" {
        err = ObjectNotFound{
          Bucket: bucket,
          Object: object,
        }
      } else {
        err = BucketNotFound{Bucket: bucket}
      }
    case http.StatusBadRequest:
      err = BucketNameInvalid{Bucket: bucket}
    }
  }
  return err
}*/
