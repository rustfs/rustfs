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

use azure_core::http::ClientOptions;
use azure_identity::DeveloperToolsCredential;
use azure_storage_blob::{BlobClient, BlobClientOptions};
use azure_security_keyvault_secrets::{SecretClient, SecretClientOptions};

use crate::client::{
    admin_handler_utils::AdminError,
    api_put_object::PutObjectOptions,
    credentials::{Credentials, SignatureType, Static, Value},
    transition_api::{Options, ReadCloser, ReaderImpl, TransitionClient, TransitionCore},
};
use crate::tier::{
    tier_config::TierAzure,
    warm_backend::{WarmBackend, WarmBackendGetOpts},
    warm_backend_s3::WarmBackendS3,
};
use tracing::warn;

const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
const MAX_PARTS_COUNT: i64 = 10000;
const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

pub struct WarmBackendAzure {
    pub client: Arc<SecretClient>,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
};

impl WarmBackendAzure {
    pub async fn new(conf: &TierAzure, tier: &str) -> Result<Self, std::io::Error> {
        if conf.access_key == "" || conf.secret_key == "" {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let creds = DeveloperToolsCredential::new(Some(Value {
            access_key_id: conf.access_key.clone(),
            secret_access_key: conf.secret_key.clone(),
        }));
        let opts = SecretClientOptions {
            api_vertion: "7.5".to_string(),
            ..Default::default()
        };
        let client =
            SecretClient::new(conf.endpoint, creds.clone(), Some(opts))?;
        let client = Arc::new(client);
        Ok(Self {
            client,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix("/").unwrap_or(&conf.prefix).to_owned(),
            storage_class: "".to_string(),
        })
    }

    pub fn tier(&self) -> *blob.AccessTier {
        if az.StorageClass == "" {
          return nil
        }
        for _, t := range blob.PossibleAccessTierValues() {
          if strings.EqualFold(az.StorageClass, string(t)) {
            return &t
          }
        }
        nil
    }

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
        let part_size = optimal_part_size(length)?;
        let client = self.0.client.clone();
        let res = client
            .put_object(
                &self.0.bucket,
                &self.0.get_dest(object),
                r,
                length,
                &PutObjectOptions {
                    storage_class: self.0.storage_class.clone(),
                    part_size: part_size as u64,
                    disable_content_sha256: true,
                    user_metadata: meta,
                    ..Default::default()
                },
            )
            .await?;
        //self.ToObjectError(err, object)
        Ok(res.version_id)
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        self.0.get(object, rv, opts).await
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        self.0.remove(object, rv).await
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        /*let result = self.client
            .list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1)
            .await?;

        Ok(result.common_prefixes.len() > 0 || result.contents.len() > 0)*/
        Ok(false)
    }
}

fn azure_to_object_error(err: Error, params: Vec<String>) -> Option<error> {
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
}

fn azure_codes_to_object_error(err: Error, service_code: String, status_code: i32, bucket: String, object: String) -> Option<Error> {
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
}
