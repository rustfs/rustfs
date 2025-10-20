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

use bytes::Bytes;
use google_cloud_auth::credentials::Credentials;
use google_cloud_auth::credentials::user_account::Builder;
use google_cloud_storage as gcs;
use google_cloud_storage::client::Storage;
use std::convert::TryFrom;

use crate::client::{
    admin_handler_utils::AdminError,
    api_put_object::PutObjectOptions,
    transition_api::{Options, ReadCloser, ReaderImpl},
};
use crate::tier::{
    tier_config::TierGCS,
    warm_backend::{WarmBackend, WarmBackendGetOpts},
};
use tracing::warn;

const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
const MAX_PARTS_COUNT: i64 = 10000;
const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

pub struct WarmBackendGCS {
    pub client: Arc<Storage>,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

impl WarmBackendGCS {
    pub async fn new(conf: &TierGCS, tier: &str) -> Result<Self, std::io::Error> {
        if conf.creds == "" {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let authorized_user = serde_json::from_str(&conf.creds)?;
        let credentials = Builder::new(authorized_user)
            //.with_retry_policy(AlwaysRetry.with_attempt_limit(3))
            //.with_backoff_policy(backoff)
            .build()
            .map_err(|e| std::io::Error::other(format!("Invalid credentials JSON: {}", e)))?;

        let Ok(client) = Storage::builder()
            .with_endpoint(conf.endpoint.clone())
            .with_credentials(credentials)
            .build()
            .await
        else {
            return Err(std::io::Error::other("Storage::builder error"));
        };
        let client = Arc::new(client);
        Ok(Self {
            client,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix("/").unwrap_or(&conf.prefix).to_owned(),
            storage_class: "".to_string(),
        })
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
impl WarmBackend for WarmBackendGCS {
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let d = match r {
            ReaderImpl::Body(content_body) => content_body.to_vec(),
            ReaderImpl::ObjectBody(mut content_body) => content_body.read_all().await?,
        };
        let Ok(res) = self
            .client
            .write_object(&self.bucket, &self.get_dest(object), Bytes::from(d))
            .send_buffered()
            .await
        else {
            return Err(std::io::Error::other("write_object error"));
        };
        //self.ToObjectError(err, object)
        Ok(res.generation.to_string())
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        let Ok(mut reader) = self.client.read_object(&self.bucket, &self.get_dest(object)).send().await else {
            return Err(std::io::Error::other("read_object error"));
        };
        let mut contents = Vec::new();
        while let Ok(Some(chunk)) = reader.next().await.transpose() {
            contents.extend_from_slice(&chunk);
        }
        Ok(ReadCloser::new(std::io::Cursor::new(contents)))
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        /*self.client
        .delete_object()
        .set_bucket(&self.bucket)
        .set_object(&self.get_dest(object))
        //.set_generation(object.generation)
        .send()
        .await?;*/
        Ok(())
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        /*let result = self.client
            .list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1)
            .await?;

        Ok(result.common_prefixes.len() > 0 || result.contents.len() > 0)*/
        Ok(false)
    }
}

/*fn gcs_to_object_error(err: Error, params: Vec<String>) -> Option<Error> {
  if err == nil {
    return nil
  }

  bucket := ""
  object := ""
  uploadID := ""
  if len(params) >= 1 {
    bucket = params[0]
  }
  if len(params) == 2 {
    object = params[1]
  }
  if len(params) == 3 {
    uploadID = params[2]
  }

  // in some cases just a plain error is being returned
  switch err.Error() {
  case "storage: bucket doesn't exist":
    err = BucketNotFound{
      Bucket: bucket,
    }
    return err
  case "storage: object doesn't exist":
    if uploadID != "" {
      err = InvalidUploadID{
        UploadID: uploadID,
      }
    } else {
      err = ObjectNotFound{
        Bucket: bucket,
        Object: object,
      }
    }
    return err
  }

  googleAPIErr, ok := err.(*googleapi.Error)
  if !ok {
    // We don't interpret non MinIO errors. As minio errors will
    // have StatusCode to help to convert to object errors.
    return err
  }

  if len(googleAPIErr.Errors) == 0 {
    return err
  }

  reason := googleAPIErr.Errors[0].Reason
  message := googleAPIErr.Errors[0].Message

  switch reason {
  case "required":
    // Anonymous users does not have storage.xyz access to project 123.
    fallthrough
  case "keyInvalid":
    fallthrough
  case "forbidden":
    err = PrefixAccessDenied{
      Bucket: bucket,
      Object: object,
    }
  case "invalid":
    err = BucketNameInvalid{
      Bucket: bucket,
    }
  case "notFound":
    if object != "" {
      err = ObjectNotFound{
        Bucket: bucket,
        Object: object,
      }
      break
    }
    err = BucketNotFound{Bucket: bucket}
  case "conflict":
    if message == "You already own this bucket. Please select another name." {
      err = BucketAlreadyOwnedByYou{Bucket: bucket}
      break
    }
    if message == "Sorry, that name is not available. Please try a different one." {
      err = BucketAlreadyExists{Bucket: bucket}
      break
    }
    err = BucketNotEmpty{Bucket: bucket}
  }

  return err
}*/
