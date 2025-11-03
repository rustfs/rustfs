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

use crate::client::{
    admin_handler_utils::AdminError,
    api_put_object::PutObjectOptions,
    credentials::{Credentials, SignatureType, Static, Value},
    transition_api::{BucketLookupType, Options, ReadCloser, ReaderImpl, TransitionClient, TransitionCore},
};
use crate::tier::{
    tier_config::TierHuaweicloud,
    warm_backend::{WarmBackend, WarmBackendGetOpts},
    warm_backend_s3::WarmBackendS3,
};
use tracing::warn;

const MAX_MULTIPART_PUT_OBJECT_SIZE: i64 = 1024 * 1024 * 1024 * 1024 * 5;
const MAX_PARTS_COUNT: i64 = 10000;
const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

pub struct WarmBackendHuaweicloud(WarmBackendS3);

impl WarmBackendHuaweicloud {
    pub async fn new(conf: &TierHuaweicloud, tier: &str) -> Result<Self, std::io::Error> {
        if conf.access_key == "" || conf.secret_key == "" {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let u = match url::Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(e) => {
                return Err(std::io::Error::other(e.to_string()));
            }
        };

        let creds = Credentials::new(Static(Value {
            access_key_id: conf.access_key.clone(),
            secret_access_key: conf.secret_key.clone(),
            session_token: "".to_string(),
            signer_type: SignatureType::SignatureV4,
            ..Default::default()
        }));
        let opts = Options {
            creds,
            secure: u.scheme() == "https",
            //transport: GLOBAL_RemoteTargetTransport,
            trailing_headers: true,
            region: conf.region.clone(),
            bucket_lookup: BucketLookupType::BucketLookupDNS,
            ..Default::default()
        };
        let scheme = u.scheme();
        let default_port = if scheme == "https" { 443 } else { 80 };
        let client = TransitionClient::new(
            &format!("{}:{}", u.host_str().expect("err"), u.port().unwrap_or(default_port)),
            opts,
            "huaweicloud",
        )
        .await?;

        let client = Arc::new(client);
        let core = TransitionCore(Arc::clone(&client));
        Ok(Self(WarmBackendS3 {
            client,
            core,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.strip_suffix("/").unwrap_or(&conf.prefix).to_owned(),
            storage_class: "".to_string(),
        }))
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendHuaweicloud {
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
        self.0.in_use().await
    }
}

fn optimal_part_size(object_size: i64) -> Result<i64, std::io::Error> {
    let mut object_size = object_size;
    if object_size == -1 {
        object_size = MAX_MULTIPART_PUT_OBJECT_SIZE;
    }

    if object_size > MAX_MULTIPART_PUT_OBJECT_SIZE {
        return Err(std::io::Error::other("entity too large"));
    }

    let configured_part_size = MIN_PART_SIZE;
    let mut part_size_flt = object_size as f64 / MAX_PARTS_COUNT as f64;
    part_size_flt = (part_size_flt as f64 / configured_part_size as f64).ceil() * configured_part_size as f64;

    let part_size = part_size_flt as i64;
    if part_size == 0 {
        return Ok(MIN_PART_SIZE);
    }
    Ok(part_size)
}
