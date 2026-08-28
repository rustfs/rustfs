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

use crate::services::tier::{
    tier_config::TierAzure,
    warm_backend::{
        S3CompatibleWarmBackendParams, WarmBackend, WarmBackendGetOpts, build_transition_put_options,
        new_s3_compatible_warm_backend, optimal_part_size,
    },
    warm_backend_s3::WarmBackendS3,
};
use rustfs_s3_client::transition_api::{BucketLookupType, ReadCloser, ReaderImpl};
use rustfs_utils::egress::validate_outbound_url;

const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;

pub struct WarmBackendAzure(WarmBackendS3);

impl WarmBackendAzure {
    pub async fn new(conf: &TierAzure, tier: &str) -> Result<Self, std::io::Error> {
        Ok(Self(
            new_s3_compatible_warm_backend(S3CompatibleWarmBackendParams {
                endpoint: &conf.endpoint,
                access_key: &conf.access_key,
                secret_key: &conf.secret_key,
                bucket: &conf.bucket,
                prefix: &conf.prefix,
                region: &conf.region,
                bucket_lookup: BucketLookupType::BucketLookupDNS,
                provider_tag: "azure",
                validate_endpoint: validate_outbound_url,
            })
            .await?,
        ))
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
        let part_size = optimal_part_size(length, MIN_PART_SIZE)?;
        let client = self.0.client.clone();
        let res = client
            .put_object(&self.0.bucket, &self.0.get_dest(object), r, length, &{
                let mut opts = build_transition_put_options(self.0.storage_class.clone(), meta);
                opts.part_size = part_size as u64;
                opts.disable_content_sha256 = true;
                opts
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tier::tier_config::TierAzure;

    /// The SSRF guard itself is exercised once, generically, in
    /// `warm_backend::tests` (see backlog#2040/backlog#2041 and
    /// rustfs/rustfs#6764) — this test only pins that this provider's
    /// production constructor really is wired through that shared path.
    #[tokio::test]
    async fn new_rejects_loopback_endpoint_before_network_setup() {
        let conf = TierAzure {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "tier-bucket".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        match WarmBackendAzure::new(&conf, "tier").await {
            Ok(_) => panic!("loopback endpoint should be rejected"),
            Err(err) => assert!(err.to_string().contains("not allowed")),
        }
    }
}
