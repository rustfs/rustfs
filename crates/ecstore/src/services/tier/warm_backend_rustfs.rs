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

use std::collections::HashMap;

use crate::services::tier::{
    tier_config::TierRustFS,
    warm_backend::{
        S3CompatibleWarmBackendParams, TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts, build_transition_put_options,
        new_s3_compatible_warm_backend, optimal_part_size,
    },
    warm_backend_s3::WarmBackendS3,
};
use rustfs_s3_client::transition_api::{BucketLookupType, ReadCloser, ReaderImpl};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};

const _MAX_PART_SIZE: i64 = 1024 * 1024 * 1024 * 5;
const MIN_PART_SIZE: i64 = 1024 * 1024 * 128;
// Debug-only opt-in for single-host test/dev setups; release builds always reject loopback.
const ALLOW_LOOPBACK_TIER_ENDPOINT_ENV: &str = "RUSTFS_TIER_RUSTFS_ALLOW_LOOPBACK_ENDPOINT";

fn validate_rustfs_tier_endpoint(url: &url::Url) -> Result<(), OutboundUrlError> {
    let allow_loopback = cfg!(debug_assertions)
        && std::env::var(ALLOW_LOOPBACK_TIER_ENDPOINT_ENV)
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    validate_rustfs_tier_endpoint_inner(url, allow_loopback)
}

fn validate_rustfs_tier_endpoint_inner(url: &url::Url, allow_loopback: bool) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address" | "loopback host",
            ..
        }) if allow_loopback => Ok(()),
        result => result,
    }
}

pub struct WarmBackendRustFS(WarmBackendS3);

impl WarmBackendRustFS {
    pub async fn new(conf: &TierRustFS, tier: &str) -> Result<Self, std::io::Error> {
        // This provider reports endpoint problems with its own wording (and keeps the
        // `url::ParseError` as the io::Error source) while the shared constructor carries the
        // MinIO-derived texts. Unifying the two is a separate change, so the endpoint is
        // pre-validated here, after the credential and bucket checks so the order in which the
        // shared constructor would report the same failures is preserved.
        if conf.access_key.is_empty() || conf.secret_key.is_empty() {
            return Err(std::io::Error::other("both access and secret keys are required"));
        }

        if conf.bucket.is_empty() {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let u = match url::Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(e) => return Err(std::io::Error::other(e)),
        };

        if u.host_str().is_none() {
            return Err(std::io::Error::other("endpoint URL must include a host"));
        }

        Ok(Self(
            new_s3_compatible_warm_backend(S3CompatibleWarmBackendParams {
                endpoint: &conf.endpoint,
                access_key: &conf.access_key,
                secret_key: &conf.secret_key,
                bucket: &conf.bucket,
                prefix: &conf.prefix,
                region: &conf.region,
                // RustFS tier endpoints are path-style, so bucket addressing stays on
                // `BucketLookupAuto`; pinning DNS here would break those endpoints.
                bucket_lookup: BucketLookupType::BucketLookupAuto,
                provider_tag: "rustfs",
                // Debug-only, env-gated loopback exception for this provider's own e2e tier
                // tests (rustfs/rustfs#6773); every other provider passes plain
                // `validate_outbound_url`.
                validate_endpoint: validate_rustfs_tier_endpoint,
            })
            .await?,
        ))
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendRustFS {
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

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        self.0.probe_transition_candidate(object).await
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        self.0.in_use().await
    }
}

#[async_trait::async_trait]
impl crate::services::tier::warm_backend::TransitionCandidateReconciler for WarmBackendRustFS {
    async fn probe_transition_candidate_for(
        &self,
        object: &str,
        identity: crate::services::tier::warm_backend::TransitionCandidateIdentity,
    ) -> Result<TransitionCandidateProbe, std::io::Error> {
        crate::services::tier::warm_backend::TransitionCandidateReconciler::probe_transition_candidate_for(
            &self.0, object, identity,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    use super::*;

    fn rustfs_tier(endpoint: &str) -> TierRustFS {
        TierRustFS {
            endpoint: endpoint.to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            bucket: "bucket".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn new_returns_error_when_endpoint_has_no_host() {
        let conf = rustfs_tier("rustfs://");

        let outcome = AssertUnwindSafe(WarmBackendRustFS::new(&conf, "tier")).catch_unwind().await;

        let result = outcome.expect("initialization should return an error instead of panicking");
        let err = match result {
            Ok(_) => panic!("endpoint without host must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("host"), "expected host validation error, got: {err}");
    }

    #[tokio::test]
    async fn new_rejects_loopback_endpoint_before_network_setup() {
        let conf = rustfs_tier("https://127.0.0.1:9000");

        match WarmBackendRustFS::new(&conf, "tier").await {
            Ok(_) => panic!("loopback endpoint should be rejected"),
            Err(err) => assert!(err.to_string().contains("not allowed")),
        }
    }

    #[test]
    fn loopback_opt_in_does_not_allow_other_private_endpoints() {
        let loopback = url::Url::parse("https://127.0.0.1:9000").unwrap();
        assert!(validate_rustfs_tier_endpoint_inner(&loopback, true).is_ok());

        let private = url::Url::parse("https://10.0.0.1:9000").unwrap();
        assert!(validate_rustfs_tier_endpoint_inner(&private, true).is_err());
    }
}
