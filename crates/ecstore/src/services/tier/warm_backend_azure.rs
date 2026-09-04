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
        S3CompatibleWarmBackendParams, TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts, build_transition_put_options,
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

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        // Azure currently uses the shared S3/SigV4 transport, but its normal
        // object path cannot persist exact remote versions across mixed
        // RustFS releases. The mutation probe may still detect and precisely
        // remove a versioned test object before rejecting that configuration.
        self.0
            .probe_transition_candidate_with_raw_version_header(object, "x-amz-version-id")
            .await
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        self.0.in_use().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::tier::tier_config::TierAzure;
    use rustfs_s3_client::{
        credentials::{Credentials, SignatureType, Static, Value},
        transition_api::{Options, TransitionClient, TransitionCore},
    };
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn read_request_head(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).await.expect("fixture request should be readable");
            assert_ne!(read, 0, "connection closed before request headers were received");
            request.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

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

    #[tokio::test]
    async fn versioned_candidate_cleanup_uses_the_exact_s3_version_without_enabling_data_versions() {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("test listener should bind: {err}"),
        };
        let endpoint = listener
            .local_addr()
            .expect("listener local address should be available")
            .to_string();
        let fixture = tokio::spawn(async move {
            let (mut get_stream, _) = listener.accept().await.expect("fixture should accept candidate GET");
            let get_request = read_request_head(&mut get_stream).await;
            get_stream
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nx-amz-version-id: azure-version\r\nConnection: close\r\n\r\nx",
                )
                .await
                .expect("fixture should write candidate GET response");

            let (mut delete_stream, _) = listener.accept().await.expect("fixture should accept exact DELETE");
            let delete_request = read_request_head(&mut delete_stream).await;
            delete_stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("fixture should write exact DELETE response");
            (get_request, delete_request)
        });
        let client = Arc::new(
            TransitionClient::new(
                &endpoint,
                Options {
                    creds: Credentials::new(Static(Value {
                        access_key_id: "access-key".to_string(),
                        secret_access_key: "secret-key".to_string(),
                        signer_type: SignatureType::SignatureV4,
                        ..Default::default()
                    })),
                    region: "us-east-1".to_string(),
                    bucket_lookup: BucketLookupType::BucketLookupPath,
                    max_retries: 1,
                    ..Default::default()
                },
                "azure",
            )
            .await
            .expect("fixture client should build"),
        );
        let backend = WarmBackendAzure(WarmBackendS3 {
            core: TransitionCore(Arc::clone(&client)),
            client,
            bucket: "bucket".to_string(),
            prefix: String::new(),
            storage_class: String::new(),
        });
        assert!(
            !backend.0.client.provider_version_capabilities().exact_get_delete,
            "probe-only version discovery must not change Azure's persisted data-path contract"
        );

        let candidate = backend
            .probe_transition_candidate("probe")
            .await
            .expect("Azure candidate should be discovered");
        assert_eq!(candidate, TransitionCandidateProbe::VersionedPresent("azure-version".to_string()));
        backend
            .remove_exact("probe", "azure-version")
            .await
            .expect("Azure candidate should be deleted by exact version");

        let (get_request, delete_request) = fixture.await.expect("fixture should join");
        assert!(get_request.to_ascii_lowercase().contains("\r\nrange: bytes=0-0\r\n"));
        assert!(
            delete_request
                .lines()
                .next()
                .is_some_and(|line| line.contains("DELETE /bucket/probe?versionId=azure-version "))
        );
    }
}
