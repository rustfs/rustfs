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
use url::Url;

use crate::client::{
    api_get_options::GetObjectOptions,
    api_list::ListObjectsOptions,
    api_put_object::PutObjectOptions,
    api_remove::{RemoveObjectOptions, RemoveObjectResult},
    api_s3_datatypes::ListVersionsResult,
    credentials::{Credentials, SignatureType, Static, Value},
    transition_api::{BucketLookupType, Options, TransitionClient, TransitionCore},
    transition_api::{ReadCloser, ReaderImpl},
};
use crate::error::ErrorResponse;
use crate::error::error_resp_to_object_err;
use crate::services::tier::{
    tier_config::TierS3,
    warm_backend::{TransitionCandidateProbe, WarmBackend, WarmBackendGetOpts, build_transition_put_options},
};
use http::HeaderMap;
use rustfs_utils::egress::validate_outbound_url;
use rustfs_utils::path::SLASH_SEPARATOR;
use s3s::dto::BucketVersioningStatus;

pub struct WarmBackendS3 {
    pub client: Arc<TransitionClient>,
    pub core: TransitionCore,
    pub bucket: String,
    pub prefix: String,
    pub storage_class: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoteBucketVersioning {
    Disabled,
    Suspended,
    Enabled,
}

fn remote_bucket_versioning_from_status(status: Option<&str>) -> Result<RemoteBucketVersioning, std::io::Error> {
    Ok(match status {
        Some(BucketVersioningStatus::ENABLED) => RemoteBucketVersioning::Enabled,
        Some(BucketVersioningStatus::SUSPENDED) => RemoteBucketVersioning::Suspended,
        Some(status) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("remote tier bucket returned unsupported versioning status {status}"),
            ));
        }
        None => RemoteBucketVersioning::Disabled,
    })
}

impl WarmBackendS3 {
    pub async fn new(conf: &TierS3, _tier: &str) -> Result<Self, std::io::Error> {
        Self::new_with_bucket_lookup(conf, BucketLookupType::BucketLookupAuto, "s3").await
    }

    pub(crate) async fn new_with_bucket_lookup(
        conf: &TierS3,
        bucket_lookup: BucketLookupType,
        tier_type: &str,
    ) -> Result<Self, std::io::Error> {
        let u = match Url::parse(&conf.endpoint) {
            Ok(u) => u,
            Err(err) => {
                return Err(std::io::Error::other(err.to_string()));
            }
        };
        validate_outbound_url(&u).map_err(|err| std::io::Error::other(format!("tier endpoint is not allowed: {err}")))?;

        if conf.aws_role_web_identity_token_file == "" && conf.aws_role_arn != ""
            || conf.aws_role_web_identity_token_file != "" && conf.aws_role_arn == ""
        {
            return Err(std::io::Error::other("both the token file and the role ARN are required"));
        } else if conf.access_key == "" && conf.secret_key != "" || conf.access_key != "" && conf.secret_key == "" {
            return Err(std::io::Error::other("both the access and secret keys are required"));
        } else if conf.aws_role
            && (conf.aws_role_web_identity_token_file != ""
                || conf.aws_role_arn != ""
                || conf.access_key != ""
                || conf.secret_key != "")
        {
            return Err(std::io::Error::other(
                "AWS Role cannot be activated with static credentials or the web identity token file",
            ));
        } else if conf.bucket == "" {
            return Err(std::io::Error::other("no bucket name was provided"));
        }

        let creds: Credentials<Static>;

        if conf.access_key != "" && conf.secret_key != "" {
            //creds = Credentials::new_static_v4(conf.access_key, conf.secret_key, "");
            creds = Credentials::new(Static(Value {
                access_key_id: conf.access_key.clone(),
                secret_access_key: conf.secret_key.clone(),
                session_token: "".to_string(),
                signer_type: SignatureType::SignatureV4,
                ..Default::default()
            }));
        } else {
            return Err(std::io::Error::other("insufficient parameters for S3 backend authentication"));
        }
        let opts = Options {
            creds,
            secure: u.scheme() == "https",
            region: conf.region.clone(),
            bucket_lookup,
            ..Default::default()
        };
        let host = u
            .host()
            .ok_or_else(|| std::io::Error::other("Invalid endpoint URL: missing host"))?;
        let client = TransitionClient::new(&host.to_string(), opts, tier_type).await?;

        let client = Arc::new(client);
        let core = TransitionCore(Arc::clone(&client));
        Ok(Self {
            client,
            core,
            bucket: conf.bucket.clone(),
            prefix: conf.prefix.clone().trim_matches('/').to_string(),
            storage_class: conf.storage_class.clone(),
        })
    }

    pub fn get_dest(&self, object: &str) -> String {
        let mut dest_obj = object.to_string();
        if self.prefix != "" {
            dest_obj = format!("{}/{}", &self.prefix, object);
        }
        return dest_obj;
    }

    pub(crate) async fn remove_with_result(&self, object: &str, rv: &str) -> Result<RemoveObjectResult, std::io::Error> {
        let mut opts = RemoveObjectOptions::default();
        if !rv.is_empty() {
            opts.version_id = rv.to_string();
        }
        self.client
            .remove_object_inner(&self.bucket, &self.get_dest(object), opts)
            .await
    }

    pub(crate) async fn get_with_headers(
        &self,
        object: &str,
        rv: &str,
        opts: WarmBackendGetOpts,
    ) -> Result<(HeaderMap, ReadCloser), std::io::Error> {
        let mut gopts = GetObjectOptions::default();

        if !rv.is_empty() {
            gopts.version_id = rv.to_string();
        }
        if opts.start_offset >= 0 && opts.length > 0 {
            gopts
                .set_range(opts.start_offset, opts.start_offset + opts.length - 1)
                .map_err(std::io::Error::other)?;
        }
        let (_, headers, reader) = self.core.get_object(&self.bucket, &self.get_dest(object), &gopts).await?;
        Ok((headers, reader))
    }

    async fn remote_bucket_versioning(&self) -> Result<RemoteBucketVersioning, std::io::Error> {
        let config = self.client.get_bucket_versioning(&self.bucket).await?;
        remote_bucket_versioning_from_status(config.status.as_ref().map(|status| status.as_str()))
    }

    async fn list_transition_candidate_versions(&self, object: &str) -> Result<ListVersionsResult, std::io::Error> {
        let mut opts = ListObjectsOptions::default();
        opts.set("prefix", &self.get_dest(object));
        opts.set("max-keys", "2");
        self.client.list_object_versions_query(&self.bucket, &opts, "", "", "").await
    }
}

fn classify_transition_candidate_versions(
    remote_object: &str,
    bucket_versioning: RemoteBucketVersioning,
    versions: &ListVersionsResult,
) -> TransitionCandidateProbe {
    if versions.is_truncated {
        return TransitionCandidateProbe::Ambiguous;
    }

    if versions.delete_markers.iter().any(|marker| marker.key == remote_object) {
        return TransitionCandidateProbe::Ambiguous;
    }

    let mut exact_versions = versions.versions.iter().filter(|version| version.key == remote_object);
    let Some(version) = exact_versions.next() else {
        return TransitionCandidateProbe::Missing;
    };
    if exact_versions.next().is_some() {
        return TransitionCandidateProbe::Ambiguous;
    }

    match bucket_versioning {
        RemoteBucketVersioning::Disabled => TransitionCandidateProbe::UnversionedPresent,
        RemoteBucketVersioning::Suspended if version.version_id == "null" => {
            TransitionCandidateProbe::VersionedPresent(version.version_id.clone())
        }
        RemoteBucketVersioning::Suspended | RemoteBucketVersioning::Enabled if !version.version_id.is_empty() => {
            TransitionCandidateProbe::VersionedPresent(version.version_id.clone())
        }
        RemoteBucketVersioning::Suspended | RemoteBucketVersioning::Enabled => TransitionCandidateProbe::Ambiguous,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::api_s3_datatypes::{ListVersionsResult, Version};

    #[tokio::test]
    async fn new_rejects_loopback_endpoint_before_network_setup() {
        let conf = TierS3 {
            endpoint: "https://127.0.0.1:9000".to_string(),
            bucket: "tier-bucket".to_string(),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        match WarmBackendS3::new(&conf, "tier").await {
            Ok(_) => panic!("loopback endpoint should be rejected"),
            Err(err) => assert!(err.to_string().contains("not allowed")),
        }
    }

    fn list_versions(versions: &[(&str, &str)], delete_markers: &[(&str, &str)], is_truncated: bool) -> ListVersionsResult {
        ListVersionsResult {
            versions: versions
                .iter()
                .map(|(key, version_id)| Version {
                    key: (*key).to_string(),
                    version_id: (*version_id).to_string(),
                    ..Default::default()
                })
                .collect(),
            delete_markers: delete_markers
                .iter()
                .map(|(key, version_id)| Version {
                    key: (*key).to_string(),
                    version_id: (*version_id).to_string(),
                    ..Default::default()
                })
                .collect(),
            is_truncated,
            ..Default::default()
        }
    }

    #[test]
    fn transition_candidate_probe_classifier_is_fail_closed() {
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Disabled,
                &list_versions(&[], &[], false),
            ),
            TransitionCandidateProbe::Missing
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Disabled,
                &list_versions(&[("archive/object", "")], &[], false),
            ),
            TransitionCandidateProbe::UnversionedPresent
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Enabled,
                &list_versions(&[("archive/object", "version-a")], &[], false),
            ),
            TransitionCandidateProbe::VersionedPresent("version-a".to_string())
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Suspended,
                &list_versions(&[("archive/object", "null")], &[], false),
            ),
            TransitionCandidateProbe::VersionedPresent("null".to_string())
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Enabled,
                &list_versions(&[("archive/object", "")], &[], false),
            ),
            TransitionCandidateProbe::Ambiguous
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Enabled,
                &list_versions(&[("archive/object", "version-a"), ("archive/object", "version-b")], &[], false),
            ),
            TransitionCandidateProbe::Ambiguous
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Enabled,
                &list_versions(&[("archive/object", "version-a")], &[("archive/object", "marker-a")], false),
            ),
            TransitionCandidateProbe::Ambiguous
        );
        assert_eq!(
            classify_transition_candidate_versions(
                "archive/object",
                RemoteBucketVersioning::Enabled,
                &list_versions(&[("archive/object", "version-a")], &[], true),
            ),
            TransitionCandidateProbe::Ambiguous
        );
    }

    #[test]
    fn remote_bucket_versioning_status_parser_fails_closed() {
        assert_eq!(
            remote_bucket_versioning_from_status(None).expect("absent status means disabled"),
            RemoteBucketVersioning::Disabled
        );
        assert_eq!(
            remote_bucket_versioning_from_status(Some(BucketVersioningStatus::ENABLED)).expect("enabled status should parse"),
            RemoteBucketVersioning::Enabled
        );
        assert_eq!(
            remote_bucket_versioning_from_status(Some(BucketVersioningStatus::SUSPENDED)).expect("suspended status should parse"),
            RemoteBucketVersioning::Suspended
        );
        let err = remote_bucket_versioning_from_status(Some("UnexpectedStatus"))
            .expect_err("unknown versioning status must fail closed");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}

#[async_trait::async_trait]
impl WarmBackend for WarmBackendS3 {
    async fn put_with_meta(
        &self,
        object: &str,
        r: ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> Result<String, std::io::Error> {
        let client = self.client.clone();
        let res = client
            .put_object(&self.bucket, &self.get_dest(object), r, length, &{
                let mut opts = build_transition_put_options(self.storage_class.clone(), meta);
                opts.send_content_md5 = true;
                opts
            })
            .await?;
        Ok(res.version_id)
    }

    async fn put(&self, object: &str, r: ReaderImpl, length: i64) -> Result<String, std::io::Error> {
        self.put_with_meta(object, r, length, HashMap::new()).await
    }

    async fn get(&self, object: &str, rv: &str, opts: WarmBackendGetOpts) -> Result<ReadCloser, std::io::Error> {
        self.get_with_headers(object, rv, opts).await.map(|(_, reader)| reader)
    }

    async fn remove(&self, object: &str, rv: &str) -> Result<(), std::io::Error> {
        self.remove_with_result(object, rv).await.map(|_| ())
    }

    async fn probe_transition_candidate(&self, object: &str) -> Result<TransitionCandidateProbe, std::io::Error> {
        let bucket_versioning = self.remote_bucket_versioning().await?;
        let versions = self.list_transition_candidate_versions(object).await?;
        Ok(classify_transition_candidate_versions(
            &self.get_dest(object),
            bucket_versioning,
            &versions,
        ))
    }

    async fn in_use(&self) -> Result<bool, std::io::Error> {
        let result = self
            .core
            .list_objects_v2(&self.bucket, &self.prefix, "", "", SLASH_SEPARATOR, 1)
            .await?;

        Ok(result.common_prefixes.len() > 0 || result.contents.len() > 0)
    }
}
