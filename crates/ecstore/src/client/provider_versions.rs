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

use std::io::{Error, ErrorKind};

use http::HeaderMap;

const X_AMZ_VERSION_ID: &str = "x-amz-version-id";
const X_OSS_VERSION_ID: &str = "x-oss-version-id";
const X_COS_VERSION_ID: &str = "x-cos-version-id";
const X_OBS_VERSION_ID: &str = "x-obs-version-id";
const MAX_REMOTE_VERSION_ID_LEN: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BucketVersioningState {
    Unknown,
    Disabled,
    Suspended,
    Enabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RemoteVersion {
    Unknown,
    Disabled,
    SuspendedNull,
    Exact(String),
}

impl RemoteVersion {
    pub(crate) fn exact_id(&self) -> Option<&str> {
        match self {
            Self::SuspendedNull => Some("null"),
            Self::Exact(version_id) => Some(version_id),
            Self::Unknown | Self::Disabled => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderVersionCapabilities {
    raw_version_header: Option<&'static str>,
    pub(crate) exact_get_delete: bool,
}

impl ProviderVersionCapabilities {
    pub(crate) fn for_tier_type(tier_type: &str) -> Self {
        if tier_type.eq_ignore_ascii_case("s3")
            || tier_type.eq_ignore_ascii_case("rustfs")
            || tier_type.eq_ignore_ascii_case("minio")
            || tier_type.eq_ignore_ascii_case("r2")
            || tier_type.eq_ignore_ascii_case("wasabi")
        {
            Self {
                raw_version_header: Some(X_AMZ_VERSION_ID),
                exact_get_delete: true,
            }
        } else if tier_type.eq_ignore_ascii_case("aliyun") {
            Self {
                raw_version_header: Some(X_OSS_VERSION_ID),
                exact_get_delete: true,
            }
        } else if tier_type.eq_ignore_ascii_case("tencent") {
            Self {
                raw_version_header: Some(X_COS_VERSION_ID),
                exact_get_delete: true,
            }
        } else if tier_type.eq_ignore_ascii_case("huaweicloud") {
            Self {
                raw_version_header: Some(X_OBS_VERSION_ID),
                exact_get_delete: true,
            }
        } else {
            Self {
                raw_version_header: None,
                exact_get_delete: false,
            }
        }
    }

    pub(crate) fn raw_version_id(self, headers: &HeaderMap) -> Result<Option<&str>, Error> {
        let Some(header_name) = self.raw_version_header else {
            return Ok(None);
        };
        let Some(value) = headers.get(header_name) else {
            return Ok(None);
        };
        let value = value
            .to_str()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "remote object version id is not valid ASCII"))?;
        validate_remote_version_id(value)?;
        Ok(Some(value))
    }

    pub(crate) fn remote_version(self, headers: &HeaderMap, versioning: BucketVersioningState) -> Result<RemoteVersion, Error> {
        let Some(value) = self.raw_version_id(headers)? else {
            return Ok(match versioning {
                BucketVersioningState::Disabled => RemoteVersion::Disabled,
                BucketVersioningState::Unknown | BucketVersioningState::Suspended | BucketVersioningState::Enabled => {
                    RemoteVersion::Unknown
                }
            });
        };
        if value == "null" {
            return Ok(RemoteVersion::SuspendedNull);
        }
        Ok(RemoteVersion::Exact(value.to_string()))
    }
}

fn validate_remote_version_id(version_id: &str) -> Result<(), Error> {
    if version_id.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "remote tier returned an empty object version id header",
        ));
    }
    if version_id.len() > MAX_REMOTE_VERSION_ID_LEN {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "remote tier returned an oversized object version id header",
        ));
    }
    if version_id.chars().any(char::is_control) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "remote tier returned an object version id containing control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BucketVersioningState, ProviderVersionCapabilities, RemoteVersion};
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn provider_version_header_matrix_preserves_opaque_versions() {
        for (tier_type, header_name) in [
            ("s3", "x-amz-version-id"),
            ("S3", "x-amz-version-id"),
            ("rustfs", "x-amz-version-id"),
            ("RustFS", "x-amz-version-id"),
            ("minio", "x-amz-version-id"),
            ("MinIO", "x-amz-version-id"),
            ("r2", "x-amz-version-id"),
            ("R2", "x-amz-version-id"),
            ("wasabi", "x-amz-version-id"),
            ("Wasabi", "x-amz-version-id"),
            ("aliyun", "x-oss-version-id"),
            ("Aliyun", "x-oss-version-id"),
            ("tencent", "x-cos-version-id"),
            ("Tencent", "x-cos-version-id"),
            ("huaweicloud", "x-obs-version-id"),
            ("Huaweicloud", "x-obs-version-id"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header_name, HeaderValue::from_static("opaque.version_01"));
            let capabilities = ProviderVersionCapabilities::for_tier_type(tier_type);

            assert_eq!(capabilities.raw_version_id(&headers).expect("raw version"), Some("opaque.version_01"));
            assert_eq!(
                capabilities
                    .remote_version(&headers, BucketVersioningState::Enabled)
                    .expect("remote version"),
                RemoteVersion::Exact("opaque.version_01".to_string())
            );
        }
    }

    #[test]
    fn provider_version_header_matrix_does_not_cross_read_sibling_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-version-id", HeaderValue::from_static("aws-version"));
        headers.insert("x-cos-version-id", HeaderValue::from_static("cos-version"));

        assert_eq!(
            ProviderVersionCapabilities::for_tier_type("s3")
                .raw_version_id(&headers)
                .expect("aws raw version"),
            Some("aws-version")
        );
        assert_eq!(
            ProviderVersionCapabilities::for_tier_type("tencent")
                .raw_version_id(&headers)
                .expect("cos raw version"),
            Some("cos-version")
        );
    }

    #[test]
    fn provider_version_missing_header_is_unknown_until_bucket_state_is_known() {
        let headers = HeaderMap::new();
        let capabilities = ProviderVersionCapabilities::for_tier_type("aliyun");

        assert_eq!(
            capabilities
                .remote_version(&headers, BucketVersioningState::Unknown)
                .expect("unknown versioning"),
            RemoteVersion::Unknown
        );
        assert_eq!(
            capabilities
                .remote_version(&headers, BucketVersioningState::Disabled)
                .expect("disabled versioning"),
            RemoteVersion::Disabled
        );
    }

    #[test]
    fn provider_version_rejects_empty_or_oversized_headers() {
        let oversized = "v".repeat(1025);
        for bad in ["", oversized.as_str()] {
            let mut headers = HeaderMap::new();
            headers.insert("x-oss-version-id", HeaderValue::from_str(bad).expect("test header value"));

            assert!(
                ProviderVersionCapabilities::for_tier_type("aliyun")
                    .raw_version_id(&headers)
                    .is_err()
            );
        }
    }
}
