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

//! Exact support-bundle manifest and detached-signature documents.

use serde::Serialize;
use uuid::{Uuid, Variant, Version};

use super::collectors::DataClassification;
use super::redaction::{REDACTION_VERSION, RULESET_HASH};

pub(super) const FORMAT_VERSION: &str = "rustfs.connect.support.bundleManifest/1";
pub(super) const PROTOCOL_VERSION: &str = "v1";
pub(super) const CLASSIFICATION_REGISTRY_VERSION: u8 = 1;
pub(super) const SIGNATURE_ALGORITHM: &str = "ES256";
pub(super) const SIGNED_FILE: &str = "manifest.json";
pub(super) const SIGNATURE_FILE: &str = "manifest.sig";
pub(super) const DOMAIN_TAG: &str = "rustfs-support-bundle-v1";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BundleManifest<'a> {
    format_version: &'static str,
    protocol_version: &'static str,
    bundle_uid: &'a str,
    organization_name: &'a str,
    cluster_name: &'a str,
    device_name: &'a str,
    device_key_id: &'a str,
    nonce: &'a str,
    produced_at: &'a str,
    redaction_version: &'static str,
    ruleset_hash: &'static str,
    classification_registry_version: u8,
    entries: &'a [BundleManifestEntry],
}

impl<'a> BundleManifest<'a> {
    pub(super) fn new(
        identity: &'a BundleIdentity,
        device_key_id: &'a str,
        nonce: &'a str,
        produced_at: &'a str,
        entries: &'a [BundleManifestEntry],
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            protocol_version: PROTOCOL_VERSION,
            bundle_uid: &identity.bundle_uid,
            organization_name: &identity.organization_name,
            cluster_name: &identity.cluster_name,
            device_name: &identity.device_name,
            device_key_id,
            nonce,
            produced_at,
            redaction_version: REDACTION_VERSION,
            ruleset_hash: RULESET_HASH,
            classification_registry_version: CLASSIFICATION_REGISTRY_VERSION,
            entries,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BundleManifestEntry {
    pub(super) path: &'static str,
    #[serde(rename = "type")]
    pub(super) entry_type: &'static str,
    pub(super) size_bytes: u64,
    pub(super) sha256: String,
    pub(super) classification: DataClassification,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BundleSignature<'a> {
    algorithm: &'static str,
    key_id: &'a str,
    value: &'a str,
    signed_file: &'static str,
    domain_separation_tag: &'static str,
}

impl<'a> BundleSignature<'a> {
    pub(super) fn new(key_id: &'a str, value: &'a str) -> Self {
        Self {
            algorithm: SIGNATURE_ALGORITHM,
            key_id,
            value,
            signed_file: SIGNED_FILE,
            domain_separation_tag: DOMAIN_TAG,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct BundleIdentity {
    pub(super) bundle_uid: String,
    pub(super) organization_name: String,
    pub(super) cluster_name: String,
    pub(super) device_name: String,
}

impl BundleIdentity {
    pub(super) fn parse(bundle_uid: &str, device_name: &str) -> Option<Self> {
        if !is_uuid_v7(bundle_uid) {
            return None;
        }

        let parts = device_name.split('/').collect::<Vec<_>>();
        if parts.len() != 6
            || parts[0] != "organizations"
            || parts[2] != "clusters"
            || parts[4] != "clusterDevices"
            || !is_uuid_v7(parts[1])
            || !is_uuid_v7(parts[3])
            || !is_uuid_v7(parts[5])
        {
            return None;
        }

        Some(Self {
            bundle_uid: bundle_uid.to_owned(),
            organization_name: parts[..2].join("/"),
            cluster_name: parts[..4].join("/"),
            device_name: device_name.to_owned(),
        })
    }
}

fn is_uuid_v7(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| {
        uuid.get_version() == Some(Version::SortRand) && uuid.get_variant() == Variant::RFC4122 && uuid.to_string() == value
    })
}
