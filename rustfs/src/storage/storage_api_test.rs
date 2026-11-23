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

use rustfs_gateway_types::cors_tagging::parse_cors;
use rustfs_gateway_types::persistence::{
    parse_bucket_encryption, parse_bucket_encryption_dto, parse_cors_dto, parse_versioning, parse_versioning_dto, parse_website,
    parse_website_dto, serialize_bucket_encryption_dto, serialize_cors_dto, serialize_versioning_dto, serialize_website_dto,
};

#[test]
fn versioning_round_trips_including_the_minio_extensions() {
    let xml = b"<VersioningConfiguration><ExcludeFolders>true</ExcludeFolders>\
        <ExcludedPrefixes><Prefix>tmp/</Prefix></ExcludedPrefixes><Status>Enabled</Status></VersioningConfiguration>";
    let stored = parse_versioning(xml).expect("stored versioning");
    assert_eq!(stored.status.as_deref(), Some("Enabled"));
    assert_eq!(stored.exclude_folders, Some(true));
    assert_eq!(stored.excluded_prefixes, Some(vec![Some("tmp/".to_owned())]));
    assert!(stored.mfa_delete.is_none());
    let encoded = super::ecstore_bucket::utils::serialize(&stored).expect("serialize stored versioning");
    let config = parse_versioning_dto(&encoded).expect("gateway configuration");
    assert_eq!(serialize_versioning_dto(&config), xml);
}

#[test]
fn an_unknown_versioning_status_survives_the_round_trip_for_the_use_case_to_refuse() {
    let xml = b"<VersioningConfiguration><Status>Invalid</Status></VersioningConfiguration>";
    let config = parse_versioning_dto(xml).expect("string-backed gateway status");
    let stored = parse_versioning(&serialize_versioning_dto(&config)).expect("prepare versioning update");
    assert_eq!(stored.status.as_deref(), Some("Invalid"));
}

#[test]
fn encryption_cors_and_website_round_trip() {
    let xml = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
        <KMSMasterKeyID>key</KMSMasterKeyID><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
        <BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>";
    let stored = parse_bucket_encryption(xml).expect("stored encryption");
    assert!(stored.rules[0].blocked_encryption_types.is_none());
    let encoded = super::ecstore_bucket::utils::serialize(&stored).expect("serialize stored encryption");
    let config = parse_bucket_encryption_dto(&encoded).expect("gateway encryption");
    assert_eq!(serialize_bucket_encryption_dto(&config).unwrap(), xml);

    let xml = b"<CORSConfiguration><CORSRule><AllowedHeader>*</AllowedHeader><AllowedMethod>GET</AllowedMethod>\
        <AllowedOrigin>https://example.com</AllowedOrigin><ID>r</ID><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>";
    let stored = parse_cors(xml).expect("stored CORS");
    assert!(stored.cors_rules[0].expose_headers.is_none());
    let encoded = super::ecstore_bucket::utils::serialize(&stored).expect("serialize stored CORS");
    let config = parse_cors_dto(&encoded).expect("gateway CORS");
    assert!(config.cors_rules[0].expose_headers.is_empty());
    assert_eq!(serialize_cors_dto(&config), xml);

    let xml = b"<WebsiteConfiguration><ErrorDocument><Key>404.html</Key></ErrorDocument>\
        <IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>";
    let stored = parse_website(xml).expect("stored website");
    let encoded = super::ecstore_bucket::utils::serialize(&stored).expect("serialize stored website");
    let config = parse_website_dto(&encoded).expect("gateway website");
    assert!(config.redirect_all_requests_to.is_none());
    assert!(config.routing_rules.is_none());
    assert_eq!(serialize_website_dto(&config), xml);
}

#[test]
fn tagging_keeps_keyless_tags() {
    use rustfs_gateway_types::persistence::{parse_tagging_dto, serialize_tagging_dto};

    let xml = b"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag><Tag></Tag></TagSet></Tagging>";
    let tagging = parse_tagging_dto(xml).expect("read stored tags");
    let pairs: Vec<_> = tagging
        .tag_set
        .iter()
        .map(|tag| (tag.key.as_deref(), tag.value.as_deref()))
        .collect();
    assert_eq!(pairs, [(Some("k"), Some("v")), (None, None)]);
    let stored = rustfs_gateway_types::cors_tagging::parse_tagging(&serialize_tagging_dto(&tagging)).expect("read back tags");
    assert_eq!(stored, rustfs_gateway_types::cors_tagging::parse_tagging(xml).unwrap());
}
