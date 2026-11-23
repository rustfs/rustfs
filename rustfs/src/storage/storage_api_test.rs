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

use crate::app::dto_values::stored_config;
use rustfs_gateway_types::cors_tagging::parse_cors;
use rustfs_gateway_types::persistence::{
    parse_bucket_encryption, parse_bucket_encryption_dto, parse_cors_dto, parse_object_lock, parse_versioning,
    parse_versioning_dto, parse_website, parse_website_dto, serialize_bucket_encryption_dto, serialize_cors_dto,
    serialize_versioning_dto, serialize_website_dto,
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
    let config = stored_config(&stored, parse_versioning_dto).expect("gateway configuration");
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
#[serial_test::serial]
fn object_lock_round_trips_the_default_retention() {
    use super::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient, shared_gating_ecstore};
    use crate::runtime_sources::{AppContext, ServerContextSlot};
    use crate::server::TestGateway;
    use bytes::Bytes;
    use http::{Method, StatusCode};
    use http_body_util::BodyExt;
    use md5::Digest;
    use std::sync::Arc;

    run_large_stack_test("bucket-config-object-lock", || async {
        let ambient = shared_gating_ambient().await;
        let store = shared_gating_ecstore().await;
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        let gateway = TestGateway::over(&context, slot);
        let bucket = format!("lock-config-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create object-lock bucket");

        let xml = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
            <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>";
        let target = format!("/{bucket}?object-lock");
        let checksum = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(xml));
        let put = gateway
            .signed(Method::PUT, &target, vec![("content-md5", checksum)], Bytes::from_static(xml))
            .await
            .unwrap();
        let put_status = put.status();
        let put_body = put.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(put_status, StatusCode::OK, "{}", String::from_utf8_lossy(&put_body));
        let (stored, _) = super::ecstore_bucket::metadata_sys::get_object_lock_config(&bucket)
            .await
            .expect("stored lock config");
        assert_eq!(stored.object_lock_enabled.as_deref(), Some("Enabled"));
        let get = gateway.signed(Method::GET, &target, vec![], Bytes::new()).await.unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        let body = get.into_body().collect().await.unwrap().to_bytes();
        let returned = parse_object_lock(&body).expect("get lock configuration");
        assert_eq!(returned, stored);
        let retention = returned.rule.unwrap().default_retention.unwrap();
        assert_eq!(retention.mode.as_deref(), Some("GOVERNANCE"));
        assert_eq!(retention.days, Some(30));
        assert_eq!(retention.years, None);
    });
}

#[test]
fn encryption_cors_and_website_round_trip() {
    let xml = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
        <KMSMasterKeyID>key</KMSMasterKeyID><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault>\
        <BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>";
    let stored = parse_bucket_encryption(xml).expect("stored encryption");
    assert!(stored.rules[0].blocked_encryption_types.is_none());
    let config = stored_config(&stored, parse_bucket_encryption_dto).expect("gateway encryption");
    assert_eq!(serialize_bucket_encryption_dto(&config).unwrap(), xml);

    let xml = b"<CORSConfiguration><CORSRule><AllowedHeader>*</AllowedHeader><AllowedMethod>GET</AllowedMethod>\
        <AllowedOrigin>https://example.com</AllowedOrigin><ID>r</ID><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>";
    let stored = parse_cors(xml).expect("stored CORS");
    assert!(stored.cors_rules[0].expose_headers.is_none());
    let config = stored_config(&stored, parse_cors_dto).expect("gateway CORS");
    assert!(config.cors_rules[0].expose_headers.is_empty());
    assert_eq!(serialize_cors_dto(&config), xml);

    let xml = b"<WebsiteConfiguration><ErrorDocument><Key>404.html</Key></ErrorDocument>\
        <IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>";
    let stored = parse_website(xml).expect("stored website");
    let config = stored_config(&stored, parse_website_dto).expect("gateway website");
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
