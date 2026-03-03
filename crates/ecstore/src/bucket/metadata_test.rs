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

use super::metadata::BucketMetadata;
use time::OffsetDateTime;

/// Full BucketMetadata hex (all fields populated).
const TEST_BUCKET_METADATA_HEX: &str = "de0019a44e616d65b27275737466732d636f6d7061742d74657374a743726561746564c70c050000000065920080075bcd15ab4c6f636b456e61626c6564c3b0506f6c696379436f6e6669674a534f4ec4907b2256657273696f6e223a22323031322d31302d3137222c2253746174656d656e74223a5b7b22456666656374223a22416c6c6f77222c225072696e636970616c223a222a222c22416374696f6e223a2273333a4765744f626a656374222c225265736f75726365223a2261726e3a6177733a73333a3a3a7275737466732d636f6d7061742d746573742f2a227d5d7db54e6f74696669636174696f6e436f6e666967584d4cc4963c4e6f74696669636174696f6e436f6e66696775726174696f6e3e3c436c6f75645761746368436f6e66696775726174696f6e3e3c49643e6e313c2f49643e3c4576656e743e73333a4f626a656374437265617465643a2a3c2f4576656e743e3c2f436c6f75645761746368436f6e66696775726174696f6e3e3c2f4e6f74696669636174696f6e436f6e66696775726174696f6e3eb24c6966656379636c65436f6e666967584d4cc48c3c4c6966656379636c65436f6e66696775726174696f6e3e3c52756c653e3c49443e72756c65313c2f49443e3c5374617475733e456e61626c65643c2f5374617475733e3c45787069726174696f6e3e3c446179733e33303c2f446179733e3c2f45787069726174696f6e3e3c2f52756c653e3c2f4c6966656379636c65436f6e66696775726174696f6e3eb34f626a6563744c6f636b436f6e666967584d4cc4b83c4f626a6563744c6f636b436f6e66696775726174696f6e3e3c4f626a6563744c6f636b456e61626c65643e456e61626c65643c2f4f626a6563744c6f636b456e61626c65643e3c52756c653e3c44656661756c74526574656e74696f6e3e3c4d6f64653e474f5645524e414e43453c2f4d6f64653e3c446179733e373c2f446179733e3c2f44656661756c74526574656e74696f6e3e3c2f52756c653e3c2f4f626a6563744c6f636b436f6e66696775726174696f6e3eb356657273696f6e696e67436f6e666967584d4cc44b3c56657273696f6e696e67436f6e66696775726174696f6e3e3c5374617475733e456e61626c65643c2f5374617475733e3c2f56657273696f6e696e67436f6e66696775726174696f6e3eb3456e6372797074696f6e436f6e666967584d4cc4c03c53657276657253696465456e6372797074696f6e436f6e66696775726174696f6e3e3c52756c653e3c4170706c7953657276657253696465456e6372797074696f6e427944656661756c743e3c535345416c676f726974686d3e4145533235363c2f535345416c676f726974686d3e3c2f4170706c7953657276657253696465456e6372797074696f6e427944656661756c743e3c2f52756c653e3c2f53657276657253696465456e6372797074696f6e436f6e66696775726174696f6e3eb054616767696e67436f6e666967584d4cc4503c54616767696e673e3c5461675365743e3c5461673e3c4b65793e456e763c2f4b65793e3c56616c75653e546573743c2f56616c75653e3c2f5461673e3c2f5461675365743e3c2f54616767696e673eaf51756f7461436f6e6669674a534f4ec4707b2271756f7461223a313037333734313832342c2271756f74615f74797065223a2248617264222c22637265617465645f6174223a22323032342d30312d30315430303a30303a30305a222c22757064617465645f6174223a22323032342d30312d30315430303a30303a30305a227db45265706c69636174696f6e436f6e666967584d4cc4e73c5265706c69636174696f6e436f6e66696775726174696f6e3e3c526f6c653e61726e3a6177733a69616d3a3a3132333435363738393031323a726f6c652f7265706c3c2f526f6c653e3c52756c653e3c49443e72313c2f49443e3c5374617475733e456e61626c65643c2f5374617475733e3c5072656669783e646f632f3c2f5072656669783e3c44657374696e6174696f6e3e3c4275636b65743e61726e3a6177733a73333a3a3a646573743c2f4275636b65743e3c2f44657374696e6174696f6e3e3c2f52756c653e3c2f5265706c69636174696f6e436f6e66696775726174696f6e3eb74275636b657454617267657473436f6e6669674a534f4ec4535b7b22656e64706f696e74223a22687474703a2f2f7461726765742e6578616d706c652e636f6d222c227461726765744275636b6574223a227462222c22726567696f6e223a2275732d656173742d31227d5dbb4275636b657454617267657473436f6e6669674d6574614a534f4ec42d7b227265706c69636174696f6e4964223a227265706c2d31222c2273796e634d6f6465223a226173796e63227db5506f6c696379436f6e666967557064617465644174c70c050000000065a5022000000000b94f626a6563744c6f636b436f6e666967557064617465644174c70c050000000065a5022000000000b9456e6372797074696f6e436f6e666967557064617465644174c70c050000000065a5022000000000b654616767696e67436f6e666967557064617465644174c70c050000000065a5022000000000b451756f7461436f6e666967557064617465644174c70c050000000065a5022000000000ba5265706c69636174696f6e436f6e666967557064617465644174c70c050000000065a5022000000000b956657273696f6e696e67436f6e666967557064617465644174c70c050000000065a5022000000000b84c6966656379636c65436f6e666967557064617465644174c70c050000000065a5022000000000bb4e6f74696669636174696f6e436f6e666967557064617465644174c70c050000000065a5022000000000bc4275636b657454617267657473436f6e666967557064617465644174c70c050000000065a5022000000000d9204275636b657454617267657473436f6e6669674d657461557064617465644174c70c050000000065a5022000000000";

#[tokio::test]
async fn marshal_msg() {
    let bm = BucketMetadata::new("dada");

    let buf = bm.marshal_msg().unwrap();

    let new = BucketMetadata::unmarshal(&buf).unwrap();

    assert_eq!(bm.name, new.name);
}

/// Verifies that serialized time uses msgp ext type 5.
#[tokio::test]
async fn marshal_msg_uses_time_format() {
    let mut bm = BucketMetadata::new("test-bucket");
    bm.created = OffsetDateTime::from_unix_timestamp(1704067200).unwrap(); // 2024-01-01 00:00:00 UTC

    let buf = bm.marshal_msg().unwrap();

    // msgp uses ext8 (0xc7), len 12, type 5 for time
    assert!(
        buf.windows(3).any(|w| w == [0xc7, 0x0c, 0x05]),
        "serialized data should contain msgp time ext (0xc7 0x0c 0x05)"
    );
}

#[tokio::test]
async fn unmarshal_test_bucket_metadata() {
    use faster_hex::hex_decode;

    let mut bytes = vec![0u8; TEST_BUCKET_METADATA_HEX.len() / 2];
    hex_decode(TEST_BUCKET_METADATA_HEX.as_bytes(), &mut bytes).expect("valid hex");
    let bm = BucketMetadata::unmarshal(&bytes).expect("RustFS must unmarshal MinIO format");

    assert_eq!(bm.name, "rustfs-compat-test");
    assert_eq!(bm.created.unix_timestamp(), 1704067200);
    assert_eq!(bm.created.nanosecond(), 123456789);
    assert!(bm.lock_enabled);

    assert!(!bm.policy_config_json.is_empty());
    assert!(bm.policy_config_json.starts_with(b"{\"Version\""));
    assert!(!bm.notification_config_xml.is_empty());
    assert!(bm.notification_config_xml.starts_with(b"<Notification"));
    assert!(!bm.lifecycle_config_xml.is_empty());
    assert!(bm.lifecycle_config_xml.starts_with(b"<Lifecycle"));
    assert!(!bm.object_lock_config_xml.is_empty());
    assert!(bm.object_lock_config_xml.starts_with(b"<ObjectLock"));
    assert!(!bm.versioning_config_xml.is_empty());
    assert!(bm.versioning_config_xml.starts_with(b"<Versioning"));
    assert!(!bm.encryption_config_xml.is_empty());
    assert!(bm.encryption_config_xml.starts_with(b"<ServerSide"));
    assert!(!bm.tagging_config_xml.is_empty());
    assert!(bm.tagging_config_xml.starts_with(b"<Tagging"));
    assert!(!bm.quota_config_json.is_empty());
    assert!(bm.quota_config_json.starts_with(b"{\"quota\""));
    assert!(!bm.replication_config_xml.is_empty());
    assert!(bm.replication_config_xml.starts_with(b"<Replication"));
    assert!(!bm.bucket_targets_config_json.is_empty());
    assert!(bm.bucket_targets_config_json.starts_with(b"[{"));
    assert!(!bm.bucket_targets_config_meta_json.is_empty());
    assert!(bm.bucket_targets_config_meta_json.starts_with(b"{\"replication"));

    let updated_sec = 1705312800; // 2024-01-15 12:00:00 UTC
    assert_eq!(bm.policy_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.object_lock_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.encryption_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.tagging_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.quota_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.replication_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.versioning_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.lifecycle_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.notification_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.bucket_targets_config_updated_at.unix_timestamp(), updated_sec);
    assert_eq!(bm.bucket_targets_config_meta_updated_at.unix_timestamp(), updated_sec);

    assert!(bm.cors_config_xml.is_empty());
    assert!(bm.public_access_block_config_xml.is_empty());
    assert!(bm.bucket_acl_config_json.is_empty());
}

#[tokio::test]
async fn marshal_msg_complete_example() {
    // Create a complete BucketMetadata with various configurations
    let mut bm = BucketMetadata::new("test-bucket");

    // Set creation time to current time
    bm.created = OffsetDateTime::now_utc();
    bm.lock_enabled = true;

    // Add policy configuration
    let policy_json = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#;
    bm.policy_config_json = policy_json.as_bytes().to_vec();
    bm.policy_config_updated_at = OffsetDateTime::now_utc();

    // Add lifecycle configuration
    let lifecycle_xml = r#"<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;
    bm.lifecycle_config_xml = lifecycle_xml.as_bytes().to_vec();
    bm.lifecycle_config_updated_at = OffsetDateTime::now_utc();

    // Add versioning configuration
    let versioning_xml = r#"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"#;
    bm.versioning_config_xml = versioning_xml.as_bytes().to_vec();
    bm.versioning_config_updated_at = OffsetDateTime::now_utc();

    // Add encryption configuration
    let encryption_xml = r#"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"#;
    bm.encryption_config_xml = encryption_xml.as_bytes().to_vec();
    bm.encryption_config_updated_at = OffsetDateTime::now_utc();

    // Add tagging configuration
    let tagging_xml = r#"<Tagging><TagSet><Tag><Key>Environment</Key><Value>Test</Value></Tag><Tag><Key>Owner</Key><Value>RustFS</Value></Tag></TagSet></Tagging>"#;
    bm.tagging_config_xml = tagging_xml.as_bytes().to_vec();
    bm.tagging_config_updated_at = OffsetDateTime::now_utc();

    // Add quota configuration
    let quota_json =
        r#"{"quota":1073741824,"quota_type":"Hard","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}"#; // 1GB quota
    bm.quota_config_json = quota_json.as_bytes().to_vec();
    bm.quota_config_updated_at = OffsetDateTime::now_utc();

    // Add object lock configuration
    let object_lock_xml = r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#;
    bm.object_lock_config_xml = object_lock_xml.as_bytes().to_vec();
    bm.object_lock_config_updated_at = OffsetDateTime::now_utc();

    // Add notification configuration
    let notification_xml = r#"<NotificationConfiguration><CloudWatchConfiguration><Id>notification1</Id><Event>s3:ObjectCreated:*</Event><CloudWatchConfiguration><LogGroupName>test-log-group</LogGroupName></CloudWatchConfiguration></CloudWatchConfiguration></NotificationConfiguration>"#;
    bm.notification_config_xml = notification_xml.as_bytes().to_vec();
    bm.notification_config_updated_at = OffsetDateTime::now_utc();

    // Add replication configuration
    let replication_xml = r#"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication-role</Role><Rule><ID>rule1</ID><Status>Enabled</Status><Prefix>documents/</Prefix><Destination><Bucket>arn:aws:s3:::destination-bucket</Bucket></Destination></Rule></ReplicationConfiguration>"#;
    bm.replication_config_xml = replication_xml.as_bytes().to_vec();
    bm.replication_config_updated_at = OffsetDateTime::now_utc();

    // Add bucket targets configuration
    let bucket_targets_json = r#"[{"endpoint":"http://target1.example.com","credentials":{"accessKey":"key1","secretKey":"secret1"},"targetBucket":"target-bucket-1","region":"us-east-1"},{"endpoint":"http://target2.example.com","credentials":{"accessKey":"key2","secretKey":"secret2"},"targetBucket":"target-bucket-2","region":"us-west-2"}]"#;
    bm.bucket_targets_config_json = bucket_targets_json.as_bytes().to_vec();
    bm.bucket_targets_config_updated_at = OffsetDateTime::now_utc();

    // Add bucket targets meta configuration
    let bucket_targets_meta_json = r#"{"replicationId":"repl-123","syncMode":"async","bandwidth":"100MB"}"#;
    bm.bucket_targets_config_meta_json = bucket_targets_meta_json.as_bytes().to_vec();
    bm.bucket_targets_config_meta_updated_at = OffsetDateTime::now_utc();

    // Add public access block configuration
    let public_access_block_xml = r#"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>"#;
    bm.public_access_block_config_xml = public_access_block_xml.as_bytes().to_vec();
    bm.public_access_block_config_updated_at = OffsetDateTime::now_utc();

    let bucket_acl = r#"{"owner":{"id":"rustfsadmin","display_name":"RustFS Tester"},"grants":[{"grantee":{"grantee_type":"CanonicalUser","id":"rustfsadmin","display_name":"RustFS Tester","uri":null,"email_address":null},"permission":"FULL_CONTROL"}]}"#;
    bm.bucket_acl_config_json = bucket_acl.as_bytes().to_vec();
    bm.bucket_acl_config_updated_at = OffsetDateTime::now_utc();

    // Test serialization
    let buf = bm.marshal_msg().unwrap();
    assert!(!buf.is_empty(), "Serialized buffer should not be empty");

    // Test deserialization
    let deserialized_bm = BucketMetadata::unmarshal(&buf).unwrap();

    // Verify all fields are correctly serialized and deserialized
    assert_eq!(bm.name, deserialized_bm.name);
    assert_eq!(bm.created.unix_timestamp(), deserialized_bm.created.unix_timestamp());
    assert_eq!(bm.lock_enabled, deserialized_bm.lock_enabled);

    // Verify configuration data
    assert_eq!(bm.policy_config_json, deserialized_bm.policy_config_json);
    assert_eq!(bm.lifecycle_config_xml, deserialized_bm.lifecycle_config_xml);
    assert_eq!(bm.versioning_config_xml, deserialized_bm.versioning_config_xml);
    assert_eq!(bm.encryption_config_xml, deserialized_bm.encryption_config_xml);
    assert_eq!(bm.tagging_config_xml, deserialized_bm.tagging_config_xml);
    assert_eq!(bm.quota_config_json, deserialized_bm.quota_config_json);
    assert_eq!(bm.public_access_block_config_xml, deserialized_bm.public_access_block_config_xml);
    assert_eq!(bm.bucket_acl_config_json, deserialized_bm.bucket_acl_config_json);
    assert_eq!(bm.object_lock_config_xml, deserialized_bm.object_lock_config_xml);
    assert_eq!(bm.notification_config_xml, deserialized_bm.notification_config_xml);
    assert_eq!(bm.replication_config_xml, deserialized_bm.replication_config_xml);
    assert_eq!(bm.bucket_targets_config_json, deserialized_bm.bucket_targets_config_json);
    assert_eq!(bm.bucket_targets_config_meta_json, deserialized_bm.bucket_targets_config_meta_json);

    // Verify timestamps (comparing unix timestamps to avoid precision issues)
    assert_eq!(
        bm.policy_config_updated_at.unix_timestamp(),
        deserialized_bm.policy_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.lifecycle_config_updated_at.unix_timestamp(),
        deserialized_bm.lifecycle_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.versioning_config_updated_at.unix_timestamp(),
        deserialized_bm.versioning_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.encryption_config_updated_at.unix_timestamp(),
        deserialized_bm.encryption_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.tagging_config_updated_at.unix_timestamp(),
        deserialized_bm.tagging_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.quota_config_updated_at.unix_timestamp(),
        deserialized_bm.quota_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.object_lock_config_updated_at.unix_timestamp(),
        deserialized_bm.object_lock_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.notification_config_updated_at.unix_timestamp(),
        deserialized_bm.notification_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.replication_config_updated_at.unix_timestamp(),
        deserialized_bm.replication_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.bucket_targets_config_updated_at.unix_timestamp(),
        deserialized_bm.bucket_targets_config_updated_at.unix_timestamp()
    );
    assert_eq!(
        bm.bucket_targets_config_meta_updated_at.unix_timestamp(),
        deserialized_bm.bucket_targets_config_meta_updated_at.unix_timestamp()
    );

    // Test that the serialized data contains expected content
    let buf_str = String::from_utf8_lossy(&buf);
    assert!(buf_str.contains("test-bucket"), "Serialized data should contain bucket name");

    // Verify the buffer size is reasonable (should be larger due to all the config data)
    assert!(buf.len() > 1000, "Buffer should be substantial in size due to all configurations");

    println!("✅ Complete BucketMetadata serialization test passed");
    println!("   - Bucket name: {}", deserialized_bm.name);
    println!("   - Lock enabled: {}", deserialized_bm.lock_enabled);
    println!("   - Policy config size: {} bytes", deserialized_bm.policy_config_json.len());
    println!("   - Lifecycle config size: {} bytes", deserialized_bm.lifecycle_config_xml.len());
    println!("   - Serialized buffer size: {} bytes", buf.len());
}
