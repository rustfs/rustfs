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

//! Compare bytes produced by S3 writes to independent, single-drive volumes.

use crate::common::{RustFSTestEnvironment, admin_request, rustfs_binary_path, signed_s3_request_with_headers};
use crate::dual_stack_differential::reference::binary_version;
use crate::fake_s3_target::{FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target};
use crate::on_demand_migration::common::{ODM_ADMIN_ROUTE, ODM_MODULE_SWITCH_ENV, OdmSourceSpec};
use crate::replication_extension_test::send_set_replication_target_request;
use crate::storage_api::persistence_parity::{BucketMetadata, RUSTFS_META_BUCKET};
use crate::upgrade_compatibility_test::{set_bucket_quota, write_multipart};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketCannedAcl, ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetentionMode, Tag, Tagging};
use http::{HeaderMap, Method};
use md5::{Digest, Md5};
use rustfs_filemeta::{FileMeta, MetaObject};
use serde_value::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

type Error = Box<dyn std::error::Error + Send + Sync>;
const CONFIGS: &str = "persisted-configs";
const OBJECTS: &str = "persisted-objects";
const FIRST_PART_SIZE: usize = 5 * 1024 * 1024;
const LOGS: &str = "rustfs-access-logs";
const LOCKED: &str = "persistence-locked";
const SEEDED_OBJECTS: [(&str, &str); 3] = [(LOCKED, "locked"), (OBJECTS, "ssec"), (OBJECTS, "multipart")];
const BODY: &[u8] = b"persisted protected object";

fn bucket_metadata(volume: &Path, bucket: &str) -> Result<BucketMetadata, Error> {
    let key = BucketMetadata::new(bucket).save_file_path();
    let object = volume.join(RUSTFS_META_BUCKET).join(&key);
    let meta = FileMeta::load(&std::fs::read(object.join("xl.meta"))?)?;
    let info = meta.into_fileinfo(RUSTFS_META_BUCKET, &key, "", true, false, true)?;
    info.validate_for_metadata_read()?;
    let [part] = info.parts.as_slice() else { return Err("bucket metadata must have one part".into()) };
    let size = usize::try_from(info.size)?;
    if info.erasure.data_blocks != 1 || size != part.size || size > info.erasure.block_size {
        return Err("bucket metadata exceeds the single-drive, single-stripe fixture".into());
    }
    let framed = if info.inline_data() {
        info.data.ok_or("missing inline bucket metadata")?.to_vec()
    } else {
        let directory = info.data_dir.ok_or("missing bucket metadata data directory")?;
        std::fs::read(object.join(directory.to_string()).join(format!("part.{}", part.number)))?
    };
    let algorithm = info.erasure.get_checksum_info(part.number).algorithm;
    let (digest, payload) = framed.split_at_checked(algorithm.size()).ok_or("truncated bucket metadata")?;
    if digest.is_empty() || payload.len() != size || algorithm.hash_encode(payload).as_ref() != digest {
        return Err("invalid bucket metadata shard checksum or size".into());
    }
    BucketMetadata::check_header(payload)?;
    let fields = &payload[4..];
    if bucket == CONFIGS {
        // The production decoder defaults a missing field to empty. Require the ACL field on disk
        // so dropping it cannot look like the expected zero-byte document.
        let keys: BTreeMap<&str, serde::de::IgnoredAny> = rmp_serde::from_slice(fields)?;
        if !keys.contains_key("BucketAclConfigJSON") {
            return Err("missing BucketAclConfigJSON field".into());
        }
    }
    Ok(BucketMetadata::unmarshal(fields)?)
}

type VersionFields = BTreeMap<String, Value>;

fn read_object(volume: &Path, key: &str) -> Result<(MetaObject, VersionFields), Error> {
    let meta = FileMeta::load(&std::fs::read(volume.join(OBJECTS).join(key).join("xl.meta"))?)?;
    let [version] = meta.versions.as_slice() else {
        return Err(format!("{key}: expected one version").into());
    };
    let object = version.parse_version_meta()?.object.ok_or("missing V2 object")?;
    if object.mod_time.is_none() || object.data_dir.is_none_or(|id| id.is_nil()) {
        return Err(format!("{key}: missing modification time or data directory").into());
    }
    Ok((object, nonvolatile_fields(&version.meta, key)?))
}

fn nonvolatile_fields(raw: &[u8], key: &str) -> Result<VersionFields, Error> {
    // The production reader skips unknown keys and supplies defaults for some missing keys.
    // Keep the raw maps so both the version envelope and V2Obj preserve every field and value.
    let mut fields: VersionFields = rmp_serde::from_slice(raw)?;
    let Some(Value::Map(object_fields)) = fields.get_mut("V2Obj") else {
        return Err(format!("{key}: missing raw V2Obj map").into());
    };
    for volatile in ["MTime", "DDir"] {
        let value = object_fields
            .get_mut(&Value::String(volatile.to_owned()))
            .ok_or_else(|| format!("{key}: missing {volatile}"))?;
        *value = Value::Unit;
    }
    Ok(fields)
}

async fn put_xml(env: &RustFSTestEnvironment, query: &str, xml: &str) -> Result<(), Error> {
    let mut headers = HeaderMap::new();
    headers.insert("content-md5", base64_simd::STANDARD.encode_to_string(Md5::digest(xml)).parse()?);
    let response = signed_s3_request_with_headers(
        Method::PUT,
        &format!("{}/{CONFIGS}?{query}", env.url),
        Some(xml.to_owned()),
        Some("application/xml"),
        &env.access_key,
        &env.secret_key,
        &headers,
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!("PUT ?{query} failed with {}", response.status()).into());
    }
    Ok(())
}

struct Snapshot {
    configs: BTreeMap<&'static str, Vec<u8>>,
    objects: BTreeMap<&'static str, VersionFields>,
    multipart_files: BTreeMap<String, Vec<u8>>,
}

async fn capture(binary: &Path, peer: &Path, target: &FakeS3Target, clock: &str, clock_library: &str) -> Result<Snapshot, Error> {
    let mut env = RustFSTestEnvironment::new().await?;
    let queue = tempfile::tempdir()?;
    let clock_loader = if cfg!(target_os = "macos") {
        "DYLD_INSERT_LIBRARIES"
    } else {
        "LD_PRELOAD"
    };
    let options = [
        (clock_loader, clock_library),
        ("DYLD_FORCE_FLAT_NAMESPACE", "1"),
        ("FAKETIME", clock),
        ("FAKETIME_DONT_FAKE_MONOTONIC", "1"),
        ("TZ", "UTC"),
        ("RUSTFS_NOTIFY_ENABLE", "true"),
        ("RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY", "on"),
        ("RUSTFS_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY", target.endpoint()),
        (
            "RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY",
            queue.path().to_str().ok_or("non-UTF8 queue path")?,
        ),
        ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", target.endpoint()),
        ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", "true"),
        (ODM_MODULE_SWITCH_ENV, "true"),
        ("RUSTFS_NEW_BUCKET_DURABILITY_MODE", "strict"),
        ("RUSTFS_SCANNER_CYCLE", "1"),
        ("RUSTFS_SCANNER_START_DELAY_SECS", "0"),
    ];
    env.start_rustfs_server_from_binary_without_cleanup(binary, vec![], &options)
        .await?;
    let client = env.create_s3_client();
    write_configs(&env, &client, target).await?;
    write_objects(&client).await?;
    let key: [u8; 32] = rand::random();
    let customer_key = base64_simd::STANDARD.encode_to_string(key);
    let customer_md5 = base64_simd::STANDARD.encode_to_string(Md5::digest(key));
    client
        .put_object()
        .bucket(OBJECTS)
        .key("ssec")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_md5)
        .body(ByteStream::from_static(BODY))
        .send()
        .await?;
    client
        .create_bucket()
        .bucket(LOCKED)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    let retain_until = aws_sdk_s3::primitives::DateTime::from_secs(time::OffsetDateTime::now_utc().unix_timestamp() + 86_400);
    let locked = client
        .put_object()
        .bucket(LOCKED)
        .key("locked")
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(retain_until)
        .body(ByteStream::from_static(BODY))
        .send()
        .await?;
    let version = locked.version_id().ok_or("missing locked object version")?;
    let volume = PathBuf::from(&env.temp_dir);

    // Both tag updates start with the same writer-produced version ID, encryption IV and multipart
    // publication identity. The full raw version maps can then be compared without masking them.
    let mut originals = Vec::new();
    for (bucket, key) in SEEDED_OBJECTS {
        let path = volume.join(bucket).join(key).join("xl.meta");
        originals.push((path.clone(), std::fs::read(path)?));
    }
    let expected = tag_and_read_versions(&client, &volume, version).await?;
    env.stop_server();
    for (path, bytes) in originals {
        std::fs::write(path, bytes)?;
    }
    let snapshot = read_snapshot(&volume)?;
    env.start_rustfs_server_from_binary_without_cleanup(peer, vec![], &options)
        .await?;
    let peer_client = env.create_s3_client();
    let locked = peer_client
        .get_object()
        .bucket(LOCKED)
        .key("locked")
        .version_id(version)
        .send()
        .await?;
    let retention = peer_client
        .get_object_retention()
        .bucket(LOCKED)
        .key("locked")
        .version_id(version)
        .send()
        .await?;
    let retention = retention.retention().ok_or("missing persisted retention")?;
    let legal_hold = peer_client
        .get_object_legal_hold()
        .bucket(LOCKED)
        .key("locked")
        .version_id(version)
        .send()
        .await?;
    if locked.version_id() != Some(version)
        || legal_hold.legal_hold().and_then(|hold| hold.status()) != Some(&ObjectLockLegalHoldStatus::On)
        || retention.mode() != Some(&ObjectLockRetentionMode::Governance)
        || retention.retain_until_date() != Some(&retain_until)
        || locked.body.collect().await?.into_bytes().as_ref() != BODY
    {
        return Err("peer could not read the locked version, its contents and retention".into());
    }
    let encrypted = peer_client
        .get_object()
        .bucket(OBJECTS)
        .key("ssec")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_md5)
        .send()
        .await?;
    if encrypted.sse_customer_algorithm() != Some("AES256")
        || encrypted.sse_customer_key_md5() != Some(customer_md5.as_str())
        || encrypted.body.collect().await?.into_bytes().as_ref() != BODY
    {
        return Err("peer could not decrypt the persisted SSE-C object".into());
    }
    let multipart = peer_client
        .get_object()
        .bucket(OBJECTS)
        .key("multipart")
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    if multipart.len() != FIRST_PART_SIZE + 100
        || !multipart[..FIRST_PART_SIZE].iter().all(|&byte| byte == 0x31)
        || !multipart[FIRST_PART_SIZE..].iter().all(|&byte| byte == 0x32)
    {
        return Err("peer read different multipart data".into());
    }
    let actual = tag_and_read_versions(&peer_client, &volume, version).await?;
    for (index, (_, key)) in SEEDED_OBJECTS.iter().enumerate() {
        if actual[index] != expected[index] {
            return Err(
                format!("{key}: the two writers persisted different full xl.meta version fields for the same seed").into(),
            );
        }
    }
    env.stop_server();
    Ok(snapshot)
}

async fn tag_and_read_versions(client: &Client, volume: &Path, locked_version: &str) -> Result<Vec<VersionFields>, Error> {
    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("purpose").value("persistence-parity").build()?)
        .build()?;
    let mut versions = Vec::new();
    for (bucket, key) in SEEDED_OBJECTS {
        let version = (bucket == LOCKED).then(|| locked_version.to_owned());
        client
            .put_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version.clone())
            .tagging(tags.clone())
            .send()
            .await?;
        let actual = client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version)
            .send()
            .await?;
        if actual.tag_set() != tags.tag_set() {
            return Err(format!("{key}: tagging update was not visible").into());
        }
        let meta = FileMeta::load(&std::fs::read(volume.join(bucket).join(key).join("xl.meta"))?)?;
        let [version] = meta.versions.as_slice() else {
            return Err(format!("{key}: expected one tagged version").into());
        };
        versions.push(rmp_serde::from_slice(&version.meta)?);
    }
    Ok(versions)
}

async fn write_configs(env: &RustFSTestEnvironment, client: &Client, target: &FakeS3Target) -> Result<(), Error> {
    env.create_test_bucket(LOGS).await?;
    env.create_test_bucket(OBJECTS).await?;
    env.create_test_bucket("persistence-warehouse").await?;
    env.create_test_bucket("persistence-migration").await?;
    client
        .create_bucket()
        .bucket(CONFIGS)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    macro_rules! golden {
        ($path:literal) => {
            include_str!(concat!("../../ecstore/tests/fixtures/bucket-config-goldens/", $path, ".in.xml"))
        };
    }
    let target_request = serde_json::json!({
        "endpoint": target.address(), "credentials": {"accessKey": FAKE_ACCESS_KEY, "secretKey": FAKE_SECRET_KEY},
        "targetbucket": "interop-dr", "type": "replication", "secure": false,
        "deploymentID": "0b6f3c2a-5e6a-4d2b-9c1e-7a8f9d0e1b2c"
    });
    let response = send_set_replication_target_request(env, CONFIGS, false, target_request).await?;
    if !response.status().is_success() {
        return Err(format!("set remote target failed with {}", response.status()).into());
    }
    let target_arn: String = response.json().await?;
    let replication = golden!("replication/rustfs").replace("arn:rustfs:replication:", "arn:minio:replication:");
    if !replication.contains(&format!("<Bucket>{target_arn}</Bucket>")) {
        return Err("registered target does not match the golden replication destination".into());
    }
    for (query, xml) in [
        ("versioning", golden!("versioning/minio")),
        ("notification", golden!("notification/rustfs")),
        ("replication", replication.as_str()),
        ("lifecycle", golden!("lifecycle/minio")),
        ("object-lock", golden!("object_lock/rustfs")),
        ("encryption", golden!("encryption/minio")),
        ("tagging", golden!("tagging/rustfs")),
        ("cors", golden!("cors/rustfs")),
        ("logging", golden!("logging/rustfs")),
        ("website", golden!("website/rustfs")),
        ("accelerate", golden!("accelerate/rustfs")),
        ("requestPayment", golden!("request_payment/rustfs")),
        ("publicAccessBlock", golden!("public_access_block/rustfs")),
    ] {
        put_xml(env, query, xml).await?;
    }
    client
        .put_bucket_acl()
        .bucket(CONFIGS)
        .acl(BucketCannedAcl::Private)
        .send()
        .await?;
    let acl = client.get_bucket_acl().bucket(CONFIGS).send().await?;
    let owner = acl.owner().and_then(|owner| owner.id()).ok_or("ACL owner missing")?;
    let [grant] = acl.grants() else { return Err("expected exactly one ACL grant".into()) };
    let grantee = grant.grantee().ok_or("missing ACL grantee")?;
    if owner.is_empty()
        || grant.permission().map(|p| p.as_str()) != Some("FULL_CONTROL")
        || grantee.r#type().as_str() != "CanonicalUser"
        || grantee.id().is_some()
    {
        return Err("GET ACL changed its canonical owner grant".into());
    }
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny", "Principal": "*", "Action": "s3:DeleteObject",
            "Resource": format!("arn:aws:s3:::{CONFIGS}/protected/*"),
        }],
    });
    client
        .put_bucket_policy()
        .bucket(CONFIGS)
        .policy(policy.to_string())
        .send()
        .await?;
    set_bucket_quota(env, CONFIGS, 16 * 1024 * 1024).await?;
    let migration = OdmSourceSpec::for_fake_source(target, "migration-source");
    for (path, body) in [
        ("/iceberg/v1/buckets/persistence-warehouse".to_owned(), None),
        (format!("{ODM_ADMIN_ROUTE}/persistence-migration"), Some(migration.to_json().to_string())),
    ] {
        let (status, _) = admin_request(&env.url, Method::PUT, &path, body, &env.access_key, &env.secret_key).await?;
        if !status.is_success() {
            return Err(format!("configuring {path} failed with {status}").into());
        }
    }

    Ok(())
}

async fn write_objects(client: &Client) -> Result<(), Error> {
    for key in ["put", "tagged"] {
        client
            .put_object()
            .bucket(OBJECTS)
            .key(key)
            .metadata("purpose", "byte-parity")
            .body(ByteStream::from_static(b"persisted object contents"))
            .send()
            .await?;
    }
    client
        .copy_object()
        .bucket(OBJECTS)
        .key("copy")
        .copy_source(format!("{OBJECTS}/put"))
        .send()
        .await?;
    let tags = Tagging::builder()
        .tag_set(Tag::builder().key("type").value("persisted").build()?)
        .build()?;
    client
        .put_object_tagging()
        .bucket(OBJECTS)
        .key("tagged")
        .tagging(tags)
        .send()
        .await?;
    write_multipart(client, OBJECTS, "multipart", &[vec![0x31; FIRST_PART_SIZE], vec![0x32; 100]]).await?;
    Ok(())
}

fn read_snapshot(volume: &Path) -> Result<Snapshot, Error> {
    let b = bucket_metadata(volume, CONFIGS)?;
    let configs = BTreeMap::from([
        ("acl", b.bucket_acl_config_json),
        ("accelerate", b.accelerate_config_xml),
        ("cors", b.cors_config_xml),
        ("encryption", b.encryption_config_xml),
        ("lifecycle", b.lifecycle_config_xml),
        ("logging", b.logging_config_xml),
        ("notification", b.notification_config_xml),
        ("object_lock", b.object_lock_config_xml),
        ("public_access_block", b.public_access_block_config_xml),
        ("request_payment", b.request_payment_config_xml),
        ("tagging", b.tagging_config_xml),
        ("versioning", b.versioning_config_xml),
        ("website", b.website_config_xml),
        ("durability", b.durability_config_json),
        ("policy", b.policy_config_json),
        ("quota", b.quota_config_json),
        ("replication", b.replication_config_xml),
        ("targets", b.bucket_targets_config_json),
        ("table", bucket_metadata(volume, "persistence-warehouse")?.table_bucket_config_json),
        ("odm", bucket_metadata(volume, "persistence-migration")?.on_demand_migration_config_json),
    ]);
    let mut objects = BTreeMap::new();
    for key in ["put", "copy", "tagged"] {
        let (_, fields) = read_object(volume, key)?;
        objects.insert(key, fields);
    }
    let (multipart, _) = read_object(volume, "multipart")?;
    if multipart.part_numbers != [1, 2] || multipart.part_sizes != [FIRST_PART_SIZE, 100] {
        return Err("xl.meta does not describe both uploaded parts".into());
    }
    let directory = volume
        .join(OBJECTS)
        .join("multipart")
        .join(multipart.data_dir.ok_or("missing multipart data directory")?.to_string());
    let mut multipart_files = BTreeMap::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| "invalid multipart file name")?;
        if name.starts_with("part.") {
            let bytes = std::fs::read(entry.path())?;
            if bytes.is_empty() {
                return Err(format!("empty multipart file {name}").into());
            }
            multipart_files.insert(name, bytes);
        }
    }
    for name in ["part.1", "part.2", "part.1.meta", "part.2.meta"] {
        if !multipart_files.contains_key(name) {
            return Err(format!("missing multipart file {name}").into());
        }
    }
    Ok(Snapshot {
        configs,
        objects,
        multipart_files,
    })
}

fn save_xml_samples(root: &Path, side: &str, snapshot: &Snapshot) -> Result<(), Error> {
    let directory = root.join(side);
    std::fs::create_dir_all(&directory)?;
    let families: [&str; 13] = [
        "accelerate",
        "cors",
        "encryption",
        "lifecycle",
        "logging",
        "notification",
        "object_lock",
        "public_access_block",
        "replication",
        "request_payment",
        "tagging",
        "versioning",
        "website",
    ];
    for family in families {
        let bytes = snapshot
            .configs
            .get(family)
            .ok_or_else(|| format!("missing XML family {family}"))?;
        std::fs::write(directory.join(format!("{family}.xml")), bytes)?;
    }
    Ok(())
}

#[tokio::test]
async fn persistence_byte_parity() -> Result<(), Error> {
    let reference_binary = PathBuf::from(
        std::env::var_os("RUSTFS_PERSISTENCE_REFERENCE_BINARY").ok_or("missing pinned RUSTFS_PERSISTENCE_REFERENCE_BINARY")?,
    );
    let candidate = rustfs_binary_path();
    if binary_version(&reference_binary).await? == binary_version(&candidate).await? {
        return Err("reference and candidate binaries report identical build identities".into());
    }
    // The quota writer records the process wall clock inside its JSON document. Keep raw bytes
    // comparable while the SDK's signature clock and both servers' monotonic timers advance normally.
    let clock_library = std::env::var("RUSTFS_PERSISTENCE_CLOCK_LIBRARY")?;
    if !Path::new(&clock_library).is_absolute() || !Path::new(&clock_library).is_file() {
        return Err("RUSTFS_PERSISTENCE_CLOCK_LIBRARY must name an absolute libfaketime path".into());
    }
    let clock = time::OffsetDateTime::now_utc()
        .format(time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))?;
    let target = FakeS3Target::start().await?;
    target.create_bucket("migration-source");
    target.create_bucket_with_object_lock("interop-dr");
    let reference = capture(&reference_binary, &candidate, &target, &clock, &clock_library).await?;
    let candidate = capture(&candidate, &reference_binary, &target, &clock, &clock_library).await?;
    if let Some(directory) = std::env::var_os("RUSTFS_PERSISTENCE_XML_DIR") {
        save_xml_samples(Path::new(&directory), "reference", &reference)?;
        save_xml_samples(Path::new(&directory), "candidate", &candidate)?;
    }
    if reference.configs.len() != 20 || candidate.configs.len() != 20 {
        return Err("expected 13 XML and seven JSON fields on each side".into());
    }
    for (&field, expected) in &reference.configs {
        // Canned ACLs acknowledge the write and return one owner grant without persisting an ACL document.
        if expected.is_empty() != (field == "acl") || candidate.configs.get(field) != Some(expected) {
            return Err(format!("incorrect or different persisted bucket config: {field}").into());
        }
    }
    for (key, expected) in &reference.objects {
        if candidate.objects.get(key) != Some(expected) {
            return Err(format!("different persisted xl.meta fields: {key}").into());
        }
    }
    if candidate.multipart_files != reference.multipart_files {
        return Err("different multipart part files or part metadata".into());
    }
    eprintln!(
        "Compared {} bucket fields (canned ACL empty), {} independently written object records, three shared-seed full version maps in both directions, and multipart part files",
        reference.configs.len(),
        reference.objects.len()
    );
    Ok(())
}

#[test]
fn version_comparison_preserves_unknown_fields_and_absent_values() -> Result<(), Error> {
    let mut sample = serde_json::json!({"Type": 1, "v": 1, "V2Obj": {"DDir": vec![1; 16], "MTime": 1}});
    let fields = |value: &serde_json::Value| nonvolatile_fields(&rmp_serde::to_vec(value)?, "fixture");
    let original = fields(&sample)?;
    sample["V2Obj"]["DDir"] = serde_json::json!(vec![2; 16]);
    sample["V2Obj"]["MTime"] = serde_json::json!(2);
    assert_eq!(fields(&sample)?, original);
    sample["V2Obj"]["Future"] = serde_json::Value::Null;
    let with_nil = fields(&sample)?;
    assert_ne!(with_nil, original);
    sample["V2Obj"]["Future"] = serde_json::json!(false);
    let with_value = fields(&sample)?;
    assert_ne!(with_value, with_nil);
    sample["FutureEnvelope"] = serde_json::json!(1);
    assert_ne!(fields(&sample)?, with_value);
    assert!(fields(&serde_json::json!({"V2Obj": {"MTime": 1}})).is_err());
    Ok(())
}
