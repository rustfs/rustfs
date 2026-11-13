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

//! Refusal-matrix rows and behaviour of migration batch A3 (rustfs/backlog#2765): the lifecycle,
//! replication and Object Lock configuration reads.
//!
//! Configurations are stored through the bucket metadata, not through their Put operations, so
//! these tests do not depend on another increment being served.

use super::{LEGACY, MatrixRow, Outcome, Refusal, USER_SECRET_KEY, create_bucket_in_the_store, outcome, s3_client, with_stack};
use aws_sdk_s3::Client;
use futures::future::BoxFuture;
use rustfs::app::context::get_global_app_context;
use rustfs::embedded::RustFSServer;
use rustfs_ecstore::api::bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_REPLICATION_CONFIG, OBJECT_LOCK_CONFIG};
use rustfs_ecstore::api::bucket::metadata_sys;
use rustfs_iam::store::UserType;
use rustfs_policy::policy::Policy;

pub(crate) const MATRIX: &[MatrixRow] = &[];

/// Rules are disabled, so the stored documents change nothing another family's rows observe.
const LIFECYCLE: &[u8] = b"<LifecycleConfiguration><Rule><Expiration><Days>30</Days></Expiration><Filter><Prefix>logs/</Prefix>\
</Filter><ID>expire-logs</ID><Status>Disabled</Status><Transition><Days>10</Days><StorageClass>WARM</StorageClass></Transition>\
</Rule></LifecycleConfiguration>";
const REPLICATION: &[u8] = b"<ReplicationConfiguration><Role>arn:rustfs:replication:us-east-1:target:remote</Role><Rule>\
<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::remote</Bucket>\
</Destination><ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication><Filter><Prefix>a/</Prefix>\
</Filter><ID>to-remote</ID><Priority>3</Priority><Status>Disabled</Status></Rule></ReplicationConfiguration>";
const OBJECT_LOCK: &[u8] = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention>\
<Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>";

async fn store_configurations(bucket: &str) {
    for (file, document) in [
        (BUCKET_LIFECYCLE_CONFIG, LIFECYCLE),
        (BUCKET_REPLICATION_CONFIG, REPLICATION),
        (OBJECT_LOCK_CONFIG, OBJECT_LOCK),
    ] {
        metadata_sys::update(bucket, file, document.to_vec())
            .await
            .unwrap_or_else(|error| panic!("store {file} on {bucket}: {error}"));
    }
}

fn get_lifecycle<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_lifecycle_configuration().bucket(bucket).send().await) })
}

fn get_replication<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_bucket_replication().bucket(bucket).send().await) })
}

fn get_object_lock<'a>(client: &'a Client, bucket: &'a str) -> BoxFuture<'a, Outcome> {
    Box::pin(async move { outcome(client.get_object_lock_configuration().bucket(bucket).send().await) })
}

const READS: [for<'a> fn(&'a Client, &'a str) -> BoxFuture<'a, Outcome>; 3] = [get_lifecycle, get_replication, get_object_lock];

/// An ordinary name, and one RustFS creates and serves that the AWS bucket grammar reserves.
const STORED_BUCKETS: [&str; 2] = ["a3-stored", "sthree-a3-stored"];
const EMPTY_BUCKETS: [&str; 2] = ["a3-empty", "a3-empty-s3alias"];
const MISSING_BUCKET: &str = "a3-missing";
const SCOPED_USER: &str = "gatewaystacka3scoped";

fn refused(code: &str, status: u16) -> Outcome {
    Err(Refusal {
        code: Some(code.to_owned()),
        status: Some(status),
        request_ids: [None, None],
    })
}

/// The code and status of `outcome`, without its request ids.
fn answer(outcome: Outcome) -> Outcome {
    outcome.map_err(|refusal| Refusal {
        request_ids: [None, None],
        ..refusal
    })
}

/// A user allowed the three reads on `buckets` only.
async fn create_user_scoped_to(buckets: [&str; 2]) {
    let iam = get_global_app_context().expect("embedded server context").iam().handle();
    let actions = r#"["s3:GetBucketLifecycle","s3:GetReplicationConfiguration","s3:GetBucketObjectLockConfiguration"]"#;
    let [first, second] = buckets;
    let resources = format!(r#"["arn:aws:s3:::{first}","arn:aws:s3:::{second}"]"#);
    let document =
        format!(r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":{actions},"Resource":{resources}}}]}}"#);
    let policy = Policy::parse_config(document.as_bytes()).expect("scoped policy");
    iam.set_policy("a3-scoped-reads", policy).await.expect("set scoped policy");
    iam.create_user(
        SCOPED_USER,
        &rustfs_madmin::AddOrUpdateUserReq {
            secret_key: USER_SECRET_KEY.to_string(),
            policy: None,
            status: rustfs_madmin::AccountStatus::Enabled,
        },
    )
    .await
    .expect("create scoped user");
    iam.policy_db_set(SCOPED_USER, UserType::Reg, false, "a3-scoped-reads")
        .await
        .expect("attach scoped policy");
}

/// The three reads answer what is stored, refuse a missing bucket and a missing configuration
/// with their own codes, and refuse a caller allowed on other buckets only, for an ordinary and a
/// reserved-shaped name alike.
async fn the_configuration_reads_answer_what_is_stored(server: RustFSServer) {
    for bucket in STORED_BUCKETS.into_iter().chain(EMPTY_BUCKETS) {
        create_bucket_in_the_store(bucket).await;
    }
    for bucket in STORED_BUCKETS {
        store_configurations(bucket).await;
    }
    create_user_scoped_to(STORED_BUCKETS).await;
    let endpoint = server.endpoint();
    let root = s3_client(&endpoint, server.access_key(), server.secret_key());
    let scoped = s3_client(&endpoint, SCOPED_USER, USER_SECRET_KEY);

    let absent = [
        refused("NoSuchLifecycleConfiguration", 404),
        refused("ReplicationConfigurationNotFoundError", 404),
        refused("ObjectLockConfigurationNotFoundError", 404),
    ];
    for (read, absent) in READS.into_iter().zip(absent) {
        assert_eq!(answer(read(&root, MISSING_BUCKET).await), refused("NoSuchBucket", 404));
        for (stored, empty) in STORED_BUCKETS.into_iter().zip(EMPTY_BUCKETS) {
            assert_eq!(answer(read(&root, empty).await), absent, "{empty}");
            assert_eq!(answer(read(&scoped, empty).await), refused("AccessDenied", 403), "{empty}");
            assert_eq!(read(&scoped, stored).await, Ok(()), "{stored}");
        }
    }
    for bucket in STORED_BUCKETS {
        assert_answers_what_is_stored(&root, bucket).await;
    }

    server.shutdown().await;
}

async fn assert_answers_what_is_stored(root: &Client, bucket: &str) {
    let lifecycle = root
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("stored lifecycle");
    let rules: Vec<_> = lifecycle
        .rules()
        .iter()
        .map(|rule| {
            let transitions: Vec<_> = rule
                .transitions()
                .iter()
                .map(|transition| (transition.days(), transition.storage_class().map(|class| class.as_str().to_owned())))
                .collect();
            let expiration = rule.expiration().and_then(|expiration| expiration.days());
            let prefix = rule.filter().and_then(|filter| filter.prefix()).map(str::to_owned);
            (
                rule.id().map(str::to_owned),
                rule.status().as_str().to_owned(),
                prefix,
                expiration,
                transitions,
            )
        })
        .collect();
    let expected_rule = (
        Some("expire-logs".to_owned()),
        "Disabled".to_owned(),
        Some("logs/".to_owned()),
        Some(30),
        vec![(Some(10), Some("WARM".to_owned()))],
    );
    assert_eq!(rules, [expected_rule]);

    let replication = root
        .get_bucket_replication()
        .bucket(bucket)
        .send()
        .await
        .expect("stored replication");
    let config = replication.replication_configuration().expect("a replication configuration");
    assert_eq!(config.role(), "arn:rustfs:replication:us-east-1:target:remote");
    let rules: Vec<_> = config
        .rules()
        .iter()
        .map(|rule| {
            (
                rule.id().map(str::to_owned),
                rule.priority(),
                rule.status().as_str().to_owned(),
                rule.destination().map(|destination| destination.bucket().to_owned()),
                rule.existing_object_replication()
                    .map(|existing| existing.status().as_str().to_owned()),
                rule.delete_marker_replication()
                    .and_then(|marker| marker.status())
                    .map(|status| status.as_str().to_owned()),
            )
        })
        .collect();
    let expected_rule = (
        Some("to-remote".to_owned()),
        Some(3),
        "Disabled".to_owned(),
        Some("arn:aws:s3:::remote".to_owned()),
        Some("Enabled".to_owned()),
        Some("Enabled".to_owned()),
    );
    assert_eq!(rules, [expected_rule]);

    let lock = root
        .get_object_lock_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("stored object lock");
    let config = lock.object_lock_configuration().expect("an object lock configuration");
    assert_eq!(config.object_lock_enabled().map(|enabled| enabled.as_str()), Some("Enabled"));
    let retention = config
        .rule()
        .and_then(|rule| rule.default_retention())
        .expect("a default retention");
    assert_eq!(
        (retention.mode().map(|mode| mode.as_str()), retention.days(), retention.years()),
        (Some("GOVERNANCE"), Some(1), None)
    );
}

#[test]
fn the_legacy_stack_answers_the_configuration_reads_as_stored() {
    with_stack(LEGACY, the_configuration_reads_answer_what_is_stored);
}
