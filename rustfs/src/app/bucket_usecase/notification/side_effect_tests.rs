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

//! Store and callback observations through the legacy Put entrypoint and shared use case.

use super::*;
use crate::app::bucket_usecase::test_support::build_request_with_req_info;
use crate::app::gating_test_env::{app_context_from_current_environment, run_large_stack_test, shared_gating_ambient};
use crate::app::storage_api::bucket_usecase::access::ReqInfo;
use crate::app::storage_api::bucket_usecase::helper::MutationCompletionProbe;
use crate::app::storage_api::s3::dto::{
    FilterRule, LambdaFunctionConfiguration, NotificationConfiguration, NotificationConfigurationFilter,
    PutBucketNotificationConfigurationInput, QueueConfiguration, S3KeyFilter, TopicConfiguration,
};
use crate::app::storage_api::s3::{S3, S3Access, S3ErrorCode, S3Request};
use crate::app::storage_api::test::ecfs::FS;
use crate::runtime_sources::{NotifyInterface, ServerContextSlot};
use http::Method;
use rustfs_notify::{EventArgs, NotificationError};
use std::sync::Mutex;

const EMPTY_XML: &[u8] = b"<NotificationConfiguration></NotificationConfiguration>";
const OLD_XML: &[u8] =
    b"<NotificationConfiguration><EventBridgeConfiguration></EventBridgeConfiguration></NotificationConfiguration>";
const NEW_XML: &[u8] = concat!(
    "<NotificationConfiguration>",
    "<CloudFunctionConfiguration><Event>s3:Future:Unrecognized</Event><Id>lambda</Id><CloudFunction>arn:rustfs:sqs:us-east-1:3:webhook</CloudFunction></CloudFunctionConfiguration>",
    "<QueueConfiguration><Event>s3:ObjectCreated:*</Event><Event>s3:Future:Unrecognized</Event>",
    "<Filter><S3Key><FilterRule><Name>Prefix</Name><Value>logs/%2F</Value></FilterRule>",
    "<FilterRule><Name>Suffix</Name><Value>.csv</Value></FilterRule></S3Key></Filter>",
    "<Id>queue</Id><Queue>arn:rustfs:sqs:us-east-1:1:webhook</Queue></QueueConfiguration>",
    "<TopicConfiguration><Event>s3:ObjectRemoved:*</Event><Id>topic</Id><Topic>arn:rustfs:sqs:us-east-1:2:webhook</Topic></TopicConfiguration>",
    "</NotificationConfiguration>"
).as_bytes();

type RuntimeRule = (Vec<EventName>, String, String, Vec<TargetID>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Callback {
    Validate,
    ReloadStarted,
    ReloadCompleted,
    Clear,
    Add,
}

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Rules(Callback, String, Vec<RuntimeRule>, Vec<u8>),
    Reload(Callback, &'static str, bool),
    Clear(Vec<u8>),
}

struct RecordingNotify {
    bucket: String,
    fail_at: Option<Callback>,
    calls: Arc<Mutex<Vec<Observation>>>,
    active_rules: Mutex<Vec<RuntimeRule>>,
}

async fn read_notification_xml(bucket: &str) -> Vec<u8> {
    crate::app::storage_api::test::get_global_bucket_metadata_sys()
        .expect("test bucket metadata")
        .read()
        .await
        .get_config_from_disk(bucket)
        .await
        .expect("load notification bytes from disk")
        .notification_config_xml
}

impl RecordingNotify {
    fn maybe_fail(&self, callback: Callback) -> Result<(), NotificationError> {
        if self.fail_at == Some(callback) {
            return Err(NotificationError::RuleConfiguration("fixture failure".into()));
        }
        Ok(())
    }

    async fn record_rules(&self, callback: Callback, bucket: &str, region: &str, rules: &[RuntimeRule]) {
        assert_eq!(bucket, self.bucket);
        let xml = read_notification_xml(bucket).await;
        self.calls
            .lock()
            .unwrap()
            .push(Observation::Rules(callback, region.to_owned(), rules.to_vec(), xml));
    }
}

#[async_trait::async_trait]
impl NotifyInterface for RecordingNotify {
    async fn notify(&self, _args: EventArgs) {
        panic!("bucket notification configuration must not emit an object event");
    }

    async fn validate_event_specific_rules(
        &self,
        bucket: &str,
        region: &str,
        rules: &[RuntimeRule],
    ) -> Result<(), NotificationError> {
        self.record_rules(Callback::Validate, bucket, region, rules).await;
        self.maybe_fail(Callback::Validate)
    }

    async fn add_event_specific_rules(&self, bucket: &str, region: &str, rules: &[RuntimeRule]) -> Result<(), NotificationError> {
        self.record_rules(Callback::Add, bucket, region, rules).await;
        self.maybe_fail(Callback::Add)?;
        *self.active_rules.lock().unwrap() = rules.to_vec();
        Ok(())
    }

    async fn clear_bucket_notification_rules(&self, bucket: &str) -> Result<(), NotificationError> {
        assert_eq!(bucket, self.bucket);
        let xml = read_notification_xml(bucket).await;
        self.calls.lock().unwrap().push(Observation::Clear(xml));
        self.maybe_fail(Callback::Clear)?;
        self.active_rules.lock().unwrap().clear();
        Ok(())
    }
}

fn configuration() -> NotificationConfiguration {
    NotificationConfiguration {
        queue_configurations: Some(vec![QueueConfiguration {
            id: Some("queue".into()),
            events: vec![
                "s3:ObjectCreated:*".to_owned().into(),
                "s3:Future:Unrecognized".to_owned().into(),
            ],
            queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".into(),
            filter: Some(NotificationConfigurationFilter {
                key: Some(S3KeyFilter {
                    filter_rules: Some(vec![
                        FilterRule {
                            name: Some("Prefix".to_owned().into()),
                            value: Some("logs/%2F".into()),
                        },
                        FilterRule {
                            name: Some("Suffix".to_owned().into()),
                            value: Some(".csv".into()),
                        },
                    ]),
                }),
            }),
        }]),
        topic_configurations: Some(vec![TopicConfiguration {
            id: Some("topic".into()),
            events: vec!["s3:ObjectRemoved:*".to_owned().into()],
            topic_arn: "arn:rustfs:sqs:us-east-1:2:webhook".into(),
            filter: None,
        }]),
        lambda_function_configurations: Some(vec![LambdaFunctionConfiguration {
            id: Some("lambda".into()),
            events: vec!["s3:Future:Unrecognized".to_owned().into()],
            lambda_function_arn: "arn:rustfs:sqs:us-east-1:3:webhook".into(),
            filter: None,
        }]),
        ..Default::default()
    }
}

fn expected_rules() -> Vec<RuntimeRule> {
    [
        (vec![EventName::parse("s3:ObjectCreated:*").unwrap()], "logs/%2F", ".csv", "1"),
        (vec![EventName::parse("s3:ObjectRemoved:*").unwrap()], "", "", "2"),
        (Vec::new(), "", "", "3"),
    ]
    .map(|(events, prefix, suffix, id)| {
        (
            events,
            prefix.to_owned(),
            suffix.to_owned(),
            vec![TargetID {
                id: id.to_owned(),
                name: "webhook".into(),
            }],
        )
    })
    .into()
}

fn notification_request(
    bucket: &str,
    configuration: NotificationConfiguration,
    slot: &Arc<ServerContextSlot>,
) -> S3Request<PutBucketNotificationConfigurationInput> {
    let input = PutBucketNotificationConfigurationInput::builder()
        .bucket(bucket.to_owned())
        .notification_configuration(configuration)
        .build()
        .unwrap();
    let mut request = build_request_with_req_info(
        input,
        Method::PUT,
        ReqInfo {
            bucket: Some(bucket.to_owned()),
            ..Default::default()
        },
    );
    request.extensions.insert(slot.clone());
    request.uri = format!("/{bucket}?notification").parse().unwrap();
    request.region = Some("eu-west-2".parse().unwrap());
    request
}

fn check_notification_write_order() {
    run_large_stack_test("notification-write-order", || async {
        let ambient = shared_gating_ambient().await;
        let store = ambient.object_store();
        for fail_at in [None, Some(Callback::Validate), Some(Callback::Clear), Some(Callback::Add)] {
            let bucket = format!("notify-order-{}", uuid::Uuid::new_v4().simple());
            store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
            store
                .update_bucket_metadata_config(&bucket, BUCKET_NOTIFICATION_CONFIG, OLD_XML.to_vec())
                .await
                .unwrap();
            let policy = serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:PutBucketNotification"],
                    "Resource": [format!("arn:aws:s3:::{bucket}")]}]
            });
            store
                .update_bucket_metadata_config(&bucket, BUCKET_POLICY_CONFIG, serde_json::to_vec(&policy).unwrap())
                .await
                .unwrap();

            let calls = Arc::new(Mutex::new(Vec::new()));
            let recorder = Arc::new(RecordingNotify {
                bucket: bucket.clone(),
                fail_at,
                calls: calls.clone(),
                active_rules: Mutex::new(expected_rules()),
            });
            let context = Arc::new(app_context_from_current_environment(&ambient).with_test_notify_interface(recorder.clone()));
            let region = context
                .region()
                .get()
                .map_or_else(|| "eu-west-2".to_owned(), |region| region.to_string());
            let slot = ServerContextSlot::new();
            assert!(slot.install(context.clone()));
            let fs = FS::with_server_ctx(slot.clone());
            let audit = MutationCompletionProbe::install(&bucket);
            let mut request = notification_request(&bucket, configuration(), &slot);
            S3Access::put_bucket_notification_configuration(&fs, &mut request)
                .await
                .unwrap();
            let result = observe_reload(&bucket, calls.clone(), S3::put_bucket_notification_configuration(&fs, request)).await;
            let expected = expected_rules();
            let mut timeline = vec![Observation::Rules(
                Callback::Validate,
                region.clone(),
                expected.clone(),
                OLD_XML.to_vec(),
            )];
            if fail_at != Some(Callback::Validate) {
                timeline.extend([
                    Observation::Reload(Callback::ReloadStarted, "put bucket notification", false),
                    Observation::Reload(Callback::ReloadCompleted, "put bucket notification", false),
                    Observation::Clear(NEW_XML.to_vec()),
                ]);
                if fail_at != Some(Callback::Clear) {
                    timeline.push(Observation::Rules(Callback::Add, region.clone(), expected.clone(), NEW_XML.to_vec()));
                }
            }
            assert_eq!(*calls.lock().unwrap(), timeline);
            let stored = if fail_at == Some(Callback::Validate) {
                OLD_XML
            } else {
                NEW_XML
            };
            assert_eq!(read_notification_xml(&bucket).await, stored);
            let error_message = match fail_at {
                Some(Callback::Clear) => Some("Failed to clear rules: Rule configuration error: fixture failure"),
                Some(_) => Some("Failed to add rules: Rule configuration error: fixture failure"),
                None => None,
            };
            if let Some(message) = error_message {
                let error = result.expect_err("injected callback failure");
                assert_eq!(error.code(), &S3ErrorCode::InternalError);
                assert_eq!(error.message(), Some(message));
            } else {
                result.expect("configuration written");
            }
            {
                let audits = audit.audits.lock().unwrap();
                let [entry] = audits.as_slice() else { panic!("one notification audit: {audits:?}") };
                assert_eq!(entry.api.name.as_deref(), Some(S3Operation::PutBucketNotificationConfiguration.as_str()));
                assert_eq!(entry.api.bucket.as_deref(), Some(bucket.as_str()));
                assert_eq!(entry.api.status_code, Some(if fail_at.is_some() { 500 } else { 200 }));
                assert_eq!(entry.error.as_deref().filter(|message| !message.is_empty()), error_message);
            }
            assert!(audit.events.lock().unwrap().is_empty());

            if fail_at.is_none() {
                assert_eq!(*recorder.active_rules.lock().unwrap(), expected);
                let mut invalid = configuration();
                invalid.lambda_function_configurations.as_mut().unwrap()[0].lambda_function_arn = "invalid-arn".into();
                let mut invalid = notification_request(&bucket, invalid, &slot);
                S3Access::put_bucket_notification_configuration(&fs, &mut invalid)
                    .await
                    .unwrap();
                calls.lock().unwrap().clear();
                let err = observe_reload(&bucket, calls.clone(), S3::put_bucket_notification_configuration(&fs, invalid))
                    .await
                    .expect_err("the last destination has an invalid ARN");
                assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
                assert!(
                    err.message()
                        .unwrap()
                        .starts_with("Invalid ARN in notification configuration: ")
                );
                assert!(
                    calls.lock().unwrap().is_empty(),
                    "validation happens after all destinations have been built"
                );
                assert_eq!(read_notification_xml(&bucket).await, NEW_XML);
                let usecase = DefaultBucketUsecase::with_context(Some(context));
                let output = usecase
                    .execute_get_bucket_notification_configuration(gateway_dto::GetBucketNotificationConfigurationInput {
                        bucket: super::super::test_support::gateway_bucket(&bucket),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert_eq!(output.queue_configurations[0].events[1].as_str(), "s3:Future:Unrecognized");

                calls.lock().unwrap().clear();
                let mut empty = notification_request(&bucket, NotificationConfiguration::default(), &slot);
                S3Access::put_bucket_notification_configuration(&fs, &mut empty)
                    .await
                    .unwrap();
                observe_reload(&bucket, calls.clone(), S3::put_bucket_notification_configuration(&fs, empty))
                    .await
                    .unwrap();
                assert_eq!(
                    *calls.lock().unwrap(),
                    [
                        Observation::Rules(Callback::Validate, region.clone(), Vec::new(), NEW_XML.to_vec()),
                        Observation::Reload(Callback::ReloadStarted, "put bucket notification", false),
                        Observation::Reload(Callback::ReloadCompleted, "put bucket notification", false),
                        Observation::Clear(EMPTY_XML.to_vec()),
                        Observation::Rules(Callback::Add, region, Vec::new(), EMPTY_XML.to_vec()),
                    ]
                );
                assert_eq!(read_notification_xml(&bucket).await, EMPTY_XML);
                assert!(recorder.active_rules.lock().unwrap().is_empty());
            }
        }
    });
}

#[test]
#[serial_test::serial]
fn legacy_put_notification_keeps_callback_order_and_durable_bytes() {
    temp_env::with_var(rustfs_config::ENV_AUDIT_ENABLE, Some("true"), || {
        crate::server::refresh_audit_module_enabled();
        check_notification_write_order();
    });
    crate::server::refresh_audit_module_enabled();
}

async fn observe_reload<T>(bucket: &str, calls: Arc<Mutex<Vec<Observation>>>, operation: impl Future<Output = T>) -> T {
    super::super::BUCKET_METADATA_RELOAD_PROBE
        .scope(
            (
                bucket.to_owned(),
                Box::new(move |completed, operation, scanner| {
                    let callback = if completed {
                        Callback::ReloadCompleted
                    } else {
                        Callback::ReloadStarted
                    };
                    calls.lock().unwrap().push(Observation::Reload(callback, operation, scanner));
                }),
            ),
            operation,
        )
        .await
}

#[test]
fn legacy_put_notification_does_not_write_after_the_bucket_is_recreated() {
    run_large_stack_test("notification-stale-incarnation", || async {
        let ambient = shared_gating_ambient().await;
        let store = ambient.object_store();
        let bucket = format!("notify-stale-{}", uuid::Uuid::new_v4().simple());
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        let first = store.bucket_incarnation_id(&bucket).await.unwrap();
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:PutBucketNotification"],
                "Resource": [format!("arn:aws:s3:::{bucket}")]}]
        });
        store
            .update_bucket_metadata_config(&bucket, BUCKET_POLICY_CONFIG, serde_json::to_vec(&policy).unwrap())
            .await
            .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::new(RecordingNotify {
            bucket: bucket.clone(),
            fail_at: None,
            calls: calls.clone(),
            active_rules: Mutex::new(Vec::new()),
        });
        let context = Arc::new(app_context_from_current_environment(&ambient).with_test_notify_interface(recorder));
        let slot = ServerContextSlot::new();
        assert!(slot.install(context));
        let fs = FS::with_server_ctx(slot.clone());
        let mut request = notification_request(&bucket, configuration(), &slot);
        S3Access::put_bucket_notification_configuration(&fs, &mut request)
            .await
            .unwrap();
        assert_eq!(bucket_config_mutation_incarnation(&request, &bucket).unwrap(), Some(first));

        let mismatch = DefaultBucketUsecase::without_context()
            .execute_put_bucket_notification_configuration(
                RequestEnvelope::from_s3s(&request),
                "other-bucket",
                vec![NotificationDestination {
                    kind: "QueueConfiguration",
                    id: None,
                    events: Vec::new(),
                    target_arn: String::new(),
                    filters: vec![(None, None)],
                }],
                Err(rustfs_s3_types::s3_error!(MalformedXML, "persistence conversion")),
            )
            .await
            .unwrap_err();
        assert_eq!(mismatch.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
        assert_eq!(mismatch.message(), Some("bucket config mutation snapshot does not match request bucket"));
        store.delete_bucket(&bucket, &DeleteBucketOptions::default()).await.unwrap();
        store.make_bucket(&bucket, &MakeBucketOptions::default()).await.unwrap();
        assert_ne!(first, store.bucket_incarnation_id(&bucket).await.unwrap());
        store
            .update_bucket_metadata_config(&bucket, BUCKET_NOTIFICATION_CONFIG, OLD_XML.to_vec())
            .await
            .unwrap();

        let result = observe_reload(&bucket, calls.clone(), S3::put_bucket_notification_configuration(&fs, request)).await;
        assert_eq!(read_notification_xml(&bucket).await, OLD_XML);
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::NoSuchBucket);
        assert!(
            matches!(calls.lock().unwrap().as_slice(), [Observation::Rules(Callback::Validate, _, _, xml)] if xml == OLD_XML)
        );
    });
}
