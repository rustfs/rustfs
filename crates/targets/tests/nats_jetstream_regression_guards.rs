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

//! Regression guards for the NATS JetStream publish feature.
//!
//! The JetStream surface can be removed and the build still succeed unless something fails when it
//! disappears. These layered guards make a silent removal or a silent classification drift a build or
//! test failure:
//!
//! - a compile-time reference to the JetStream config surface and the target entry point,
//! - a module-presence test naming the public entry type and the publish-error variant,
//! - completeness tests over the publish-error-class enums with no wildcard arm,
//! - a server-backed end-to-end publish-and-ack test, ignored by default so the offline gate stays
//!   green without a NATS server.
//!
//! The end-to-end guard requires a running NATS server with JetStream enabled. To run it locally:
//!
//!     docker run -d --name rustfs-nats-test -p 4222:4222 nats:2 -js
//!     cargo test -p rustfs-targets --test nats_jetstream_regression_guards -- --ignored
//!
//! Override the server URL with RUSTFS_TEST_NATS_URL.

use async_nats::jetstream::context::PublishErrorKind;
use rustfs_targets::check_nats_server_available;
use rustfs_targets::error::TargetError;
use rustfs_targets::target::nats::{NATSArgs, NATSTarget};
use rustfs_targets::target::{FailedErrorClass, TargetType};

// Compile-time module-presence assertion.
//
// A const initializer names the JetStream config fields on the public target args type and the public
// target constructor by path, referencing the JetStream wiring at compile time without running
// anything. Removing the jetstream_enable, jetstream_stream_name, or jetstream_ack_timeout_secs fields
// from NATSArgs, or removing the NATSTarget constructor, while the config flag keys remain, fails to
// compile here. A static reference rather than a runtime check, so it fires at build time even when no
// test is run.
const _: fn(&NATSArgs) = |args: &NATSArgs| {
    // Each field is read by name, so removing any of the three from NATSArgs fails to compile here.
    let _enable: Option<bool> = args.jetstream_enable;
    let _stream_name: &Option<String> = &args.jetstream_stream_name;
    let _ack_timeout: Option<u64> = args.jetstream_ack_timeout_secs;

    // The publish target entry point. Naming it by path ties this assertion to the constructor that
    // wires the JetStream context, so deleting the entry point breaks the build.
    let _entry: fn(String, NATSArgs) -> Result<NATSTarget<String>, TargetError> = NATSTarget::<String>::new;
};

/// Module-presence test naming the public JetStream entry type, the health-check entry, and the
/// publish-error variant by path. Deleting the NATSTarget constructor, the check_nats_server_available
/// entry, or the JetStreamPublish error variant fails to compile this test.
#[test]
fn jetstream_public_entry_points_are_present() {
    let _entry: fn(String, NATSArgs) -> Result<NATSTarget<String>, TargetError> = NATSTarget::<String>::new;

    let _health_check = check_nats_server_available;

    // The publish path classifies a publish failure into this variant. Constructing it by name binds
    // the guard to the variant, so removing it from TargetError breaks the build.
    let publish_error = TargetError::JetStreamPublish {
        retryable: true,
        detail: "guard".to_string(),
    };
    match publish_error {
        TargetError::JetStreamPublish { retryable, detail } => {
            assert!(retryable);
            assert_eq!(detail, "guard");
        }
        other => panic!("expected the JetStream publish variant, found {other:?}"),
    }
}

/// Cross-module completeness over the publish-error class.
///
/// Lists every async-nats PublishErrorKind variant by name with no wildcard arm. The publish path
/// classifies each kind into retryable or terminal. A new variant added upstream, or a renamed or
/// removed one, fails to compile this match, forcing the classification to be revisited rather than
/// silently folding the new kind into a catch-all.
#[test]
fn publish_error_kind_is_exhaustively_enumerated() {
    fn name_of(kind: PublishErrorKind) -> &'static str {
        match kind {
            PublishErrorKind::StreamNotFound => "StreamNotFound",
            PublishErrorKind::WrongLastMessageId => "WrongLastMessageId",
            PublishErrorKind::WrongLastSequence => "WrongLastSequence",
            PublishErrorKind::TimedOut => "TimedOut",
            PublishErrorKind::BrokenPipe => "BrokenPipe",
            PublishErrorKind::MaxAckPending => "MaxAckPending",
            PublishErrorKind::MaxPayloadExceeded => "MaxPayloadExceeded",
            PublishErrorKind::Other => "Other",
        }
    }

    let all = [
        PublishErrorKind::StreamNotFound,
        PublishErrorKind::WrongLastMessageId,
        PublishErrorKind::WrongLastSequence,
        PublishErrorKind::TimedOut,
        PublishErrorKind::BrokenPipe,
        PublishErrorKind::MaxAckPending,
        PublishErrorKind::MaxPayloadExceeded,
        PublishErrorKind::Other,
    ];
    assert_eq!(
        all.len(),
        8,
        "the publish-error class has eight kinds, update the classification on a change"
    );
    for kind in all {
        assert!(!name_of(kind).is_empty());
    }
}

/// Cross-module completeness over the failed-store error class.
///
/// Lists every FailedErrorClass variant by name with no wildcard arm. The replay worker tags a failed
/// entry with one of these classes. Adding or removing a class fails to compile this match, so the
/// tagging and the operator-facing tag stay in step.
#[test]
fn failed_error_class_is_exhaustively_enumerated() {
    fn tag_of(class: FailedErrorClass) -> &'static str {
        match class {
            FailedErrorClass::Terminal => "terminal",
        }
    }

    let all = [FailedErrorClass::Terminal];
    assert_eq!(
        all.len(),
        1,
        "the failed-store error class has one variant, update the tagging on a change"
    );
    for class in all {
        assert_eq!(tag_of(class), class.as_str(), "the guard tag matches the operator-facing tag");
    }
}

fn server_url() -> String {
    std::env::var("RUSTFS_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

fn jetstream_args(subject: &str, stream_name: &str, queue_dir: &str) -> NATSArgs {
    NATSArgs {
        enable: true,
        address: server_url(),
        subject: subject.to_string(),
        username: String::new(),
        password: String::new(),
        token: String::new(),
        credentials_file: String::new(),
        tls_ca: String::new(),
        tls_client_cert: String::new(),
        tls_client_key: String::new(),
        tls_required: false,
        queue_dir: queue_dir.to_string(),
        queue_limit: 0,
        jetstream_enable: Some(true),
        jetstream_stream_name: Some(stream_name.to_string()),
        jetstream_ack_timeout_secs: Some(30),
        target_type: TargetType::NotifyEvent,
    }
}

/// Server-backed end-to-end publish-and-ack guard.
///
/// Provisions a writable stream as an operator would, then drives the real target publish path with
/// the JetStream flag on: init connects and validates the stream, save persists the event to the
/// on-disk queue, send_from_store publishes and awaits the real PublishAck and clears the entry on it.
/// The stream message count rising and the queue draining proves the message was published and
/// acknowledged through the production code, the user-visible signal a build-only check cannot see.
///
/// Ignored by default because it needs a running NATS server with JetStream.
#[tokio::test]
#[ignore]
async fn end_to_end_publish_is_acked_on_the_stream() {
    use rustfs_targets::EventName;
    use rustfs_targets::Target;
    use rustfs_targets::target::EntityTarget;
    use std::sync::Arc;
    use uuid::Uuid;

    let suffix = Uuid::new_v4().simple().to_string();
    let stream_name = format!("RUSTFS_TEST_{suffix}");
    let subject = format!("rustfs.guard.{suffix}");
    let queue_dir = std::env::temp_dir().join(format!("rustfs-nats-guard-{suffix}"));
    let queue_dir = queue_dir.to_string_lossy().to_string();

    // Provision the stream as the operator would. RustFS validates it, never creates it.
    let client = async_nats::connect(server_url()).await.expect("connect to NATS");
    let context = async_nats::jetstream::new(client);
    let _ = context.delete_stream(&stream_name).await;
    context
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            duplicate_window: std::time::Duration::from_secs(300),
            ..Default::default()
        })
        .await
        .expect("provision the stream");

    let args = jetstream_args(&subject, &stream_name, &queue_dir);
    let target = NATSTarget::<String>::new("guard-target".to_string(), args).expect("build the flag-on target");

    target
        .init()
        .await
        .expect("init connects and validates the operator-provisioned stream");

    let count_before = stream_message_count(&context, &stream_name).await;

    let event = EntityTarget {
        object_name: "guard/object.txt".to_string(),
        bucket_name: "guard-bucket".to_string(),
        event_name: EventName::ObjectCreatedPut,
        data: "guard-payload".to_string(),
    };
    target
        .save(Arc::new(event))
        .await
        .expect("save persists the event to the queue");

    let keys = target.store().expect("the flag-on target has a queue store").list();
    assert_eq!(keys.len(), 1, "the saved event is queued exactly once");
    target
        .send_from_store(keys[0].clone())
        .await
        .expect("the queued event publishes and is acked");

    let count_after = stream_message_count(&context, &stream_name).await;
    assert_eq!(
        count_after,
        count_before + 1,
        "the published event is durably on the stream after its ack"
    );

    let remaining = target.store().expect("the flag-on target has a queue store").list();
    assert!(remaining.is_empty(), "the queue entry is cleared on the ack");

    target.close().await.expect("close drains the target");
    let _ = context.delete_stream(&stream_name).await;
    let _ = std::fs::remove_dir_all(&queue_dir);
}

async fn stream_message_count(context: &async_nats::jetstream::Context, stream_name: &str) -> u64 {
    let mut stream = context.get_stream(stream_name).await.expect("look up the stream");
    let info = stream.info().await.expect("read the stream info");
    info.state.messages
}
