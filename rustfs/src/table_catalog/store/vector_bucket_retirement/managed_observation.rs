// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Bounded, test-only diagnostics before application error conversion.

use super::*;
use serde_json::{Value, json};
use std::{error::Error, future::Future, time::Instant};

const MAX_EVENTS: usize = 32;
const MAX_TEXT_BYTES: usize = 2048;
const MAX_SOURCE_DEPTH: usize = 4;

tokio::task_local! {
    static REQUEST: Value;
}

pub(crate) async fn with_catalog_retirement_request<F: Future>(
    request_id: Uuid,
    http_request_id: Option<&str>,
    future: F,
) -> F::Output {
    REQUEST
        .scope(
            json!({"request_id":request_id,"http_request_id":http_request_id.map(bounded_text)}),
            future,
        )
        .await
}

#[derive(Default)]
pub(super) struct Diagnostics {
    events: Vec<Value>,
    overflow: bool,
}

impl CatalogRetirementObservationHandle {
    pub(crate) fn managed_delete_diagnostics(&self) -> Value {
        let diagnostics = self._registration.managed_diagnostics.lock();
        json!({"events":diagnostics.events,"overflow":diagnostics.overflow,"capacity":MAX_EVENTS})
    }
}

struct Invocation {
    registration: Weak<Registration>,
    bucket: VectorBucketBindingV1,
    instance_id: Uuid,
    invocation_id: Uuid,
    request: Option<Value>,
    started: Instant,
}
impl Invocation {
    fn record(&self, phase: &'static str, outcome: Option<Value>) {
        let Some(registration) = self.registration.upgrade() else {
            return;
        };
        let mut diagnostics = registration.managed_diagnostics.lock();
        if diagnostics.events.len() == MAX_EVENTS {
            diagnostics.overflow = true;
            return;
        }
        let sequence = diagnostics.events.len() + 1;
        diagnostics.events.push(json!({
            "sequence":sequence,"phase":phase,"bucket":self.bucket,
            "instance_id":self.instance_id,"invocation_id":self.invocation_id,
            "request":self.request,"elapsed_ms":self.started.elapsed().as_millis(),"outcome":outcome,
        }));
    }
}

pub(crate) struct CatalogRetirementDeleteProbe {
    invocation: Option<Arc<Invocation>>,
    managed_started: bool,
    returned: bool,
}
impl CatalogRetirementDeleteProbe {
    pub(crate) fn begin(bucket: &BucketHandle, instance_id: Uuid) -> Self {
        let registration = VectorBucketBindingV1::from_handle(bucket).ok().and_then(|binding| {
            OBSERVERS
                .lock()
                .iter()
                .filter_map(Weak::upgrade)
                .find(|observer| observer.bucket == binding)
        });
        let invocation = registration.map(|registration| {
            Arc::new(Invocation {
                bucket: registration.bucket.clone(),
                registration: Arc::downgrade(&registration),
                instance_id,
                invocation_id: Uuid::new_v4(),
                request: REQUEST.try_with(Clone::clone).ok(),
                started: Instant::now(),
            })
        });
        if let Some(invocation) = &invocation {
            invocation.record("CatalogScopeStarted", None);
        }
        Self {
            invocation,
            managed_started: false,
            returned: false,
        }
    }
    pub(crate) fn catalog_scope_returned<T>(&mut self, result: &Result<T, StorageError>) {
        self.returned = result.is_err();
        if let Some(invocation) = &self.invocation {
            invocation.record("CatalogScopeReturned", Some(outcome(result)));
        }
    }
    pub(crate) fn managed_started(&mut self) {
        self.managed_started = true;
        if let Some(invocation) = &self.invocation {
            invocation.record("ManagedStarted", None);
        }
    }
    pub(crate) fn callback(&self) -> CallbackProbe {
        CallbackProbe {
            invocation: self.invocation.clone(),
            entered: false,
            returned: false,
        }
    }
    pub(crate) fn managed_returned<T>(&mut self, result: &Result<T, StorageError>) {
        self.returned = true;
        if let Some(invocation) = &self.invocation {
            invocation.record("ManagedReturned", Some(outcome(result)));
        }
    }
}
impl Drop for CatalogRetirementDeleteProbe {
    fn drop(&mut self) {
        if !self.returned
            && let Some(invocation) = &self.invocation
        {
            let phase = if self.managed_started {
                "ManagedDroppedWithoutResult"
            } else {
                "CatalogScopeDroppedWithoutResult"
            };
            invocation.record(phase, None);
        }
    }
}

pub(crate) struct CallbackProbe {
    invocation: Option<Arc<Invocation>>,
    entered: bool,
    returned: bool,
}
impl CallbackProbe {
    pub(crate) fn entered(&mut self) {
        self.entered = true;
        if let Some(invocation) = &self.invocation {
            invocation.record("CallbackStarted", None);
        }
    }
    pub(crate) fn returned<T>(&mut self, result: &Result<T, StorageError>) {
        self.returned = true;
        if let Some(invocation) = &self.invocation {
            invocation.record("CallbackReturned", Some(outcome(result)));
        }
    }
}
impl Drop for CallbackProbe {
    fn drop(&mut self) {
        // Drop also covers unwinding; only the outer result can establish its cause.
        if self.entered
            && !self.returned
            && let Some(invocation) = &self.invocation
        {
            invocation.record("CallbackDroppedWithoutResult", None);
        }
    }
}

fn bounded_text(value: &str) -> Value {
    let end = value.floor_char_boundary(MAX_TEXT_BYTES.min(value.len()));
    json!({"value":&value[..end],"truncated":end != value.len()})
}

fn outcome<T>(result: &Result<T, StorageError>) -> Value {
    match result {
        Ok(_) => json!({"result":"Succeeded"}),
        Err(error) => json!({"result":"Failed","error":storage_error(error, 0)}),
    }
}

fn storage_error(error: &StorageError, depth: usize) -> Value {
    let mut value = json!({"domain":"Storage","category":format!("{:?}",error.code())});
    match error {
        StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket,
            object,
            required,
            achieved,
        } => {
            value["mode"] = json!(mode);
            value["bucket"] = bounded_text(bucket);
            value["object"] = bounded_text(object);
            value["required"] = json!(required);
            value["achieved"] = json!(achieved);
        }
        StorageError::Lock(error) => value["source"] = lock_error(error, depth + 1),
        StorageError::Io(error) => value["source"] = io_error(error, depth + 1),
        StorageError::InsufficientReadQuorum(bucket, object) | StorageError::InsufficientWriteQuorum(bucket, object) => {
            value["bucket"] = bounded_text(bucket);
            value["object"] = bounded_text(object);
        }
        _ => {}
    }
    value
}

fn catalog_error(error: &TableCatalogStoreError, depth: usize) -> Value {
    let category = match error {
        TableCatalogStoreError::NotFound(_) => "NotFound",
        TableCatalogStoreError::NamespaceNotFound(_) => "NamespaceNotFound",
        TableCatalogStoreError::TableNotFound(_) => "TableNotFound",
        TableCatalogStoreError::AlreadyExists(_) => "AlreadyExists",
        TableCatalogStoreError::Conflict(_) => "Conflict",
        TableCatalogStoreError::Invalid(_) => "Invalid",
        TableCatalogStoreError::Unsupported(_) => "Unsupported",
        TableCatalogStoreError::Unavailable(_) => "Unavailable",
        TableCatalogStoreError::LockUnavailable { .. } => "LockUnavailable",
        TableCatalogStoreError::Internal(_) => "Internal",
        TableCatalogStoreError::Io { .. } => "Io",
    };
    let mut value = json!({"domain":"Catalog","category":category});
    match error {
        TableCatalogStoreError::LockUnavailable { source, .. } => value["source"] = lock_error(source, depth + 1),
        TableCatalogStoreError::Io {
            classification,
            context,
            source,
        } => {
            value["classification"] = json!(format!("{classification:?}"));
            value["context"] = bounded_text(context);
            value["source"] = source_error(source.as_ref(), depth + 1);
        }
        _ => {}
    }
    value
}

fn lock_error(error: &rustfs_lock::LockError, depth: usize) -> Value {
    use rustfs_lock::LockError;
    match error {
        LockError::Timeout { resource, timeout } => {
            json!({"domain":"Lock","category":"Timeout","resource":bounded_text(resource),"timeout_ms":timeout.as_millis()})
        }
        LockError::ResourceNotFound { resource } => {
            json!({"domain":"Lock","category":"ResourceNotFound","resource":bounded_text(resource)})
        }
        LockError::AlreadyLocked { resource, .. } => {
            json!({"domain":"Lock","category":"AlreadyLocked","resource":bounded_text(resource)})
        }
        LockError::InsufficientNodes { required, available } => {
            json!({"domain":"Lock","category":"InsufficientNodes","required":required,"available":available})
        }
        LockError::QuorumNotReached { required, achieved } => {
            json!({"domain":"Lock","category":"QuorumNotReached","required":required,"achieved":achieved})
        }
        LockError::Network { source, .. }
        | LockError::Serialization { source, .. }
        | LockError::Deserialization { source, .. } => {
            let category = match error {
                LockError::Network { .. } => "Network",
                LockError::Serialization { .. } => "Serialization",
                _ => "Deserialization",
            };
            json!({"domain":"Lock","category":category,"source":source_error(source.as_ref(), depth + 1)})
        }
        LockError::PermissionDenied { .. } => json!({"domain":"Lock","category":"PermissionDenied"}),
        LockError::Internal { .. } => json!({"domain":"Lock","category":"Internal"}),
        LockError::InvalidHandle { .. } => json!({"domain":"Lock","category":"InvalidHandle"}),
        LockError::Configuration { .. } => json!({"domain":"Lock","category":"Configuration"}),
        LockError::QueueFull { .. } => json!({"domain":"Lock","category":"QueueFull"}),
        LockError::NotOwner { .. } => json!({"domain":"Lock","category":"NotOwner"}),
    }
}

fn io_error(error: &std::io::Error, depth: usize) -> Value {
    json!({
        "domain":"Io","category":format!("{:?}",error.kind()),"raw_os_error":error.raw_os_error(),
        "source":error.get_ref().map(|source|source_error(source, depth + 1)),
    })
}

fn source_error(error: &(dyn Error + 'static), depth: usize) -> Value {
    if depth >= MAX_SOURCE_DEPTH {
        return json!({"domain":"Uninspected","reason":"DepthLimit"});
    }
    if let Some(error) = error.downcast_ref::<TableCatalogStoreError>() {
        catalog_error(error, depth)
    } else if let Some(error) = error.downcast_ref::<StorageError>() {
        storage_error(error, depth)
    } else if let Some(error) = error.downcast_ref::<rustfs_lock::LockError>() {
        lock_error(error, depth)
    } else if let Some(error) = error.downcast_ref::<std::io::Error>() {
        io_error(error, depth)
    } else {
        json!({"domain":"Uninspected","reason":"UnrecognizedType"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> VectorBucketBindingV1 {
        VectorBucketBindingV1 {
            version: 1,
            storage_bucket_id: "rustfs-v-demo".into(),
            bucket_incarnation: Uuid::new_v4(),
            account_namespace_id: String::new(),
            region: "us-east-1".into(),
            public_name: "vectors-demo".into(),
        }
    }

    #[test]
    fn managed_delete_diagnostics_preserve_typed_errors_without_error_payloads() {
        let error = StorageError::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: "rustfs-v-demo".into(),
            object: "publication.lock".into(),
            required: 1,
            achieved: 0,
        };
        let value = storage_error(&error, 0);
        assert_eq!(value["category"], "NamespaceLockQuorumUnavailable");
        assert_eq!(value["mode"], "write");
        assert_eq!(value["bucket"], json!({"value":"rustfs-v-demo","truncated":false}));
        assert_eq!(value["object"], json!({"value":"publication.lock","truncated":false}));
        assert_eq!(value["required"], 1);
        assert_eq!(value["achieved"], 0);
        let error = StorageError::other(TableCatalogStoreError::LockUnavailable {
            message: "sensitive-free-form-message".into(),
            source: Box::new(rustfs_lock::LockError::QuorumNotReached {
                required: 3,
                achieved: 1,
            }),
        });
        let value = storage_error(&error, 0);
        assert_eq!(value["source"]["source"]["domain"], "Catalog");
        assert_eq!(value["source"]["source"]["category"], "LockUnavailable");
        assert_eq!(value["source"]["source"]["source"]["category"], "QuorumNotReached");
        assert_eq!(value["source"]["source"]["source"]["required"], 3);
        assert_eq!(value["source"]["source"]["source"]["achieved"], 1);
        assert!(!value.to_string().contains("sensitive-free-form-message"));
        let denied = StorageError::other(TableCatalogStoreError::Io {
            classification: CatalogIoErrorKind::Denied,
            context: "authorize Vector maintenance preparation",
            source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        });
        let value = storage_error(&denied, 0);
        assert_eq!(value["source"]["source"]["classification"], "Denied");
        assert_eq!(value["source"]["source"]["context"]["value"], "authorize Vector maintenance preparation");
        for (error, expected) in [
            (StorageError::Timeout, "Timeout"),
            (StorageError::ErasureReadQuorum, "ErasureReadQuorum"),
            (StorageError::ErasureWriteQuorum, "ErasureWriteQuorum"),
            (
                StorageError::InsufficientReadQuorum("bucket".into(), "object".into()),
                "InsufficientReadQuorum",
            ),
            (
                StorageError::InsufficientWriteQuorum("bucket".into(), "object".into()),
                "InsufficientWriteQuorum",
            ),
        ] {
            assert_eq!(storage_error(&error, 0)["category"], expected);
        }
    }

    #[tokio::test]
    async fn managed_delete_diagnostics_bind_request_and_reject_other_incarnation() {
        let bucket = binding();
        let observer = observe_catalog_retirement(bucket.clone(), Arc::new(|_| Box::pin(async { Ok(()) }))).unwrap();
        let mut replacement = bucket.clone();
        replacement.bucket_incarnation = Uuid::new_v4();
        drop(CatalogRetirementDeleteProbe::begin(&replacement.to_handle().unwrap(), Uuid::new_v4()));
        assert!(observer.managed_delete_diagnostics()["events"].as_array().unwrap().is_empty());
        let instance_id = Uuid::new_v4();
        let request_id = Uuid::new_v4();
        with_catalog_retirement_request(request_id, Some("actual-http-id"), async {
            let mut probe = CatalogRetirementDeleteProbe::begin(&bucket.to_handle().unwrap(), instance_id);
            probe.catalog_scope_returned(&Ok::<_, StorageError>(()));
            probe.managed_started();
            let callback = probe.callback();
            drop(callback);
            probe.managed_returned(&Err::<(), _>(StorageError::Timeout));
        })
        .await;
        let snapshot = observer.managed_delete_diagnostics();
        let events = snapshot["events"].as_array().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[3]["phase"], "ManagedReturned");
        assert_eq!(events[3]["outcome"]["error"]["category"], "Timeout");
        for event in events {
            assert_eq!(event["bucket"], json!(bucket));
            assert_eq!(event["instance_id"], json!(instance_id));
            assert_eq!(event["invocation_id"], events[0]["invocation_id"]);
            assert_eq!(event["request"]["request_id"], json!(request_id));
            assert_eq!(event["request"]["http_request_id"], json!({"value":"actual-http-id","truncated":false}));
        }
        assert_eq!(snapshot["overflow"], false);
        let mut pending = CatalogRetirementDeleteProbe::begin(&bucket.to_handle().unwrap(), instance_id);
        drop(observer);
        let replacement_observer = observe_catalog_retirement(bucket, Arc::new(|_| Box::pin(async { Ok(()) }))).unwrap();
        pending.managed_returned(&Ok::<_, StorageError>(()));
        assert!(
            replacement_observer.managed_delete_diagnostics()["events"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn managed_delete_diagnostics_bound_events_and_source_payloads() {
        let bucket = binding();
        let observer = observe_catalog_retirement(bucket.clone(), Arc::new(|_| Box::pin(async { Ok(()) }))).unwrap();
        for _ in 0..MAX_EVENTS {
            drop(CatalogRetirementDeleteProbe::begin(&bucket.to_handle().unwrap(), Uuid::new_v4()));
        }
        let snapshot = observer.managed_delete_diagnostics();
        assert_eq!(snapshot["events"].as_array().unwrap().len(), MAX_EVENTS);
        assert_eq!(snapshot["events"][0]["phase"], "CatalogScopeStarted");
        assert_eq!(snapshot["overflow"], true);
        let text = bounded_text(&"\u{1f600}".repeat(MAX_TEXT_BYTES));
        assert_eq!(text["value"].as_str().unwrap().len(), MAX_TEXT_BYTES);
        assert_eq!(text["truncated"], true);
        let unknown = std::io::Error::other("sensitive-free-form-message");
        let value = io_error(&unknown, 0);
        assert!(!value.to_string().contains("sensitive-free-form-message"));
    }
}
