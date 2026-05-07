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

use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType, queue_store_subdir_name,
    },
};
use async_trait::async_trait;
use rustfs_config::{NATS_CREDENTIALS_FILE, NATS_TLS_CA, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{error, info, instrument};

#[derive(Debug, Clone)]
pub struct NATSArgs {
    pub enable: bool,
    pub address: String,
    pub subject: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub credentials_file: String,
    pub tls_ca: String,
    pub tls_client_cert: String,
    pub tls_client_key: String,
    pub tls_required: bool,
    pub queue_dir: String,
    pub queue_limit: u64,
    pub target_type: TargetType,
}

impl NATSArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_nats_address(&self.address)?;
        validate_nats_auth(self)?;

        if self.subject.trim().is_empty() || self.subject.chars().any(char::is_whitespace) {
            return Err(TargetError::Configuration(
                "NATS subject cannot be empty or contain whitespace".to_string(),
            ));
        }

        if !self.credentials_file.is_empty() && !Path::new(&self.credentials_file).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_CREDENTIALS_FILE} must be an absolute path")));
        }
        if !self.tls_ca.is_empty() && !Path::new(&self.tls_ca).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CA} must be an absolute path")));
        }
        if !self.tls_client_cert.is_empty() && !Path::new(&self.tls_client_cert).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_CERT} must be an absolute path")));
        }
        if !self.tls_client_key.is_empty() && !Path::new(&self.tls_client_key).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_KEY} must be an absolute path")));
        }
        if self.tls_client_cert.is_empty() != self.tls_client_key.is_empty() {
            return Err(TargetError::Configuration(
                "NATS tls_client_cert and tls_client_key must be specified together".to_string(),
            ));
        }

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("NATS queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }
}

pub fn validate_nats_address(address: &str) -> Result<async_nats::ServerAddr, TargetError> {
    let server = async_nats::ServerAddr::from_str(address)
        .map_err(|e| TargetError::Configuration(format!("Invalid NATS address: {e}")))?;

    if server.has_user_pass() {
        return Err(TargetError::Configuration("NATS address must not embed username or password".to_string()));
    }

    Ok(server)
}

fn validate_nats_auth(args: &NATSArgs) -> Result<(), TargetError> {
    let mut auth_methods = 0usize;

    if !args.token.is_empty() {
        auth_methods += 1;
    }

    if !args.credentials_file.is_empty() {
        auth_methods += 1;
    }

    let has_user = !args.username.is_empty();
    let has_password = !args.password.is_empty();
    if has_user || has_password {
        if has_user != has_password {
            return Err(TargetError::Configuration(
                "NATS username and password must be specified together".to_string(),
            ));
        }
        auth_methods += 1;
    }

    if auth_methods > 1 {
        return Err(TargetError::Configuration(
            "NATS supports only one auth method at a time: token, username/password, or credentials_file".to_string(),
        ));
    }

    Ok(())
}

pub async fn connect_nats(args: &NATSArgs) -> Result<async_nats::Client, TargetError> {
    args.validate()?;

    let mut options = async_nats::ConnectOptions::new().require_tls(args.tls_required);

    if !args.token.is_empty() {
        options = options.token(args.token.clone());
    } else if !args.username.is_empty() {
        options = options.user_and_password(args.username.clone(), args.password.clone());
    } else if !args.credentials_file.is_empty() {
        options = options
            .credentials_file(&args.credentials_file)
            .await
            .map_err(|e| TargetError::Configuration(format!("Failed to load NATS credentials file: {e}")))?;
    }

    if !args.tls_ca.is_empty() {
        options = options.add_root_certificates(PathBuf::from(&args.tls_ca));
    }
    if !args.tls_client_cert.is_empty() {
        options = options.add_client_certificate(PathBuf::from(&args.tls_client_cert), PathBuf::from(&args.tls_client_key));
    }

    options
        .connect(args.address.clone())
        .await
        .map_err(|e| TargetError::Network(format!("Failed to connect to NATS server: {e}")))
}

pub struct NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: NATSArgs,
    client: Mutex<Option<async_nats::Client>>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    connected: AtomicBool,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(NATSTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            client: Mutex::new(self.client.lock().unwrap().clone()),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: AtomicBool::new(self.connected.load(Ordering::SeqCst)),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: NATSArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Nats.as_str().to_string());
        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let specific_queue_path = base_path.join(queue_store_subdir_name(ChannelTargetType::Nats.as_str(), &target_id.id));
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
            };
            let store = QueueStore::<QueuedPayload>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(target_id = %target_id, error = %e, "Failed to open store for NATS target");
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        Ok(Self {
            id: target_id,
            args,
            client: Mutex::new(None),
            store: queue_store,
            connected: AtomicBool::new(false),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
    }

    async fn get_or_connect(&self) -> Result<async_nats::Client, TargetError> {
        if let Some(client) = self.client.lock().unwrap().clone() {
            return Ok(client);
        }

        let client = connect_nats(&self.args).await?;
        client
            .flush()
            .await
            .map_err(|e| TargetError::Network(format!("Failed to flush NATS connection: {e}")))?;
        self.connected.store(true, Ordering::SeqCst);

        let mut guard = self.client.lock().unwrap();
        let shared = guard.get_or_insert_with(|| client.clone()).clone();
        Ok(shared)
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);
        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.clone()],
        };
        let body = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;
        let meta = QueuedPayloadMeta::new(
            event.event_name,
            event.bucket_name.clone(),
            event.object_name.clone(),
            "application/json",
            body.len(),
        );
        Ok(QueuedPayload::new(meta, body))
    }

    async fn send_body(&self, body: Vec<u8>) -> Result<(), TargetError> {
        let client = self.get_or_connect().await?;
        client
            .publish(self.args.subject.clone(), body.into())
            .await
            .map_err(|e| TargetError::Request(format!("Failed to publish NATS message: {e}")))?;
        self.delivery_counters.record_success();
        Ok(())
    }
}

#[async_trait]
impl<E> Target<E> for NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        let client = self.get_or_connect().await?;
        client
            .flush()
            .await
            .map_err(|e| TargetError::Network(format!("NATS health check failed: {e}")))?;
        Ok(true)
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            let encoded = match queued.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::Storage(format!("Failed to encode queued payload: {err}")));
                }
            };
            if let Err(e) = store.put_raw(&encoded) {
                self.delivery_counters.record_final_failure();
                return Err(TargetError::Storage(format!("Failed to save event to store: {e}")));
            }
            Ok(())
        } else {
            if let Err(err) = self.send_body(queued.body).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, _key: Key, body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        self.send_body(body).await
    }

    async fn close(&self) -> Result<(), TargetError> {
        let client = self.client.lock().unwrap().take();
        self.connected.store(false, Ordering::SeqCst);
        if let Some(client) = client {
            client
                .drain()
                .await
                .map_err(|e| TargetError::Network(format!("Failed to drain NATS client: {e}")))?;
        }
        info!(target_id = %self.id, "NATS target closed");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            return Ok(());
        }
        let _ = self.get_or_connect().await?;
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters
            .snapshot(self.store.as_deref().map_or(0, |store| store.len() as u64))
    }

    fn record_final_failure(&self) {
        self.delivery_counters.record_final_failure();
    }
}
