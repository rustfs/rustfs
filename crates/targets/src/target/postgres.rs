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

//! PostgreSQL event notification target.
//!
//! Persists S3 events into a user-provided PostgreSQL table using the
//! `Target` trait. Two output formats are supported:
//!
//! - `namespace` (default): single row per object key, UPSERT on each event.
//! - `access`: append-only audit log with one row per delivered event.
//!
//! TLS is provided via `tokio-postgres-rustls` with rustls + aws-lc-rs.
//! When `tls_ca` is empty the connector loads native OS trust roots.
//! Connection pooling is delegated to `deadpool-postgres`; the pool itself
//! is `Clone`, so no `Mutex` is required around it.

use crate::{
    StoreError, Target,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType, build_queued_payload,
    },
};
use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use rustfs_config::{POSTGRES_TLS_CA, POSTGRES_TLS_CLIENT_CERT, POSTGRES_TLS_CLIENT_KEY};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_postgres::Config;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

/// Output format selection for the PostgreSQL target.
///
/// - `Namespace`: single-row UPSERT per object key (MinIO `namespace` style).
/// - `Access`: append-only insert per event (audit/compliance use case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresFormat {
    Namespace,
    Access,
}

impl PostgresFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            PostgresFormat::Namespace => "namespace",
            PostgresFormat::Access => "access",
        }
    }
}

/// Parses the `format` configuration value.
///
/// Accepts case-insensitive `"namespace"` or `"access"`. Defaults to
/// `Namespace` when the value is missing or empty.
pub fn parse_postgres_format(value: Option<&str>) -> Result<PostgresFormat, TargetError> {
    let raw = value.unwrap_or("").trim();
    if raw.is_empty() {
        return Ok(PostgresFormat::Namespace);
    }
    match raw.to_ascii_lowercase().as_str() {
        "namespace" => Ok(PostgresFormat::Namespace),
        "access" => Ok(PostgresFormat::Access),
        other => Err(TargetError::Configuration(format!(
            "PostgreSQL format must be 'namespace' or 'access', got: {other}"
        ))),
    }
}

/// Validates a PostgreSQL identifier (schema or table name).
///
/// Accepts only `^[A-Za-z_][A-Za-z0-9_]*$`. Quoted identifiers, dots, and
/// special characters are intentionally rejected to keep SQL string
/// construction safe without runtime escaping.
pub fn validate_pg_identifier(name: &str, kind: &str) -> Result<(), TargetError> {
    if name.is_empty() {
        return Err(TargetError::Configuration(format!("PostgreSQL {kind} cannot be empty")));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty checked above");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(TargetError::Configuration(format!(
            "PostgreSQL {kind} must start with a letter or underscore"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(TargetError::Configuration(format!(
                "PostgreSQL {kind} must match ^[A-Za-z_][A-Za-z0-9_]*$"
            )));
        }
    }
    Ok(())
}

/// PostgreSQL target configuration.
///
/// Implements a manual `Debug` that masks the password to prevent secret
/// leakage through logging or `tracing::instrument` capture.
#[derive(Clone)]
pub struct PostgresArgs {
    pub enable: bool,

    // Connection
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,

    // Schema/Table/Format
    pub schema: String,
    pub table: String,
    pub format: PostgresFormat,

    // TLS
    pub tls_required: bool,
    pub tls_ca: String,
    pub tls_client_cert: String,
    pub tls_client_key: String,

    // Queue
    pub queue_dir: String,
    pub queue_limit: u64,

    pub target_type: TargetType,
}

impl fmt::Debug for PostgresArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresArgs")
            .field("enable", &self.enable)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", if self.password.is_empty() { &"" } else { &"***REDACTED***" })
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("table", &self.table)
            .field("format", &self.format)
            .field("tls_required", &self.tls_required)
            .field("tls_ca", &self.tls_ca)
            .field("tls_client_cert", &self.tls_client_cert)
            .field(
                "tls_client_key",
                if self.tls_client_key.is_empty() {
                    &""
                } else {
                    &"***REDACTED***"
                },
            )
            .field("queue_dir", &self.queue_dir)
            .field("queue_limit", &self.queue_limit)
            .field("target_type", &self.target_type)
            .finish()
    }
}

impl PostgresArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        if self.host.trim().is_empty() {
            return Err(TargetError::Configuration("PostgreSQL host cannot be empty".to_string()));
        }
        if self.user.trim().is_empty() {
            return Err(TargetError::Configuration("PostgreSQL user cannot be empty".to_string()));
        }
        if self.database.trim().is_empty() {
            return Err(TargetError::Configuration("PostgreSQL database cannot be empty".to_string()));
        }

        validate_pg_identifier(&self.schema, "schema")?;
        validate_pg_identifier(&self.table, "table")?;

        // TLS pair must be both empty or both set
        if self.tls_client_cert.is_empty() != self.tls_client_key.is_empty() {
            return Err(TargetError::Configuration(format!(
                "PostgreSQL {POSTGRES_TLS_CLIENT_CERT} and {POSTGRES_TLS_CLIENT_KEY} must be specified together"
            )));
        }

        // Optional TLS path values must be absolute when present
        if !self.tls_ca.is_empty() && !Path::new(&self.tls_ca).is_absolute() {
            return Err(TargetError::Configuration(format!("{POSTGRES_TLS_CA} must be an absolute path")));
        }
        if !self.tls_client_cert.is_empty() && !Path::new(&self.tls_client_cert).is_absolute() {
            return Err(TargetError::Configuration(format!("{POSTGRES_TLS_CLIENT_CERT} must be an absolute path")));
        }
        if !self.tls_client_key.is_empty() && !Path::new(&self.tls_client_key).is_absolute() {
            return Err(TargetError::Configuration(format!("{POSTGRES_TLS_CLIENT_KEY} must be an absolute path")));
        }

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration(
                "PostgreSQL queue directory must be an absolute path".to_string(),
            ));
        }

        Ok(())
    }
}

/// Returns the qualified `"schema"."table"` SQL identifier for `args`.
///
/// Both schema and table are pre-validated in `PostgresArgs::validate()` so the
/// values cannot contain quote, dot, or whitespace characters; double-quoting
/// preserves case-sensitivity for users who created their tables with quoted
/// identifiers.
pub fn qualified_table(schema: &str, table: &str) -> String {
    format!(r#""{schema}"."{table}""#)
}

/// SQL for the `namespace` format. Performs UPSERT keyed on the object key.
pub fn namespace_upsert_sql(schema: &str, table: &str) -> String {
    format!(
        "INSERT INTO {} (key, value) VALUES ($1, $2::jsonb) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        qualified_table(schema, table)
    )
}

/// SQL for the `access` format. Append-only with `event_id` as PK so that
/// store-replay scenarios silently skip duplicates while distinct events still
/// land as separate rows.
pub fn access_insert_sql(schema: &str, table: &str) -> String {
    format!(
        "INSERT INTO {} (event_id, event_name, key, value, queued_at_ms) \
         VALUES ($1, $2, $3, $4::jsonb, $5) \
         ON CONFLICT (event_id) DO NOTHING",
        qualified_table(schema, table)
    )
}

/// SQL used by both `init()` and the connectivity probe to verify the table
/// exists and is readable without producing rows or triggering side effects.
pub fn table_probe_sql(schema: &str, table: &str) -> String {
    format!("SELECT 1 FROM {} LIMIT 0", qualified_table(schema, table))
}

/// Builds a rustls `ClientConfig` for the PostgreSQL connection.
///
/// When `tls_ca` is empty the OS native trust store is used via
/// `rustls-native-certs` (0.8 API: `CertificateResult { certs, errors }`).
/// When `tls_client_cert` and `tls_client_key` are both set the connection
/// uses mTLS authentication; otherwise no client cert is sent.
pub fn build_tls_config(args: &PostgresArgs) -> Result<rustls::ClientConfig, TargetError> {
    let mut root_store = rustls::RootCertStore::empty();

    if args.tls_ca.is_empty() {
        let result = rustls_native_certs::load_native_certs();
        if !result.errors.is_empty() {
            warn!(error_count = result.errors.len(), "some native CA certs failed to load");
        }
        if result.certs.is_empty() {
            return Err(TargetError::Configuration(
                "no native CA certs available; specify tls_ca explicitly".to_string(),
            ));
        }
        for cert in result.certs {
            // Skip individual add failures; corrupted certs in the system store
            // shouldn't block the rest from loading.
            let _ = root_store.add(cert);
        }
    } else {
        let pem = std::fs::read(&args.tls_ca)
            .map_err(|e| TargetError::Configuration(format!("failed to read {POSTGRES_TLS_CA}: {e}")))?;
        let mut reader = BufReader::new(pem.as_slice());
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert = cert.map_err(|e| TargetError::Configuration(format!("invalid {POSTGRES_TLS_CA}: {e}")))?;
            root_store
                .add(cert)
                .map_err(|e| TargetError::Configuration(format!("failed to add CA cert: {e}")))?;
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let client_config = if !args.tls_client_cert.is_empty() && !args.tls_client_key.is_empty() {
        let cert_pem = std::fs::read(&args.tls_client_cert)
            .map_err(|e| TargetError::Configuration(format!("failed to read {POSTGRES_TLS_CLIENT_CERT}: {e}")))?;
        let key_pem = std::fs::read(&args.tls_client_key)
            .map_err(|e| TargetError::Configuration(format!("failed to read {POSTGRES_TLS_CLIENT_KEY}: {e}")))?;

        let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_slice()))
            .collect::<Result<_, _>>()
            .map_err(|e| TargetError::Configuration(format!("invalid {POSTGRES_TLS_CLIENT_CERT}: {e}")))?;

        let key = rustls_pemfile::private_key(&mut BufReader::new(key_pem.as_slice()))
            .map_err(|e| TargetError::Configuration(format!("invalid {POSTGRES_TLS_CLIENT_KEY}: {e}")))?
            .ok_or_else(|| TargetError::Configuration(format!("no private key found in {POSTGRES_TLS_CLIENT_KEY}")))?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| TargetError::Configuration(format!("invalid mTLS pair: {e}")))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(client_config)
}

/// Builds the deadpool-postgres `Pool` used by the target.
///
/// `args.tls_required` decides whether the connection is plain TCP or wrapped
/// in rustls. The pool is `Clone` and cheap to share across `clone_box`.
pub fn build_pool(args: &PostgresArgs) -> Result<Pool, TargetError> {
    let mut pg_config = Config::new();
    pg_config
        .host(&args.host)
        .port(args.port)
        .user(&args.user)
        .dbname(&args.database);
    if !args.password.is_empty() {
        pg_config.password(&args.password);
    }

    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };

    let manager = if args.tls_required {
        let tls_config = build_tls_config(args)?;
        let connector = MakeRustlsConnect::new(tls_config);
        Manager::from_config(pg_config, connector, manager_config)
    } else {
        Manager::from_config(pg_config, tokio_postgres::NoTls, manager_config)
    };

    Pool::builder(manager)
        .build()
        .map_err(|e| TargetError::Configuration(format!("failed to build PostgreSQL pool: {e}")))
}

/// Maps a `tokio_postgres::Error` to the proper `TargetError` variant.
///
/// Connection-class errors (SQLSTATE 08, closed connection, IO) become
/// `NotConnected` so the queue store retains the payload for replay.
/// Schema and constraint problems (SQLSTATE 23, 42) become `Configuration`
/// so they are surfaced to the operator without endless retry.
pub fn map_pg_error(err: &tokio_postgres::Error, context: &str) -> TargetError {
    if err.is_closed() {
        return TargetError::NotConnected;
    }
    if let Some(db_err) = err.as_db_error() {
        let class = db_err.code().code().get(..2).unwrap_or("");
        return match class {
            "08" => TargetError::NotConnected,
            "28" => TargetError::Authentication(format!("{context}: {db_err}")),
            "23" | "42" => TargetError::Configuration(format!("{context}: {db_err}")),
            "40" => TargetError::Request(format!("{context}: {db_err}")),
            _ => TargetError::Request(format!("{context}: {db_err}")),
        };
    }
    TargetError::Network(format!("{context}: {err}"))
}

/// Maps a `deadpool_postgres::PoolError` to the proper `TargetError` variant.
pub fn map_pool_error(err: deadpool_postgres::PoolError, context: &str) -> TargetError {
    match err {
        deadpool_postgres::PoolError::Timeout(_) => TargetError::Timeout(format!("{context}: pool timeout")),
        deadpool_postgres::PoolError::Backend(pg_err) => {
            if pg_err.is_closed() {
                TargetError::NotConnected
            } else {
                TargetError::Network(format!("{context}: {pg_err}"))
            }
        }
        deadpool_postgres::PoolError::Closed => TargetError::NotConnected,
        other => TargetError::Request(format!("{context}: {other}")),
    }
}

/// PostgreSQL notification target.
///
/// Holds a cloneable `deadpool_postgres::Pool` rather than a `Mutex<Option<Pool>>`
/// so that `clone_box` does not duplicate connection state. The optional
/// `QueueStore` provides at-least-once delivery semantics consistent with the
/// other built-in targets.
pub struct PostgresTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: PostgresArgs,
    pool: Pool,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> PostgresTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(PostgresTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            pool: self.pool.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: PostgresArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Postgres.as_str().to_string());
        let pool = build_pool(&args)?;

        let queue_store = if !args.queue_dir.is_empty() {
            let base_path = PathBuf::from(&args.queue_dir);
            let specific_queue_path = base_path.join(format!("rustfs-{}-{}", ChannelTargetType::Postgres.as_str(), target_id.id));
            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => rustfs_config::notify::NOTIFY_STORE_EXTENSION,
            };
            let store = QueueStore::<QueuedPayload>::new(specific_queue_path, args.queue_limit, extension);
            if let Err(e) = store.open() {
                error!(target_id = %target_id, error = %e, "Failed to open store for PostgreSQL target");
                return Err(TargetError::Storage(format!("{e}")));
            }
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        Ok(Self {
            id: target_id,
            args,
            pool,
            store: queue_store,
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Sends a serialized event body to PostgreSQL using the configured format.
    ///
    /// Identifier validation has already happened in `PostgresArgs::validate()`,
    /// so `qualified_table` cannot produce a malformed SQL string here.
    async fn send_body(&self, body: &[u8], event_id: &str, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| map_pool_error(e, "PostgreSQL pool checkout failed"))?;

        let payload: serde_json::Value =
            serde_json::from_slice(body).map_err(|e| TargetError::Serialization(format!("Failed to parse JSON payload: {e}")))?;

        let key = format!("{}/{}", meta.bucket_name, meta.object_name);

        let result = match self.args.format {
            PostgresFormat::Namespace => {
                let sql = namespace_upsert_sql(&self.args.schema, &self.args.table);
                client.execute(&sql, &[&key, &payload]).await
            }
            PostgresFormat::Access => {
                let sql = access_insert_sql(&self.args.schema, &self.args.table);
                let event_name_str = format!("{:?}", meta.event_name);
                let queued_at_ms = meta.queued_at_unix_ms as i64;
                client
                    .execute(&sql, &[&event_id, &event_name_str, &key, &payload, &queued_at_ms])
                    .await
            }
        };

        match result {
            Ok(_) => {
                self.delivery_counters.record_success();
                Ok(())
            }
            Err(err) => Err(map_pg_error(&err, "PostgreSQL insert failed")),
        }
    }

    /// Probes the table from `init()`. Failure is non-fatal when a queue is
    /// configured: events buffer in the store until the schema is fixed.
    async fn probe_table(&self) -> Result<(), TargetError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| map_pool_error(e, "PostgreSQL pool checkout failed during init probe"))?;
        let sql = table_probe_sql(&self.args.schema, &self.args.table);
        client
            .execute(sql.as_str(), &[])
            .await
            .map_err(|e| map_pg_error(&e, "PostgreSQL table probe failed"))?;
        Ok(())
    }
}

#[async_trait]
impl<E> Target<E> for PostgresTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        if !self.is_enabled() {
            return Ok(false);
        }

        match tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| map_pool_error(e, "PostgreSQL pool checkout failed"))?;
            client
                .execute("SELECT 1", &[])
                .await
                .map_err(|e| map_pg_error(&e, "PostgreSQL liveness probe failed"))?;
            Ok::<(), TargetError>(())
        })
        .await
        {
            Ok(Ok(())) => Ok(true),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(TargetError::Timeout("PostgreSQL liveness probe timed out after 10s".to_string())),
        }
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match build_queued_payload(event.as_ref()) {
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
            // No queue: deliver immediately. Fresh UUID acts as the access-format
            // event_id so retries from the caller produce distinct rows.
            let event_id = Uuid::new_v4().to_string();
            if let Err(err) = self.send_body(&queued.body, &event_id, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        // Use the store key as a stable event_id so replays of the same physical
        // event are idempotent under the access-format composite PK.
        let event_id = key.to_string();
        self.send_body(&body, &event_id, &meta).await
    }

    async fn close(&self) -> Result<(), TargetError> {
        self.pool.close();
        info!(target_id = %self.id, "PostgreSQL target closed");
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
        match self.probe_table().await {
            Ok(()) => Ok(()),
            Err(err) if self.store.is_some() => {
                warn!(target_id = %self.id, error = %err, "PostgreSQL init probe failed; events will buffer in store");
                Ok(())
            }
            Err(err) => Err(err),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> PostgresArgs {
        PostgresArgs {
            enable: true,
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "secret".to_string(),
            database: "rustfs_events".to_string(),
            schema: "public".to_string(),
            table: "rustfs_events_namespace".to_string(),
            format: PostgresFormat::Namespace,
            tls_required: false,
            tls_ca: String::new(),
            tls_client_cert: String::new(),
            tls_client_key: String::new(),
            queue_dir: String::new(),
            queue_limit: 100_000,
            target_type: TargetType::NotifyEvent,
        }
    }

    #[test]
    fn validate_disabled_skips_all_checks() {
        let args = PostgresArgs {
            enable: false,
            host: String::new(),
            user: String::new(),
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            ..base_args()
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn validate_accepts_base_args() {
        assert!(base_args().validate().is_ok());
    }

    #[tokio::test]
    async fn is_active_returns_false_when_disabled() {
        let target = PostgresTarget::<String>::new(
            "postgres:test".to_string(),
            PostgresArgs {
                enable: false,
                ..base_args()
            },
        )
        .expect("disabled target should still construct");

        assert!(!target.is_active().await.expect("disabled target should not probe"));
    }

    #[test]
    fn validate_rejects_empty_host() {
        let args = PostgresArgs {
            host: String::new(),
            ..base_args()
        };
        let err = args.validate().expect_err("empty host should fail");
        assert!(err.to_string().contains("host cannot be empty"));
    }

    #[test]
    fn validate_rejects_empty_database() {
        let args = PostgresArgs {
            database: String::new(),
            ..base_args()
        };
        let err = args.validate().expect_err("empty database should fail");
        assert!(err.to_string().contains("database cannot be empty"));
    }

    #[test]
    fn validate_rejects_invalid_schema_identifier() {
        let args = PostgresArgs {
            schema: "public; DROP TABLE".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("invalid schema should fail");
        assert!(err.to_string().contains("schema"));
    }

    #[test]
    fn validate_rejects_invalid_table_identifier() {
        let args = PostgresArgs {
            table: "events;".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("invalid table should fail");
        assert!(err.to_string().contains("table"));
    }

    #[test]
    fn validate_rejects_table_starting_with_digit() {
        let args = PostgresArgs {
            table: "1events".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("digit-leading table should fail");
        assert!(err.to_string().contains("table"));
    }

    #[test]
    fn validate_rejects_mtls_without_key() {
        let args = PostgresArgs {
            tls_client_cert: "/etc/ssl/client.pem".to_string(),
            tls_client_key: String::new(),
            ..base_args()
        };
        let err = args.validate().expect_err("missing key should fail");
        assert!(err.to_string().contains("must be specified together"));
    }

    #[test]
    fn validate_rejects_relative_queue_dir() {
        let args = PostgresArgs {
            queue_dir: "relative/path".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("relative queue_dir should fail");
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn validate_rejects_relative_tls_ca() {
        let args = PostgresArgs {
            tls_ca: "ca.pem".to_string(),
            ..base_args()
        };
        let err = args.validate().expect_err("relative tls_ca should fail");
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn parse_format_defaults_to_namespace() {
        assert_eq!(parse_postgres_format(None).expect("ok"), PostgresFormat::Namespace);
        assert_eq!(parse_postgres_format(Some("")).expect("ok"), PostgresFormat::Namespace);
        assert_eq!(parse_postgres_format(Some("  ")).expect("ok"), PostgresFormat::Namespace);
    }

    #[test]
    fn parse_format_accepts_variants() {
        assert_eq!(parse_postgres_format(Some("namespace")).expect("ok"), PostgresFormat::Namespace);
        assert_eq!(parse_postgres_format(Some("ACCESS")).expect("ok"), PostgresFormat::Access);
        assert_eq!(parse_postgres_format(Some("Access")).expect("ok"), PostgresFormat::Access);
    }

    #[test]
    fn parse_format_rejects_unknown() {
        let err = parse_postgres_format(Some("structured")).expect_err("unknown format should fail");
        assert!(err.to_string().contains("must be 'namespace' or 'access'"));
    }

    #[test]
    fn debug_masks_password() {
        let args = base_args();
        let rendered = format!("{args:?}");
        assert!(!rendered.contains("secret"), "password leaked: {rendered}");
        assert!(rendered.contains("***REDACTED***"));
    }

    #[test]
    fn debug_masks_password_when_empty_shows_blank() {
        let args = PostgresArgs {
            password: String::new(),
            ..base_args()
        };
        let rendered = format!("{args:?}");
        assert!(!rendered.contains("***REDACTED***"));
    }

    #[test]
    fn qualified_table_double_quotes_both_parts() {
        assert_eq!(qualified_table("public", "events"), r#""public"."events""#);
        assert_eq!(qualified_table("audit", "rustfs_events"), r#""audit"."rustfs_events""#);
    }

    #[test]
    fn namespace_upsert_uses_on_conflict_update() {
        let sql = namespace_upsert_sql("public", "events");
        assert!(sql.contains("ON CONFLICT (key) DO UPDATE"));
        assert!(sql.contains(r#""public"."events""#));
        assert!(sql.contains("$2::jsonb"));
    }

    #[test]
    fn access_insert_uses_event_id_pk_with_on_conflict_do_nothing() {
        let sql = access_insert_sql("public", "events_access");
        assert!(sql.contains("event_id"));
        assert!(sql.contains("ON CONFLICT (event_id) DO NOTHING"));
        assert!(sql.contains(r#""public"."events_access""#));
        assert!(sql.contains("$4::jsonb"));
    }

    #[test]
    fn table_probe_does_not_select_rows() {
        let sql = table_probe_sql("public", "events");
        assert!(sql.contains("LIMIT 0"));
        assert!(sql.contains(r#""public"."events""#));
    }

    #[test]
    fn validate_pg_identifier_accepts_alphanumerics() {
        assert!(validate_pg_identifier("events", "table").is_ok());
        assert!(validate_pg_identifier("rustfs_events_v2", "table").is_ok());
        assert!(validate_pg_identifier("_underscored", "table").is_ok());
    }

    #[test]
    fn validate_pg_identifier_rejects_dot_and_quote() {
        assert!(validate_pg_identifier("public.events", "table").is_err());
        assert!(validate_pg_identifier("events\"DROP", "table").is_err());
        assert!(validate_pg_identifier("a b", "table").is_err());
    }
}
