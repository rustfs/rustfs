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
        TargetType, delete_stored_payload,
    },
};
use async_trait::async_trait;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts, prelude::Queryable};
use rustfs_config::notify::NOTIFY_STORE_EXTENSION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Arguments for configuring a MySQL notification target.
///
/// Contains all configuration values needed to connect to a MySQL/TiDB
/// database and write event notification records.
#[derive(Debug, Clone)]
pub struct MySqlArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// MySQL data source name in format: `<user>:<password>@tcp(<host>:<port>)/<database>`
    pub dsn_string: String,
    /// Target table name, accepts `identifier` or `database.identifier`
    pub table: String,
    /// Directory for persistent queue storage; must be an absolute path if non-empty
    pub queue_dir: String,
    /// Maximum number of events stored in the local queue
    pub queue_limit: u64,
    /// Maximum number of open MySQL connections in the pool (0 relies on the underlying library default)
    pub max_open_connections: usize,
    /// The target type (notify or audit)
    pub target_type: TargetType,
}

impl MySqlArgs {
    /// Validates the MySQL target configuration.
    pub fn validate(&self) -> Result<(), TargetError> {
        // If the target is disabled, validation is skipped.
        if !self.enable {
            return Ok(());
        }

        if self.dsn_string.trim().is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string cannot be empty".to_string()));
        }

        let _ = MySqlDsn::parse(&self.dsn_string)?;

        validate_table_name(&self.table)?;

        if !self.queue_dir.is_empty() {
            let path = std::path::Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("MySQL queue_dir must be an absolute path".to_string()));
            }
        }

        Ok(())
    }
}

/// Parsed representation of a MySQL DSN string.
///
/// Produced by [`MySqlDsn::parse`] and consumed by the MySQL
/// target runtime to build connection options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlDsn {
    /// MySQL user name
    pub user: String,
    /// MySQL password (plaintext, must be redacted before logging)
    pub password: String,
    /// MySQL server hostname or IP address
    pub host: String,
    /// MySQL server TCP port
    pub port: u16,
    /// Target database name
    pub database: String,
    /// Whether TLS is enabled
    pub tls: bool,
}

impl MySqlDsn {
    /// Parses a MySQL DSN string into its components.
    ///
    /// Supported formats:
    /// ```text
    /// <user>:<password>@tcp(<host>:<port>)/<database>
    /// mysql://<user>:<password>@tcp(<host>:<port>)/<database>
    /// ```
    ///
    /// Only `?tls=true`, `?tls=false`, and bare `?tls` are accepted;
    /// other TLS query parameters (`verify_ca`, etc.) are rejected.
    pub fn parse(dsn_string: &str) -> Result<MySqlDsn, TargetError> {
        let input = dsn_string.trim();
        if input.is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string cannot be empty".to_string()));
        }

        let remainder = input
            .strip_prefix("mysql://")
            .or_else(|| input.strip_prefix("MYSQL://"))
            .unwrap_or(input);

        let (body, query) = match remainder.split_once('?') {
            Some((b, q)) => (b, Some(q)),
            None => (remainder, None),
        };

        let mut tls = false;
        if let Some(query) = query {
            for param in query.split('&') {
                let param = param.trim();
                if param.is_empty() {
                    continue;
                }
                let (key, value) = param.split_once('=').unwrap_or((param, ""));
                match key.trim().to_ascii_lowercase().as_str() {
                    "tls" => {
                        let val = value.trim().to_ascii_lowercase();
                        if val == "true" || val == "tls" || val.is_empty() {
                            tls = true;
                        } else if val == "false" {
                            tls = false;
                        } else {
                            return Err(TargetError::Configuration(format!(
                                "unsupported value '{}' for TLS query parameter; use tls=true",
                                val
                            )));
                        }
                    }
                    _ => {
                        return Err(TargetError::Configuration(format!("unsupported MySQL DSN query parameter '{}'", key)));
                    }
                }
            }
        }

        let Some((credentials, host_part)) = body.split_once('@') else {
            return Err(TargetError::Configuration(
                "MySQL dsn_string must contain user:password@tcp(host:port)/database".to_string(),
            ));
        };

        let Some((user, password)) = credentials.split_once(':') else {
            return Err(TargetError::Configuration("MySQL dsn_string must contain user:password".to_string()));
        };

        let user = user.trim();
        let password = password.trim();

        if user.is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string user is empty".to_string()));
        }

        let host_part = host_part.trim();

        let Some(host_part_rest) = host_part.strip_prefix("tcp(") else {
            return Err(TargetError::Configuration("MySQL dsn_string must use tcp(host:port) format".to_string()));
        };

        let Some((host_port, rest)) = host_part_rest.split_once(')') else {
            return Err(TargetError::Configuration(
                "MySQL dsn_string missing closing ')' after host:port".to_string(),
            ));
        };

        let (host, port_str) = host_port
            .split_once(':')
            .ok_or_else(|| TargetError::Configuration("MySQL dsn_string host:port is required".to_string()))?;

        let host = host.trim();
        let port_str = port_str.trim();

        if host.is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string host is empty".to_string()));
        }

        let port: u16 = port_str
            .parse()
            .map_err(|_| TargetError::Configuration(format!("MySQL dsn_string port '{}' is not a valid u16", port_str)))?;

        let database = rest
            .strip_prefix('/')
            .ok_or_else(|| TargetError::Configuration("MySQL dsn_string must include /database after host:port".to_string()))?
            .trim();

        if database.is_empty() {
            return Err(TargetError::Configuration("MySQL dsn_string database is empty".to_string()));
        }

        Ok(MySqlDsn {
            user: user.to_string(),
            password: password.to_string(),
            host: host.to_string(),
            port,
            database: database.to_string(),
            tls,
        })
    }
}

/// Returns a redacted version of the DSN string with the password replaced by `***`.
pub(crate) fn redact_mysql_dsn(dsn_string: &str) -> String {
    let input = dsn_string.trim();
    if input.is_empty() {
        return String::new();
    }

    let remainder = input
        .strip_prefix("mysql://")
        .or_else(|| input.strip_prefix("MYSQL://"))
        .unwrap_or(input);

    let prefix = if input.starts_with("mysql://") {
        "mysql://"
    } else if input.starts_with("MYSQL://") {
        "MYSQL://"
    } else {
        ""
    };

    match remainder.split_once('@') {
        Some((credentials, host_part)) => match credentials.split_once(':') {
            Some((user, _)) => format!("{}{}:***@{}", prefix, user.trim(), host_part.trim()),
            None => format!("{prefix}***@{host_part}"),
        },
        None => format!("{prefix}***"),
    }
}

fn is_valid_identifier_segment(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }

    let mut chars = segment.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }

    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return false;
        }
    }

    true
}

pub(crate) fn validate_table_name(table: &str) -> Result<(), TargetError> {
    let table = table.trim();

    if table.is_empty() {
        return Err(TargetError::Configuration("MySQL table name is empty".to_string()));
    }

    if table.contains('.') {
        let parts: Vec<&str> = table.splitn(2, '.').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(TargetError::Configuration(format!(
                "MySQL table name '{}' is invalid; use identifier or database.identifier",
                table
            )));
        }

        if !is_valid_identifier_segment(parts[0]) {
            return Err(TargetError::Configuration(format!(
                "MySQL database name '{}' in '{}' is not a valid identifier",
                parts[0], table
            )));
        }

        if !is_valid_identifier_segment(parts[1]) {
            return Err(TargetError::Configuration(format!(
                "MySQL table name '{}' in '{}' is not a valid identifier",
                parts[1], table
            )));
        }
    } else if !is_valid_identifier_segment(table) {
        return Err(TargetError::Configuration(format!(
            "MySQL table name '{}' is not a valid identifier",
            table
        )));
    }

    Ok(())
}

pub(crate) fn quote_table_name(table: &str) -> Result<String, TargetError> {
    let table = table.trim();

    if table.contains('.') {
        let parts: Vec<&str> = table.splitn(2, '.').collect();
        Ok(format!("`{}`.`{}`", parts[0].trim(), parts[1].trim()))
    } else {
        Ok(format!("`{}`", table))
    }
}

/// Extracts `event_time` from a serialized event JSON body.
///
/// Reads `Records[0].eventTime` from the JSON payload, parses it as an
/// RFC 3339 timestamp, and returns it formatted as a MySQL DATETIME(6)
/// string (`YYYY-MM-DD HH:MM:SS.ffffff`).
///
/// Returns an error if the field is missing, not a string, or cannot
/// be parsed; never falls back to the current time.
pub(crate) fn extract_event_time(body: &[u8]) -> Result<String, TargetError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| TargetError::Serialization(format!("Failed to parse event_data JSON: {e}")))?;

    let event_time = value
        .get("Records")
        .and_then(|r| r.get(0))
        .and_then(|r| r.get("eventTime"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| TargetError::Serialization("event_data is missing Records[0].eventTime".to_string()))?;

    let dt = chrono::DateTime::parse_from_rfc3339(event_time)
        .map_err(|e| TargetError::Serialization(format!("Failed to parse eventTime '{}': {}", event_time, e)))?;

    Ok(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

async fn validate_existing_schema(conn: &mut Conn, table: &str) -> Result<(), TargetError> {
    let quoted = quote_table_name(table)?;
    let sql = format!("SHOW COLUMNS FROM {quoted}");

    let columns: Vec<mysql_async::Row> = conn
        .query(sql)
        .await
        .map_err(|e| TargetError::Initialization(format!("Failed to check MySQL table schema: {e}")))?;

    let mut has_event_time = false;
    let mut has_event_data = false;

    for row in &columns {
        let field: String = row.get(0).unwrap_or_default();
        let col_type: String = row.get(1).unwrap_or_default();
        let nullable: String = row.get(2).unwrap_or_default();

        if field == "event_time" {
            has_event_time = true;
            if !col_type.to_lowercase().starts_with("datetime") {
                return Err(TargetError::Initialization(
                    "MySQL table column 'event_time' must be DATETIME type".to_string(),
                ));
            }
            if nullable.to_lowercase() != "no" {
                return Err(TargetError::Initialization(
                    "MySQL table column 'event_time' must be NOT NULL".to_string(),
                ));
            }
        } else if field == "event_data" {
            has_event_data = true;
            if col_type.to_lowercase() != "json" {
                return Err(TargetError::Initialization(
                    "MySQL table column 'event_data' must be JSON type".to_string(),
                ));
            }
            if nullable.to_lowercase() != "no" {
                return Err(TargetError::Initialization(
                    "MySQL table column 'event_data' must be NOT NULL".to_string(),
                ));
            }
        }
    }

    if !has_event_time {
        return Err(TargetError::Initialization(
            "MySQL table is missing required column 'event_time'".to_string(),
        ));
    }
    if !has_event_data {
        return Err(TargetError::Initialization(
            "MySQL table is missing required column 'event_data'".to_string(),
        ));
    }

    Ok(())
}

/// A notification target that writes events to a MySQL/TiDB table.
///
/// Each event is appended as a new row with `event_time` and `event_data`
/// columns. The target supports at-least-once delivery semantics via a
/// local `QueueStore` that replays events after transient MySQL outages.
///
/// # Configuration example using `rc`
///
/// ```bash
/// rc admin config set ALIAS notify_mysql:primary \
///   enable=on \
///   dsn_string="rustfs:password@tcp(mysql.example.com:3306)/rustfs_events?tls=true" \
///   table="rustfs_events" \
///   queue_dir="/var/lib/rustfs/events" \
///   queue_limit="100000" \
///   max_open_connections="2"
/// ```
///
/// # Environment variables
///
/// ```bash
/// RUSTFS_NOTIFY_MYSQL_ENABLE=on
/// RUSTFS_NOTIFY_MYSQL_DSN_STRING=rustfs:password@tcp(127.0.0.1:3306)/rustfs_events
/// RUSTFS_NOTIFY_MYSQL_TABLE=rustfs_events
/// RUSTFS_NOTIFY_MYSQL_QUEUE_DIR=/opt/rustfs/events
/// RUSTFS_NOTIFY_MYSQL_QUEUE_LIMIT=100000
/// RUSTFS_NOTIFY_MYSQL_MAX_OPEN_CONNECTIONS=2
/// ```
pub struct MySqlTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Unique target identifier (name + type)
    id: TargetID,
    /// Parsed configuration for this MySQL target
    args: MySqlArgs,
    /// Optional persistent queue store for at-least-once delivery
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    /// Lazily-initialized MySQL connection pool
    pool: Arc<Mutex<Option<Pool>>>,
    /// Success/failure counters exposed via `delivery_snapshot`
    delivery_counters: Arc<TargetDeliveryCounters>,
    /// Zero-sized marker for the event type `E`
    _phantom: PhantomData<E>,
}

impl<E> MySqlTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Creates a new MySqlTarget.
    pub fn new(id: String, args: MySqlArgs) -> Result<Self, TargetError> {
        args.validate()?;

        let target_id = TargetID::new(id, ChannelTargetType::MySql.as_str().to_string());

        // If `queue_dir` is non-empty, a `QueueStore` is created for persistent at-least-once delivery.
        let queue_store = if !args.queue_dir.is_empty() {
            let queue_dir =
                PathBuf::from(&args.queue_dir).join(format!("rustfs-{}-{}", ChannelTargetType::MySql.as_str(), target_id.id));

            let extension = match args.target_type {
                TargetType::AuditLog => rustfs_config::audit::AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => NOTIFY_STORE_EXTENSION,
            };

            let store = QueueStore::<QueuedPayload>::new(queue_dir, args.queue_limit, extension);
            if let Err(e) = store.open() {
                return Err(TargetError::Storage(format!("Failed to open MySQL queue store: {e}")));
            }

            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        info!(target_id = %target_id.id, table = %args.table, "MySQL target created");

        Ok(MySqlTarget {
            id: target_id,
            args,
            store: queue_store,
            // Pool is lazily initialized on first use to avoid unnecessary connections at startup and allow for better error handling
            pool: Arc::new(Mutex::new(None)),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: PhantomData,
        })
    }

    /// Returns or lazily initializes the MySQL connection pool.
    ///
    /// # Errors
    ///
    /// | Scenario | Error variant |
    /// |---|---|
    /// | Connection refused / host unreachable / TLS handshake failed | `NotConnected` |
    /// | `SELECT 1` health check failed | `NotConnected` |
    /// | DDL permission denied / `CREATE TABLE` failed | `Initialization` |
    /// | Existing table has incompatible schema | `Initialization` |
    /// | DSN parse failure / invalid config | `Configuration` |
    async fn get_or_init_pool(&self) -> Result<Pool, TargetError> {
        {
            let guard = self.pool.lock().await;
            if let Some(pool) = guard.as_ref() {
                return Ok(pool.clone());
            }
        }

        let dsn = MySqlDsn::parse(&self.args.dsn_string)?;

        let mut builder = OptsBuilder::default()
            .user(Some(dsn.user.clone()))
            .pass(Some(dsn.password.clone()))
            .ip_or_hostname(dsn.host.clone())
            .tcp_port(dsn.port)
            .db_name(Some(dsn.database.clone()));

        if dsn.tls {
            rustls::crypto::aws_lc_rs::default_provider().install_default().ok();
            builder = builder.ssl_opts(Some(SslOpts::default()));
        } else {
            warn!(
                "MySQL target '{}' is configured without TLS. This is insecure and should not be used in production.",
                self.id
            );
        }

        // When max_open_connections is 0, no explicit upper bound is set —
        // mysql_async uses its default pool constraints (10–100).
        if self.args.max_open_connections > 0 {
            let constraints = PoolConstraints::new(1, self.args.max_open_connections).ok_or_else(|| {
                TargetError::Configuration(format!(
                    "MySQL max_open_connections must be >= 1, got {}",
                    self.args.max_open_connections
                ))
            })?;
            builder = builder.pool_opts(PoolOpts::default().with_constraints(constraints));
        }

        let opts = Opts::from(builder);
        let pool = Pool::new(opts);

        // Uses a double-check pattern: the mutex guard is only held for
        // short reads/writes to the pool cache. All I/O (connecting,
        // DDL, schema validation) happens outside the lock so that
        // concurrent callers are not blocked by a slow MySQL server.
        let mut conn = pool.get_conn().await.map_err(|_| TargetError::NotConnected)?;

        conn.query_drop("SELECT 1").await.map_err(|_| TargetError::NotConnected)?;

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (event_time DATETIME(6) NOT NULL, event_data JSON NOT NULL)",
            quote_table_name(&self.args.table)?
        );
        conn.query_drop(ddl)
            .await
            .map_err(|e| TargetError::Initialization(format!("Failed to create MySQL table: {e}")))?;

        validate_existing_schema(&mut conn, &self.args.table).await?;

        // Double-check: another caller may have initialized the pool
        // while we were doing I/O.
        let mut guard = self.pool.lock().await;
        if let Some(existing) = guard.as_ref() {
            debug!(
                "MySQL pool for target '{}' was initialized by another task during setup; using existing pool",
                self.id
            );
            return Ok(existing.clone());
        }
        *guard = Some(pool.clone());
        Ok(pool)
    }

    /// Serializes an event into a `QueuedPayload` using `TargetLog` for
    /// the JSON body with `EventName`, `Key`, and `Records`.
    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);

        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.data.clone()],
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

    /// Inserts an event directly into the MySQL table.
    async fn insert_event(&self, body: &[u8], meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(
            target_id = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            event = %meta.event_name,
            payload_len = body.len(),
            "Inserting MySQL event"
        );

        let pool = self.get_or_init_pool().await?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| TargetError::Network(format!("Failed to get MySQL connection: {e}")))?;

        let event_time = extract_event_time(body)?;
        let event_data =
            std::str::from_utf8(body).map_err(|e| TargetError::Serialization(format!("Event body is not valid UTF-8: {e}")))?;

        let sql = format!(
            "INSERT INTO {} (event_time, event_data) VALUES (?, CAST(? AS JSON))",
            quote_table_name(&self.args.table)?
        );

        conn.exec_drop(sql, (event_time.as_str(), event_data)).await.map_err(|e| {
            error!(target_id = %self.id, error = %e, "Failed to insert MySQL event");
            TargetError::Request(format!("Failed to insert event: {e}"))
        })?;

        self.delivery_counters.record_success();
        debug!(target_id = %self.id, "MySQL event inserted");
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(MySqlTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            pool: Arc::clone(&self.pool),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<E> Target<E> for MySqlTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        if !self.args.enable {
            return Ok(false);
        }

        // TODO: should we add a timeout here to avoid hanging if the database is unreachable?
        let pool = self.get_or_init_pool().await?;
        let mut conn = pool.get_conn().await.map_err(|_| TargetError::NotConnected)?;
        conn.query_drop("SELECT 1").await.map_err(|_| TargetError::NotConnected)?;
        debug!("MySQL target '{}' is reachable", self.id);
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
            // persist the event to a local queue before attempting to insert into MySQL. This will allow us to guarantee at-least-once delivery even if the database is temporarily unreachable or if the process crashes after acknowledging receipt but before writing to the database.
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

            debug!("Event saved to queue store for MySQL target: {}", self.id);
            Ok(())
        } else {
            if let Err(err) = self.insert_event(&queued.body, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }

            Ok(())
        }
    }

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(target_id = %self.id, key = %key, payload_len = body.len(), "Sending queued payload from store to MySQL target");

        match extract_event_time(&body) {
            Ok(_) => {}
            Err(_) => {
                // If the payload is missing the required eventTime field or it cannot be parsed, we consider it corrupted and drop it to avoid blocking the queue with undeliverable entries.
                // - log a warning with the target ID and key, but do not include the body in the log to avoid exposing potentially sensitive information
                // - attempt to delete the corrupted entry from the store if possible
                // - record a final failure in the delivery counters.
                error!(
                    target_id = %self.id,
                    key = %key,
                    "Corrupted queued MySQL payload: missing or invalid Records[0].eventTime; dropping entry"
                );

                if let Some(store) = &self.store {
                    if let Err(e) = delete_stored_payload(store.as_ref(), &key) {
                        error!(target_id = %self.id, key=%key, error = %e, "Failed to delete corrupted queue entry");
                    }
                }

                self.delivery_counters.record_final_failure();
                return Err(TargetError::Dropped(format!(
                    "Dropped corrupted queued MySQL payload {key}: missing or invalid Records[0].eventTime"
                )));
            }
        }

        if let Err(e) = self.insert_event(&body, &meta).await {
            if matches!(e, TargetError::NotConnected) {
                warn!(target_id = %self.id, "MySQL not reachable, event remains in queue store");
                return Err(TargetError::NotConnected);
            }
            if matches!(e, TargetError::Timeout(_)) {
                warn!(target_id = %self.id, "MySQL timeout, event remains in queue store");
                return Err(e);
            }
            error!(target_id = %self.id, error = %e, "Failed to send event from store");
            return Err(e);
        }

        debug!(target_id = %self.id, key = %key, "MySQL event replayed from store");
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        let mut guard = self.pool.lock().await;
        *guard = None;
        info!("MySQL target closed: {}", self.id);
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = crate::StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.args.enable {
            debug!("MySQL target '{}' is disabled, skipping initialization", self.id);
            return Ok(());
        }
        self.get_or_init_pool().await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dsn_format() {
        let dsn = MySqlDsn::parse("rustfs:secret123@tcp(mysql.example.com:3306)/rustfs_events").expect("valid DSN");
        assert_eq!(dsn.user, "rustfs");
        assert_eq!(dsn.password, "secret123");
        assert_eq!(dsn.host, "mysql.example.com");
        assert_eq!(dsn.port, 3306);
        assert_eq!(dsn.database, "rustfs_events");
        assert!(!dsn.tls);
    }

    #[test]
    fn parse_dsn_with_mysql_prefix() {
        let dsn = MySqlDsn::parse("mysql://rustfs:password@tcp(127.0.0.1:3306)/mydb").expect("valid DSN with prefix");
        assert_eq!(dsn.user, "rustfs");
        assert_eq!(dsn.password, "password");
        assert_eq!(dsn.host, "127.0.0.1");
        assert_eq!(dsn.port, 3306);
        assert_eq!(dsn.database, "mydb");
    }

    #[test]
    fn parse_dsn_with_tls_true() {
        let dsn = MySqlDsn::parse("rustfs:password@tcp(127.0.0.1:3306)/mydb?tls=true").expect("valid DSN with TLS");
        assert!(dsn.tls);
    }

    #[test]
    fn parse_dsn_with_tls_bare() {
        let dsn = MySqlDsn::parse("rustfs:password@tcp(127.0.0.1:3306)/mydb?tls").expect("bare tls param");
        assert!(dsn.tls);
    }

    #[test]
    fn parse_dsn_rejects_unsupported_tls_params() {
        let err =
            MySqlDsn::parse("rustfs:password@tcp(127.0.0.1:3306)/mydb?verify_ca=true").expect_err("verify_ca should be rejected");
        assert!(err.to_string().contains("verify_ca"));

        let err = MySqlDsn::parse("rustfs:password@tcp(127.0.0.1:3306)/mydb?verify_identity=true")
            .expect_err("verify_identity should be rejected");
        assert!(err.to_string().contains("verify_identity"));

        let err = MySqlDsn::parse("rustfs:password@tcp(127.0.0.1:3306)/mydb?built_in_roots=true")
            .expect_err("built_in_roots should be rejected");
        assert!(err.to_string().contains("built_in_roots"));
    }

    #[test]
    fn parse_dsn_rejects_empty() {
        let err = MySqlDsn::parse("").expect_err("empty DSN");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn parse_dsn_rejects_missing_at() {
        let err = MySqlDsn::parse("rustfs:password").expect_err("missing @");
        assert!(err.to_string().contains("must contain user:password@"));
    }

    #[test]
    fn parse_dsn_rejects_non_tcp() {
        let err = MySqlDsn::parse("rustfs:password@unix(/tmp/mysql.sock)/mydb").expect_err("non-tcp should be rejected");
        assert!(err.to_string().contains("tcp("));
    }

    #[test]
    fn redact_dsn_masks_password() {
        let redacted = redact_mysql_dsn("rustfs:secret123@tcp(mysql.example.com:3306)/rustfs_events");
        assert_eq!(redacted, "rustfs:***@tcp(mysql.example.com:3306)/rustfs_events");
    }

    #[test]
    fn redact_dsn_with_mysql_prefix() {
        let redacted = redact_mysql_dsn("mysql://rustfs:secret123@tcp(127.0.0.1:3306)/mydb");
        assert_eq!(redacted, "mysql://rustfs:***@tcp(127.0.0.1:3306)/mydb");
    }

    #[test]
    fn redact_dsn_empty_password() {
        let redacted = redact_mysql_dsn("root:@tcp(127.0.0.1:4000)/testdb");
        assert_eq!(redacted, "root:***@tcp(127.0.0.1:4000)/testdb");
    }

    #[test]
    fn validate_table_name_accepts_valid_identifier() {
        validate_table_name("rustfs_events").expect("valid table name");
        validate_table_name("my_db.events").expect("valid db.table");
        validate_table_name("_events").expect("valid starting underscore");
        validate_table_name("table_2").expect("valid with numbers");
    }

    #[test]
    fn validate_table_name_rejects_invalid() {
        let err = validate_table_name("").expect_err("empty");
        assert!(err.to_string().contains("empty"));

        let err = validate_table_name("1table").expect_err("starts with digit");
        assert!(err.to_string().contains("not a valid identifier"));

        let err = validate_table_name("my-table").expect_err("contains dash");
        assert!(err.to_string().contains("not a valid identifier"));

        let err = validate_table_name(".table").expect_err("empty db part");
        assert!(err.to_string().contains("invalid"));

        let err = validate_table_name("db.").expect_err("empty table part");
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn quote_table_name_quotes_simple() {
        let quoted = quote_table_name("rustfs_events").expect("valid");
        assert_eq!(quoted, "`rustfs_events`");
    }

    #[test]
    fn quote_table_name_quotes_database_table() {
        let quoted = quote_table_name("my_db.events").expect("valid");
        assert_eq!(quoted, "`my_db`.`events`");
    }

    #[test]
    fn extract_event_time_parses_valid_rfc3339() {
        let body =
            br#"{"EventName":"s3:ObjectCreated:Put","Key":"bucket/obj.txt","Records":[{"eventTime":"2026-05-03T10:00:00Z"}]}"#;
        let result = extract_event_time(body).expect("valid event_time");
        assert!(result.starts_with("2026-05-03 10:00:00"));
    }

    #[test]
    fn extract_event_time_missing_field_errors() {
        let body = br#"{"EventName":"s3:ObjectCreated:Put","Key":"bucket/obj.txt","Records":[]}"#;
        let err = extract_event_time(body).expect_err("missing eventTime should fail");
        assert!(err.to_string().contains("missing Records[0].eventTime"));
    }

    #[test]
    fn extract_event_time_non_string_errors() {
        let body = br#"{"EventName":"s3:ObjectCreated:Put","Records":[{"eventTime":123}]}"#;
        let err = extract_event_time(body).expect_err("non-string eventTime should fail");
        assert!(err.to_string().contains("missing Records[0].eventTime"));
    }

    #[test]
    fn extract_event_time_malformed_rfc3339_errors() {
        let body = br#"{"Records":[{"eventTime":"not-a-date"}]}"#;
        let err = extract_event_time(body).expect_err("malformed date should fail");
        assert!(err.to_string().contains("Failed to parse eventTime"));
    }

    #[test]
    fn extract_event_time_missing_records_errors() {
        let body = br#"{"EventName":"s3:ObjectCreated:Put"}"#;
        let err = extract_event_time(body).expect_err("missing Records should fail");
        assert!(err.to_string().contains("missing Records[0].eventTime"));
    }

    #[test]
    fn queued_payload_round_trip_preserves_event_data() {
        let target: MySqlTarget<serde_json::Value> = MySqlTarget::new(
            "test".to_string(),
            MySqlArgs {
                enable: false,
                dsn_string: "rustfs:pass@tcp(127.0.0.1:3306)/db".to_string(),
                table: "events".to_string(),
                queue_dir: String::new(),
                queue_limit: 0,
                max_open_connections: 2,
                target_type: TargetType::NotifyEvent,
            },
        )
        .expect("valid args");

        let entity = EntityTarget {
            object_name: "bucket%2Fobj.txt".to_string(),
            bucket_name: "testbucket".to_string(),
            event_name: rustfs_s3_common::EventName::ObjectCreatedPut,
            data: serde_json::json!({"eventTime": "2026-05-03T10:00:00Z"}),
        };

        let payload = target.build_queued_payload(&entity).expect("build payload");
        let encoded = payload.encode().expect("encode");
        let decoded = QueuedPayload::decode(&encoded).expect("decode");

        assert_eq!(decoded.meta.event_name, payload.meta.event_name);
        assert_eq!(decoded.meta.bucket_name, "testbucket");
        assert_eq!(decoded.meta.object_name, "bucket%2Fobj.txt");
        assert_eq!(decoded.meta.content_type, "application/json");

        let body_str = std::str::from_utf8(&decoded.body).expect("utf8 body");
        assert!(body_str.contains("\"EventName\""));
        assert!(body_str.contains("\"Key\""));
        assert!(body_str.contains("testbucket"));
        assert!(body_str.contains("\"Records\""));
        assert!(body_str.contains("\"eventTime\""));
    }

    #[test]
    fn send_raw_from_store_drops_corrupted_payload() {
        let tmpdir = tempfile::TempDir::new().expect("temp dir");
        let queue_dir = tmpdir.path().to_str().expect("valid path").to_string();

        let target: MySqlTarget<serde_json::Value> = MySqlTarget::new(
            "test-corrupted".to_string(),
            MySqlArgs {
                enable: false,
                dsn_string: "rustfs:pass@tcp(127.0.0.1:3306)/db".to_string(),
                table: "events".to_string(),
                queue_dir,
                queue_limit: 10,
                max_open_connections: 2,
                target_type: TargetType::NotifyEvent,
            },
        )
        .expect("valid args");

        let body = br#"{"Records":[]}"#.to_vec();
        let meta = QueuedPayloadMeta::new(
            rustfs_s3_common::EventName::ObjectCreatedPut,
            "testbucket".to_string(),
            "obj.txt".to_string(),
            "application/json",
            body.len(),
        );

        let encoded = QueuedPayload::new(meta.clone(), body.clone())
            .encode()
            .expect("encode queued payload");

        let stored_key = target.store().unwrap().put_raw(&encoded).expect("put raw");

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let result = rt.block_on(target.send_raw_from_store(stored_key.clone(), body, meta));

        match result {
            Err(TargetError::Dropped(msg)) => {
                assert!(msg.contains("Dropped"));
                assert!(msg.contains("eventTime"));
            }
            other => panic!("expected TargetError::Dropped, got {:?}", other),
        }

        assert!(
            target.store().unwrap().get_raw(&stored_key).is_err(),
            "corrupted entry should have been deleted from store"
        );

        assert_eq!(target.delivery_snapshot().failed_messages, 1);
    }

    #[test]
    fn send_raw_from_store_replays_valid_payload() {
        let tmpdir = tempfile::TempDir::new().expect("temp dir");
        let queue_dir = tmpdir.path().to_str().expect("valid path").to_string();

        let target: MySqlTarget<serde_json::Value> = MySqlTarget::new(
            "test-valid-replay".to_string(),
            MySqlArgs {
                enable: false,
                dsn_string: "rustfs:pass@tcp(127.0.0.1:3306)/db".to_string(),
                table: "events".to_string(),
                queue_dir,
                queue_limit: 10,
                max_open_connections: 2,
                target_type: TargetType::NotifyEvent,
            },
        )
        .expect("valid args");

        let body =
            br#"{"EventName":"s3:ObjectCreated:Put","Key":"bucket/obj.txt","Records":[{"eventTime":"2026-05-03T10:00:00Z"}]}"#
                .to_vec();
        let meta = QueuedPayloadMeta::new(
            rustfs_s3_common::EventName::ObjectCreatedPut,
            "testbucket".to_string(),
            "obj.txt".to_string(),
            "application/json",
            body.len(),
        );

        let encoded = QueuedPayload::new(meta.clone(), body.clone())
            .encode()
            .expect("encode queued payload");

        let stored_key = target.store().unwrap().put_raw(&encoded).expect("put raw");

        // With enable=false and no real MySQL, the insert will fail at
        // pool init. But send_raw_from_store validates event_time before
        // insert, so valid payloads pass the time check. We verify the
        // payload is NOT treated as corrupted.
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let result = rt.block_on(target.send_raw_from_store(stored_key.clone(), body, meta));

        assert!(!matches!(result, Err(TargetError::Dropped(_))), "valid payload should not return Dropped");

        // Verify entry is NOT deleted on non-Dropped errors
        assert!(target.store().unwrap().get_raw(&stored_key).is_ok(), "valid entry should remain in store");
    }
}
