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

//! PostgreSQL notification target integration tests.
//!
//! These tests require a running PostgreSQL server. They are `#[ignore]` by
//! default so CI never runs them. To run locally
//! (podman recommended; docker works too):
//!
//! ```bash
//! podman run -d --name rustfs-pg-test -p 5432:5432 \
//!     -e POSTGRES_PASSWORD=rustfs -e POSTGRES_DB=rustfs_events \
//!     docker.io/library/postgres:16
//! ```
//!
//! Wait for PostgreSQL to be ready (look for `database system is ready` in logs),
//! then set `RUSTFS_TEST_PG_DSN` and run:
//!
//! ```bash
//! export RUSTFS_TEST_PG_DSN="postgres://postgres:rustfs@localhost:5432/rustfs_events"
//! cargo test -p rustfs-targets --test postgres_integration -- --ignored
//! ```
//!
//! Clean up:
//!
//! ```bash
//! podman rm -f rustfs-pg-test
//! ```

use rustfs_s3_common::EventName;
use rustfs_targets::Target;
use rustfs_targets::check_postgres_server_available;
use rustfs_targets::target::EntityTarget;
use rustfs_targets::target::TargetType;
use rustfs_targets::target::postgres::{PostgresArgs, PostgresDsn, PostgresFormat, PostgresTarget};
use serde_json::Value;
use std::sync::Arc;
use tokio_postgres::NoTls;
use url::Url;
use uuid::Uuid;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn test_args(table: &str, format: PostgresFormat) -> PostgresArgs {
    let dsn = env_or("RUSTFS_TEST_PG_DSN", "postgres://postgres:rustfs@localhost:5432/rustfs_events");
    let schema = PostgresDsn::parse(&dsn)
        .expect("RUSTFS_TEST_PG_DSN must be a valid PostgreSQL DSN")
        .schema;
    PostgresArgs {
        enable: true,
        dsn_string: dsn,
        schema,
        table: table.to_string(),
        format,
        tls_required: false,
        tls_ca: String::new(),
        tls_client_cert: String::new(),
        tls_client_key: String::new(),
        queue_dir: String::new(),
        queue_limit: 100_000,
        target_type: TargetType::NotifyEvent,
    }
}

fn with_search_path(dsn: &str, schema: &str) -> String {
    let mut url = Url::parse(dsn).expect("RUSTFS_TEST_PG_DSN must be a valid PostgreSQL DSN URL");
    url.query_pairs_mut().clear().append_pair("search_path", schema);
    url.to_string()
}

async fn raw_client(args: &PostgresArgs) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&args.dsn_string, NoTls)
        .await
        .expect("connect to postgres test server");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn unique_table(prefix: &str) -> String {
    let suffix = Uuid::new_v4().simple().to_string();
    format!("{prefix}_{}", &suffix[..16])
}

fn entity_for(bucket: &str, object: &str) -> Arc<EntityTarget<serde_json::Value>> {
    Arc::new(EntityTarget {
        bucket_name: bucket.to_string(),
        object_name: object.to_string(),
        event_name: EventName::ObjectCreatedPut,
        data: serde_json::json!({"bucket": bucket, "object": object}),
    })
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_check_postgres_server_available_with_existing_table() {
    let args = test_args("pg_class", PostgresFormat::Namespace);
    // Use a real existing table: pg_class always exists.
    let mut args = args;
    args.dsn_string = with_search_path(&args.dsn_string, "pg_catalog");
    args.schema = "pg_catalog".to_string();
    args.table = "pg_class".to_string();

    check_postgres_server_available(&args)
        .await
        .expect("connectivity probe should succeed against pg_catalog.pg_class");
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_check_postgres_server_available_missing_table_fails() {
    let args = test_args("does_not_exist_table_xyz", PostgresFormat::Namespace);
    let result = check_postgres_server_available(&args).await;
    assert!(result.is_err(), "missing table should fail the probe");
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_namespace_format_upsert_replaces_value() {
    let table = unique_table("rustfs_test_namespace");
    let args = test_args(&table, PostgresFormat::Namespace);

    // Setup: create the namespace table.
    let setup = raw_client(&args).await;
    setup
        .execute(
            &format!("CREATE TABLE \"{}\" (key VARCHAR PRIMARY KEY, value JSONB NOT NULL)", table),
            &[],
        )
        .await
        .expect("create namespace table");

    // Build target and deliver two events for the same key.
    let target: PostgresTarget<serde_json::Value> =
        PostgresTarget::new("test_namespace".to_string(), args.clone()).expect("construct target");

    target
        .save(entity_for("bucket1", "obj-A"))
        .await
        .expect("first save should succeed");
    target
        .save(entity_for("bucket1", "obj-A"))
        .await
        .expect("second save should succeed");

    // Verify only one row exists for the key (UPSERT collapsed).
    let row = setup
        .query_one(&format!("SELECT count(*)::bigint FROM \"{}\" WHERE key = $1", table), &[&"bucket1/obj-A"])
        .await
        .expect("count rows");
    let count: i64 = row.get(0);
    assert_eq!(count, 1, "namespace format should keep only one row per key");

    // Cleanup.
    setup
        .execute(&format!("DROP TABLE \"{}\"", table), &[])
        .await
        .expect("drop namespace table");
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_access_format_appends_distinct_events() {
    let table = unique_table("rustfs_test_access");
    let args = test_args(&table, PostgresFormat::Access);

    let setup = raw_client(&args).await;
    setup
        .execute(
            &format!(
                "CREATE TABLE \"{}\" (\
                    event_id TEXT PRIMARY KEY, \
                    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
                    event_name TEXT NOT NULL, \
                    key TEXT NOT NULL, \
                    value JSONB NOT NULL, \
                    queued_at_ms BIGINT NOT NULL\
                )",
                table
            ),
            &[],
        )
        .await
        .expect("create access table");

    let target: PostgresTarget<serde_json::Value> =
        PostgresTarget::new("test_access".to_string(), args.clone()).expect("construct target");

    // Two distinct events for different objects produce two rows.
    target.save(entity_for("bucket1", "obj-A")).await.expect("save A");
    target.save(entity_for("bucket1", "obj-B")).await.expect("save B");

    let row = setup
        .query_one(&format!("SELECT count(*)::bigint FROM \"{}\"", table), &[])
        .await
        .expect("count rows");
    let count: i64 = row.get(0);
    assert_eq!(count, 2, "access format should append two distinct rows");

    setup
        .execute(&format!("DROP TABLE \"{}\"", table), &[])
        .await
        .expect("drop access table");
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_access_format_replay_is_idempotent() {
    let table = unique_table("rustfs_test_access_replay");
    let args = test_args(&table, PostgresFormat::Access);

    let setup = raw_client(&args).await;
    setup
        .execute(
            &format!(
                "CREATE TABLE \"{}\" (\
                    event_id TEXT PRIMARY KEY, \
                    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
                    event_name TEXT NOT NULL, \
                    key TEXT NOT NULL, \
                    value JSONB NOT NULL, \
                    queued_at_ms BIGINT NOT NULL\
                )",
                table
            ),
            &[],
        )
        .await
        .expect("create access table");

    // Insert the same row twice with the same event_id via direct SQL — this
    // simulates store replay where send_raw_from_store is called twice with
    // the same Key.
    let event_id = Uuid::new_v4().to_string();
    let payload: Value = serde_json::json!({"EventName": "s3:ObjectCreated:Put", "Key": "bucket1/obj-A", "Records": []});
    let queued_at_ms: i64 = 1234567890;

    let sql = format!(
        "INSERT INTO \"{}\" (event_id, event_name, key, value, queued_at_ms) \
         VALUES ($1, $2, $3, $4::jsonb, $5) ON CONFLICT (event_id) DO NOTHING",
        table
    );
    setup
        .execute(&sql, &[&event_id, &"s3:ObjectCreated:Put", &"bucket1/obj-A", &payload, &queued_at_ms])
        .await
        .expect("first insert");
    setup
        .execute(&sql, &[&event_id, &"s3:ObjectCreated:Put", &"bucket1/obj-A", &payload, &queued_at_ms])
        .await
        .expect("second insert (should be silent skip)");

    let row = setup
        .query_one(&format!("SELECT count(*)::bigint FROM \"{}\"", table), &[])
        .await
        .expect("count rows");
    let count: i64 = row.get(0);
    assert_eq!(count, 1, "ON CONFLICT (event_id) DO NOTHING should make replay idempotent");

    setup
        .execute(&format!("DROP TABLE \"{}\"", table), &[])
        .await
        .expect("drop access table");
}

#[tokio::test]
#[ignore = "requires running PostgreSQL server"]
async fn test_init_succeeds_against_existing_table() {
    let table = unique_table("rustfs_test_init");
    let args = test_args(&table, PostgresFormat::Namespace);

    let setup = raw_client(&args).await;
    setup
        .execute(
            &format!("CREATE TABLE \"{}\" (key VARCHAR PRIMARY KEY, value JSONB NOT NULL)", table),
            &[],
        )
        .await
        .expect("create table");

    let target: PostgresTarget<serde_json::Value> =
        PostgresTarget::new("test_init".to_string(), args.clone()).expect("construct target");
    target.init().await.expect("init should succeed against existing table");
    target.close().await.expect("close should succeed");

    setup
        .execute(&format!("DROP TABLE \"{}\"", table), &[])
        .await
        .expect("drop table");
}

#[tokio::test]
async fn test_invalid_identifier_rejected_at_construction() {
    // No #[ignore] — pure validation, no DB needed.
    let args = test_args("malicious; DROP TABLE users", PostgresFormat::Namespace);
    match PostgresTarget::<serde_json::Value>::new("bad_id".to_string(), args) {
        Ok(_) => panic!("malicious table identifier must fail at construction"),
        Err(e) => assert!(e.to_string().contains("table"), "unexpected error: {e}"),
    }
}
