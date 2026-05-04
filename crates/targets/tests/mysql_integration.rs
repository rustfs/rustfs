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

//! MySQL notification target integration tests.
//!
//! These tests require a running MySQL 8.0+ or TiDB 8.5+ instance.
//! Set `RUSTFS_MYSQL_TEST_DSN` to enable them:
//!
//! ```bash
//! RUSTFS_MYSQL_TEST_DSN="user:pass@tcp(127.0.0.1:3306)/testdb" \
//!   cargo test -p rustfs-targets -- --ignored
//! ```

use mysql_async::{Opts, OptsBuilder, Pool, SslOpts, prelude::Queryable};
use rustfs_targets::{Target, TargetError, target::mysql::*, target::*};
use std::env;
use std::sync::Arc;
use tempfile::TempDir;

fn test_dsn() -> String {
    env::var("RUSTFS_MYSQL_TEST_DSN").expect("RUSTFS_MYSQL_TEST_DSN must be set")
}

fn table_name(prefix: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{ts}")
}

fn make_args(dsn: &str, table: &str, queue_dir: &str) -> MySqlArgs {
    MySqlArgs {
        enable: true,
        dsn_string: dsn.to_string(),
        table: table.to_string(),
        format: "access".to_string(),
        queue_dir: queue_dir.to_string(),
        queue_limit: 100,
        max_open_connections: 2,
        target_type: TargetType::NotifyEvent,
    }
}

fn make_entity(bucket: &str, object: &str, event_name: rustfs_s3_common::EventName) -> EntityTarget<serde_json::Value> {
    EntityTarget {
        object_name: object.to_string(),
        bucket_name: bucket.to_string(),
        event_name,
        data: serde_json::json!({"eventTime": "2026-05-03T10:00:00Z"}),
    }
}

fn build_test_pool(dsn_string: &str) -> Pool {
    let parsed = MySqlDsn::parse(dsn_string).expect("parse test DSN");

    let mut builder = OptsBuilder::default()
        .user(Some(parsed.user))
        .pass(Some(parsed.password))
        .ip_or_hostname(parsed.host)
        .tcp_port(parsed.port)
        .db_name(Some(parsed.database));

    if parsed.tls {
        rustls::crypto::aws_lc_rs::default_provider().install_default().ok();
        builder = builder.ssl_opts(Some(SslOpts::default()));
    }

    Pool::new(Opts::from(builder))
}

async fn drop_table(dsn: &str, table: &str) {
    let pool = build_test_pool(dsn);
    let mut conn = pool.get_conn().await.expect("get conn for drop table");
    let _ = conn
        .query_drop(format!("DROP TABLE IF EXISTS `{}`", table.replace('.', "`.`")))
        .await;
}

#[ignore]
#[tokio::test]
async fn direct_write_and_read() {
    let dsn = test_dsn();
    let table = table_name("test_direct");
    let target: MySqlTarget<serde_json::Value> =
        MySqlTarget::new("direct".to_string(), make_args(&dsn, &table, "")).expect("create target");

    target.init().await.expect("init");

    let entity = make_entity("mybucket", "obj.txt", rustfs_s3_common::EventName::ObjectCreatedPut);
    target.save(Arc::new(entity)).await.expect("save");

    let pool = build_test_pool(&dsn);
    let mut conn = pool.get_conn().await.expect("get conn");
    let rows: Vec<mysql_async::Row> = conn.query(format!("SELECT * FROM `{table}`")).await.expect("select");
    assert_eq!(rows.len(), 1);

    let data: String = mysql_async::from_value(rows[0].get(1).unwrap());
    assert!(data.contains("mybucket"), "event_data should contain bucket name, got: {data}");

    drop_table(&dsn, &table).await;
}

#[ignore]
#[tokio::test]
async fn delete_appends_row_does_not_remove_old() {
    let dsn = test_dsn();
    let table = table_name("test_delete");
    let target: MySqlTarget<serde_json::Value> =
        MySqlTarget::new("delete".to_string(), make_args(&dsn, &table, "")).expect("create target");

    target.init().await.expect("init");

    let put = make_entity("mybucket", "obj.txt", rustfs_s3_common::EventName::ObjectCreatedPut);
    target.save(Arc::new(put)).await.expect("save put");

    let delete = make_entity("mybucket", "obj.txt", rustfs_s3_common::EventName::ObjectRemovedDelete);
    target.save(Arc::new(delete)).await.expect("save delete");

    let pool = build_test_pool(&dsn);
    let mut conn = pool.get_conn().await.expect("get conn");
    let rows: Vec<mysql_async::Row> = conn.query(format!("SELECT * FROM `{table}`")).await.expect("select");
    assert_eq!(rows.len(), 2, "both PUT and DELETE should produce rows");

    drop_table(&dsn, &table).await;
}

#[ignore]
#[tokio::test]
async fn queue_store_saves_entry_and_replays() {
    let dsn = test_dsn();
    let table = table_name("test_queue");
    let tmpdir = TempDir::new().expect("temp dir");
    let queue_dir = tmpdir.path().to_str().expect("valid path");

    let target: MySqlTarget<serde_json::Value> =
        MySqlTarget::new("queue".to_string(), make_args(&dsn, &table, queue_dir)).expect("create target");

    let entity = make_entity("mybucket", "obj.txt", rustfs_s3_common::EventName::ObjectCreatedPut);
    target.save(Arc::new(entity)).await.expect("save to queue");

    let store = target.store().expect("store should exist");
    assert_eq!(store.len(), 1, "one entry should be in queue");

    // Init creates the table; no rows should exist yet
    target.init().await.expect("init");

    {
        let pool = build_test_pool(&dsn);
        let mut conn = pool.get_conn().await.expect("get conn");
        let rows: Vec<mysql_async::Row> = conn.query(format!("SELECT * FROM `{table}`")).await.expect("select");
        assert_eq!(rows.len(), 0, "no row should exist before replay");
    }

    for key in store.list() {
        target.send_from_store(key).await.expect("replay should succeed");
    }

    let pool = build_test_pool(&dsn);
    let mut conn = pool.get_conn().await.expect("get conn");
    let rows: Vec<mysql_async::Row> = conn.query(format!("SELECT * FROM `{table}`")).await.expect("select");
    assert_eq!(rows.len(), 1, "one row should exist after replay");
    assert_eq!(store.len(), 0, "queue should be empty after replay");

    drop_table(&dsn, &table).await;
}

#[ignore]
#[tokio::test]
async fn duplicate_replay_produces_duplicate_rows() {
    let dsn = test_dsn();
    let table = table_name("test_dupe");
    let tmpdir = TempDir::new().expect("temp dir");
    let queue_dir = tmpdir.path().to_str().expect("valid path");

    let target: MySqlTarget<serde_json::Value> =
        MySqlTarget::new("dupe".to_string(), make_args(&dsn, &table, queue_dir)).expect("create target");

    let entity = make_entity("mybucket", "obj.txt", rustfs_s3_common::EventName::ObjectCreatedPut);
    target.save(Arc::new(entity)).await.expect("save to queue");

    target.init().await.expect("init");

    let store = target.store().expect("store should exist");
    let keys: Vec<_> = store.list();

    for key in &keys {
        let raw = store.get_raw(key).expect("get raw");
        let queued = QueuedPayload::decode(&raw).expect("decode");

        // Replay twice: duplicate rows are expected (at-least-once)
        for _ in 0..2 {
            target
                .send_raw_from_store(key.clone(), queued.body.clone(), queued.meta.clone())
                .await
                .expect("replay");
        }
        let _ = store.del(key);
    }

    let pool = build_test_pool(&dsn);
    let mut conn = pool.get_conn().await.expect("get conn");
    let rows: Vec<mysql_async::Row> = conn.query(format!("SELECT * FROM `{table}`")).await.expect("select");
    assert_eq!(rows.len(), 2, "duplicate replay should produce 2 rows");

    drop_table(&dsn, &table).await;
}

#[ignore]
#[tokio::test]
async fn incompatible_schema_init_fails() {
    let dsn = test_dsn();
    let table = table_name("test_schema");

    {
        let pool = build_test_pool(&dsn);
        let mut conn = pool.get_conn().await.expect("get conn");
        conn.query_drop(format!("CREATE TABLE IF NOT EXISTS `{table}` (wrong_col INT NOT NULL)"))
            .await
            .expect("create incompatible table");
    }

    let target = MySqlTarget::<serde_json::Value>::new("schema".to_string(), make_args(&dsn, &table, "")).expect("create target");

    let result = target.init().await;

    match result {
        Err(TargetError::Initialization(msg)) => {
            assert!(
                msg.contains("event_time") || msg.contains("event_data"),
                "error should mention missing columns, got: {msg}"
            );
        }
        other => panic!("expected Initialization error, got {:?}", other),
    }

    drop_table(&dsn, &table).await;
}
