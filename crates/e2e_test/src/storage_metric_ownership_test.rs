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

//! Native OTLP -> Collector -> Prometheus contract, including a rolling upgrade.

use crate::common::RustFSTestClusterEnvironment;
use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;
use std::fs::{self, File};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct ToolProcess(Child);
impl Drop for ToolProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn required_binary(name: &str) -> TestResult<PathBuf> {
    let path = PathBuf::from(std::env::var(name).map_err(|_| format!("{name} must name a pinned executable"))?);
    if !path.is_file() {
        return Err(format!("{name} does not name a file: {}", path.display()).into());
    }
    Ok(path)
}

fn free_port() -> TestResult<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

fn start_tool(binary: &Path, args: &[String], log: &Path) -> TestResult<ToolProcess> {
    let log = File::create(log)?;
    Ok(ToolProcess(
        Command::new(binary)
            .args(args)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    ))
}

async fn query(client: &reqwest::Client, base: &str, expression: &str) -> TestResult<Value> {
    let mut url = reqwest::Url::parse(&format!("{base}/api/v1/query"))?;
    url.query_pairs_mut().append_pair("query", expression);
    let result: Value = client.get(url).send().await?.error_for_status()?.json().await?;
    if result["status"] != "success" {
        return Err(format!("PromQL failed: {result}").into());
    }
    Ok(result["data"]["result"].clone())
}

async fn await_count(client: &reqwest::Client, base: &str, selector: &str, expected: u64) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let result = query(client, base, &format!("count({selector}) or vector(0)")).await;
        if let Ok(rows) = &result {
            if rows[0]["value"][1].as_str().and_then(|value| value.parse::<u64>().ok()) == Some(expected) {
                println!("PASS count={expected}: {selector}");
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("expected {expected} for {selector}; last result: {result:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn validate_dashboard_queries(client: &reqwest::Client, base: &str, observer: &str) -> TestResult {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.docker/observability/grafana/dashboards/rustfs.json");
    let dashboard: Value = serde_json::from_str(&fs::read_to_string(path)?)?;
    for name in ["storage_cluster", "storage_observer"] {
        let variable = dashboard["templating"]["list"]
            .as_array()
            .ok_or("dashboard variables")?
            .iter()
            .find(|variable| variable["name"] == name)
            .ok_or("storage selection variable")?;
        assert_eq!(variable["multi"], false, "storage views must select one {name}");
        assert_eq!(variable["includeAll"], false, "storage views must select one {name}");
    }
    let mut pending = dashboard["panels"]
        .as_array()
        .ok_or("dashboard panels")?
        .iter()
        .collect::<Vec<_>>();
    let mut checked = 0;
    while let Some(panel) = pending.pop() {
        if let Some(children) = panel["panels"].as_array() {
            pending.extend(children);
        }
        for target in panel["targets"].as_array().into_iter().flatten() {
            let Some(expression) = target["expr"].as_str() else { continue };
            if !expression.contains("rustfs:storage:current") {
                continue;
            }
            if expression.contains("collection_scope=\"cluster\"") {
                assert!(
                    expression.contains("observer=\"$storage_observer\""),
                    "global views must select one observer: {expression}"
                );
            }
            let mut expression = expression.to_string();
            for (name, value) in [
                ("$__rate_interval", "5m"),
                ("$storage_cluster", "metrics-e2e"),
                ("$storage_observer", observer),
                ("$drive_api", ".*"),
                ("$server", ".*"),
                ("$drive", ".*"),
                ("$job", "rustfs"),
            ] {
                expression = expression.replace(name, value);
            }
            query(client, base, &expression).await?;
            checked += 1;
        }
    }
    assert!(checked > 0, "the storage dashboard queries must be exercised");
    println!("PASS: {checked} storage dashboard queries against the live pipeline");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "external tools: pinned Collector, Prometheus, previous release and current RustFS binaries"]
async fn storage_metric_ownership_pipeline() -> TestResult {
    let baseline = required_binary("RUSTFS_METRICS_BASELINE_BINARY")?;
    let current = required_binary("CARGO_BIN_EXE_rustfs")?;
    let collector = required_binary("RUSTFS_OTELCOL_BINARY")?;
    let prometheus = required_binary("RUSTFS_PROMETHEUS_BINARY")?;
    let temp = tempfile::Builder::new().prefix("rustfs-storage-metrics-").tempdir()?;
    let work = if let Ok(path) = std::env::var("RUSTFS_METRICS_E2E_ARTIFACTS") {
        let path = PathBuf::from(path);
        fs::create_dir_all(&path)?;
        tempfile::Builder::new().prefix("storage-run-").tempdir_in(path)?.keep()
    } else {
        temp.path().to_path_buf()
    };
    println!("Metrics pipeline logs: {}", work.display());
    let otlp = free_port()?;
    let scrape = free_port()?;
    let prom = free_port()?;
    let collector_config = work.join("collector.yaml");
    fs::write(
        &collector_config,
        format!(
            r#"receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:{otlp}
exporters:
  prometheus:
    endpoint: 127.0.0.1:{scrape}
    send_timestamps: true
    metric_expiration: 5m
    resource_to_telemetry_conversion:
      enabled: true
service:
  telemetry:
    metrics:
      level: none
    logs:
      level: warn
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
"#
        ),
    )?;
    let rules = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.docker/observability/prometheus-rules/rustfs-storage.yml")
        .canonicalize()?;
    let prom_config = work.join("prometheus.yaml");
    fs::write(
        &prom_config,
        format!(
            r#"global:
  scrape_interval: 1s
  evaluation_interval: 1s
rule_files:
  - '{}'
scrape_configs:
  - job_name: rustfs
    static_configs:
      - targets: ['127.0.0.1:{scrape}']
"#,
            rules.display()
        ),
    )?;
    // Use the shipped expressions, with only the test evaluation interval shortened.
    let test_rules = work.join("storage-rules.yaml");
    fs::write(&test_rules, fs::read_to_string(&rules)?.replace("interval: 15s", "interval: 1s"))?;
    fs::write(
        &prom_config,
        fs::read_to_string(&prom_config)?.replace(&rules.display().to_string(), &test_rules.display().to_string()),
    )?;
    let _collector = start_tool(
        &collector,
        &[format!("--config={}", collector_config.display())],
        &work.join("collector.log"),
    )?;
    let _prometheus = start_tool(
        &prometheus,
        &[
            format!("--config.file={}", prom_config.display()),
            format!("--web.listen-address=127.0.0.1:{prom}"),
            format!("--storage.tsdb.path={}", work.join("prometheus-data").display()),
        ],
        &work.join("prometheus.log"),
    )?;
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;
    let prom_url = format!("http://127.0.0.1:{prom}");
    await_count(&http, &prom_url, "up{job=\"rustfs\"} == 1", 1).await?;

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.set_env("NO_PROXY", "127.0.0.1,localhost");
    cluster.set_env("RUSTFS_OBS_METRIC_ENDPOINT", format!("http://127.0.0.1:{otlp}/v1/metrics"));
    cluster.set_env("OTEL_RESOURCE_ATTRIBUTES", "rustfs.cluster.id=metrics-e2e");
    cluster.set_env("RUSTFS_OBS_METER_INTERVAL", "2");
    cluster.set_env("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "true");
    cluster.set_env("RUSTFS_OBS_LOGS_EXPORT_ENABLED", "false");
    cluster.set_env("RUSTFS_OBS_TRACES_EXPORT_ENABLED", "false");
    cluster.set_env("RUSTFS_METRICS_NODE_INTERVAL", "2");
    cluster.set_env("RUSTFS_METRICS_CLUSTER_INTERVAL", "5");
    for index in 0..4 {
        // Four localhost processes otherwise share the startup resource IP.
        // Distinct test host IDs model the four hosts in a distributed deployment.
        cluster.set_node_env(
            index,
            "OTEL_RESOURCE_ATTRIBUTES",
            format!("rustfs.cluster.id=metrics-e2e,host.id=metrics-node-{index}"),
        )?;
        cluster.set_node_capture_log_path(index, work.join(format!("node-{index}.log")).display().to_string())?;
    }
    cluster.start_with_binary(&baseline).await?;
    // Reproduce the original four observers x four global drives before fixing it.
    await_count(&http, &prom_url, "rustfs_system_drive_total_bytes{collection_scope=\"\",drive!=\"\"}", 16).await?;
    let local = "rustfs:storage:current{source_metric=\"rustfs_system_drive_total_bytes\",collection_scope=\"local\",rustfs_cluster_id=\"metrics-e2e\"}";
    for index in 0..4 {
        cluster.stop_node_gracefully(index).await?;
        cluster.start_node_from_binary(index, &current).await?;
        await_count(&http, &prom_url, local, u64::try_from(index + 1)?).await?;
    }
    let rows = query(&http, &prom_url, local).await?;
    for row in rows.as_array().ok_or("expected a metric vector")? {
        assert_eq!(
            row["metric"]["observer"], row["metric"]["server"],
            "a node must only export its own detailed drives"
        );
    }
    let observer = cluster.nodes[0].address.clone();
    let inventory = format!(
        "rustfs:storage:current{{source_metric=\"rustfs_cluster_drive_present\",collection_scope=\"cluster\",rustfs_cluster_id=\"metrics-e2e\",observer=\"{observer}\"}}"
    );
    await_count(&http, &prom_url, &inventory, 4).await?;
    cluster.create_test_bucket("metrics-ownership").await?;
    let client = cluster.create_s3_client(0)?;
    for index in 0..8 {
        client
            .put_object()
            .bucket("metrics-ownership")
            .key(format!("object-{index}"))
            .body(ByteStream::from(vec![7_u8; 4096]))
            .send()
            .await?;
    }
    await_count(
        &http,
        &prom_url,
        "count by (server) (rustfs:storage:current{source_metric=\"rustfs_system_drive_api_calls_total\",collection_scope=\"local\"})",
        4,
    ).await?;
    let counters = query(
        &http,
        &prom_url,
        "rustfs:storage:current{source_metric=\"rustfs_system_drive_api_calls_total\",collection_scope=\"local\"}",
    )
    .await?;
    assert!(
        !counters.as_array().ok_or("expected counters")?.is_empty(),
        "exercise actual storage counters"
    );
    for row in counters.as_array().ok_or("expected counters")? {
        assert_eq!(row["metric"]["observer"], row["metric"]["server"]);
        assert!(
            !row["metric"]["disk_id"].as_str().unwrap_or_default().is_empty(),
            "counters must carry physical disk identity"
        );
    }
    validate_dashboard_queries(&http, &prom_url, &observer).await?;
    for index in (1..4).rev() {
        cluster.stop_node(index)?;
        // The Collector stays alive; cached samples must not keep stopped owners fresh.
        await_count(&http, &prom_url, local, u64::try_from(index)?).await?;
        await_count(&http, &prom_url, &inventory, 4).await?;
        let unavailable = format!(
            "rustfs:storage:current{{source_metric=\"rustfs_cluster_drive_runtime_state\",collection_scope=\"cluster\",rustfs_cluster_id=\"metrics-e2e\",observer=\"{observer}\",state=~\"offline|unknown|suspect\"}} == 1"
        );
        await_count(&http, &prom_url, &unavailable, u64::try_from(4 - index)?).await?;
    }
    cluster.stop();
    await_count(&http, &prom_url, local, 0).await?;
    cluster.start_with_binary(&current).await?;
    await_count(&http, &prom_url, local, 4).await?;
    await_count(&http, &prom_url, &inventory, 4).await?;
    let restored = client.get_object().bucket("metrics-ownership").key("object-0").send().await?;
    assert_eq!(restored.body.collect().await?.into_bytes().as_ref(), vec![7_u8; 4096].as_slice());
    println!("PASS: baseline duplication; four rolling upgrades; owner identity; counters; 4 -> 3 -> 2 -> 1 -> 0 -> 4 recovery");
    Ok(())
}
