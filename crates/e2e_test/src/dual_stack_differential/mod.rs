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

//! Dual-stack differential: one `rustfs` binary, two processes, one SDK script (rustfs/backlog#2740).
//!
//! Stack A always runs `RUSTFS_S3_STACK=legacy`. Stack B runs the value of `RUSTFS_E2E_STACK_B`
//! (default `legacy` until the gateway stack is assembled), and `RUSTFS_E2E_STACK_B_REGION`, when
//! set, starts B in another region so that a difference can be injected by hand. Both servers
//! answer the script in `scenarios`, which covers every operation of `impl S3 for FS`; `compare`
//! reports each `(op, field, a, b)` on which the answers differ, and every run writes its report
//! to `<target>/dual-stack/<run>.json`. Two processes, not two embedded servers: bucket metadata
//! is process-global, so two stacks in one process would read each other's buckets.

mod compare;
mod scenarios;

use crate::common::{RustFSTestEnvironment, init_logging, workspace_root};
use compare::{Difference, Observation, Report, compare, compared_cases, compared_ops, render};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const STACK_A: &str = "legacy";
const ENV_STACK_B: &str = "RUSTFS_E2E_STACK_B";
const ENV_STACK_B_REGION: &str = "RUSTFS_E2E_STACK_B_REGION";
/// Becomes `gateway` once rustfs/backlog#2734 T2.5 assembles the gateway stack.
const DEFAULT_STACK_B: &str = "legacy";
/// The region the negative control gives stack A explicitly, so an inherited `RUSTFS_REGION`
/// cannot erase the injected difference.
const CONTROL_REGION_A: &str = "us-east-1";
const CONTROL_REGION_B: &str = "eu-west-1";
/// `normal` cases the legacy stack must answer with a 2xx. The comparison alone cannot tell a
/// script that exercises real behavior from one in which both stacks fail the same way. Five of
/// the 73 fail on legacy by design of this script: PutBucketReplication (no registered remote
/// target), GetBucketReplication (so no configuration), PutBucketNotificationConfiguration (no
/// notification target configured), RestoreObject (the object is not archived) and
/// GetObjectTorrent (legacy answers `NoSuchKey`).
const MIN_NORMAL_SUCCESSES: usize = 68;

struct PairConfig {
    stack_b: String,
    region_a: Option<String>,
    region_b: Option<String>,
}

impl PairConfig {
    fn from_env() -> Self {
        Self {
            stack_b: std::env::var(ENV_STACK_B).unwrap_or_else(|_| DEFAULT_STACK_B.to_owned()),
            region_a: None,
            region_b: std::env::var(ENV_STACK_B_REGION).ok(),
        }
    }
}

/// Starts one server on `stack`, overriding its region only when asked.
async fn start_stack(stack: &str, region: Option<&str>) -> Result<RustFSTestEnvironment, BoxError> {
    let mut env = RustFSTestEnvironment::new().await?;
    let mut vars = vec![("RUSTFS_S3_STACK", stack)];
    if let Some(region) = region {
        vars.push(("RUSTFS_REGION", region));
    }
    env.start_rustfs_server_without_cleanup_with_env(&vars).await?;
    Ok(env)
}

/// Starts stack A and stack B side by side, each with its own port and data directory.
async fn spawn_pair(config: &PairConfig) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment), BoxError> {
    tokio::try_join!(
        start_stack(STACK_A, config.region_a.as_deref()),
        start_stack(&config.stack_b, config.region_b.as_deref())
    )
}

fn report_directory() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from);
    let target = if target.is_absolute() {
        target
    } else {
        workspace_root().join(target)
    };
    target.join("dual-stack")
}

struct Run {
    report: Report,
    path: PathBuf,
    a: Vec<Observation>,
}

/// Runs the script against both stacks concurrently and writes the report.
async fn run_differential(label: &str, config: &PairConfig) -> Run {
    let (server_a, server_b) = spawn_pair(config).await.expect("start stack A and stack B");
    let (a, b) = tokio::join!(Box::pin(scenarios::run(&server_a)), Box::pin(scenarios::run(&server_b)));
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    let report = Report {
        schema: 1,
        run: format!("{label}-{millis}-{}", std::process::id()),
        stack_a: STACK_A.to_owned(),
        stack_b: config.stack_b.clone(),
        region_a: config.region_a.clone(),
        region_b: config.region_b.clone(),
        compared_ops: compared_ops(&a, &b).len(),
        compared_cases: compared_cases(&a, &b),
        differences: compare(&a, &b),
    };
    let path = report.write_to(&report_directory()).expect("write the dual-stack report");
    Run { report, path, a }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn both_stacks_answer_every_operation_identically() {
        init_logging();
        let config = PairConfig::from_env();
        let run = run_differential("self-comparison", &config).await;
        let report = &run.report;

        let mut observed: Vec<_> = run.a.iter().map(|observation| observation.op).collect();
        observed.sort_unstable();
        observed.dedup();
        assert_eq!(
            observed,
            scenarios::OPERATIONS,
            "the script must drive exactly the operations of impl S3 for FS"
        );
        assert_eq!(report.compared_ops, scenarios::OPERATIONS.len(), "report: {}", run.path.display());
        assert!(
            report.differences.is_empty(),
            "stack {} and stack {} differ on {} of {} cases (report: {}):\n{}",
            report.stack_a,
            report.stack_b,
            report.differences.len(),
            report.compared_cases,
            run.path.display(),
            render(&report.differences)
        );

        let not_4xx: Vec<_> = run
            .a
            .iter()
            .filter(|observation| observation.case == "negative" && !(400..500).contains(&observation.status))
            .collect();
        assert!(not_4xx.is_empty(), "negative cases must fail with a 4xx on {STACK_A}: {not_4xx:#?}");
        let successes = run
            .a
            .iter()
            .filter(|observation| observation.case == "normal" && observation.is_success())
            .count();
        assert!(
            successes >= MIN_NORMAL_SUCCESSES,
            "only {successes} normal cases succeeded on {STACK_A}: {:#?}",
            run.a
        );
    }

    #[tokio::test]
    async fn differential_detects_an_injected_difference() {
        init_logging();
        let config = PairConfig {
            stack_b: STACK_A.to_owned(),
            region_a: Some(CONTROL_REGION_A.to_owned()),
            region_b: Some(CONTROL_REGION_B.to_owned()),
        };
        let run = run_differential("injected-region", &config).await;
        let expected = Difference {
            op: "GetBucketLocation".to_owned(),
            case: "normal".to_owned(),
            field: "body.location_constraint".to_owned(),
            a: CONTROL_REGION_A.to_owned(),
            b: CONTROL_REGION_B.to_owned(),
        };
        assert!(
            run.report.differences.contains(&expected),
            "B started in {CONTROL_REGION_B} but GetBucketLocation was not reported (report: {}):\n{}",
            run.path.display(),
            render(&run.report.differences)
        );
    }
}
