// Copyright 2026 RustFS Team
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

//! Build-bound evidence for scanner and heal restart tests.

use crate::common::ClusterTopology;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(serde::Deserialize)]
struct EvidenceBuild {
    sha256: String,
}

#[derive(serde::Deserialize)]
struct RestartEvidenceRun {
    schema: u32,
    run_id: String,
    source_revision: String,
    test_build: serde_json::Value,
    binary: EvidenceBuild,
    test_binary: EvidenceBuild,
}

#[derive(Clone, Copy)]
pub(crate) struct ScannerHealEvidenceCase {
    pub(crate) id: &'static str,
    pub(crate) oracle: &'static str,
    pub(crate) evidence: &'static str,
    pub(crate) unclean_shutdown_marker: bool,
    pub(crate) topology: EvidenceTopology,
    pub(crate) storage_class_standard: Option<&'static str>,
    pub(crate) erasure_set_drive_count: Option<&'static str>,
}

#[derive(Clone, Copy)]
pub(crate) struct EvidenceTopology {
    pub(crate) nodes: usize,
    pub(crate) drives_per_node: usize,
}

impl EvidenceTopology {
    pub(crate) const fn new(nodes: usize, drives_per_node: usize) -> Self {
        Self { nodes, drives_per_node }
    }

    pub(crate) fn total_drives(self) -> usize {
        self.nodes * self.drives_per_node
    }

    pub(crate) fn cluster_topology(self) -> ClusterTopology {
        ClusterTopology::single_pool_multidrive(self.nodes, self.drives_per_node)
    }
}

pub(crate) struct RestartEvidenceContext {
    directory: PathBuf,
    run: RestartEvidenceRun,
    case: ScannerHealEvidenceCase,
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(crate) fn restart_evidence_run(
    binary: &Path,
    case: ScannerHealEvidenceCase,
) -> Result<Option<RestartEvidenceContext>, Box<dyn Error + Send + Sync>> {
    let Some(directory) = std::env::var_os("RUSTFS_SCANNER_HEAL_RUN_DIR") else {
        return Ok(None);
    };
    if case.id.is_empty()
        || case.oracle.is_empty()
        || !case.oracle.ends_with(".json")
        || case.oracle.contains('/')
        || case.oracle.contains('\\')
        || case.oracle.contains("..")
        || !matches!(case.evidence, "process-restart" | "process-crash-restart")
        || (case.evidence == "process-crash-restart") != case.unclean_shutdown_marker
    {
        return Err("invalid scanner/heal evidence case".into());
    }
    let directory = PathBuf::from(directory);
    let receipt = directory.join("run.json");
    if receipt.metadata()?.len() > 1024 * 1024 {
        return Err("oversized scanner/heal execution receipt".into());
    }
    let run: RestartEvidenceRun = serde_json::from_slice(&std::fs::read(receipt)?)?;
    if run.schema != 1 || run.run_id.len() != 32 || run.source_revision.len() != 40 {
        return Err("invalid scanner/heal execution identity".into());
    }
    let built = compiled_test_identity();
    for key in ["source_revision", "dirty", "lock_blob", "features"] {
        assert_eq!(built[key], run.test_build[key], "compiled test identity differs for {key}");
    }
    assert_eq!(file_sha256(binary)?, run.binary.sha256, "server binary must match the run receipt");
    assert_eq!(
        file_sha256(&std::env::current_exe()?)?,
        run.test_binary.sha256,
        "test executable must match the run receipt"
    );
    if directory.join(case.oracle).exists() {
        return Err("scanner/heal oracle already exists; create a new execution receipt".into());
    }
    Ok(Some(RestartEvidenceContext { directory, run, case }))
}

fn compiled_test_identity() -> serde_json::Value {
    serde_json::json!({
        "source_revision": env!("RUSTFS_E2E_BUILD_COMMIT"),
        "dirty": env!("RUSTFS_E2E_BUILD_DIRTY") != "false",
        "lock_blob": env!("RUSTFS_E2E_BUILD_LOCK"),
        "features": env!("RUSTFS_E2E_BUILD_FEATURES"),
        "target": env!("RUSTFS_E2E_BUILD_TARGET"),
        "profile": env!("RUSTFS_E2E_BUILD_PROFILE"),
        "rustflags_hex": env!("RUSTFS_E2E_BUILD_RUSTFLAGS_HEX"),
    })
}

pub(crate) struct RestartObservation {
    pub(crate) nodes: usize,
    pub(crate) drives_per_node: usize,
    pub(crate) pid_before: u32,
    pub(crate) pid_after: u32,
    pub(crate) unclean_shutdown_marker: bool,
    pub(crate) objects: Vec<serde_json::Value>,
    pub(crate) node_listings: Vec<Vec<String>>,
}

impl RestartEvidenceContext {
    pub(crate) fn write(self, binary: &Path, observed: RestartObservation) -> Result<(), Box<dyn Error + Send + Sync>> {
        assert_ne!(observed.pid_before, observed.pid_after, "target must be a new process");
        assert_eq!(file_sha256(binary)?, self.run.binary.sha256, "server build changed during restart");
        let evidence = serde_json::json!({
            "schema": 1, "case": self.case.id, "evidence": self.case.evidence,
            "run_id": self.run.run_id, "source_revision": self.run.source_revision,
            "test_build": compiled_test_identity(),
            "binary_sha256": self.run.binary.sha256,
            "test_binary_sha256": self.run.test_binary.sha256,
            "topology": {"nodes": observed.nodes, "drives_per_node": observed.drives_per_node},
            "pid_before": observed.pid_before, "pid_after": observed.pid_after,
            "unclean_shutdown_marker": observed.unclean_shutdown_marker,
            "objects": observed.objects, "node_listings": observed.node_listings,
        });
        let data = serde_json::to_vec(&evidence)?;
        if data.len() > 1024 * 1024 {
            return Err("scanner/heal oracle exceeds the 1 MiB artifact budget".into());
        }
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join(self.case.oracle))?;
        output.write_all(&data)?;
        output.sync_all()?;
        Ok(())
    }
}
