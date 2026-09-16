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

use super::*;
use crate::heal::outcome::{HealAbortReason, HealExecutionOutcome};
use std::io::Write;
use tokio::io::AsyncReadExt;

// This prefix must not overlap either namespace understood by schema-1 readers.
pub(super) const ROOT_REPORT_PREFIX: &str = "heal-terminal-report-";
const ROOT_REPORT_SCHEMA: u32 = 1;
pub(super) const MAX_ROOT_REPORT_BYTES: usize = 8 * 1024 * 1024;

/// The unchanged legacy terminal is the commit marker and rollback fence.
/// Its original timestamp also anchors refinements of a cancelled worker's report.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RootHealReport {
    schema: u32,
    terminal: RootHealTerminal,
    outcome: Option<HealTaskOutcome>,
    progress: Option<HealProgress>,
    result_items_truncated: bool,
    seqed_items: Vec<(u64, HealResultItem)>,
    next_seq: u64,
    min_seq: u64,
}

impl RootHealReport {
    pub(super) fn from_completed(terminal: RootHealTerminal, completed: &CompletedHealStatus) -> Self {
        Self {
            schema: ROOT_REPORT_SCHEMA,
            terminal,
            outcome: completed.outcome.as_deref().cloned(),
            progress: completed.progress.clone(),
            result_items_truncated: completed.result_items_truncated,
            seqed_items: completed.seqed_items.clone(),
            next_seq: completed.next_seq,
            min_seq: completed.min_seq,
        }
    }

    pub(super) fn terminal(&self) -> &RootHealTerminal {
        &self.terminal
    }

    pub(super) fn into_completed(self) -> CompletedHealStatus {
        CompletedHealStatus {
            outcome: self.outcome.map(Arc::new),
            progress: self.progress,
            result_items_truncated: self.result_items_truncated,
            seqed_items: self.seqed_items,
            next_seq: self.next_seq,
            min_seq: self.min_seq,
            ..self.terminal.into_completed()
        }
    }

    pub(super) fn encode(&self) -> Result<EcstoreDiskBytes> {
        struct BoundedWriter(Vec<u8>);
        impl Write for BoundedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > MAX_ROOT_REPORT_BYTES.saturating_sub(self.0.len()) {
                    return Err(std::io::Error::other("root heal report exceeds its size limit"));
                }
                self.0.extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = BoundedWriter(Vec::new());
        serde_json::to_writer(&mut writer, self)
            .map_err(|error| Error::Serialization(format!("Serialize root heal report: {error}")))?;
        Ok(writer.0.into())
    }
}

pub(super) fn report_path(task_id: &str) -> Result<String> {
    let _ = terminal_path(task_id)?;
    Ok(format!("{ROOT_REPORT_PREFIX}{task_id}.json"))
}

fn decode_report(task_id: &str, bytes: &[u8]) -> Result<RootHealReport> {
    let mut report: RootHealReport = serde_json::from_slice(bytes)
        .map_err(|error| Error::Serialization(format!("Invalid root heal report {task_id}: {error}")))?;
    report.terminal.validate(task_id)?;
    if report.schema != ROOT_REPORT_SCHEMA {
        return Err(Error::Other(format!("Unsupported root heal report schema for {task_id}")));
    }
    if let Some(outcome) = &report.outcome
        && (matches!(outcome.execution, HealExecutionOutcome::Pending | HealExecutionOutcome::Running)
            || (report.terminal.status == HealTaskStatus::Cancelled
                && outcome.execution != HealExecutionOutcome::Aborted(HealAbortReason::Cancelled)))
    {
        return Err(Error::Other(format!("Non-terminal or mismatched heal outcome for {task_id}")));
    }
    let mut previous = None;
    for (seq, item) in &mut report.seqed_items {
        if *seq < report.min_seq || *seq >= report.next_seq || previous.is_some_and(|previous| previous >= *seq) {
            return Err(Error::Other(format!("Invalid root heal report cursor for {task_id}")));
        }
        previous = Some(*seq);
        // Serde's string growth must not inflate the original retention accounting.
        for value in [
            &mut item.heal_item_type,
            &mut item.bucket,
            &mut item.object,
            &mut item.version_id,
            &mut item.detail,
        ] {
            value.shrink_to_fit();
        }
        for infos in [&mut item.before, &mut item.after] {
            infos.drives.shrink_to_fit();
            for drive in &mut infos.drives {
                drive.uuid.shrink_to_fit();
                drive.endpoint.shrink_to_fit();
                drive.state.shrink_to_fit();
            }
        }
    }
    if report.min_seq > report.next_seq {
        return Err(Error::Other(format!("Invalid root heal report cursor range for {task_id}")));
    }
    let mut window = CompletedHealStatus {
        seqed_items: std::mem::take(&mut report.seqed_items),
        ..report.terminal.clone().into_completed()
    };
    let count = window.seqed_items.len();
    window.bound_result_window();
    if window.seqed_items.len() != count {
        return Err(Error::Other(format!("Root heal report result window exceeds its limit for {task_id}")));
    }
    report.seqed_items = window.seqed_items;
    Ok(report)
}

pub(super) async fn read_report(disk: &DiskStore, task_id: &str) -> Result<Option<(RootHealReport, EcstoreDiskBytes)>> {
    let path = report_path(task_id)?;
    let reader = match EcstoreDiskAPI::read_file(disk.as_ref(), RUSTFS_META_BUCKET, &path).await {
        Ok(reader) => reader,
        Err(DiskError::FileNotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(MAX_ROOT_REPORT_BYTES + 1).map_err(Error::other)?)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_ROOT_REPORT_BYTES {
        return Err(Error::Other(format!("Root heal report exceeds its size limit for {task_id}")));
    }
    Ok(Some((decode_report(task_id, &bytes)?, bytes.into())))
}

pub(super) async fn write_report(disk: &DiskStore, task_id: &str, report: &RootHealReport) -> Result<()> {
    let bytes = report.encode()?;
    let previous = read_report(disk, task_id).await?;
    if previous.as_ref().is_some_and(|(_, current)| *current == bytes) {
        return Ok(());
    }
    match EcstoreDiskAPI::compare_and_update_file(
        disk.as_ref(),
        RUSTFS_META_BUCKET,
        &report_path(task_id)?,
        previous.map(|(_, bytes)| bytes),
        Some(bytes),
    )
    .await?
    {
        EcstoreConditionalFileUpdate::Updated => Ok(()),
        _ => Err(Error::Other(format!("Root heal report changed while publishing {task_id}"))),
    }
}

pub(super) async fn remove_report(disk: &DiskStore, task_id: &str, bytes: EcstoreDiskBytes) -> Result<()> {
    match EcstoreDiskAPI::compare_and_update_file(disk.as_ref(), RUSTFS_META_BUCKET, &report_path(task_id)?, Some(bytes), None)
        .await?
    {
        EcstoreConditionalFileUpdate::Updated => Ok(()),
        _ => Err(Error::Other(format!("Root heal report changed while pruning {task_id}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_report_encoding_rejects_oversized_payload() {
        let task_id = Uuid::new_v4().to_string();
        let terminal = RootHealTerminal::cancelled(&task_id, &HealType::Cluster, HealOptions::default())
            .expect("cluster is an administrator recovery type");
        let mut completed = terminal.clone().into_completed();
        completed.progress = Some(HealProgress {
            current_object: Some("x".repeat(MAX_ROOT_REPORT_BYTES)),
            ..Default::default()
        });
        let report = RootHealReport::from_completed(terminal, &completed);
        assert!(
            report
                .encode()
                .expect_err("bounded writer must reject oversized JSON")
                .to_string()
                .contains("size limit")
        );
    }
}
