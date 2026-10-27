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

//! Opt-in attribution for the buffered local shard batch path.

use crate::get_stage_metrics_enabled;
use metrics::{Counter, Histogram};
use std::time::Duration;

/// One decision per reader-setup attempt, rather than per S3 request.
#[derive(Clone, Copy, Debug)]
pub enum LocalBatchReadDecision {
    Selected,
    MmapDisabled,
    CopySource,
    ReadCapExceeded,
    InlineData,
    NonLocalShard,
    InvalidPath,
    NoLocalShards,
    UnsupportedPlatform,
}

impl LocalBatchReadDecision {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::MmapDisabled => "mmap_disabled",
            Self::CopySource => "copy_source",
            Self::ReadCapExceeded => "read_cap_exceeded",
            Self::InlineData => "inline_data",
            Self::NonLocalShard => "non_local_shard",
            Self::InvalidPath => "invalid_path",
            Self::NoLocalShards => "no_local_shards",
            Self::UnsupportedPlatform => "unsupported_platform",
        }
    }
}

/// Record eligibility, including path preparation only when it actually ran.
pub fn record_local_batch_read_decision(decision: LocalBatchReadDecision, preparation: Option<Duration>) {
    if !get_stage_metrics_enabled() {
        return;
    }
    let decision = decision.as_str();
    counter!("rustfs_io_get_object_local_batch_decisions_total", "decision" => decision).increment(1);
    if let Some(duration) = preparation {
        histogram!("rustfs_io_get_object_local_batch_prepare_seconds", "decision" => decision).record(duration.as_secs_f64());
    }
}

/// Stages of the existing buffered positioned-read implementation.
#[derive(Clone, Copy)]
pub enum LocalBatchReadStage {
    QueueWait,
    Execution,
    Metadata,
    Open,
    Read,
}

impl LocalBatchReadStage {
    const fn index(self) -> usize {
        match self {
            Self::QueueWait => 0,
            Self::Execution => 1,
            Self::Metadata => 2,
            Self::Open => 3,
            Self::Read => 4,
        }
    }
}

/// Handles captured before dispatch, kept alive by the actual blocking worker.
///
/// Capturing the recorder once avoids per-I/O registration and also preserves
/// completion samples when the caller stops waiting. No timers or handles are
/// created with GET-stage metrics disabled. The snapshot applies to the whole
/// submitted batch, including work that finishes after the switch is disabled.
pub struct LocalBatchReadMetrics {
    stages: [[Histogram; 2]; 5],
    shards: [Counter; 2],
    requested_bytes: [Counter; 2],
    read_bytes: [Counter; 2],
    batch_size: Histogram,
}

fn outcome(success: bool) -> &'static str {
    if success { "success" } else { "error" }
}

impl LocalBatchReadMetrics {
    /// Resolve bounded-label handles only for an explicitly instrumented batch.
    /// Boxing the enabled state keeps the blocking task small when disabled.
    pub fn capture() -> Option<Box<Self>> {
        if !get_stage_metrics_enabled() {
            return None;
        }
        let stages = ["queue_wait", "execution", "metadata", "open", "read"].map(|stage| {
            [false, true].map(|success| {
                histogram!("rustfs_io_get_object_local_batch_stage_seconds", "stage" => stage, "outcome" => outcome(success))
            })
        });
        Some(Box::new(Self {
            stages,
            shards: [false, true]
                .map(|success| counter!("rustfs_io_get_object_local_batch_shards_total", "outcome" => outcome(success))),
            requested_bytes: [false, true]
                .map(|success| counter!("rustfs_io_get_object_local_batch_requested_bytes_total", "outcome" => outcome(success))),
            read_bytes: [false, true]
                .map(|success| counter!("rustfs_io_get_object_local_batch_read_bytes_total", "outcome" => outcome(success))),
            batch_size: histogram!("rustfs_io_get_object_local_batch_shards"),
        }))
    }

    /// Count shards actually submitted to this blocking batch.
    pub fn record_batch(&self, shards: usize) {
        self.batch_size.record(crate::shard_read_fanout_to_f64(shards));
    }

    /// Record an executed stage; unattempted stages receive no sample.
    ///
    /// Queue wait succeeds when the worker starts. Execution covers the whole
    /// batch, while metadata/open/read describe individual I/O operations.
    /// Read timing excludes buffer allocation; execution timing includes it.
    pub fn record_stage(&self, stage: LocalBatchReadStage, success: bool, duration: Duration) {
        self.stages[stage.index()][usize::from(success)].record(duration.as_secs_f64());
    }

    /// Separate completed shard outcomes and bytes actually returned by read_at.
    pub fn record_shard(&self, success: bool, requested: usize, read: usize) {
        let index = usize::from(success);
        self.shards[index].increment(1);
        self.requested_bytes[index].increment(u64::try_from(requested).unwrap_or(u64::MAX));
        self.read_bytes[index].increment(u64::try_from(read).unwrap_or(u64::MAX));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::set_get_stage_metrics_enabled;
    use crate::tests::{METRICS_FLAG_LOCK, counter_total, histogram_samples};
    use metrics_util::debugging::DebuggingRecorder;
    use std::time::Duration;

    #[test]
    fn local_batch_read_metrics_are_disabled_without_stage_opt_in() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            set_get_stage_metrics_enabled(false);
            assert!(LocalBatchReadMetrics::capture().is_none());
            record_local_batch_read_decision(LocalBatchReadDecision::Selected, Some(Duration::from_millis(1)));
        });
        assert!(snapshotter.snapshot().into_vec().is_empty());
    }

    #[test]
    fn local_batch_read_handles_record_worker_costs_and_actual_bytes() {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let observation = metrics::with_local_recorder(&recorder, || {
            set_get_stage_metrics_enabled(true);
            record_local_batch_read_decision(LocalBatchReadDecision::Selected, Some(Duration::from_millis(2)));
            LocalBatchReadMetrics::capture().expect("enabled batch metrics must capture recorder handles")
        });
        set_get_stage_metrics_enabled(false);
        std::thread::spawn(move || {
            observation.record_stage(LocalBatchReadStage::QueueWait, true, Duration::from_millis(3));
            observation.record_stage(LocalBatchReadStage::Metadata, false, Duration::from_millis(4));
            observation.record_shard(false, 1024, 0);
            observation.record_shard(true, 16, 16);
            observation.record_batch(2);
        })
        .join()
        .expect("worker should record through the captured handles");
        set_get_stage_metrics_enabled(false);

        let rows = snapshotter.snapshot().into_vec();
        assert_eq!(counter_total(&rows, "rustfs_io_get_object_local_batch_decisions_total"), Some(1));
        assert_eq!(counter_total(&rows, "rustfs_io_get_object_local_batch_shards_total"), Some(2));
        assert_eq!(counter_total(&rows, "rustfs_io_get_object_local_batch_requested_bytes_total"), Some(1040));
        assert_eq!(counter_total(&rows, "rustfs_io_get_object_local_batch_read_bytes_total"), Some(16));
        assert_eq!(histogram_samples(&rows, "rustfs_io_get_object_local_batch_prepare_seconds"), vec![0.002]);
        assert_eq!(
            histogram_samples(&rows, "rustfs_io_get_object_local_batch_stage_seconds"),
            vec![0.003, 0.004]
        );
        assert_eq!(histogram_samples(&rows, "rustfs_io_get_object_local_batch_shards"), vec![2.0]);
        for (key, _, _, _) in rows {
            for label in key.key().labels() {
                assert!(matches!(label.key(), "decision" | "stage" | "outcome"));
            }
        }
    }
}
