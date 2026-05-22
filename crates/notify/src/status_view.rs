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

use crate::integration::{NotificationMetricSnapshot, NotificationMetrics};
use hashbrown::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct NotifyStatusView {
    metrics: Arc<NotificationMetrics>,
}

impl NotifyStatusView {
    pub fn new(metrics: Arc<NotificationMetrics>) -> Self {
        Self { metrics }
    }

    pub fn get_status(&self) -> HashMap<String, String> {
        let mut status = HashMap::new();

        status.insert("uptime_seconds".to_string(), self.metrics.uptime().as_secs().to_string());
        status.insert("processing_events".to_string(), self.metrics.processing_count().to_string());
        status.insert("processed_events".to_string(), self.metrics.processed_count().to_string());
        status.insert("failed_events".to_string(), self.metrics.failed_count().to_string());
        status.insert("skipped_events".to_string(), self.metrics.skipped_count().to_string());

        status
    }

    pub fn snapshot_metrics(&self) -> NotificationMetricSnapshot {
        self.metrics.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyStatusView;
    use crate::integration::NotificationMetrics;
    use std::sync::Arc;

    #[test]
    fn status_view_reports_empty_metrics_snapshot() {
        let status_view = NotifyStatusView::new(Arc::new(NotificationMetrics::new()));

        let snapshot = status_view.snapshot_metrics();
        assert_eq!(snapshot.current_send_in_progress, 0);
        assert_eq!(snapshot.events_errors_total, 0);
        assert_eq!(snapshot.events_sent_total, 0);
        assert_eq!(snapshot.events_skipped_total, 0);
    }

    #[test]
    fn status_view_exposes_status_map_keys() {
        let status_view = NotifyStatusView::new(Arc::new(NotificationMetrics::new()));
        let status = status_view.get_status();

        assert!(status.contains_key("uptime_seconds"));
        assert!(status.contains_key("processing_events"));
        assert!(status.contains_key("processed_events"));
        assert!(status.contains_key("failed_events"));
        assert!(status.contains_key("skipped_events"));
    }
}
