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

//! Prometheus text exposition format renderer.

use super::PrometheusMetric;
use std::collections::HashSet;
use std::fmt::Write;

/// Render metrics in Prometheus text exposition format
pub fn render_metrics(metrics: &[PrometheusMetric]) -> String {
    let mut output = String::new();
    let mut seen_metrics: HashSet<&str> = HashSet::new();

    for metric in metrics {
        // Add HELP and TYPE only once per metric name
        if !seen_metrics.contains(metric.name.as_str()) {
            let _ = writeln!(output, "# HELP {} {}", metric.name, escape_help(&metric.help));
            let _ = writeln!(output, "# TYPE {} {}", metric.name, metric.metric_type.as_str());
            seen_metrics.insert(&metric.name);
        }

        // Render metric line
        if metric.labels.is_empty() {
            let _ = writeln!(output, "{} {}", metric.name, format_value(metric.value));
        } else {
            let labels: Vec<String> = metric
                .labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
                .collect();
            let _ = writeln!(output, "{}{{{}}} {}", metric.name, labels.join(","), format_value(metric.value));
        }
    }

    output
}

/// Escape label values per Prometheus spec
fn escape_label_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Escape help text
fn escape_help(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

/// Format float value for Prometheus
fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string()
    } else if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        format!("{}", v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prometheus::MetricType;

    #[test]
    fn test_render_simple_metric() {
        let metrics = vec![PrometheusMetric::new(
            "test_counter",
            MetricType::Counter,
            "A test counter",
            42.0,
        )];
        let output = render_metrics(&metrics);
        assert!(output.contains("# HELP test_counter A test counter"));
        assert!(output.contains("# TYPE test_counter counter"));
        assert!(output.contains("test_counter 42"));
    }

    #[test]
    fn test_render_metric_with_labels() {
        let metric = PrometheusMetric::new("http_requests", MetricType::Counter, "HTTP requests", 100.0)
            .with_label("method", "GET")
            .with_label("status", "200");
        let output = render_metrics(&[metric]);
        assert!(output.contains("http_requests{method=\"GET\",status=\"200\"} 100"));
    }

    #[test]
    fn test_escape_label_value() {
        assert_eq!(escape_label_value("hello\\world"), "hello\\\\world");
        assert_eq!(escape_label_value("say \"hi\""), "say \\\"hi\\\"");
        assert_eq!(escape_label_value("line1\nline2"), "line1\\nline2");
    }
}
