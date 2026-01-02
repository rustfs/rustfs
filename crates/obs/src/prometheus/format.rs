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
//!
//! This module renders metrics in the standard Prometheus text format.
//! Optimized for minimal allocations and fast rendering.

use super::PrometheusMetric;
use std::collections::HashSet;
use std::fmt::Write;

/// Estimated bytes per metric line for pre-allocation.
/// Average metric line: name(30) + labels(50) + value(20) + help(60) + type(30) = ~190
const BYTES_PER_METRIC: usize = 200;

/// Render metrics in Prometheus text exposition format.
///
/// This function is optimized for performance:
/// - Pre-allocates the output buffer based on metric count
/// - Avoids redundant string allocations where possible
/// - Uses write! macros for efficient string building
#[must_use]
pub fn render_metrics(metrics: &[PrometheusMetric]) -> String {
    if metrics.is_empty() {
        return String::new();
    }

    // Pre-allocate based on estimated output size
    let mut output = String::with_capacity(metrics.len() * BYTES_PER_METRIC);
    let mut seen_metrics: HashSet<&str> = HashSet::with_capacity(metrics.len());

    for metric in metrics {
        // Add HELP and TYPE only once per metric name
        if !seen_metrics.contains(metric.name) {
            // Write HELP line
            let _ = write!(output, "# HELP {} ", metric.name);
            write_escaped_help(&mut output, metric.help);
            output.push('\n');

            // Write TYPE line
            let _ = writeln!(output, "# TYPE {} {}", metric.name, metric.metric_type.as_str());
            seen_metrics.insert(metric.name);
        }

        // Render metric line
        output.push_str(metric.name);

        if !metric.labels.is_empty() {
            output.push('{');
            for (i, (key, value)) in metric.labels.iter().enumerate() {
                if i > 0 {
                    output.push(',');
                }
                output.push_str(key);
                output.push_str("=\"");
                write_escaped_label_value(&mut output, value);
                output.push('"');
            }
            output.push('}');
        }

        output.push(' ');
        write_value(&mut output, metric.value);
        output.push('\n');
    }

    output
}

/// Write an escaped label value directly to the output buffer.
///
/// This avoids creating intermediate String allocations by writing
/// directly to the output and only escaping characters that need it.
#[inline]
fn write_escaped_label_value(output: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            _ => output.push(c),
        }
    }
}

/// Write escaped help text directly to the output buffer.
#[inline]
fn write_escaped_help(output: &mut String, help: &str) {
    for c in help.chars() {
        match c {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            _ => output.push(c),
        }
    }
}

/// Write a float value in Prometheus format directly to the output buffer.
#[inline]
fn write_value(output: &mut String, v: f64) {
    if v.is_nan() {
        output.push_str("NaN");
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            output.push_str("+Inf");
        } else {
            output.push_str("-Inf");
        }
    } else if v.fract() == 0.0 && v.abs() < 1e15 {
        // Integer value - format without decimal point
        let _ = write!(output, "{:.0}", v);
    } else {
        let _ = write!(output, "{}", v);
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
        let mut output = String::new();
        write_escaped_label_value(&mut output, "hello\\world");
        assert_eq!(output, "hello\\\\world");

        output.clear();
        write_escaped_label_value(&mut output, "say \"hi\"");
        assert_eq!(output, "say \\\"hi\\\"");

        output.clear();
        write_escaped_label_value(&mut output, "line1\nline2");
        assert_eq!(output, "line1\\nline2");
    }

    #[test]
    fn test_render_empty_metrics() {
        let output = render_metrics(&[]);
        assert!(output.is_empty());
    }

    #[test]
    fn test_render_multiple_metrics_same_name() {
        let metrics = vec![
            PrometheusMetric::new("http_requests", MetricType::Counter, "HTTP requests", 100.0)
                .with_label("method", "GET"),
            PrometheusMetric::new("http_requests", MetricType::Counter, "HTTP requests", 50.0)
                .with_label("method", "POST"),
        ];
        let output = render_metrics(&metrics);

        // HELP and TYPE should appear only once
        assert_eq!(output.matches("# HELP http_requests").count(), 1);
        assert_eq!(output.matches("# TYPE http_requests").count(), 1);

        // Both metric values should be present
        assert!(output.contains("http_requests{method=\"GET\"} 100"));
        assert!(output.contains("http_requests{method=\"POST\"} 50"));
    }

    #[test]
    fn test_render_special_values() {
        let metrics = vec![
            PrometheusMetric::new("nan_metric", MetricType::Gauge, "NaN value", f64::NAN),
            PrometheusMetric::new("inf_metric", MetricType::Gauge, "Inf value", f64::INFINITY),
            PrometheusMetric::new("neg_inf_metric", MetricType::Gauge, "Neg Inf value", f64::NEG_INFINITY),
        ];
        let output = render_metrics(&metrics);

        assert!(output.contains("nan_metric NaN"));
        assert!(output.contains("inf_metric +Inf"));
        assert!(output.contains("neg_inf_metric -Inf"));
    }
}
