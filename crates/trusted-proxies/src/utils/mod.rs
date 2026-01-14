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

//! Utility functions and helpers for the trusted proxy system.

mod ip;
mod validation;

pub use ip::*;
pub use validation::*;

/// Collection of general utility functions.
#[derive(Debug, Clone)]
pub struct Utils;

impl Utils {
    /// Generates a unique trace ID.
    pub fn generate_trace_id() -> String {
        format!("trace-{}", uuid::Uuid::new_v4())
    }

    /// Generates a unique span ID.
    pub fn generate_span_id() -> String {
        format!("span-{}", uuid::Uuid::new_v4())
    }

    /// Safely parses a string into a `usize`, returning a default value on failure.
    pub fn safe_parse_usize(s: &str, default: usize) -> usize {
        s.parse().unwrap_or(default)
    }

    /// Safely parses a string into a `u64`, returning a default value on failure.
    pub fn safe_parse_u64(s: &str, default: u64) -> u64 {
        s.parse().unwrap_or(default)
    }

    /// Safely parses a string into a boolean, returning a default value on failure.
    pub fn safe_parse_bool(s: &str, default: bool) -> bool {
        match s.to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        }
    }

    /// Formats a `Duration` into a human-readable string.
    pub fn format_duration(duration: std::time::Duration) -> String {
        if duration.as_secs() > 0 {
            format!("{:.2}s", duration.as_secs_f64())
        } else if duration.as_millis() > 0 {
            format!("{}ms", duration.as_millis())
        } else if duration.as_micros() > 0 {
            format!("{}µs", duration.as_micros())
        } else {
            format!("{}ns", duration.as_nanos())
        }
    }

    /// Returns the current UTC timestamp in RFC 3339 format.
    pub fn current_timestamp() -> String {
        chrono::Utc::now().to_rfc3339()
    }

    /// Safely retrieves an environment variable.
    pub fn get_env_var(key: &str) -> Option<String> {
        std::env::var(key).ok()
    }

    /// Retrieves an environment variable or returns a default value if not set.
    pub fn get_env_var_or(key: &str, default: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| default.to_string())
    }

    /// Checks if an environment variable is set.
    pub fn has_env_var(key: &str) -> bool {
        std::env::var(key).is_ok()
    }
}
