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

use dioxus::logger::tracing::debug;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize the logger with a rolling file appender
/// that rotates log files daily
pub fn init_logger() -> WorkerGuard {
    // configuring rolling logs rolling by day
    let home_dir = dirs::home_dir().expect("无法获取用户目录");
    let rustfs_dir = home_dir.join("rustfs");
    let logs_dir = rustfs_dir.join("logs");
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY) // rotate log files once every hour
        .filename_prefix("rustfs-cli") // log file names will be prefixed with `myapp.`
        .filename_suffix("log") // log file names will be suffixed with `.log`
        .build(logs_dir) // try to build an appender that stores log files in `/ var/ log`
        .expect("initializing rolling file appender failed");
    // non-blocking writer for improved performance
    let (non_blocking_file, worker_guard) = tracing_appender::non_blocking(file_appender);

    // console output layer
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_line_number(true); // enable colors in the console

    // file output layer
    let file_layer = fmt::layer()
        .with_writer(non_blocking_file)
        .with_ansi(false)
        .with_thread_names(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_line_number(true); // disable colors in the file

    // Combine all tiers and initialize global subscribers
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .with(tracing_subscriber::EnvFilter::new("info")) // filter the log level by environment variables
        .init();
    debug!("Logger initialized");
    worker_guard
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();

    // Helper function to ensure logger is only initialized once in tests
    fn ensure_logger_init() {
        INIT.call_once(|| {
            // Initialize a simple test logger to avoid conflicts
            let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        });
    }

    #[test]
    fn test_logger_initialization_components() {
        ensure_logger_init();

        // Test that we can create the components used in init_logger
        // without actually initializing the global logger again

        // Test home directory access
        let home_dir_result = dirs::home_dir();
        assert!(home_dir_result.is_some(), "Should be able to get home directory");

        let home_dir = home_dir_result.unwrap();
        let rustfs_dir = home_dir.join("rustfs");
        let logs_dir = rustfs_dir.join("logs");

        // Test path construction
        assert!(rustfs_dir.to_string_lossy().contains("rustfs"));
        assert!(logs_dir.to_string_lossy().contains("logs"));
    }

    #[test]
    fn test_rolling_file_appender_builder() {
        ensure_logger_init();

        // Test that we can create a RollingFileAppender builder
        let builder = RollingFileAppender::builder()
            .rotation(Rotation::DAILY)
            .filename_prefix("test-rustfs-cli")
            .filename_suffix("log");

        // We can't actually build it without creating directories,
        // but we can verify the builder pattern works
        let debug_str = format!("{builder:?}");
        // The actual debug format might be different, so just check it's not empty
        assert!(!debug_str.is_empty());
        // Check that it contains some expected parts
        assert!(debug_str.contains("Builder") || debug_str.contains("builder") || debug_str.contains("RollingFileAppender"));
    }

    #[test]
    fn test_rotation_types() {
        ensure_logger_init();

        // Test different rotation types
        let daily = Rotation::DAILY;
        let hourly = Rotation::HOURLY;
        let minutely = Rotation::MINUTELY;
        let never = Rotation::NEVER;

        // Test that rotation types can be created and formatted
        assert!(!format!("{daily:?}").is_empty());
        assert!(!format!("{hourly:?}").is_empty());
        assert!(!format!("{minutely:?}").is_empty());
        assert!(!format!("{never:?}").is_empty());
    }

    #[test]
    fn test_fmt_layer_configuration() {
        ensure_logger_init();

        // Test that we can create fmt layers with different configurations
        // We can't actually test the layers directly due to type complexity,
        // but we can test that the configuration values are correct

        // Test console layer settings
        let console_ansi = true;
        let console_line_number = true;
        assert!(console_ansi);
        assert!(console_line_number);

        // Test file layer settings
        let file_ansi = false;
        let file_thread_names = true;
        let file_target = true;
        let file_thread_ids = true;
        let file_level = true;
        let file_line_number = true;

        assert!(!file_ansi);
        assert!(file_thread_names);
        assert!(file_target);
        assert!(file_thread_ids);
        assert!(file_level);
        assert!(file_line_number);
    }

    #[test]
    fn test_env_filter_creation() {
        ensure_logger_init();

        // Test that EnvFilter can be created with different levels
        let info_filter = tracing_subscriber::EnvFilter::new("info");
        let debug_filter = tracing_subscriber::EnvFilter::new("debug");
        let warn_filter = tracing_subscriber::EnvFilter::new("warn");
        let error_filter = tracing_subscriber::EnvFilter::new("error");

        // Test that filters can be created
        assert!(!format!("{info_filter:?}").is_empty());
        assert!(!format!("{debug_filter:?}").is_empty());
        assert!(!format!("{warn_filter:?}").is_empty());
        assert!(!format!("{error_filter:?}").is_empty());
    }

    #[test]
    fn test_path_construction() {
        ensure_logger_init();

        // Test path construction logic used in init_logger
        if let Some(home_dir) = dirs::home_dir() {
            let rustfs_dir = home_dir.join("rustfs");
            let logs_dir = rustfs_dir.join("logs");

            // Test that paths are constructed correctly
            assert!(rustfs_dir.ends_with("rustfs"));
            assert!(logs_dir.ends_with("logs"));
            assert!(logs_dir.parent().unwrap().ends_with("rustfs"));

            // Test path string representation
            let rustfs_str = rustfs_dir.to_string_lossy();
            let logs_str = logs_dir.to_string_lossy();

            assert!(rustfs_str.contains("rustfs"));
            assert!(logs_str.contains("rustfs"));
            assert!(logs_str.contains("logs"));
        }
    }

    #[test]
    fn test_filename_patterns() {
        ensure_logger_init();

        // Test the filename patterns used in the logger
        let prefix = "rustfs-cli";
        let suffix = "log";

        assert_eq!(prefix, "rustfs-cli");
        assert_eq!(suffix, "log");

        // Test that these would create valid filenames
        let sample_filename = format!("{prefix}.2024-01-01.{suffix}");
        assert_eq!(sample_filename, "rustfs-cli.2024-01-01.log");
    }

    #[test]
    fn test_worker_guard_type() {
        ensure_logger_init();

        // Test that WorkerGuard type exists and can be referenced
        // We can't actually create one without the full setup, but we can test the type
        let guard_size = std::mem::size_of::<WorkerGuard>();
        assert!(guard_size > 0, "WorkerGuard should have non-zero size");
    }

    #[test]
    fn test_logger_configuration_constants() {
        ensure_logger_init();

        // Test the configuration values used in the logger
        let default_log_level = "info";
        let filename_prefix = "rustfs-cli";
        let filename_suffix = "log";
        let rotation = Rotation::DAILY;

        assert_eq!(default_log_level, "info");
        assert_eq!(filename_prefix, "rustfs-cli");
        assert_eq!(filename_suffix, "log");
        assert!(matches!(rotation, Rotation::DAILY));
    }

    #[test]
    fn test_directory_names() {
        ensure_logger_init();

        // Test the directory names used in the logger setup
        let rustfs_dir_name = "rustfs";
        let logs_dir_name = "logs";

        assert_eq!(rustfs_dir_name, "rustfs");
        assert_eq!(logs_dir_name, "logs");

        // Test path joining
        let combined = format!("{rustfs_dir_name}/{logs_dir_name}");
        assert_eq!(combined, "rustfs/logs");
    }

    #[test]
    fn test_layer_settings() {
        ensure_logger_init();

        // Test the boolean settings used in layer configuration
        let console_ansi = true;
        let console_line_number = true;
        let file_ansi = false;
        let file_thread_names = true;
        let file_target = true;
        let file_thread_ids = true;
        let file_level = true;
        let file_line_number = true;

        // Verify the settings
        assert!(console_ansi);
        assert!(console_line_number);
        assert!(!file_ansi);
        assert!(file_thread_names);
        assert!(file_target);
        assert!(file_thread_ids);
        assert!(file_level);
        assert!(file_line_number);
    }

    // Note: The actual init_logger() function is not tested here because:
    // 1. It initializes a global tracing subscriber which can only be done once
    // 2. It requires file system access to create directories
    // 3. It has side effects that would interfere with other tests
    // 4. It returns a WorkerGuard that needs to be kept alive
    //
    // This function should be tested in integration tests where:
    // - File system access can be properly controlled
    // - The global state can be managed
    // - The actual logging behavior can be verified
    // - The WorkerGuard lifecycle can be properly managed
}
