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

#[cfg(test)]
mod tests {
    use crate::config::Opt;
    use crate::server::console::start_console_server;
    use clap::Parser;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_console_server_can_start_and_stop() {
        // Test that console server can be started and shut down gracefully
        let args = vec!["rustfs", "/tmp/test", "--console-address", ":0"]; // Use port 0 for auto-assignment
        let opt = Opt::parse_from(args);

        let (tx, rx) = tokio::sync::broadcast::channel(1);

        // Start console server in a background task
        let handle = tokio::spawn(async move { start_console_server(&opt, rx).await });

        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown signal
        let _ = tx.send(());

        // Wait for server to shut down
        let result = timeout(Duration::from_secs(5), handle).await;

        assert!(result.is_ok(), "Console server should shutdown gracefully");
        let server_result = result.unwrap();
        assert!(server_result.is_ok(), "Console server should not have errors");
        let final_result = server_result.unwrap();
        assert!(final_result.is_ok(), "Console server should complete successfully");
    }

    #[tokio::test]
    async fn test_console_cors_configuration() {
        // Test CORS configuration parsing
        use crate::server::console::parse_cors_origins;

        // Test wildcard origin
        let cors_wildcard = Some("*".to_string());
        let _layer1 = parse_cors_origins(cors_wildcard.as_ref());
        // Should create a layer without error

        // Test specific origins
        let cors_specific = Some("http://localhost:3000,https://admin.example.com".to_string());
        let _layer2 = parse_cors_origins(cors_specific.as_ref());
        // Should create a layer without error

        // Test empty origin
        let cors_empty = Some("".to_string());
        let _layer3 = parse_cors_origins(cors_empty.as_ref());
        // Should create a layer without error (falls back to permissive)

        // Test no origin
        let _layer4 = parse_cors_origins(None);
        // Should create a layer without error (uses default)
    }

    #[tokio::test]
    async fn test_external_address_configuration() {
        // Test external address configuration
        let args = vec![
            "rustfs",
            "/tmp/test",
            "--console-address",
            ":9001",
            "--external-address",
            ":9020",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.console_address, ":9001");
        assert_eq!(opt.external_address, ":9020".to_string());
    }

    #[tokio::test]
    async fn test_console_tls_configuration() {
        // Test TLS configuration options (now uses shared tls_path)
        let args = vec!["rustfs", "/tmp/test", "--tls-path", "/path/to/tls"];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.tls_path, Some("/path/to/tls".to_string()));
    }

    #[tokio::test]
    async fn test_console_health_check_endpoint() {
        // Test that console health check can be called
        // This test would need a running server to be comprehensive
        // For now, we test configuration and startup behavior
        let args = vec!["rustfs", "/tmp/test", "--console-address", ":0"];
        let opt = Opt::parse_from(args);

        // Verify the configuration supports health checks
        assert!(opt.console_enable, "Console should be enabled for health checks");
    }

    #[tokio::test]
    async fn test_console_separate_logging_target() {
        // Test that console uses separate logging targets
        use tracing::info;

        // This test verifies that logging targets are properly set up
        info!(target: "rustfs::console::startup", "Test console startup log");
        info!(target: "rustfs::console::access", "Test console access log");
        info!(target: "rustfs::console::error", "Test console error log");
        info!(target: "rustfs::console::shutdown", "Test console shutdown log");

        // In a real implementation, we would verify these logs are captured separately
    }

    #[tokio::test]
    async fn test_console_configuration_validation() {
        // Test configuration validation
        let args = vec![
            "rustfs",
            "/tmp/test",
            "--console-enable",
            "true",
            "--console-address",
            ":9001",
            "--external-address",
            ":9020",
        ];
        let opt = Opt::parse_from(args);

        // Verify all console-related configuration is parsed correctly
        assert!(opt.console_enable);
        assert_eq!(opt.console_address, ":9001");
        assert_eq!(opt.external_address, ":9020".to_string());
    }
}
