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
    use crate::server::console::start_console_server;
    use crate::config::Opt;
    use clap::Parser;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_console_server_can_start_and_stop() {
        // Test that console server can be started and shut down gracefully
        let args = vec!["rustfs", "/tmp/test", "--console-address", ":0"]; // Use port 0 for auto-assignment
        let opt = Opt::parse_from(args);
        
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        
        // Start console server in a background task
        let handle = tokio::spawn(async move {
            start_console_server(&opt, rx).await
        });
        
        // Give it a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Send shutdown signal
        let _ = tx.send(());
        
        // Wait for server to shutdown
        let result = timeout(Duration::from_secs(5), handle).await;
        
        assert!(result.is_ok(), "Console server should shutdown gracefully");
        let server_result = result.unwrap();
        assert!(server_result.is_ok(), "Console server should not have errors");
        let final_result = server_result.unwrap();
        assert!(final_result.is_ok(), "Console server should complete successfully");
    }

    #[tokio::test]
    async fn test_console_disabled_returns_immediately() {
        // Test that when console is disabled, the function returns immediately
        let args = vec!["rustfs", "/tmp/test", "--console-enable", "false"];
        let opt = Opt::parse_from(args);
        
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        
        let start = tokio::time::Instant::now();
        let result = start_console_server(&opt, rx).await;
        let duration = start.elapsed();
        
        assert!(result.is_ok(), "Disabled console server should return Ok");
        assert!(duration < Duration::from_millis(100), "Disabled console should return immediately");
    }
}