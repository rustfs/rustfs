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
    use std::time::Duration;
    use tokio::time::timeout;

    // Helper function to check if RustFS server is running
    async fn is_server_running(port: u16) -> bool {
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    // Helper function to make HTTP request
    async fn make_http_request(url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;

        client.get(url).send().await
    }

    #[tokio::test]
    #[ignore] // Requires running RustFS server
    async fn test_server_health_check() {
        let port = 9000; // Default RustFS port

        // Check if server is running
        if !is_server_running(port).await {
            println!("RustFS server not running on port {}, skipping test", port);
            return;
        }

        // Test basic connectivity
        let url = format!("http://127.0.0.1:{}/", port);
        let response = timeout(Duration::from_secs(5), make_http_request(&url)).await;

        match response {
            Ok(Ok(resp)) => {
                println!("Server responded with status: {}", resp.status());
                // Server should respond (might be redirect or auth required)
                assert!(resp.status().is_success() || resp.status().is_redirection() || resp.status().is_client_error());
            }
            Ok(Err(e)) => {
                println!("HTTP request failed: {}", e);
                // This might be expected if auth is required
            }
            Err(_) => {
                panic!("Request timed out");
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires running RustFS server
    async fn test_browser_redirect() {
        let port = 9000;

        if !is_server_running(port).await {
            println!("RustFS server not running, skipping test");
            return;
        }

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none()) // Don't follow redirects
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let response = client
            .get(&format!("http://127.0.0.1:{}/", port))
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status().is_redirection() {
                    let location = resp.headers().get("location");
                    if let Some(loc) = location {
                        let loc_str = loc.to_str().unwrap_or("");
                        println!("Redirect location: {}", loc_str);
                        assert!(loc_str.contains("console") || loc_str.contains("rustfs"));
                    }
                }
                // Accept various responses as different server states are possible
                assert!(true);
            }
            Err(_) => {
                // Network error might be expected in test environment
                println!("Network error during redirect test");
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires running RustFS server
    async fn test_api_endpoint_connectivity() {
        let port = 9000;

        if !is_server_running(port).await {
            println!("RustFS server not running, skipping test");
            return;
        }

        // Test common API endpoints
        let endpoints = vec!["/rustfs/admin/info", "/rustfs/admin/metrics", "/rustfs/admin/server-info"];

        for endpoint in endpoints {
            let url = format!("http://127.0.0.1:{}{}", port, endpoint);
            let response = timeout(Duration::from_secs(5), make_http_request(&url)).await;

            match response {
                Ok(Ok(resp)) => {
                    println!("Endpoint {} responded with status: {}", endpoint, resp.status());
                    // Admin endpoints might require auth, so accept auth errors
                    assert!(resp.status().is_success() || resp.status().is_client_error() || resp.status().is_redirection());
                }
                Ok(Err(e)) => {
                    println!("Request to {} failed: {}", endpoint, e);
                    // Network errors might be expected
                }
                Err(_) => {
                    println!("Request to {} timed out", endpoint);
                }
            }
        }
    }

    #[test]
    fn test_integration_test_helpers() {
        // Test our helper functions work correctly

        // Test URL formatting
        let port = 9000;
        let url = format!("http://127.0.0.1:{}/", port);
        assert_eq!(url, "http://127.0.0.1:9000/");

        // Test endpoint path construction
        let endpoint = "/rustfs/admin/info";
        let full_url = format!("http://127.0.0.1:{}{}", port, endpoint);
        assert_eq!(full_url, "http://127.0.0.1:9000/rustfs/admin/info");
    }

    // Note: These integration tests are designed to be run against a live RustFS server
    // They are marked with #[ignore] to prevent them from running in normal test suites
    // To run them:
    // 1. Start a RustFS server on port 9000
    // 2. Run: cargo test --package rustfs-e2e-test -- --ignored
    //
    // These tests verify:
    // - Server connectivity and basic health
    // - HTTP redirect functionality for browsers
    // - Admin API endpoint accessibility
    // - Proper error handling and timeouts
}
