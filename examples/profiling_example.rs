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

//! Performance Profiling Example for RustFS
//! 
//! This example demonstrates how to use the integrated pprof performance
//! profiling functionality in RustFS to analyze performance bottlenecks.
//! 
//! ## Usage
//! 
//! 1. First, start RustFS with profiling enabled:
//!    ```bash
//!    RUSTFS_ENABLE_PROFILING=true cargo run --bin rustfs -- \
//!        --address 127.0.0.1:9000 \
//!        --access-key rustfsadmin \
//!        --secret-key rustfsadmin \
//!        /tmp/data1 /tmp/data2 /tmp/data3 /tmp/data4
//!    ```
//! 
//! 2. Then run this example to collect and analyze profiles:
//!    ```bash
//!    cargo run --example profiling_example
//!    ```

use reqwest;
use std::time::Duration;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("RustFS Performance Profiling Example");
    println!("=====================================");
    
    let base_url = "http://127.0.0.1:9000";
    let client = reqwest::Client::new();
    
    // Check profiling status
    println!("\n1. Checking profiling status...");
    let status_url = format!("{}/rustfs/admin/debug/pprof/status", base_url);
    
    match client.get(&status_url).send().await {
        Ok(response) => {
            let status_text = response.text().await?;
            println!("Status response: {}", status_text);
            
            if status_text.contains("\"enabled\":\"false\"") {
                println!("\n❌ Profiling is disabled!");
                println!("To enable profiling, restart RustFS with:");
                println!("RUSTFS_ENABLE_PROFILING=true cargo run --bin rustfs -- [your-args]");
                return Ok(());
            }
        }
        Err(e) => {
            println!("❌ Failed to check status: {}", e);
            println!("Make sure RustFS is running on {}", base_url);
            return Ok(());
        }
    }
    
    println!("✅ Profiling is enabled!");
    
    // Collect CPU profile
    println!("\n2. Collecting CPU profile...");
    let profile_url = format!("{}/rustfs/admin/debug/pprof/profile", base_url);
    
    // Collect a 10-second CPU profile in flamegraph format
    let profile_params = [
        ("seconds", "10"),
        ("format", "flamegraph")
    ];
    
    println!("📊 Starting 10-second profile collection...");
    let start_time = std::time::Instant::now();
    
    match client.get(&profile_url).query(&profile_params).send().await {
        Ok(response) => {
            let elapsed = start_time.elapsed();
            println!("✅ Profile collection completed in {:?}", elapsed);
            
            if response.status().is_success() {
                let content_type = response.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown");
                
                let body = response.bytes().await?;
                
                if content_type.contains("svg") {
                    // Save SVG flamegraph
                    let filename = "rustfs_profile_flamegraph.svg";
                    std::fs::write(filename, &body)?;
                    println!("💾 Flamegraph saved to: {}", filename);
                    println!("🔥 Open the SVG file in a web browser to view the flamegraph");
                } else {
                    println!("📄 Profile data size: {} bytes", body.len());
                    println!("📄 Content type: {}", content_type);
                }
            } else {
                let error_text = response.text().await?;
                println!("❌ Profile collection failed: {}", error_text);
            }
        }
        Err(e) => {
            println!("❌ Failed to collect profile: {}", e);
        }
    }
    
    // Collect protobuf profile for analysis with pprof tools
    println!("\n3. Collecting protobuf profile...");
    let protobuf_params = [
        ("seconds", "5"),
        ("format", "protobuf")
    ];
    
    match client.get(&profile_url).query(&protobuf_params).send().await {
        Ok(response) => {
            if response.status().is_success() {
                let body = response.bytes().await?;
                let filename = "rustfs_profile.pb";
                std::fs::write(filename, &body)?;
                println!("💾 Protobuf profile saved to: {}", filename);
                println!("🔧 Analyze with: go tool pprof {}", filename);
            } else {
                let error_text = response.text().await?;
                println!("❌ Protobuf profile collection failed: {}", error_text);
            }
        }
        Err(e) => {
            println!("❌ Failed to collect protobuf profile: {}", e);
        }
    }
    
    println!("\n4. Performance Analysis Tips");
    println!("============================");
    println!("🔍 Flamegraph Analysis:");
    println!("   - Width of boxes = CPU time spent");
    println!("   - Height = call stack depth");
    println!("   - Color = different functions");
    println!("   - Look for wide boxes (hot functions)");
    println!();
    println!("🔧 Command-line Analysis:");
    println!("   go tool pprof rustfs_profile.pb");
    println!("   (top) - Show top CPU consumers");
    println!("   (list function_name) - Show function details");
    println!("   (web) - Open web interface");
    println!();
    println!("📈 Continuous Profiling:");
    println!("   - Set up monitoring to collect profiles regularly");
    println!("   - Compare profiles before/after code changes");
    println!("   - Profile under different load conditions");
    
    Ok(())
}

/// Example function to generate some CPU load for demonstration
#[allow(dead_code)]
async fn simulate_cpu_load() {
    println!("🔥 Simulating CPU load...");
    
    let handles: Vec<_> = (0..4).map(|i| {
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let mut counter = 0u64;
            
            while start.elapsed() < Duration::from_secs(30) {
                // Simulate some CPU-intensive work
                for _ in 0..1000 {
                    counter = counter.wrapping_add(1);
                    counter = counter.wrapping_mul(17);
                    counter = counter ^ 0xdeadbeef;
                }
                
                if counter % 100000 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            
            println!("Worker {} finished with counter: {}", i, counter);
        })
    }).collect();
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    println!("✅ CPU load simulation completed");
}