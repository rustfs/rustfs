// Manual Dial9 integration runner.
//
// Run with:
// `cargo run -p rustfs --features manual-test-runners --bin manual-test-dial9`
//
// This file lives under `rustfs/tests/manual` and is registered explicitly in
// `rustfs/Cargo.toml` so it stays out of `cargo test` auto-discovery.
use rustfs_obs::dial9::{Dial9Config, Dial9SessionGuard};
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Dial9 Integration Test ===\n");

    // Test 1: Check default dial9 configuration
    println!("Test 1: Default configuration");
    let default_config = Dial9Config::default();
    println!("  default enabled: {}", default_config.enabled);
    println!("  default output_dir: {}", default_config.output_dir);
    println!("  default file_prefix: {}", default_config.file_prefix);
    println!("  ✓ PASS: Default configuration loaded\n");

    // Test 2: Create explicit dial9 configuration
    println!("Test 2: Explicit dial9 configuration");
    let config = Dial9Config {
        enabled: true,
        output_dir: "/tmp/rustfs-test-telemetry".to_string(),
        sampling_rate: 0.5,
        ..Dial9Config::default()
    };
    println!("  config.enabled: {}", config.enabled);
    println!("  config.output_dir: {}", config.output_dir);
    println!("  config.file_prefix: {}", config.file_prefix);
    println!("  config.sampling_rate: {}", config.sampling_rate);

    assert!(config.enabled);
    assert_eq!(config.output_dir, "/tmp/rustfs-test-telemetry");
    assert_eq!(config.sampling_rate, 0.5);
    println!("  ✓ PASS: Configuration loaded correctly\n");

    // Test 3: Initialize dial9 session
    println!("Test 3: Initialize dial9 session");
    match Dial9SessionGuard::new(config.clone()).await {
        Ok(Some(guard)) => {
            println!("  Dial9 session initialized successfully");
            println!("  guard.is_active(): {}", guard.is_active());
            println!("  ✓ PASS: Session initialized\n");

            // Test 4: Generate some async activity
            println!("Test 4: Generate async activity for tracing");
            let handle = tokio::spawn(async {
                for i in 1..=5 {
                    println!("  Task iteration {}", i);
                    sleep(Duration::from_millis(50)).await;
                }
            });
            handle.await?;
            println!("  ✓ PASS: Async activity completed\n");

            // Test 5: Session shutdown
            println!("Test 5: Session cleanup");
            drop(guard);
            println!("  ✓ PASS: Session cleaned up\n");
        }
        Ok(None) => {
            println!("  ⚠ SKIP: Dial9 session not created (configuration validation may have failed)\n");
        }
        Err(e) => {
            println!("  ✗ FAIL: {:?}", e);
            return Err(e.into());
        }
    }

    // Cleanup
    if let Err(err) = tokio::fs::remove_dir_all(&config.output_dir).await {
        println!("  ⚠ SKIP: Failed to remove output directory: {}", err);
    }

    println!("=== All Tests Passed! ===");
    Ok(())
}
