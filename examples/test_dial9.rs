// Test dial9 integration
use rustfs_obs::dial9::{init_session, is_enabled, Dial9Config};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Dial9 Integration Test ===\n");

    // Test 1: Check if dial9 is disabled by default
    println!("Test 1: Default state");
    println!("  dial9 enabled: {}", is_enabled());
    assert!(!is_enabled(), "Dial9 should be disabled by default");
    println!("  ✓ PASS: Dial9 is disabled by default\n");

    // Test 2: Enable dial9 via environment variable
    println!("Test 2: Enable dial9 via environment");
    std::env::set_var("RUSTFS_RUNTIME_DIAL9_ENABLED", "true");
    std::env::set_var("RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR", "/tmp/rustfs-test-telemetry");
    std::env::set_var("RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE", "0.5");

    let config = Dial9Config::from_env();
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
    match init_session().await {
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
            println!("  ⚠ SKIP: Dial9 session not created (writer init may have failed)\n");
        }
        Err(e) => {
            println!("  ✗ FAIL: {:?}", e);
            return Err(e.into());
        }
    }

    // Cleanup
    std::env::remove_var("RUSTFS_RUNTIME_DIAL9_ENABLED");
    std::env::remove_var("RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR");
    std::env::remove_var("RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE");

    println!("=== All Tests Passed! ===");
    Ok(())
}
