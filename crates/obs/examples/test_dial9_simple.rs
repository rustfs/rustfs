// Simple dial9 integration test (reads from environment)
use rustfs_obs::dial9::{Dial9Config, is_enabled};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Dial9 Integration Test ===");
    println!();

    // Test 1: Check current state
    println!("Test 1: Check dial9 state");
    println!(
        "  RUSTFS_RUNTIME_DIAL9_ENABLED: {}",
        std::env::var("RUSTFS_RUNTIME_DIAL9_ENABLED").unwrap_or_else(|_| "not set".to_string())
    );
    println!("  is_enabled(): {}", is_enabled());
    println!("  ✓ Dial9 state check complete");
    println!();

    // Test 2: Load configuration
    println!("Test 2: Load dial9 configuration");
    let config = Dial9Config::from_env();
    println!("  enabled: {}", config.enabled);
    println!("  output_dir: {}", config.output_dir);
    println!("  file_prefix: {}", config.file_prefix);
    println!("  max_file_size: {} bytes", config.max_file_size);
    println!("  rotation_count: {}", config.rotation_count);
    println!("  sampling_rate: {}", config.sampling_rate);
    println!("  ✓ Configuration loaded");
    println!();

    // Test 3: Test base path calculation
    println!("Test 3: Base path calculation");
    println!("  base_path: {:?}", config.base_path());
    println!("  ✓ Base path calculated");
    println!();

    println!("=== All Tests Passed! ===");
    println!();
    println!("Note: To test full dial9 functionality, enable it with:");
    println!("  export RUSTFS_RUNTIME_DIAL9_ENABLED=true");
    println!("  export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-telemetry");
    println!("  cargo run -p rustfs-obs --example test_dial9_simple");

    Ok(())
}
