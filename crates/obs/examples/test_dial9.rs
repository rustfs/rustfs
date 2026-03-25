// Test dial9 integration example
use rustfs_obs::dial9::{Dial9Config, is_enabled};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Dial9 Integration Test ===\n");

    // Test 1: Check initial dial9 state
    println!("Test 1: Default state");
    let initial_enabled = is_enabled();
    println!("  dial9 enabled: {}", initial_enabled);
    if initial_enabled {
        println!("  ⚠ SKIP: Dial9 is already enabled via environment; skipping default-disabled assertion\n");
    } else {
        println!("  ✓ PASS: Dial9 is disabled by default\n");
    }

    // Test 2: Load default configuration
    println!("Test 2: Default configuration");
    let config = Dial9Config::from_env();
    println!("  enabled: {}", config.enabled);
    println!("  output_dir: {}", config.output_dir);
    println!("  file_prefix: {}", config.file_prefix);
    println!("  max_file_size: {} bytes", config.max_file_size);
    println!("  rotation_count: {}", config.rotation_count);
    println!("  s3_bucket: {:?}", config.s3_bucket);
    println!("  s3_prefix: {:?}", config.s3_prefix);
    println!("  sampling_rate: {}", config.sampling_rate);
    println!("  ✓ PASS: Default configuration loaded\n");

    // Test 3: Configuration validation
    println!("Test 3: Configuration validation");
    if !initial_enabled {
        assert!(!config.enabled, "Should be disabled by default");
        assert_eq!(config.s3_bucket, None, "S3 bucket should be None by default");
        assert_eq!(config.s3_prefix, None, "S3 prefix should be None by default");
        println!("  ✓ PASS: Configuration validated\n");
    } else {
        println!("  ⚠ SKIP: Configuration validation skipped (dial9 is enabled)\n");
    }

    println!("=== All Tests Passed! ===");
    println!();
    println!("Note: To test with dial9 enabled, set environment variables:");
    println!("  export RUSTFS_RUNTIME_DIAL9_ENABLED=true");
    println!("  export RUSTFS_RUNTIME_DIAL9_OUTPUT_DIR=/tmp/rustfs-test-telemetry");
    println!("  export RUSTFS_RUNTIME_DIAL9_SAMPLING_RATE=0.5");
    println!("  export RUSTFS_RUNTIME_DIAL9_S3_BUCKET=my-bucket");
    println!("  export RUSTFS_RUNTIME_DIAL9_S3_PREFIX=telemetry/");
    println!("  cargo run -p rustfs-obs --example test_dial9");

    Ok(())
}
