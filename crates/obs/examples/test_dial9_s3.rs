// Test dial9 S3 configuration
use rustfs_obs::dial9::{Dial9Config, is_enabled};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Dial9 S3 Configuration Test ===");
    println!();

    // Test 1: Default S3 configuration (should be None/None)
    println!("Test 1: Default S3 configuration");
    let default_config = Dial9Config::default();
    println!("  s3_bucket: {:?}", default_config.s3_bucket);
    println!("  s3_prefix: {:?}", default_config.s3_prefix);
    assert_eq!(default_config.s3_bucket, None);
    assert_eq!(default_config.s3_prefix, None);
    println!("  ✓ PASS: Default S3 config is None/None");
    println!();

    // Test 2: Check if dial9 is enabled
    println!("Test 2: Check dial9 enabled state");
    println!("  is_enabled(): {}", is_enabled());
    println!(
        "  RUSTFS_RUNTIME_DIAL9_ENABLED: {}",
        std::env::var("RUSTFS_RUNTIME_DIAL9_ENABLED").unwrap_or_else(|_| "not set".to_string())
    );
    println!();

    // Test 3: Load configuration from environment
    println!("Test 3: Load configuration from environment");
    let config = Dial9Config::from_env();
    println!("  enabled: {}", config.enabled);
    println!("  s3_bucket: {:?}", config.s3_bucket);
    println!("  s3_prefix: {:?}", config.s3_prefix);
    println!("  ✓ PASS: Configuration loaded");
    println!();

    // Only test S3 config if dial9 is enabled
    if !config.enabled {
        println!("  ⚠ SKIP: Dial9 is disabled, S3 config not loaded");
        println!("  To test S3 configuration:");
        println!("    export RUSTFS_RUNTIME_DIAL9_ENABLED=true");
        println!("    export RUSTFS_RUNTIME_DIAL9_S3_BUCKET=my-bucket");
        println!("    export RUSTFS_RUNTIME_DIAL9_S3_PREFIX=telemetry/");
        println!("    cargo run -p rustfs-obs --example test_dial9_s3");
        return Ok(());
    }

    // Test 4: Configuration summary
    println!("Test 4: Configuration summary");
    println!("  S3 upload enabled: {}", config.s3_bucket.is_some());
    if let Some(bucket) = &config.s3_bucket {
        println!("  S3 bucket: {}", bucket);
    }
    if let Some(prefix) = &config.s3_prefix {
        println!("  S3 prefix: {}", prefix);
    }
    println!("  ✓ PASS: Configuration summary displayed");
    println!();

    println!("=== All Tests Passed! ===");

    Ok(())
}
