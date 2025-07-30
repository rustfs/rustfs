use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use reqwest::Client as HttpClient;
use std::env;

pub struct TestContext {
    pub s3_client: S3Client,
    pub bucket_prefix: String,
}

pub struct AdminTestContext {
    pub admin_client: HttpClient,
    pub base_url: String,
}

pub async fn setup_test_context() -> Result<TestContext, Box<dyn std::error::Error + Send + Sync>> {
    // Configure for local RustFS service
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new("rustfsadmin", "rustfsadmin", None, None, "static"))
        .endpoint_url("http://localhost:9000")
        .load()
        .await;

    let s3_client = S3Client::from_conf(aws_sdk_s3::Config::from(&config).to_builder().force_path_style(true).build());

    // Generate unique bucket prefix for test isolation
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let bucket_prefix = format!("test-{timestamp}-");

    Ok(TestContext {
        s3_client,
        bucket_prefix,
    })
}

pub async fn setup_admin_test_context() -> Result<AdminTestContext, Box<dyn std::error::Error + Send + Sync>> {
    let admin_client = HttpClient::new();
    let base_url = env::var("ADMIN_API_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    Ok(AdminTestContext { admin_client, base_url })
}

pub async fn cleanup_test_context(context: TestContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Clean up any test buckets created during testing
    println!("Cleaning up test context with prefix: {}", context.bucket_prefix);
    Ok(())
}

pub async fn cleanup_admin_test_context(_context: AdminTestContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Clean up any test buckets created during testing
    println!("Cleaning up admin test context");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_context_setup() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let context = setup_test_context().await?;
        assert!(!context.bucket_prefix.is_empty());
        cleanup_test_context(context).await?;
        Ok(())
    }
}
