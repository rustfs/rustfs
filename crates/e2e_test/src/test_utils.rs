use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use reqwest::{Client as HttpClient, Method};
use std::env;
use http::Request as HttpRequest;
use s3s::Body;

const ADMIN_ACCESS_KEY: &str = "rustfsadmin";
const ADMIN_SECRET_KEY: &str = "rustfsadmin";
const DEFAULT_REGION: &str = "us-east-1";

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
    // RustFS admin API is served under the same HTTP server as S3, default :9000
    // You can override via env ADMIN_API_BASE_URL if running on a different port or host
    let base_url = env::var("ADMIN_API_BASE_URL").unwrap_or_else(|_| "http://localhost:9000".to_string());

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

/// Build SigV4-signed headers for an admin API call.
/// We use UNSIGNED-PAYLOAD for simplicity as the server accepts it.
fn build_sigv4_headers(method: &Method, url: &str) -> Result<http::HeaderMap, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = HttpRequest::builder().method(method.as_str()).uri(url);
    // Hint signer to use UNSIGNED-PAYLOAD
    builder = builder.header(
        "X-Amz-Content-Sha256",
        rustfs_signer::constants::UNSIGNED_PAYLOAD,
    );
    let req = builder.body(Body::empty())?;
    let signed = rustfs_signer::sign_v4(
        req,
        0,
        ADMIN_ACCESS_KEY,
        ADMIN_SECRET_KEY,
        "",
        DEFAULT_REGION,
    );
    Ok(signed.headers().clone())
}

/// Send a SigV4-signed JSON POST to admin API and return the response.
pub async fn admin_post_json_signed(
    client: &HttpClient,
    full_url: &str,
    payload: &serde_json::Value,
) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
    let headers = build_sigv4_headers(&Method::POST, full_url)?;
    let mut rb = client.post(full_url);
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        match name_str {
            "authorization" | "x-amz-date" | "x-amz-content-sha256" | "x-amz-security-token" => {
                rb = rb.header(name_str, value.to_str().unwrap_or_default());
            }
            _ => {}
        }
    }
    Ok(rb.json(payload).send().await?)
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
