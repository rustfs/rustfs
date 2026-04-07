// Integration test demonstrating the embedded RustFS server API.
//
// This test starts a RustFS server in-process and exercises it via the
// standard AWS S3 SDK — exactly as you would in your own integration tests.

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use rustfs::embedded::{RustFSServerBuilder, find_available_port};

/// Helper: create an S3 client pointed at the embedded server.
fn s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    let config = Config::builder()
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

#[tokio::test]
async fn test_embedded_server_basic_s3_operations() {
    // 1. Pick a free port and start the embedded server.
    let port = find_available_port().expect("find free port");
    let server = RustFSServerBuilder::new()
        .address(format!("127.0.0.1:{port}"))
        .access_key("testaccesskey")
        .secret_key("testsecretkey")
        .build()
        .await
        .expect("start embedded server");

    let endpoint = server.endpoint();
    assert!(endpoint.contains(&port.to_string()));

    // 2. Create an S3 client and perform basic operations.
    let client = s3_client(&endpoint, server.access_key(), server.secret_key());

    // Create bucket
    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("create bucket");

    // Put object
    let body = ByteStream::from_static(b"hello rustfs embedded!");
    client
        .put_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .body(body)
        .send()
        .await
        .expect("put object");

    // Get object
    let resp = client
        .get_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .send()
        .await
        .expect("get object");

    let data = resp.body.collect().await.expect("read body").into_bytes();
    assert_eq!(data.as_ref(), b"hello rustfs embedded!");

    // List objects
    let list = client
        .list_objects_v2()
        .bucket("test-bucket")
        .send()
        .await
        .expect("list objects");
    assert_eq!(list.key_count(), Some(1));

    // Delete object
    client
        .delete_object()
        .bucket("test-bucket")
        .key("greeting.txt")
        .send()
        .await
        .expect("delete object");

    // Delete bucket
    client
        .delete_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("delete bucket");

    // 3. Shut down.
    server.shutdown().await;
}
