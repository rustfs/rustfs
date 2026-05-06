use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use tracing::info;

const RANGE_HEAD_BUCKET: &str = "range-head-test-bucket";
const RANGE_HEAD_KEY: &str = "range-head-object.bin";
const ACCEPT_RANGES_BYTES: &str = "bytes";

#[tokio::test]
#[serial]
async fn head_object_advertises_accept_ranges() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting HeadObject Accept-Ranges regression test");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(Vec::new()).await?;

    let client = env.create_s3_client();
    env.create_test_bucket(RANGE_HEAD_BUCKET).await?;

    client
        .put_object()
        .bucket(RANGE_HEAD_BUCKET)
        .key(RANGE_HEAD_KEY)
        .body(ByteStream::from_static(b"0123456789abcdef"))
        .send()
        .await?;

    let head = client
        .head_object()
        .bucket(RANGE_HEAD_BUCKET)
        .key(RANGE_HEAD_KEY)
        .send()
        .await?;
    assert_eq!(
        head.accept_ranges(),
        Some(ACCEPT_RANGES_BYTES),
        "HeadObject should advertise byte range support"
    );

    client
        .delete_object()
        .bucket(RANGE_HEAD_BUCKET)
        .key(RANGE_HEAD_KEY)
        .send()
        .await?;
    env.delete_test_bucket(RANGE_HEAD_BUCKET).await?;
    env.stop_server();

    Ok(())
}
