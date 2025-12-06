#![cfg(test)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketCannedAcl, ObjectCannedAcl};
use serial_test::serial;
use std::error::Error;

const ENDPOINT: &str = "http://localhost:9000";
const ADMIN_ACCESS_KEY: &str = "rustfsadmin";
const ADMIN_SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "acl-full-test-bucket";

#[tokio::test]
#[serial]
async fn test_obj_acl_private() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;
    let key = "obj-private.txt";

    // Admin uploads Private object
    assert_success(
        admin
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("secret".as_bytes().to_vec()))
            .acl(ObjectCannedAcl::Private)
            .send()
            .await,
        "Admin upload private object",
    );

    // User reads (should be denied)
    assert_access_denied(user.get_object().bucket(BUCKET).key(key).send().await);

    // User overwrites (should be denied)
    assert_access_denied(
        user.put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("hacked".as_bytes().to_vec()))
            .send()
            .await,
    );

    cleanup_bucket(&admin).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_obj_acl_public_read() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;
    let key = "obj-public-read.txt";

    // Admin uploads PublicRead object
    assert_success(
        admin
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("hello world".as_bytes().to_vec()))
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await,
        "Admin upload public-read object",
    );

    // User reads (should succeed)
    assert_success(user.get_object().bucket(BUCKET).key(key).send().await, "User read public-read object");

    // User overwrites (should be denied - PublicRead does not include Write permission)
    assert_access_denied(
        user.put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("hacked".as_bytes().to_vec()))
            .send()
            .await,
    );

    cleanup_bucket(&admin).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_obj_acl_public_read_write() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;
    let key = "obj-public-rw.txt";

    // Admin uploads PublicReadWrite object
    assert_success(
        admin
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("original".as_bytes().to_vec()))
            .acl(ObjectCannedAcl::PublicReadWrite)
            .send()
            .await,
        "Admin upload public-read-write object",
    );

    // User reads (should succeed)
    assert_success(
        user.get_object().bucket(BUCKET).key(key).send().await,
        "User read public-read-write object",
    );

    // User overwrites (should succeed)
    assert_success(
        user.put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("user_modified".as_bytes().to_vec()))
            .send()
            .await,
        "User overwrite public-read-write object",
    );

    cleanup_bucket(&admin).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_obj_acl_authenticated_read() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;
    let key = "obj-auth-read.txt";

    // Admin uploads AuthenticatedRead object
    assert_success(
        admin
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("auth only".as_bytes().to_vec()))
            .acl(ObjectCannedAcl::AuthenticatedRead)
            .send()
            .await,
        "Admin upload authenticated-read object",
    );

    // User (authenticated) reads (should be denied)
    assert_access_denied(user.get_object().bucket(BUCKET).key(key).send().await);

    // User overwrites (should be denied - read permission does not include write)
    assert_access_denied(
        user.put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from("hacked".as_bytes().to_vec()))
            .send()
            .await,
    );

    cleanup_bucket(&admin).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_bucket_acl_private() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;

    // Set Bucket to Private
    assert_success(
        admin
            .put_bucket_acl()
            .bucket(BUCKET)
            .acl(BucketCannedAcl::Private)
            .send()
            .await,
        "Set bucket ACL to Private",
    );

    // User tries to ListObjects (should be denied)
    assert_access_denied(user.list_objects_v2().bucket(BUCKET).send().await);

    cleanup_bucket(&admin).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_bucket_acl_public_read() -> Result<(), Box<dyn Error>> {
    let (admin, user) = setup_env().await?;

    // Set Bucket to PublicRead
    assert_success(
        admin
            .put_bucket_acl()
            .bucket(BUCKET)
            .acl(BucketCannedAcl::PublicRead)
            .send()
            .await,
        "Set bucket ACL to PublicRead",
    );

    // Put an object to avoid empty List (should not error even if empty)
    admin
        .put_object()
        .bucket(BUCKET)
        .key("test.txt")
        .body(ByteStream::from("test".as_bytes().to_vec()))
        .send()
        .await?;

    // User tries to ListObjects (should succeed)
    assert_success(
        user.list_objects_v2().bucket(BUCKET).send().await,
        "User list objects in PublicRead bucket",
    );

    cleanup_bucket(&admin).await;
    Ok(())
}

// Helper functions
async fn create_s3_client(access_key: &str, secret_key: &str) -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let credentials = Credentials::new(access_key, secret_key, None, None, "static");

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(credentials)
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

async fn create_anonymous_s3_client() -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(ENDPOINT)
        .no_credentials()
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

fn assert_access_denied<T, E>(result: Result<T, SdkError<E>>)
where
    E: std::fmt::Debug,
{
    match result {
        Ok(_) => panic!("❌ Expected AccessDenied (403), but operation SUCCEEDED."),
        Err(SdkError::ServiceError(context)) => {
            let code = context.raw().status().as_u16();
            if code != 403 {
                panic!(
                    "❌ Expected AccessDenied (403), but got status code: {}. Error: {:?}",
                    code,
                    context.err()
                );
            }
        }
        Err(e) => panic!("❌ Expected ServiceError(403), got other error: {:?}", e),
    }
}

fn assert_success<T, E>(result: Result<T, SdkError<E>>, msg: &str) -> T
where
    E: std::fmt::Debug,
{
    match result {
        Ok(val) => val,
        Err(e) => panic!("❌ Expected SUCCESS for '{}', but got error: {:?}", msg, e),
    }
}

async fn setup_env() -> Result<(Client, Client), Box<dyn Error>> {
    let admin = create_s3_client(ADMIN_ACCESS_KEY, ADMIN_SECRET_KEY).await;
    let user = create_anonymous_s3_client().await;

    cleanup_bucket(&admin).await;

    admin.create_bucket().bucket(BUCKET).send().await?;

    Ok((admin, user))
}

async fn cleanup_bucket(client: &Client) {
    if let Ok(objects) = client.list_objects_v2().bucket(BUCKET).send().await {
        if let Some(contents) = objects.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    let _ = client.delete_object().bucket(BUCKET).key(&key).send().await;
                }
            }
        }
    }
    let _ = client.delete_bucket().bucket(BUCKET).send().await;
}
