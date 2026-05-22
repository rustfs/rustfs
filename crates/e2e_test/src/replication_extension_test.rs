// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::common::{
    RustFSTestEnvironment, awscurl_available, awscurl_post_sts_form_urlencoded, init_logging, local_http_client,
};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use aws_sdk_s3::{Client, Config};
use http::header::{CONTENT_TYPE, HOST};
use reqwest::StatusCode;
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_madmin::{
    AddServiceAccountReq, ListServiceAccountsResp, PeerInfo, PeerSite, ReplicateAddStatus, ReplicateEditStatus,
    ReplicateRemoveStatus, SRRemoveReq, SRResyncOpStatus, SRStatusInfo, SiteReplicationInfo, SyncStatus,
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::collections::BTreeMap;
use std::error::Error;
use time::Duration as TimeDuration;
use tokio::time::{Duration, sleep};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusResponse {
    #[serde(rename = "Targets", default)]
    targets: Vec<ReplicationResetStatusTarget>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusTarget {
    #[serde(rename = "Arn", default)]
    arn: String,
    #[serde(rename = "ResetID", default)]
    reset_id: String,
    #[serde(rename = "Status", default)]
    status: String,
}

async fn signed_request(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(request.body(Body::empty())?, content_len, access_key, secret_key, "", "us-east-1");

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let client = local_http_client();
    let mut request_builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

async fn signed_request_with_session_token(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if !session_token.is_empty() {
        request = request.header("x-amz-security-token", session_token);
    }
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(
        request.body(Body::empty())?,
        content_len,
        access_key,
        secret_key,
        session_token,
        "us-east-1",
    );

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let client = local_http_client();
    let mut request_builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn parse_assume_role_credentials(xml: &str) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
    let access_key = extract_xml_tag(xml, "AccessKeyId").ok_or("missing AccessKeyId in AssumeRole response")?;
    let secret_key = extract_xml_tag(xml, "SecretAccessKey").ok_or("missing SecretAccessKey in AssumeRole response")?;
    let session_token = extract_xml_tag(xml, "SessionToken").ok_or("missing SessionToken in AssumeRole response")?;
    Ok((access_key, secret_key, session_token))
}

async fn set_replication_target(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    target_env: &RustFSTestEnvironment,
    target_bucket: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let body = serde_json::json!({
        "endpoint": target_env.address,
        "credentials": {
            "accessKey": target_env.access_key,
            "secretKey": target_env.secret_key
        },
        "targetbucket": target_bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    let response = signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("set remote target failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let arn: String = serde_json::from_slice(&body)?;
    Ok(arn)
}

async fn send_set_replication_target_request(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    update: bool,
    body: serde_json::Value,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    if update {
        url.push_str("&update=true");
    }
    signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await
}

async fn put_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arn: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>rule-1</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>Enabled</Status>
    </DeleteMarkerReplication>
    <ExistingObjectReplication>
      <Status>Enabled</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{target_arn}</Bucket>
    </Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication failed: {status} {body}").into());
    }

    Ok(())
}

async fn put_bucket_replication_rules(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arns: &[&str],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut rules = String::new();
    for (idx, target_arn) in target_arns.iter().enumerate() {
        rules.push_str(&format!(
            r#"
  <Rule>
    <ID>rule-{}</ID>
    <Priority>{}</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>Enabled</Status>
    </DeleteMarkerReplication>
    <ExistingObjectReplication>
      <Status>Enabled</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{}</Bucket>
    </Destination>
  </Rule>"#,
            idx + 1,
            idx + 1,
            target_arn
        ));
    }

    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>{rules}
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication with multiple rules failed: {status} {body}").into());
    }

    Ok(())
}

async fn delete_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication", env.url);
    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn enable_bucket_versioning(env: &RustFSTestEnvironment, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = env.create_s3_client();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

fn create_user_s3_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "e2e-site-replication");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

async fn admin_create_user(
    env: &RustFSTestEnvironment,
    username: &str,
    secret_key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, username);
    let body = serde_json::json!({
        "secretKey": secret_key,
        "status": "enabled"
    });
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("create user failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_add_canned_policy(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    policy: &serde_json::Value,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(policy.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add canned policy failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_attach_policy_to_user(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    username: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        env.url, policy_name, username
    );
    let response = signed_request(http::Method::PUT, &url, &env.access_key, &env.secret_key, Some(Vec::new()), None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("attach policy to user failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_update_group_members(
    env: &RustFSTestEnvironment,
    group_name: &str,
    members: &[&str],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/update-group-members", env.url);
    let body = serde_json::json!({
        "group": group_name,
        "members": members,
        "isRemove": false,
        "groupStatus": "enabled"
    });
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("update group members failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_attach_policy_to_group(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    group_name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=true",
        env.url, policy_name, group_name
    );
    let response = signed_request(http::Method::PUT, &url, &env.access_key, &env.secret_key, Some(Vec::new()), None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("attach policy to group failed: {status} {body}").into());
    }

    Ok(())
}

async fn wait_for_replicated_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_body: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes();
                let body = String::from_utf8(body.to_vec())?;
                if body == expected_body {
                    return Ok(());
                }
                return Err(format!("replicated object body mismatch: expected {expected_body}, got {body}").into());
            }
            Err(_err) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

async fn run_replication_check(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-check", env.url);
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn remove_replication_target(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/remove-remote-target?bucket={}&arn={}",
        env.url,
        urlencoding::encode(bucket),
        urlencoding::encode(arn)
    );
    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn remove_replication_target_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
    arn: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/remove-remote-target", env.url);
    let mut separator = '?';

    if let Some(bucket) = bucket {
        url.push(separator);
        separator = '&';
        url.push_str("bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }

    if let Some(arn) = arn {
        url.push(separator);
        url.push_str("arn=");
        url.push_str(&urlencoding::encode(arn));
    }

    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn add_service_account(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    req: &AddServiceAccountReq,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-service-account", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        signer_access_key,
        signer_secret_key,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add service account failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let parsed: serde_json::Value = serde_json::from_slice(&body)?;
    let credentials = parsed
        .get("credentials")
        .ok_or("add service account response missing credentials")?;
    let access_key = credentials
        .get("accessKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing access key")?
        .to_string();
    let secret_key = credentials
        .get("secretKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing secret key")?
        .to_string();

    Ok((access_key, secret_key))
}

async fn add_service_account_with_session_token(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    session_token: &str,
    req: &AddServiceAccountReq,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-service-account", env.url);
    let response = signed_request_with_session_token(
        http::Method::PUT,
        &url,
        signer_access_key,
        signer_secret_key,
        session_token,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add service account with session token failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let parsed: serde_json::Value = serde_json::from_slice(&body)?;
    let credentials = parsed
        .get("credentials")
        .ok_or("add service account response missing credentials")?;
    let access_key = credentials
        .get("accessKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing access key")?
        .to_string();
    let secret_key = credentials
        .get("secretKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing secret key")?
        .to_string();

    Ok((access_key, secret_key))
}

async fn list_service_accounts(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    user: Option<&str>,
) -> Result<ListServiceAccountsResp, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/list-service-accounts", env.url);
    if let Some(user) = user {
        url.push_str("?user=");
        url.push_str(&urlencoding::encode(user));
    }

    let response = signed_request(http::Method::GET, &url, signer_access_key, signer_secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("list service accounts failed: {status} {body}").into());
    }

    Ok(response.json().await?)
}

async fn get_account_info(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/accountinfo", env.url);
    let response = signed_request(http::Method::GET, &url, signer_access_key, signer_secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("account info failed: {status} {body}").into());
    }

    Ok(response.json().await?)
}

async fn wait_for_service_accounts(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    user: Option<&str>,
    expected: &[&str],
) -> Result<ListServiceAccountsResp, Box<dyn Error + Send + Sync>> {
    for _ in 0..20 {
        let resp = list_service_accounts(env, signer_access_key, signer_secret_key, user).await?;
        let access_keys: Vec<&str> = resp.accounts.iter().map(|account| account.access_key.as_str()).collect();
        if expected
            .iter()
            .all(|expected_key| access_keys.iter().any(|actual| actual == expected_key))
        {
            return Ok(resp);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("service accounts did not reach expected keys {expected:?} on {}", env.address).into())
}

async fn wait_for_object_on_target(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes().to_vec();
                return Ok(body);
            }
            Err(err) => {
                if err.to_string().contains("NoSuchKey") || err.to_string().contains("NotFound") {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
                return Err(err.into());
            }
        }
    }

    Err(format!("object {bucket}/{key} was not replicated in time").into())
}

async fn wait_for_user_get_object(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut last_error = None;
    for _ in 0..40 {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes().to_vec();
                return Ok(body);
            }
            Err(err) => {
                last_error = Some(err.to_string());
                sleep(Duration::from_millis(250)).await;
            }
        }
    }

    Err(format!(
        "user could not read replicated object {bucket}/{key} in time; last error: {}",
        last_error.unwrap_or_else(|| "unknown".to_string())
    )
    .into())
}

async fn list_replication_targets_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/list-remote-targets", env.url);
    if let Some(bucket) = bucket {
        url.push_str("?bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn site_replication_add(
    env: &RustFSTestEnvironment,
    sites: &[PeerSite],
) -> Result<ReplicateAddStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/add?replicateILMExpiry=false", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(sites)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication add failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_info(env: &RustFSTestEnvironment) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/info", env.url);
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication info failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_resync_op(
    env: &RustFSTestEnvironment,
    operation: &str,
    peer: &PeerInfo,
) -> Result<SRResyncOpStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/resync/op?operation={operation}", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(peer)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication resync {operation} failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_edit(
    env: &RustFSTestEnvironment,
    query: &str,
    peer: &PeerInfo,
) -> Result<ReplicateEditStatus, Box<dyn Error + Send + Sync>> {
    let url = if query.is_empty() {
        format!("{}/rustfs/admin/v3/site-replication/edit", env.url)
    } else {
        format!("{}/rustfs/admin/v3/site-replication/edit?{query}", env.url)
    };
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(peer)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication edit failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_status(env: &RustFSTestEnvironment, query: &str) -> Result<SRStatusInfo, Box<dyn Error + Send + Sync>> {
    let url = if query.is_empty() {
        format!("{}/rustfs/admin/v3/site-replication/status", env.url)
    } else {
        format!("{}/rustfs/admin/v3/site-replication/status?{query}", env.url)
    };
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication status failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_remove(
    env: &RustFSTestEnvironment,
    req: &SRRemoveReq,
) -> Result<ReplicateRemoveStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/remove", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication remove failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_state_edit(
    env: &RustFSTestEnvironment,
    body: &rustfs_madmin::SRStateEditReq,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/state/edit", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(body)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication state edit failed: {status} {body}").into());
    }

    Ok(())
}

async fn get_replication_reset_status(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<ReplicationResetStatusResponse, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-reset-status&arn={}", env.url, urlencoding::encode(arn));
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("replication reset status failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn wait_for_site_replication_enabled(
    env: &RustFSTestEnvironment,
    expected_sites: usize,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        let info = site_replication_info(env).await?;
        if info.enabled && info.sites.len() == expected_sites {
            return Ok(info);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication did not reach {expected_sites} sites on {}", env.address).into())
}

async fn wait_for_site_replication_disabled(
    env: &RustFSTestEnvironment,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    wait_for_site_replication_info(env, |info| !info.enabled && info.sites.is_empty()).await
}

async fn wait_for_site_replication_info<F>(
    env: &RustFSTestEnvironment,
    predicate: F,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>>
where
    F: Fn(&SiteReplicationInfo) -> bool,
{
    for _ in 0..40 {
        let info = site_replication_info(env).await?;
        if predicate(&info) {
            return Ok(info);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication info did not reach expected state on {}", env.address).into())
}

async fn wait_for_site_replication_status<F>(
    env: &RustFSTestEnvironment,
    query: &str,
    predicate: F,
) -> Result<SRStatusInfo, Box<dyn Error + Send + Sync>>
where
    F: Fn(&SRStatusInfo) -> bool,
{
    for _ in 0..40 {
        let status = site_replication_status(env, query).await?;
        if predicate(&status) {
            return Ok(status);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication status did not reach expected state on {}", env.address).into())
}

async fn wait_for_replication_reset_target<F>(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
    predicate: F,
) -> Result<ReplicationResetStatusTarget, Box<dyn Error + Send + Sync>>
where
    F: Fn(&ReplicationResetStatusTarget) -> bool,
{
    let mut last_seen = None;
    for _ in 0..40 {
        let status = get_replication_reset_status(env, bucket, arn).await?;
        if let Some(target) = status.targets.into_iter().find(|target| target.arn == arn) {
            if predicate(&target) {
                return Ok(target);
            }
            last_seen = Some(target);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!(
        "replication reset target {arn} for bucket {bucket} did not reach expected state; last seen: {:?}",
        last_seen
    )
    .into())
}

async fn build_replication_pair(
    enable_target_versioning: bool,
) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment, String), Box<dyn Error + Send + Sync>> {
    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    if enable_target_versioning {
        enable_bucket_versioning(&target_env, target_bucket).await?;
    }

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    Ok((source_env, target_env, source_bucket.to_string()))
}

#[tokio::test]
#[serial]
async fn test_replication_check_succeeds_with_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (_source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let response = run_replication_check(&_source_env, &source_bucket).await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await?.is_empty());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_target_without_object_lock() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-lock-src";
    let target_bucket = "replication-check-lock-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client
        .create_bucket()
        .bucket(source_bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("object lock"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-unversioned-src";
    let target_bucket = "replication-check-unversioned-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&target_env, target_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned source bucket should be rejected during remote target setup");
    let err = err.to_string();

    assert!(err.contains("400 Bad Request"), "unexpected set remote target error: {err}");
    assert!(err.contains("InvalidRequest"), "unexpected set remote target error: {err}");
    assert!(
        err.to_ascii_lowercase().contains("not versioned"),
        "unexpected set remote target error: {err}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-source-unversioned";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("versioning"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_missing_replication_config() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-missing-config";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("ReplicationConfigurationNotFoundError"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_check_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = run_replication_check(&env, "replication-check-no-such-bucket").await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_same_bucket_on_same_deployment() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-check-same-target";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let body = serde_json::json!({
        "endpoint": env.address,
        "credentials": {
            "accessKey": env.access_key,
            "secretKey": env.secret_key
        },
        "targetbucket": bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!("{}/rustfs/admin/v3/set-remote-target?bucket={}", env.url, urlencoding::encode(bucket));
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("IncorrectEndpoint"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_unversioned_target_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned target bucket should be rejected during remote target setup");
    assert!(err.to_string().contains("not versioned"), "unexpected set remote target error: {err}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_update_requires_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-needs-arn-src";
    let target_bucket = "replication-update-needs-arn-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is empty"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_update_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-missing-target-src";
    let target_bucket = "replication-update-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication",
            "arn": "arn:aws:s3:us-east-1:123456789012:replication::missing-target"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("target not found"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_set_remote_target_rejects_invalid_target_url() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-invalid-target-url-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        bucket,
        false,
        serde_json::json!({
            "endpoint": "://invalid-target-url",
            "credentials": {
                "accessKey": "replication",
                "secretKey": "replication"
            },
            "targetbucket": "target-bucket",
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("invalid target url"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_list_remote_targets_rejects_empty_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = list_replication_targets_request(&env, Some("")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("bucket is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_list_remote_targets_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = list_replication_targets_request(&env, Some("missing-replication-target-bucket")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let bucket = "replication-remove-missing-target";
    let target_bucket = "replication-remove-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let arn = set_replication_target(&source_env, bucket, &target_env, target_bucket).await?;

    let first_remove = remove_replication_target(&source_env, bucket, &arn).await?;
    assert_eq!(first_remove.status(), StatusCode::NO_CONTENT);

    let response = remove_replication_target(&source_env, bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("not found"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_missing_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "replication-remove-missing-arn";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = remove_replication_target_request(&env, Some(bucket), None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = remove_replication_target_request(
        &env,
        Some("missing-replication-remove-bucket"),
        Some("arn:aws:s3:us-east-1:123456789012:replication::missing"),
    )
    .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_remove_remote_target_rejects_target_used_by_replication() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let targets_url = format!(
        "{}/rustfs/admin/v3/list-remote-targets?bucket={}",
        source_env.url,
        urlencoding::encode(&source_bucket)
    );
    let targets_response = signed_request(
        http::Method::GET,
        &targets_url,
        &source_env.access_key,
        &source_env.secret_key,
        None,
        None,
    )
    .await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    let arn = targets
        .first()
        .and_then(|target| target.get("arn"))
        .and_then(|arn| arn.as_str())
        .ok_or("replication target arn missing")?
        .to_string();

    let response = remove_replication_target(&source_env, &source_bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("removal disallowed"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_delete_bucket_replication_removes_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-delete-config-src";
    let target_bucket = "replication-delete-config-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let delete_response = delete_bucket_replication(&source_env, source_bucket).await?;
    assert!(
        delete_response.status().is_success(),
        "unexpected delete status: {}",
        delete_response.status()
    );

    let targets_response = list_replication_targets_request(&source_env, Some(source_bucket)).await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    assert!(
        targets
            .iter()
            .all(|target| target.get("arn").and_then(|arn| arn.as_str()) != Some(target_arn.as_str())),
        "deleted replication config left stale target {target_arn}: {targets:?}"
    );

    let recreated_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &recreated_arn).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_single_bucket_replication_fans_out_to_multiple_targets() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env_a = RustFSTestEnvironment::new().await?;
    target_env_a.start_rustfs_server_without_cleanup(vec![]).await?;

    let mut target_env_b = RustFSTestEnvironment::new().await?;
    target_env_b.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-fanout-src";
    let target_bucket_a = "replication-fanout-dst-a";
    let target_bucket_b = "replication-fanout-dst-b";
    let object_key = "fanout.txt";
    let body = "payload-fanout";

    let source_client = source_env.create_s3_client();
    let target_client_a = target_env_a.create_s3_client();
    let target_client_b = target_env_b.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client_a.create_bucket().bucket(target_bucket_a).send().await?;
    target_client_b.create_bucket().bucket(target_bucket_b).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env_a, target_bucket_a).await?;
    enable_bucket_versioning(&target_env_b, target_bucket_b).await?;

    let target_arn_a = set_replication_target(&source_env, source_bucket, &target_env_a, target_bucket_a).await?;
    let target_arn_b = set_replication_target(&source_env, source_bucket, &target_env_b, target_bucket_b).await?;
    put_bucket_replication_rules(&source_env, source_bucket, &[target_arn_a.as_str(), target_arn_b.as_str()]).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object(&target_client_a, target_bucket_a, object_key, body).await?;
    wait_for_replicated_object(&target_client_b, target_bucket_b, object_key, body).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_sequential_bucket_replication_succeeds_for_multiple_buckets() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    for idx in 1..=5 {
        let source_bucket = format!("replication-multi-src-{idx}");
        let target_bucket = format!("replication-multi-dst-{idx}");
        let object_key = format!("probe-{idx}.txt");
        let body = format!("payload-{idx}");

        source_client.create_bucket().bucket(&source_bucket).send().await?;
        target_client.create_bucket().bucket(&target_bucket).send().await?;
        enable_bucket_versioning(&source_env, &source_bucket).await?;
        enable_bucket_versioning(&target_env, &target_bucket).await?;

        let target_arn = set_replication_target(&source_env, &source_bucket, &target_env, &target_bucket).await?;
        put_bucket_replication(&source_env, &source_bucket, &target_arn).await?;

        source_client
            .put_object()
            .bucket(&source_bucket)
            .key(&object_key)
            .body(ByteStream::from(body.clone().into_bytes()))
            .send()
            .await?;

        wait_for_replicated_object(&target_client, &target_bucket, &object_key, &body).await?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_replication_recovers_after_runtime_target_cache_is_cleared() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-refresh-src";
    let target_bucket = "replication-refresh-dst";
    let object_key = "probe-refresh.txt";
    let body = "payload-refresh";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    BucketTargetSys::get().delete(source_bucket).await;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object(&target_client, target_bucket, object_key, body).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_resync_start_cancel_restart_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "site-repl-resync-src";
    let target_bucket = "site-repl-resync-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;
    let remote_peer = source_info
        .sites
        .into_iter()
        .find(|peer| peer.endpoint == target_env.url)
        .ok_or("target peer missing from source site replication info")?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    for idx in 0..32 {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(format!("resync-object-{idx:02}"))
            .body(ByteStream::from(vec![b'x'; 256 * 1024]))
            .send()
            .await?;
    }

    let started = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(started.status, "success", "unexpected start result: {:?}", started);
    assert!(
        started
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "started" | "success")),
        "source bucket start status missing: {:?}",
        started
    );

    let started_target =
        wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| !target.reset_id.is_empty()).await?;
    let started_reset_id = started_target.reset_id.clone();
    assert!(
        matches!(started_target.status.as_str(), "Pending" | "Started" | "InProgress" | "Completed"),
        "unexpected start status: {:?}",
        started_target
    );

    let canceled = site_replication_resync_op(&source_env, "cancel", &remote_peer).await?;
    assert_eq!(canceled.status, "success", "unexpected cancel result: {:?}", canceled);
    assert!(
        canceled
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "canceled" | "success")),
        "source bucket cancel status missing: {:?}",
        canceled
    );

    let canceled_target =
        wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| target.status == "Canceled").await?;
    assert_eq!(canceled_target.status, "Canceled");
    assert_eq!(canceled_target.reset_id, started_reset_id);

    let restarted = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(restarted.status, "success", "unexpected restart result: {:?}", restarted);
    assert!(
        restarted
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "started" | "success")),
        "source bucket restart status missing: {:?}",
        restarted
    );
    let restart_snapshot = get_replication_reset_status(&source_env, source_bucket, &target_arn).await?;
    let restarted_target = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| {
        !target.reset_id.is_empty() && target.reset_id != started_reset_id
    })
    .await
    .map_err(|err| {
        format!(
            "restart ids: start={} restart={} snapshot={:?}; {err}",
            started_reset_id, restarted.resync_id, restart_snapshot.targets
        )
    })?;
    assert_ne!(restarted_target.reset_id, started_reset_id);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_edit_and_status_peer_state_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;
    let mut remote_peer = source_info
        .sites
        .into_iter()
        .find(|peer| peer.endpoint == target_env.url)
        .ok_or("target peer missing from source site replication info")?;

    remote_peer.sync_state = SyncStatus::Enable;
    let edit_status = site_replication_edit(&source_env, "", &remote_peer).await?;
    assert!(edit_status.success, "unexpected site edit result: {:?}", edit_status);

    let source_after_sync = wait_for_site_replication_info(&source_env, |info| {
        info.sites
            .iter()
            .any(|peer| peer.endpoint == target_env.url && peer.sync_state == SyncStatus::Enable)
    })
    .await?;
    let target_after_sync = wait_for_site_replication_info(&target_env, |info| {
        info.sites
            .iter()
            .any(|peer| peer.endpoint == target_env.url && peer.sync_state == SyncStatus::Enable)
    })
    .await?;
    assert!(
        source_after_sync
            .sites
            .iter()
            .any(|peer| peer.endpoint == target_env.url && peer.sync_state == SyncStatus::Enable)
    );
    assert!(
        target_after_sync
            .sites
            .iter()
            .any(|peer| peer.endpoint == target_env.url && peer.sync_state == SyncStatus::Enable)
    );

    let ilm_edit_status = site_replication_edit(&source_env, "enableILMExpiryReplication=true", &PeerInfo::default()).await?;
    assert!(ilm_edit_status.success, "unexpected ilm edit result: {:?}", ilm_edit_status);

    let source_after_ilm = wait_for_site_replication_info(&source_env, |info| {
        info.sites.len() == 2 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    let target_after_ilm = wait_for_site_replication_info(&target_env, |info| {
        info.sites.len() == 2 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    assert!(source_after_ilm.sites.iter().all(|peer| peer.replicate_ilm_expiry));
    assert!(target_after_ilm.sites.iter().all(|peer| peer.replicate_ilm_expiry));

    let status_query = "peer-state=true";
    let source_status = wait_for_site_replication_status(&source_env, status_query, |status| {
        status.peer_states.len() == 2
            && status
                .peer_states
                .values()
                .all(|state| state.peers.len() == 2 && state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    })
    .await?;
    let target_status = wait_for_site_replication_status(&target_env, status_query, |status| {
        status.peer_states.len() == 2
            && status
                .peer_states
                .values()
                .all(|state| state.peers.len() == 2 && state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    })
    .await?;

    assert_eq!(source_status.peer_states.len(), 2);
    assert_eq!(target_status.peer_states.len(), 2);
    assert!(source_status.peer_states.values().all(|state| state.peers.len() == 2));
    assert!(target_status.peer_states.values().all(|state| state.peers.len() == 2));
    assert!(
        source_status
            .peer_states
            .values()
            .all(|state| state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    );
    assert!(
        target_status
            .peer_states
            .values()
            .all(|state| state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_remove_all_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    let remove_status = site_replication_remove(
        &source_env,
        &SRRemoveReq {
            remove_all: true,
            ..Default::default()
        },
    )
    .await?;
    assert!(
        !remove_status.status.is_empty() && remove_status.err_detail.is_empty(),
        "unexpected site remove result: {:?}",
        remove_status
    );

    let source_after_remove = wait_for_site_replication_disabled(&source_env).await?;
    let target_after_remove = wait_for_site_replication_disabled(&target_env).await?;

    assert!(!source_after_remove.enabled);
    assert!(source_after_remove.sites.is_empty());
    assert!(!target_after_remove.enabled);
    assert!(target_after_remove.sites.is_empty());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_state_edit_fresh_and_stale_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let target_info = wait_for_site_replication_enabled(&target_env, 2).await?;
    assert!(source_info.sites.iter().all(|peer| !peer.replicate_ilm_expiry));
    assert!(target_info.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    let target_status =
        wait_for_site_replication_status(&target_env, "peer-state=true", |status| status.peer_states.len() == 2).await?;
    let current_updated_at = target_status
        .peer_states
        .values()
        .find_map(|state| state.updated_at)
        .ok_or("missing target site replication updated_at")?;

    let mut stale_peers = BTreeMap::new();
    for peer in target_info.sites {
        let mut peer = peer;
        peer.replicate_ilm_expiry = true;
        stale_peers.insert(peer.deployment_id.clone(), peer);
    }
    site_replication_state_edit(
        &target_env,
        &rustfs_madmin::SRStateEditReq {
            peers: stale_peers,
            updated_at: Some(current_updated_at - TimeDuration::seconds(1)),
        },
    )
    .await?;

    let target_after_stale = site_replication_info(&target_env).await?;
    let source_after_stale = site_replication_info(&source_env).await?;
    assert!(target_after_stale.sites.iter().all(|peer| !peer.replicate_ilm_expiry));
    assert!(source_after_stale.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    let mut fresh_peers = BTreeMap::new();
    for peer in target_after_stale.sites {
        let mut peer = peer;
        peer.replicate_ilm_expiry = true;
        fresh_peers.insert(peer.deployment_id.clone(), peer);
    }
    let fresh_updated_at = current_updated_at + TimeDuration::seconds(1);
    site_replication_state_edit(
        &target_env,
        &rustfs_madmin::SRStateEditReq {
            peers: fresh_peers,
            updated_at: Some(fresh_updated_at),
        },
    )
    .await?;

    let target_after_fresh = wait_for_site_replication_info(&target_env, |info| {
        info.sites.len() == 2 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    assert!(target_after_fresh.sites.iter().all(|peer| peer.replicate_ilm_expiry));

    let target_status_after_fresh = wait_for_site_replication_status(&target_env, "peer-state=true", |status| {
        status.peer_states.len() == 2
            && status.peer_states.values().all(|state| {
                state.updated_at == Some(fresh_updated_at) && state.peers.values().all(|peer| peer.replicate_ilm_expiry)
            })
    })
    .await?;
    assert!(target_status_after_fresh.peer_states.values().all(|state| {
        state.updated_at == Some(fresh_updated_at) && state.peers.values().all(|peer| peer.replicate_ilm_expiry)
    }));

    let source_after_fresh = site_replication_info(&source_env).await?;
    assert!(source_after_fresh.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_replicates_object_with_bucket_versioning_real_dual_node() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-versioned";
    let key = "hello.txt";
    let payload = b"site replication should replicate after enabling versioning".to_vec();

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    let replication_response = signed_request(
        http::Method::GET,
        &format!("{}/{bucket}?replication", source_env.url),
        &source_env.access_key,
        &source_env.secret_key,
        None,
        None,
    )
    .await?;
    let replication_status = replication_response.status();
    let replication_body = replication_response.text().await.unwrap_or_default();
    assert_eq!(
        replication_status,
        StatusCode::OK,
        "source bucket replication config missing after site replication setup: {replication_body}"
    );
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_replicates_policy_backed_user_access_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-policy-user";
    let key = "seed.txt";
    let payload = b"site replication policy-backed user access".to_vec();
    let policy_name = "site-repl-readonly";
    let username = "site-repl-user";
    let secret_key = "site-repl-user-secret-key-123456";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{bucket}/*")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")]
            }
        ]
    });
    admin_add_canned_policy(&source_env, policy_name, &policy).await?;
    admin_create_user(&source_env, username, secret_key).await?;
    admin_attach_policy_to_user(&source_env, policy_name, username).await?;

    let target_user_client = create_user_s3_client(&target_env, username, secret_key);
    let fetched = wait_for_user_get_object(&target_user_client, bucket, key).await?;
    assert_eq!(fetched, payload);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_replicates_group_policy_backed_access_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>>
{
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-policy-group";
    let key = "seed.txt";
    let payload = b"site replication group-policy-backed user access".to_vec();
    let policy_name = "site-repl-group-readonly";
    let group_name = "site-repl-group";
    let username = "site-repl-group-user";
    let secret_key = "site-repl-group-user-secret-key-12";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{bucket}/*")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")]
            }
        ]
    });
    admin_add_canned_policy(&source_env, policy_name, &policy).await?;
    admin_create_user(&source_env, username, secret_key).await?;
    admin_update_group_members(&source_env, group_name, &[username]).await?;
    admin_attach_policy_to_group(&source_env, policy_name, group_name).await?;

    let target_user_client = create_user_s3_client(&target_env, username, secret_key);
    let fetched = wait_for_user_get_object(&target_user_client, bucket, key).await?;
    assert_eq!(fetched, payload);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_service_account_policy_from_accountinfo_round_trips_real_single_node() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let account_info = get_account_info(&env, &env.access_key, &env.secret_key).await?;
    let policy_str = account_info
        .get("policy")
        .and_then(|value| value.as_str())
        .ok_or("account info policy should be a JSON string")?;

    let policy: serde_json::Value = serde_json::from_str(policy_str)?;
    let statements = policy
        .get("Statement")
        .and_then(|value| value.as_array())
        .ok_or("account info policy should include Statement array")?;

    assert!(!statements.is_empty(), "account info policy Statement should not be empty: {policy}");

    let req = AddServiceAccountReq {
        policy: Some(policy),
        target_user: None,
        access_key: "svcacct-info-sample".to_string(),
        secret_key: "svcacct-info-sample-secret-key-123456".to_string(),
        name: Some("svcacct-info-sample".to_string()),
        description: Some("service account created from accountinfo sample policy".to_string()),
        expiration: None,
        comment: None,
    };

    let created = add_service_account(&env, &env.access_key, &env.secret_key, &req).await?;
    assert_eq!(created.0, "svcacct-info-sample");

    let listed =
        wait_for_service_accounts(&env, &env.access_key, &env.secret_key, Some(&env.access_key), &["svcacct-info-sample"])
            .await?;
    assert!(
        listed
            .accounts
            .iter()
            .any(|account| account.access_key == "svcacct-info-sample"),
        "created service account should be listed for parent user: {:?}",
        listed.accounts
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_replicates_multiple_service_accounts_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    let first_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-alpha".to_string(),
        secret_key: "svc-alpha-secret-key-1234567890abcdef".to_string(),
        name: Some("svc-alpha".to_string()),
        description: Some("first replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let first = add_service_account(&source_env, &source_env.access_key, &source_env.secret_key, &first_req).await?;

    let target_after_first = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-alpha"],
    )
    .await?;
    assert!(
        target_after_first
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-alpha"),
        "target accounts missing svc-alpha: {:?}",
        target_after_first.accounts
    );

    let second_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-beta".to_string(),
        secret_key: "svc-beta-secret-key-1234567890abcdef1".to_string(),
        name: Some("svc-beta".to_string()),
        description: Some("second replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let _second = add_service_account(&source_env, &first.0, &first.1, &second_req).await?;

    let target_after_second = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-alpha", "svc-beta"],
    )
    .await?;
    assert!(
        target_after_second
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-beta"),
        "target accounts missing svc-beta: {:?}",
        target_after_second.accounts
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_site_replication_replicates_service_accounts_created_from_sts_session_real_dual_node() -> TestResult {
    init_logging();

    if !awscurl_available() {
        eprintln!("Skipping STS site replication service-account test because awscurl is unavailable");
        return Ok(());
    }

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.start_rustfs_server(vec![]).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    let assume_role_body = "Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600";
    let sts_xml = awscurl_post_sts_form_urlencoded(
        &format!("{}/", source_env.url.trim_end_matches('/')),
        assume_role_body,
        &source_env.access_key,
        &source_env.secret_key,
    )
    .await?;
    let (sts_access_key, sts_secret_key, sts_session_token) = parse_assume_role_credentials(&sts_xml)?;

    let first_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-sts-alpha".to_string(),
        secret_key: "svc-sts-alpha-secret-key-1234567890".to_string(),
        name: Some("svc-sts-alpha".to_string()),
        description: Some("sts-created replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let first =
        add_service_account_with_session_token(&source_env, &sts_access_key, &sts_secret_key, &sts_session_token, &first_req)
            .await?;

    let target_after_first = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-sts-alpha"],
    )
    .await?;
    assert!(
        target_after_first
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-sts-alpha"),
        "target accounts missing svc-sts-alpha: {:?}",
        target_after_first.accounts
    );

    let second_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-sts-beta".to_string(),
        secret_key: "svc-sts-beta-secret-key-1234567890a".to_string(),
        name: Some("svc-sts-beta".to_string()),
        description: Some("second replicated service account from sts-created ak".to_string()),
        expiration: None,
        comment: None,
    };
    let _second = add_service_account(&source_env, &first.0, &first.1, &second_req).await?;

    let target_after_second = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-sts-alpha", "svc-sts-beta"],
    )
    .await?;
    assert!(
        target_after_second
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-sts-beta"),
        "target accounts missing svc-sts-beta: {:?}",
        target_after_second.accounts
    );

    Ok(())
}
