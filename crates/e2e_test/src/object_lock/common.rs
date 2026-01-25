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

//! Object Lock E2E test utilities
//!
//! This module provides Object Lock-specific functionality for testing:
//! - Creating buckets with Object Lock enabled
//! - Setting retention policies
//! - Testing legal hold operations
//! - Bypass governance retention header handling

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockMode,
    ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
};
use chrono::{DateTime, Duration, Utc};
use tracing::info;

use crate::common::RustFSTestEnvironment;

/// Object Lock test environment
pub struct ObjectLockTestEnvironment {
    pub base_env: RustFSTestEnvironment,
}

impl ObjectLockTestEnvironment {
    /// Create a new Object Lock test environment
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_env = RustFSTestEnvironment::new().await?;
        Ok(Self { base_env })
    }

    /// Start RustFS server (no special flags needed for Object Lock)
    pub async fn start_rustfs(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.base_env.start_rustfs_server(Vec::new()).await
    }

    /// Get S3 client
    pub fn s3_client(&self) -> Client {
        self.base_env.create_s3_client()
    }

    /// Create a bucket with Object Lock enabled
    pub async fn create_object_lock_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.s3_client();

        // Create bucket with Object Lock enabled
        client
            .create_bucket()
            .bucket(bucket_name)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await?;

        info!("Created bucket with Object Lock enabled: {}", bucket_name);
        Ok(())
    }
}

/// Put Object Lock configuration on a bucket
pub async fn put_object_lock_configuration(
    client: &Client,
    bucket: &str,
    mode: ObjectLockRetentionMode,
    days: Option<i32>,
    years: Option<i32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut default_retention_builder = DefaultRetention::builder().mode(mode);

    if let Some(d) = days {
        default_retention_builder = default_retention_builder.days(d);
    }
    if let Some(y) = years {
        default_retention_builder = default_retention_builder.years(y);
    }

    let default_retention = default_retention_builder.build();

    let rule = ObjectLockRule::builder().default_retention(default_retention).build();

    let config = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(rule)
        .build();

    client
        .put_object_lock_configuration()
        .bucket(bucket)
        .object_lock_configuration(config)
        .send()
        .await?;

    info!("Put Object Lock configuration on bucket: {}", bucket);
    Ok(())
}

/// Convert ObjectLockRetentionMode to ObjectLockMode for put_object
fn retention_mode_to_lock_mode(mode: ObjectLockRetentionMode) -> ObjectLockMode {
    match mode.as_str() {
        "GOVERNANCE" => ObjectLockMode::Governance,
        "COMPLIANCE" => ObjectLockMode::Compliance,
        _ => ObjectLockMode::Governance,
    }
}

/// Put an object with retention
pub async fn put_object_with_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    data: &[u8],
    mode: ObjectLockRetentionMode,
    retain_until: DateTime<Utc>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // AWS SDK requires UTC time without timezone offset (e.g., "2026-01-24T11:20:14Z")
    let retain_until_str = retain_until.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let retain_until_datetime =
        aws_sdk_s3::primitives::DateTime::from_str(&retain_until_str, aws_sdk_s3::primitives::DateTimeFormat::DateTime)?;

    let lock_mode = retention_mode_to_lock_mode(mode.clone());

    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .object_lock_mode(lock_mode)
        .object_lock_retain_until_date(retain_until_datetime)
        .send()
        .await?;

    let version_id = response.version_id().unwrap_or("null").to_string();
    info!("Put object {} with retention mode {:?}, version: {}", key, mode, version_id);
    Ok(version_id)
}

/// Put an object with legal hold
pub async fn put_object_with_legal_hold(
    client: &Client,
    bucket: &str,
    key: &str,
    data: &[u8],
    legal_hold: ObjectLockLegalHoldStatus,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let legal_hold_str = format!("{:?}", legal_hold);

    let response = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .object_lock_legal_hold_status(legal_hold)
        .send()
        .await?;

    let version_id = response.version_id().unwrap_or("null").to_string();
    info!("Put object {} with legal hold {}, version: {}", key, legal_hold_str, version_id);
    Ok(version_id)
}

/// Put object retention on existing object
pub async fn put_object_retention(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    mode: ObjectLockRetentionMode,
    retain_until: DateTime<Utc>,
    bypass_governance: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // AWS SDK requires UTC time without timezone offset (e.g., "2026-01-24T11:20:14Z")
    let retain_until_str = retain_until.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let retain_until_datetime =
        aws_sdk_s3::primitives::DateTime::from_str(&retain_until_str, aws_sdk_s3::primitives::DateTimeFormat::DateTime)?;

    let retention = ObjectLockRetention::builder()
        .mode(mode.clone())
        .retain_until_date(retain_until_datetime)
        .build();

    let mut request = client
        .put_object_retention()
        .bucket(bucket)
        .key(key)
        .retention(retention)
        .bypass_governance_retention(bypass_governance);

    if let Some(vid) = version_id {
        request = request.version_id(vid);
    }

    request.send().await?;
    info!("Put object retention on {} with mode {:?}", key, mode);
    Ok(())
}

/// Put object legal hold
pub async fn put_object_legal_hold(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    status: ObjectLockLegalHoldStatus,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let status_str = format!("{:?}", status);
    let legal_hold = ObjectLockLegalHold::builder().status(status).build();

    let mut request = client.put_object_legal_hold().bucket(bucket).key(key).legal_hold(legal_hold);

    if let Some(vid) = version_id {
        request = request.version_id(vid);
    }

    request.send().await?;
    info!("Put legal hold {} on {}", status_str, key);
    Ok(())
}

/// Delete object with optional bypass governance retention
pub async fn delete_object_with_bypass(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    bypass_governance: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .bypass_governance_retention(bypass_governance);

    if let Some(vid) = version_id {
        request = request.version_id(vid);
    }

    request.send().await?;
    info!("Deleted object {} (bypass: {})", key, bypass_governance);
    Ok(())
}

/// Calculate a future retain_until date
pub fn future_retain_until(days: i64) -> DateTime<Utc> {
    Utc::now() + Duration::days(days)
}
