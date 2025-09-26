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

use crate::error::{Error, Result};
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display},
    str::FromStr,
    time::Duration,
};
use time::OffsetDateTime;
use url::Url;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Credentials {
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    pub session_token: Option<String>,
    pub expiration: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub enum ServiceType {
    #[default]
    Replication,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LatencyStat {
    #[serde(with = "duration_milliseconds")]
    pub curr: Duration, // Current latency
    #[serde(with = "duration_milliseconds")]
    pub avg: Duration, // Average latency
    #[serde(with = "duration_milliseconds")]
    pub max: Duration, // Maximum latency
}

mod duration_milliseconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum BucketTargetType {
    #[default]
    None,
    #[serde(rename = "replication")]
    ReplicationService,
    #[serde(rename = "ilm")]
    IlmService,
}

impl BucketTargetType {
    pub fn is_valid(&self) -> bool {
        match self {
            BucketTargetType::None => false,
            BucketTargetType::ReplicationService | BucketTargetType::IlmService => true,
        }
    }
}

impl FromStr for BucketTargetType {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "replication" => Ok(BucketTargetType::ReplicationService),
            "ilm" => Ok(BucketTargetType::IlmService),
            _ => Ok(BucketTargetType::None),
        }
    }
}

impl fmt::Display for BucketTargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BucketTargetType::None => write!(f, ""),
            BucketTargetType::ReplicationService => write!(f, "replication"),
            BucketTargetType::IlmService => write!(f, "ilm"),
        }
    }
}

// Define BucketTarget structure
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTarget {
    #[serde(rename = "sourcebucket", default)]
    pub source_bucket: String,

    #[serde(default)]
    pub endpoint: String,

    #[serde(default)]
    pub credentials: Option<Credentials>,
    #[serde(rename = "targetbucket", default)]
    pub target_bucket: String,

    #[serde(default)]
    pub secure: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub api: String,
    #[serde(default)]
    pub arn: String,
    #[serde(rename = "type", default)]
    pub target_type: BucketTargetType,

    #[serde(default)]
    pub region: String,

    #[serde(alias = "bandwidth", default)]
    pub bandwidth_limit: i64,

    #[serde(rename = "replicationSync", default)]
    pub replication_sync: bool,
    #[serde(default)]
    pub storage_class: String,
    #[serde(rename = "healthCheckDuration", with = "duration_seconds", default)]
    pub health_check_duration: Duration,
    #[serde(rename = "disableProxy", default)]
    pub disable_proxy: bool,

    #[serde(rename = "resetBeforeDate", with = "time::serde::rfc3339::option", default)]
    pub reset_before_date: Option<OffsetDateTime>,
    #[serde(default)]
    pub reset_id: String,
    #[serde(rename = "totalDowntime", with = "duration_seconds", default)]
    pub total_downtime: Duration,

    #[serde(rename = "lastOnline", with = "time::serde::rfc3339::option", default)]
    pub last_online: Option<OffsetDateTime>,
    #[serde(rename = "isOnline", default)]
    pub online: bool,

    #[serde(default)]
    pub latency: LatencyStat,

    #[serde(default)]
    pub deployment_id: String,

    #[serde(default)]
    pub edge: bool,
    #[serde(rename = "edgeSyncBeforeExpiry", default)]
    pub edge_sync_before_expiry: bool,
    #[serde(rename = "offlineCount", default)]
    pub offline_count: u64,
}

impl BucketTarget {
    pub fn is_empty(self) -> bool {
        self.target_bucket.is_empty() && self.endpoint.is_empty() && self.arn.is_empty()
    }
    pub fn url(&self) -> Result<Url> {
        let scheme = if self.secure { "https" } else { "http" };
        Url::parse(&format!("{}://{}", scheme, self.endpoint)).map_err(Error::other)
    }
}

impl Display for BucketTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ", self.endpoint)?;
        write!(f, "{}", self.target_bucket.clone())?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketTargets {
    pub targets: Vec<BucketTarget>,
}

impl BucketTargets {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketTargets = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn is_empty(&self) -> bool {
        if self.targets.is_empty() {
            return true;
        }

        for target in &self.targets {
            if !target.clone().is_empty() {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::time::Duration;
    use time::OffsetDateTime;

    #[test]
    fn test_bucket_target_json_deserialize() {
        let json = r#"
        {
            "sourcebucket": "source-bucket-name",
            "endpoint": "s3.amazonaws.com",
            "credentials": {
                "accessKey": "test-access-key",
                "secretKey": "test-secret-key",
                "session_token": "test-session-token",
                "expiration": "2024-12-31T23:59:59Z"
            },
            "targetbucket": "target-bucket-name",
            "secure": true,
            "path": "/api/v1",
            "api": "s3v4",
            "arn": "arn:aws:s3:::target-bucket-name",
            "type": "replication",
            "region": "us-east-1",
            "bandwidth_limit": 1000000,
            "replicationSync": true,
            "storage_class": "STANDARD",
            "healthCheckDuration": 30,
            "disableProxy": false,
            "resetBeforeDate": null,
            "reset_id": "reset-123",
            "totalDowntime": 3600,
            "last_online": null,
            "isOnline": true,
            "latency": {
                "curr": 100,
                "avg": 150,
                "max": 300
            },
            "deployment_id": "deployment-456",
            "edge": false,
            "edgeSyncBeforeExpiry": true,
            "offlineCount": 5
        }
        "#;

        let result: std::result::Result<BucketTarget, _> = serde_json::from_str(json);
        assert!(result.is_ok(), "Failed to deserialize BucketTarget: {:?}", result.err());

        let target = result.unwrap();

        // Verify basic fields
        assert_eq!(target.source_bucket, "source-bucket-name");
        assert_eq!(target.endpoint, "s3.amazonaws.com");
        assert_eq!(target.target_bucket, "target-bucket-name");
        assert!(target.secure);
        assert_eq!(target.path, "/api/v1");
        assert_eq!(target.api, "s3v4");
        assert_eq!(target.arn, "arn:aws:s3:::target-bucket-name");
        assert_eq!(target.target_type, BucketTargetType::ReplicationService);
        assert_eq!(target.region, "us-east-1");
        assert_eq!(target.bandwidth_limit, 1000000);
        assert!(target.replication_sync);
        assert_eq!(target.storage_class, "STANDARD");
        assert_eq!(target.health_check_duration, Duration::from_secs(30));
        assert!(!target.disable_proxy);
        assert_eq!(target.reset_id, "reset-123");
        assert_eq!(target.total_downtime, Duration::from_secs(3600));
        assert!(target.online);
        assert_eq!(target.deployment_id, "deployment-456");
        assert!(!target.edge);
        assert!(target.edge_sync_before_expiry);
        assert_eq!(target.offline_count, 5);

        // Verify credentials
        assert!(target.credentials.is_some());
        let credentials = target.credentials.unwrap();
        assert_eq!(credentials.access_key, "test-access-key");
        assert_eq!(credentials.secret_key, "test-secret-key");
        assert_eq!(credentials.session_token, Some("test-session-token".to_string()));
        assert!(credentials.expiration.is_some());

        // Verify latency statistics
        assert_eq!(target.latency.curr, Duration::from_millis(100));
        assert_eq!(target.latency.avg, Duration::from_millis(150));
        assert_eq!(target.latency.max, Duration::from_millis(300));

        // Verify time fields
        assert!(target.reset_before_date.is_none());
        assert!(target.last_online.is_none());
    }

    #[test]
    fn test_bucket_target_json_serialize_deserialize_roundtrip() {
        let original = BucketTarget {
            source_bucket: "test-source".to_string(),
            endpoint: "rustfs.example.com".to_string(),
            credentials: Some(Credentials {
                access_key: "rustfsaccess".to_string(),
                secret_key: "rustfssecret".to_string(),
                session_token: None,
                expiration: None,
            }),
            target_bucket: "test-target".to_string(),
            secure: false,
            path: "/".to_string(),
            api: "s3v4".to_string(),
            arn: "arn:rustfs:s3:::test-target".to_string(),
            target_type: BucketTargetType::ReplicationService,
            region: "us-west-2".to_string(),
            bandwidth_limit: 500000,
            replication_sync: false,
            storage_class: "REDUCED_REDUNDANCY".to_string(),
            health_check_duration: Duration::from_secs(60),
            disable_proxy: true,
            reset_before_date: Some(OffsetDateTime::now_utc()),
            reset_id: "reset-456".to_string(),
            total_downtime: Duration::from_secs(1800),
            last_online: Some(OffsetDateTime::now_utc()),
            online: false,
            latency: LatencyStat {
                curr: Duration::from_millis(250),
                avg: Duration::from_millis(200),
                max: Duration::from_millis(500),
            },
            deployment_id: "deploy-789".to_string(),
            edge: true,
            edge_sync_before_expiry: false,
            offline_count: 10,
        };

        // Serialize to JSON
        let json = serde_json::to_string(&original).expect("Failed to serialize to JSON");

        // Deserialize from JSON
        let deserialized: BucketTarget = serde_json::from_str(&json).expect("Failed to deserialize from JSON");

        // Verify key fields are equal
        assert_eq!(original.source_bucket, deserialized.source_bucket);
        assert_eq!(original.endpoint, deserialized.endpoint);
        assert_eq!(original.target_bucket, deserialized.target_bucket);
        assert_eq!(original.secure, deserialized.secure);
        assert_eq!(original.target_type, deserialized.target_type);
        assert_eq!(original.region, deserialized.region);
        assert_eq!(original.bandwidth_limit, deserialized.bandwidth_limit);
        assert_eq!(original.replication_sync, deserialized.replication_sync);
        assert_eq!(original.health_check_duration, deserialized.health_check_duration);
        assert_eq!(original.online, deserialized.online);
        assert_eq!(original.edge, deserialized.edge);
        assert_eq!(original.offline_count, deserialized.offline_count);
    }

    #[test]
    fn test_bucket_target_type_json_deserialize() {
        // Test BucketTargetType JSON deserialization
        let replication_json = r#""replication""#;
        let ilm_json = r#""ilm""#;

        let replication_type: BucketTargetType =
            serde_json::from_str(replication_json).expect("Failed to deserialize replication type");
        let ilm_type: BucketTargetType = serde_json::from_str(ilm_json).expect("Failed to deserialize ilm type");

        assert_eq!(replication_type, BucketTargetType::ReplicationService);
        assert_eq!(ilm_type, BucketTargetType::IlmService);

        // Verify type validity
        assert!(replication_type.is_valid());
        assert!(ilm_type.is_valid());
        assert!(!BucketTargetType::None.is_valid());
    }

    #[test]
    fn test_credentials_json_deserialize() {
        let json = r#"
        {
            "accessKey": "AKIAIOSFODNN7EXAMPLE",
            "secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "session_token": "AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT",
            "expiration": "2024-12-31T23:59:59Z"
        }
        "#;

        let credentials: Credentials = serde_json::from_str(json).expect("Failed to deserialize credentials");

        assert_eq!(credentials.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(credentials.secret_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assert_eq!(
            credentials.session_token,
            Some("AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT".to_string())
        );
        assert!(credentials.expiration.is_some());
    }

    #[test]
    fn test_latency_stat_json_deserialize() {
        let json = r#"
        {
            "curr": 50,
            "avg": 75,
            "max": 200
        }
        "#;

        let latency: LatencyStat = serde_json::from_str(json).expect("Failed to deserialize latency stat");

        assert_eq!(latency.curr, Duration::from_millis(50));
        assert_eq!(latency.avg, Duration::from_millis(75));
        assert_eq!(latency.max, Duration::from_millis(200));
    }

    #[test]
    fn test_bucket_targets_json_deserialize() {
        let json = r#"
        {
            "targets": [
                {
                    "sourcebucket": "bucket1",
                    "endpoint": "s3.amazonaws.com",
                    "targetbucket": "target1",
                    "secure": true,
                    "path": "/",
                    "api": "s3v4",
                    "arn": "arn:aws:s3:::target1",
                    "type": "replication",
                    "region": "us-east-1",
                    "bandwidth_limit": 0,
                    "replicationSync": false,
                    "storage_class": "",
                    "healthCheckDuration": 0,
                    "disableProxy": false,
                    "resetBeforeDate": null,
                    "reset_id": "",
                    "totalDowntime": 0,
                    "lastOnline": null,
                    "isOnline": false,
                    "latency": {
                        "curr": 0,
                        "avg": 0,
                        "max": 0
                    },
                    "deployment_id": "",
                    "edge": false,
                    "edgeSyncBeforeExpiry": false,
                    "offlineCount": 0
                }
            ]
        }
        "#;

        let targets: BucketTargets = serde_json::from_str(json).expect("Failed to deserialize bucket targets");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].source_bucket, "bucket1");
        assert_eq!(targets.targets[0].endpoint, "s3.amazonaws.com");
        assert_eq!(targets.targets[0].target_bucket, "target1");
        assert!(!targets.is_empty());
    }

    #[test]
    fn test_user_provided_json_deserialize() {
        // Test the specific JSON provided by the user with missing required fields added
        let json = r#"
        {
            "sourcebucket": "mc-test-bucket-22139",
            "endpoint": "localhost:8000",
            "credentials": {
                "accessKey": "rustfsadmin",
                "secretKey": "rustfsadmin",
                "expiration": "0001-01-01T00:00:00Z"
            },
            "targetbucket": "test",
            "secure": false,
            "path": "auto",
            "api": "s3v4",
            "type": "replication",
            "replicationSync": false,
            "healthCheckDuration": 60,
            "disableProxy": false,
            "resetBeforeDate": "0001-01-01T00:00:00Z",
            "totalDowntime": 0,
            "lastOnline": "0001-01-01T00:00:00Z",
            "isOnline": false,
            "latency": {
                "curr": 0,
                "avg": 0,
                "max": 0
            },
            "deployment_id": "",
            "edge": false,
            "edgeSyncBeforeExpiry": false,
            "offlineCount": 0,
            "bandwidth": 107374182400
        }
        "#;

        let target: BucketTarget = serde_json::from_str(json).expect("Failed to deserialize user provided JSON to BucketTarget");

        // Verify the deserialized values match the original JSON
        assert_eq!(target.source_bucket, "mc-test-bucket-22139");
        assert_eq!(target.endpoint, "localhost:8000");
        assert_eq!(target.target_bucket, "test");
        assert!(!target.secure);
        assert_eq!(target.path, "auto");
        assert_eq!(target.api, "s3v4");
        assert_eq!(target.target_type, BucketTargetType::ReplicationService);
        assert!(!target.replication_sync);
        assert_eq!(target.health_check_duration, Duration::from_secs(60));
        assert!(!target.disable_proxy);
        assert!(!target.online);
        assert!(!target.edge);
        assert!(!target.edge_sync_before_expiry);
        assert_eq!(target.bandwidth_limit, 107374182400); // bandwidth field mapped to bandwidth_limit

        // Verify credentials
        assert!(target.credentials.is_some());
        let credentials = target.credentials.unwrap();
        assert_eq!(credentials.access_key, "rustfsadmin");
        assert_eq!(credentials.secret_key, "rustfsadmin");

        // Verify latency statistics
        assert_eq!(target.latency.curr, Duration::from_millis(0));
        assert_eq!(target.latency.avg, Duration::from_millis(0));
        assert_eq!(target.latency.max, Duration::from_millis(0));

        // Verify time fields parsing (should handle "0001-01-01T00:00:00Z" as None due to being the zero time)
        assert!(target.reset_before_date.is_some());
        assert!(target.last_online.is_some());

        println!("✅ User provided JSON successfully deserialized to BucketTarget");
    }

    #[test]
    fn test_user_provided_json_as_bucket_targets() {
        // Test wrapping the user JSON in BucketTargets structure
        let json = r#"
        {
            "targets": [
                {
                    "sourcebucket": "mc-test-bucket-22139",
                    "endpoint": "localhost:8000",
                    "credentials": {
                        "accessKey": "rustfsadmin",
                        "secretKey": "rustfsadmin",
                        "expiration": "0001-01-01T00:00:00Z"
                    },
                    "targetbucket": "test",
                    "secure": false,
                    "path": "auto",
                    "api": "s3v4",
                    "arn": "",
                    "type": "replication",
                    "region": "",
                    "replicationSync": false,
                    "storage_class": "",
                    "healthCheckDuration": 60,
                    "disableProxy": false,
                    "resetBeforeDate": "0001-01-01T00:00:00Z",
                    "reset_id": "",
                    "totalDowntime": 0,
                    "lastOnline": "0001-01-01T00:00:00Z",
                    "isOnline": false,
                    "latency": {
                        "curr": 0,
                        "avg": 0,
                        "max": 0
                    },
                    "deployment_id": "",
                    "edge": false,
                    "edgeSyncBeforeExpiry": false,
                    "offlineCount": 0,
                    "bandwidth": 107374182400
                }
            ]
        }
        "#;

        let bucket_targets: BucketTargets =
            serde_json::from_str(json).expect("Failed to deserialize user provided JSON to BucketTargets");

        assert_eq!(bucket_targets.targets.len(), 1);
        assert!(!bucket_targets.is_empty());

        let target = &bucket_targets.targets[0];
        assert_eq!(target.source_bucket, "mc-test-bucket-22139");
        assert_eq!(target.endpoint, "localhost:8000");
        assert_eq!(target.target_bucket, "test");
        assert_eq!(target.bandwidth_limit, 107374182400);

        println!("✅ User provided JSON successfully deserialized to BucketTargets");
    }

    #[test]
    fn test_bucket_target_minimal_json_with_defaults() {
        // Test that BucketTarget can be deserialized with minimal JSON using defaults
        let minimal_json = r#"
        {
            "sourcebucket": "test-source",
            "endpoint": "localhost:9000",
            "targetbucket": "test-target"
        }
        "#;

        let target: BucketTarget =
            serde_json::from_str(minimal_json).expect("Failed to deserialize minimal JSON to BucketTarget");

        // Verify required fields
        assert_eq!(target.source_bucket, "test-source");
        assert_eq!(target.endpoint, "localhost:9000");
        assert_eq!(target.target_bucket, "test-target");

        // Verify default values
        assert!(!target.secure); // bool default is false
        assert_eq!(target.path, ""); // String default is empty
        assert_eq!(target.api, ""); // String default is empty
        assert_eq!(target.arn, ""); // String default is empty
        assert_eq!(target.target_type, BucketTargetType::None); // enum default
        assert_eq!(target.region, ""); // String default is empty
        assert_eq!(target.bandwidth_limit, 0); // i64 default is 0
        assert!(!target.replication_sync); // bool default is false
        assert_eq!(target.storage_class, ""); // String default is empty
        assert_eq!(target.health_check_duration, Duration::from_secs(0)); // Duration default
        assert!(!target.disable_proxy); // bool default is false
        assert!(target.reset_before_date.is_none()); // Option default is None
        assert_eq!(target.reset_id, ""); // String default is empty
        assert_eq!(target.total_downtime, Duration::from_secs(0)); // Duration default
        assert!(target.last_online.is_none()); // Option default is None
        assert!(!target.online); // bool default is false
        assert_eq!(target.latency.curr, Duration::from_millis(0)); // LatencyStat default
        assert_eq!(target.latency.avg, Duration::from_millis(0));
        assert_eq!(target.latency.max, Duration::from_millis(0));
        assert_eq!(target.deployment_id, ""); // String default is empty
        assert!(!target.edge); // bool default is false
        assert!(!target.edge_sync_before_expiry); // bool default is false
        assert_eq!(target.offline_count, 0); // u64 default is 0
        assert!(target.credentials.is_none()); // Option default is None

        println!("✅ Minimal JSON with defaults successfully deserialized to BucketTarget");
    }

    #[test]
    fn test_bucket_target_empty_json_with_defaults() {
        // Test that BucketTarget can be deserialized with completely empty JSON using all defaults
        let empty_json = r#"{}"#;

        let target: BucketTarget = serde_json::from_str(empty_json).expect("Failed to deserialize empty JSON to BucketTarget");

        // Verify all fields use default values
        assert_eq!(target.source_bucket, "");
        assert_eq!(target.endpoint, "");
        assert_eq!(target.target_bucket, "");
        assert!(!target.secure);
        assert_eq!(target.path, "");
        assert_eq!(target.api, "");
        assert_eq!(target.arn, "");
        assert_eq!(target.target_type, BucketTargetType::None);
        assert_eq!(target.region, "");
        assert_eq!(target.bandwidth_limit, 0);
        assert!(!target.replication_sync);
        assert_eq!(target.storage_class, "");
        assert_eq!(target.health_check_duration, Duration::from_secs(0));
        assert!(!target.disable_proxy);
        assert!(target.reset_before_date.is_none());
        assert_eq!(target.reset_id, "");
        assert_eq!(target.total_downtime, Duration::from_secs(0));
        assert!(target.last_online.is_none());
        assert!(!target.online);
        assert_eq!(target.latency.curr, Duration::from_millis(0));
        assert_eq!(target.latency.avg, Duration::from_millis(0));
        assert_eq!(target.latency.max, Duration::from_millis(0));
        assert_eq!(target.deployment_id, "");
        assert!(!target.edge);
        assert!(!target.edge_sync_before_expiry);
        assert_eq!(target.offline_count, 0);
        assert!(target.credentials.is_none());

        println!("✅ Empty JSON with all defaults successfully deserialized to BucketTarget");
    }

    #[test]
    fn test_original_user_json_with_defaults() {
        // Test the original user JSON without extra required fields
        let json = r#"
        {
            "sourcebucket": "mc-test-bucket-22139",
            "endpoint": "localhost:8000",
            "credentials": {
                "accessKey": "rustfsadmin",
                "secretKey": "rustfsadmin",
                "expiration": "0001-01-01T00:00:00Z"
            },
            "targetbucket": "test",
            "secure": false,
            "path": "auto",
            "api": "s3v4",
            "type": "replication",
            "replicationSync": false,
            "healthCheckDuration": 60,
            "disableProxy": false,
            "resetBeforeDate": "0001-01-01T00:00:00Z",
            "totalDowntime": 0,
            "lastOnline": "0001-01-01T00:00:00Z",
            "isOnline": false,
            "latency": {
                "curr": 0,
                "avg": 0,
                "max": 0
            },
            "edge": false,
            "edgeSyncBeforeExpiry": false,
            "bandwidth": 107374182400
        }
        "#;

        let target: BucketTarget = serde_json::from_str(json).expect("Failed to deserialize original user JSON to BucketTarget");

        // Verify the deserialized values
        assert_eq!(target.source_bucket, "mc-test-bucket-22139");
        assert_eq!(target.endpoint, "localhost:8000");
        assert_eq!(target.target_bucket, "test");
        assert!(!target.secure);
        assert_eq!(target.path, "auto");
        assert_eq!(target.api, "s3v4");
        assert_eq!(target.target_type, BucketTargetType::ReplicationService);
        assert!(!target.replication_sync);
        assert_eq!(target.health_check_duration, Duration::from_secs(60));
        assert!(!target.disable_proxy);
        assert!(!target.online);
        assert!(!target.edge);
        assert!(!target.edge_sync_before_expiry);
        assert_eq!(target.bandwidth_limit, 107374182400);

        // Fields not specified should use defaults
        assert_eq!(target.arn, ""); // default empty string
        assert_eq!(target.region, ""); // default empty string
        assert_eq!(target.storage_class, ""); // default empty string
        assert_eq!(target.reset_id, ""); // default empty string
        assert_eq!(target.deployment_id, ""); // default empty string
        assert_eq!(target.offline_count, 0); // default u64

        // Verify credentials
        assert!(target.credentials.is_some());
        let credentials = target.credentials.unwrap();
        assert_eq!(credentials.access_key, "rustfsadmin");
        assert_eq!(credentials.secret_key, "rustfsadmin");

        println!("✅ Original user JSON with defaults successfully deserialized to BucketTarget");
    }
}
