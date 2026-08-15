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

use crate::bucket::target::BucketTargetType;
use std::fmt::Display;
use std::str::FromStr;

pub struct ARN {
    pub arn_type: BucketTargetType,
    pub id: String,
    pub region: String,
    pub bucket: String,
}

impl ARN {
    pub fn new(arn_type: BucketTargetType, id: String, region: String, bucket: String) -> Self {
        Self {
            arn_type,
            id,
            region,
            bucket,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.arn_type.is_valid()
    }
}

impl Display for ARN {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The `minio` partition is deliberate: madmin-go's ParseARN
        // hard-rejects any other partition, so native mc/madmin tooling can
        // only decode remote-target ARNs minted in this form (backlog#1675
        // P1-7). Legacy `arn:rustfs:` ARNs persisted by older releases stay
        // readable via the FromStr whitelist below; runtime matching between
        // targets and replication rules is by full-string equality, so mixed
        // partitions coexist safely.
        write!(f, "arn:minio:{}:{}:{}:{}", self.arn_type, self.region, self.id, self.bucket)
    }
}

impl FromStr for ARN {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Partition whitelist, not just an `arn:` check: `BucketTargetType::
        // from_str(...).unwrap_or_default()` below never fails, so this is
        // the only structural gate rejecting foreign ARNs. `arn:rustfs:` is
        // the legacy partition and must stay accepted forever (persisted
        // bucket-targets.json / replication configs from older releases).
        if !s.starts_with("arn:minio:") && !s.starts_with("arn:rustfs:") {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid ARN format"));
        }

        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 6 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid ARN format"));
        }
        // Display emits `arn:rustfs:{type}:{region}:{id}:{bucket}`; read the
        // segments back in the same order so parse(display(a)) == a.
        Ok(ARN {
            arn_type: BucketTargetType::from_str(parts[2]).unwrap_or_default(),
            region: parts[3].to_string(),
            id: parts[4].to_string(),
            bucket: parts[5].to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Display emits `arn:rustfs:{type}:{region}:{id}:{bucket}` (madmin layout);
    /// FromStr must read the same positions back so parse(display(a)) == a.
    #[test]
    fn from_str_round_trips_display_with_region_and_id() {
        let arn = ARN::new(
            BucketTargetType::ReplicationService,
            "depl-123".to_string(),
            "us-east-1".to_string(),
            "bucket-a".to_string(),
        );

        let parsed = ARN::from_str(&arn.to_string()).expect("display output must parse");

        assert_eq!(parsed.arn_type, arn.arn_type);
        assert_eq!(parsed.region, arn.region, "region must survive display->parse round-trip");
        assert_eq!(parsed.id, arn.id, "id must survive display->parse round-trip");
        assert_eq!(parsed.bucket, arn.bucket);
    }

    #[test]
    fn from_str_reads_region_then_id_in_display_order() {
        let parsed = ARN::from_str("arn:rustfs:replication:us-east-1:depl-123:bucket-a").expect("valid ARN must parse");

        assert_eq!(parsed.arn_type, BucketTargetType::ReplicationService);
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.id, "depl-123");
        assert_eq!(parsed.bucket, "bucket-a");
    }

    /// RustFS commonly generates ARNs with an empty region:
    /// `arn:minio:replication::<deployment_id>:<bucket>`.
    #[test]
    fn from_str_handles_empty_region_segment() {
        let parsed = ARN::from_str("arn:minio:replication::depl-123:bucket-a").expect("valid ARN must parse");

        assert_eq!(parsed.arn_type, BucketTargetType::ReplicationService);
        assert_eq!(parsed.region, "", "region segment is empty in this form");
        assert_eq!(parsed.id, "depl-123");
        assert_eq!(parsed.bucket, "bucket-a");
    }

    /// madmin-go's `ParseARN` hard-rejects anything that does not start with
    /// `arn:minio:`, so generated ARNs must use the `minio` partition or the
    /// native mc/madmin tooling cannot decode remote-target listings.
    #[test]
    fn display_emits_minio_partition() {
        let arn = ARN::new(
            BucketTargetType::ReplicationService,
            "depl-123".to_string(),
            String::new(),
            "bucket-a".to_string(),
        );

        assert_eq!(arn.to_string(), "arn:minio:replication::depl-123:bucket-a");
    }

    /// Persisted bucket-targets.json files from older RustFS releases carry
    /// `arn:rustfs:` ARNs; the legacy partition must stay parseable forever.
    #[test]
    fn from_str_accepts_legacy_rustfs_partition() {
        let parsed = ARN::from_str("arn:rustfs:replication:us-east-1:depl-123:bucket-a").expect("legacy ARN must parse");

        assert_eq!(parsed.arn_type, BucketTargetType::ReplicationService);
        assert_eq!(parsed.region, "us-east-1");
        assert_eq!(parsed.id, "depl-123");
        assert_eq!(parsed.bucket, "bucket-a");
    }

    /// The partition whitelist is the only structural gate: `BucketTargetType::
    /// from_str(...).unwrap_or_default()` never fails, so any 6-segment string
    /// would otherwise parse as `type=None`.
    #[test]
    fn from_str_rejects_unknown_partition() {
        assert!(ARN::from_str("arn:aws:replication::depl-123:bucket-a").is_err());
        assert!(ARN::from_str("not-an-arn").is_err());
    }
}
