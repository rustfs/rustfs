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

use super::BucketTargetType;
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
        write!(f, "arn:rustfs:{}:{}:{}:{}", self.arn_type, self.region, self.id, self.bucket)
    }
}

impl FromStr for ARN {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with("arn:rustfs:") {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid ARN format"));
        }

        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 6 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid ARN format"));
        }
        Ok(ARN {
            arn_type: BucketTargetType::from_str(parts[2]).unwrap_or_default(),
            id: parts[3].to_string(),
            region: parts[4].to_string(),
            bucket: parts[5].to_string(),
        })
    }
}
