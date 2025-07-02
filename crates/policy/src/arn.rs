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
use regex::Regex;

const ARN_PREFIX_ARN: &str = "arn";
const ARN_PARTITION_RUSTFS: &str = "rustfs";
const ARN_SERVICE_IAM: &str = "iam";
const ARN_RESOURCE_TYPE_ROLE: &str = "role";

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ARN {
    pub partition: String,
    pub service: String,
    pub region: String,
    pub resource_type: String,
    pub resource_id: String,
}

impl ARN {
    pub fn new_iam_role_arn(resource_id: &str, server_region: &str) -> Result<Self> {
        let valid_resource_id_regex = Regex::new(r"^[A-Za-z0-9_/\.-]+$")?;
        if !valid_resource_id_regex.is_match(resource_id) {
            return Err(Error::other("ARN resource ID invalid"));
        }
        Ok(ARN {
            partition: ARN_PARTITION_RUSTFS.to_string(),
            service: ARN_SERVICE_IAM.to_string(),
            region: server_region.to_string(),
            resource_type: ARN_RESOURCE_TYPE_ROLE.to_string(),
            resource_id: resource_id.to_string(),
        })
    }

    pub fn parse(arn_str: &str) -> Result<Self> {
        let ps: Vec<&str> = arn_str.split(':').collect();
        if ps.len() != 6 || ps[0] != ARN_PREFIX_ARN {
            return Err(Error::other("ARN format invalid"));
        }

        if ps[1] != ARN_PARTITION_RUSTFS {
            return Err(Error::other("ARN partition invalid"));
        }

        if ps[2] != ARN_SERVICE_IAM {
            return Err(Error::other("ARN service invalid"));
        }

        if !ps[4].is_empty() {
            return Err(Error::other("ARN account-id invalid"));
        }

        let res: Vec<&str> = ps[5].splitn(2, '/').collect();
        if res.len() != 2 {
            return Err(Error::other("ARN resource invalid"));
        }

        if res[0] != ARN_RESOURCE_TYPE_ROLE {
            return Err(Error::other("ARN resource type invalid"));
        }

        let valid_resource_id_regex = Regex::new(r"^[A-Za-z0-9_/\.-]+$")?;
        if !valid_resource_id_regex.is_match(res[1]) {
            return Err(Error::other("ARN resource ID invalid"));
        }

        Ok(ARN {
            partition: ARN_PARTITION_RUSTFS.to_string(),
            service: ARN_SERVICE_IAM.to_string(),
            region: ps[3].to_string(),
            resource_type: ARN_RESOURCE_TYPE_ROLE.to_string(),
            resource_id: res[1].to_string(),
        })
    }
}

impl std::fmt::Display for ARN {
    #[allow(clippy::write_literal)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}:{}/{}",
            ARN_PREFIX_ARN,
            self.partition,
            self.service,
            self.region,
            "", // account-id is always empty in this implementation
            self.resource_type,
            self.resource_id
        )
    }
}
