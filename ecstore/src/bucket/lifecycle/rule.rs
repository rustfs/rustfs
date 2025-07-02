#![allow(unused_imports)]
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

#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use s3s::dto::{LifecycleRuleFilter, Transition};

const _ERR_TRANSITION_INVALID_DAYS: &str = "Days must be 0 or greater when used with Transition";
const _ERR_TRANSITION_INVALID_DATE: &str = "Date must be provided in ISO 8601 format";
const ERR_TRANSITION_INVALID: &str =
    "Exactly one of Days (0 or greater) or Date (positive ISO 8601 format) should be present in Transition.";
const _ERR_TRANSITION_DATE_NOT_MIDNIGHT: &str = "'Date' must be at midnight GMT";

pub trait Filter {
    fn test_tags(&self, user_tags: &str) -> bool;
    fn by_size(&self, sz: i64) -> bool;
}

impl Filter for LifecycleRuleFilter {
    fn test_tags(&self, user_tags: &str) -> bool {
        true
    }

    fn by_size(&self, sz: i64) -> bool {
        true
    }
}

pub trait TransitionOps {
    fn validate(&self) -> Result<(), std::io::Error>;
}

impl TransitionOps for Transition {
    fn validate(&self) -> Result<(), std::io::Error> {
        if !self.date.is_none() && self.days.expect("err!") > 0 {
            return Err(std::io::Error::other(ERR_TRANSITION_INVALID));
        }

        if self.storage_class.is_none() {
            return Err(std::io::Error::other("ERR_XML_NOT_WELL_FORMED"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_rule() {
        //assert!(skip_access_checks(p.to_str().unwrap()));
    }
}
