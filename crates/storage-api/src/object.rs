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

use time::OffsetDateTime;

#[derive(Debug, Default, Clone)]
pub struct HTTPPreconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

impl HTTPPreconditions {
    pub fn if_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_match.as_deref())
    }

    pub fn if_none_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_none_match.as_deref())
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectLockRetentionOptions {
    pub mode: Option<String>,
    pub retain_until: Option<OffsetDateTime>,
    pub bypass_governance: bool,
}

fn non_empty_condition_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_preconditions_ignore_empty_etag_headers() {
        let opts = HTTPPreconditions {
            if_match: Some("  ".to_owned()),
            if_none_match: Some(" * ".to_owned()),
            ..Default::default()
        };

        assert_eq!(opts.if_match_value(), None);
        assert_eq!(opts.if_none_match_value(), Some("*"));
    }

    #[test]
    fn object_lock_retention_defaults_preserve_false_bypass() {
        let opts = ObjectLockRetentionOptions::default();

        assert!(opts.mode.is_none());
        assert!(opts.retain_until.is_none());
        assert!(!opts.bypass_governance);
    }
}
