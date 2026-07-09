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

use std::collections::HashMap;

use s3s::dto::ObjectLockRetentionMode;
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use time::{OffsetDateTime, format_description};

pub fn is_object_locked_by_metadata(user_defined: &HashMap<String, String>, is_delete_marker: bool) -> bool {
    if is_delete_marker {
        return false;
    }

    if user_defined
        .get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("ON"))
    {
        return true;
    }

    let Some(mode) = user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str()) else {
        return false;
    };
    if !is_retention_mode(mode) {
        return false;
    }

    user_defined
        .get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
        .and_then(|value| OffsetDateTime::parse(value, &format_description::well_known::Iso8601::DEFAULT).ok())
        .is_some_and(|retain_until| retain_until.unix_timestamp() > OffsetDateTime::now_utc().unix_timestamp())
}

fn is_retention_mode(mode: &str) -> bool {
    mode.eq_ignore_ascii_case(ObjectLockRetentionMode::COMPLIANCE)
        || mode.eq_ignore_ascii_case(ObjectLockRetentionMode::GOVERNANCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_object_locked_by_metadata_preserves_object_lock_parser_behavior() {
        let mut user_defined = HashMap::new();
        user_defined.insert(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "ON".to_string());

        assert!(is_object_locked_by_metadata(&user_defined, false));
        assert!(!is_object_locked_by_metadata(&user_defined, true));
    }
}
