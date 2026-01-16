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

use s3s::dto::{Date, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description};

const _ERR_MALFORMED_BUCKET_OBJECT_CONFIG: &str = "invalid bucket object lock config";
const _ERR_INVALID_RETENTION_DATE: &str = "date must be provided in ISO 8601 format";
const _ERR_PAST_OBJECTLOCK_RETAIN_DATE: &str = "the retain until date must be in the future";
const _ERR_UNKNOWN_WORMMODE_DIRECTIVE: &str = "unknown WORM mode directive";
const _ERR_OBJECTLOCK_MISSING_CONTENT_MD5: &str =
    "content-MD5 HTTP header is required for Put Object requests with Object Lock parameters";
const _ERR_OBJECTLOCK_INVALID_HEADERS: &str =
    "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied";
const _ERR_MALFORMED_XML: &str = "the XML you provided was not well-formed or did not validate against our published schema";

pub fn utc_now_ntp() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

pub fn get_object_retention_meta(meta: HashMap<String, String>) -> ObjectLockRetention {
    let mut retain_until_date: Date = Date::from(OffsetDateTime::UNIX_EPOCH);

    let mut mode_str = meta.get(X_AMZ_OBJECT_LOCK_MODE.as_str().to_lowercase().as_str());
    if mode_str.is_none() {
        mode_str = Some(&meta[X_AMZ_OBJECT_LOCK_MODE.as_str()]);
    }
    let mode = if let Some(mode_str) = mode_str {
        parse_ret_mode(mode_str.as_str())
    } else {
        return ObjectLockRetention {
            mode: None,
            retain_until_date: None,
        };
    };

    let mut till_str = meta.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_lowercase().as_str());
    if till_str.is_none() {
        till_str = Some(&meta[X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str()]);
    }
    if let Some(till_str) = till_str {
        let t = OffsetDateTime::parse(till_str, &format_description::well_known::Iso8601::DEFAULT);
        if let Ok(parsed_time) = t {
            retain_until_date = Date::from(parsed_time);
        }
    }
    ObjectLockRetention {
        mode: Some(mode),
        retain_until_date: Some(retain_until_date),
    }
}

pub fn get_object_legalhold_meta(meta: HashMap<String, String>) -> ObjectLockLegalHold {
    let mut hold_str = meta.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_lowercase().as_str());
    if hold_str.is_none() {
        hold_str = Some(&meta[X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()]);
    }
    if let Some(hold_str) = hold_str {
        return ObjectLockLegalHold {
            status: Some(parse_legalhold_status(hold_str)),
        };
    }
    ObjectLockLegalHold { status: None }
}

pub fn parse_ret_mode(mode_str: &str) -> ObjectLockRetentionMode {
    match mode_str.to_uppercase().as_str() {
        "GOVERNANCE" => ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE),
        "COMPLIANCE" => ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE),
        _ => unreachable!(),
    }
}

pub fn parse_legalhold_status(hold_str: &str) -> ObjectLockLegalHoldStatus {
    match hold_str {
        "ON" => ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON),
        "OFF" => ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF),
        _ => unreachable!(),
    }
}
