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

//! Serde helpers for IAM timestamps: serialize as RFC3339 (MinIO-compatible),
//! deserialize from RFC3339 or legacy RustFS human-readable format.

use serde::{Deserialize, Deserializer, Serializer};
use time::OffsetDateTime;
use time::format_description;
use time::format_description::well_known::Rfc3339;

/// Legacy RustFS format: `YYYY-MM-DD HH:MM:SS.ffffff +00:00:00` (time crate serde-human-readable style).
static LEGACY_FORMAT: std::sync::OnceLock<time::format_description::OwnedFormatItem> = std::sync::OnceLock::new();

fn legacy_format() -> &'static time::format_description::OwnedFormatItem {
    LEGACY_FORMAT.get_or_init(|| {
        format_description::parse_owned::<2>(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
        )
        .expect("legacy format description is valid")
    });
    LEGACY_FORMAT.get().expect("initialized above")
}

fn parse_rfc3339_or_legacy(s: &str) -> Result<OffsetDateTime, time::Error> {
    OffsetDateTime::parse(s, &Rfc3339).or_else(|_| OffsetDateTime::parse(s, legacy_format()).map_err(Into::into))
}

/// Serialize as RFC3339; deserialize from RFC3339 or legacy RustFS format.
pub fn serialize<S>(dt: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    time::serde::rfc3339::serialize(dt, serializer)
}

/// Deserialize from RFC3339 or legacy RustFS human-readable format.
pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <&str>::deserialize(deserializer)?;
    parse_rfc3339_or_legacy(s).map_err(serde::de::Error::custom)
}

/// Option version: serialize as RFC3339; deserialize from RFC3339 or legacy.
pub mod option {
    use serde::{Deserialize, Deserializer, Serializer};
    use time::OffsetDateTime;

    use super::{Rfc3339, parse_rfc3339_or_legacy};

    pub fn serialize<S>(opt: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt {
            Some(dt) => {
                let s = dt.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
                serializer.serialize_some(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<&str> = Option::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(s) => parse_rfc3339_or_legacy(s).map(Some).map_err(serde::de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    struct Dt(#[serde(with = "crate::serde_datetime")] OffsetDateTime);

    #[test]
    fn test_deserialize_legacy_rustfs_timestamp() {
        // Legacy RustFS human-readable format (time serde-human-readable).
        let json = r#""2026-03-09 02:22:44.998954 +00:00:00""#;
        let Dt(dt) = serde_json::from_str(json).expect("deserialize legacy timestamp");
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), time::Month::March);
        assert_eq!(dt.day(), 9);
        assert_eq!(dt.hour(), 2);
        assert_eq!(dt.minute(), 22);
        assert_eq!(dt.second(), 44);
    }

    #[test]
    fn test_deserialize_rfc3339_timestamp() {
        let json = r#""2025-03-07T12:00:00Z""#;
        let Dt(dt) = serde_json::from_str(json).expect("deserialize RFC3339");
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), time::Month::March);
        assert_eq!(dt.day(), 7);
        assert_eq!(dt.hour(), 12);
    }
}
