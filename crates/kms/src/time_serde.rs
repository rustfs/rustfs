use jiff::{Timestamp, Zoned, tz::TimeZone};
use serde::{Deserialize, Deserializer, Serializer};

pub(crate) mod zoned {
    use super::*;

    pub(crate) fn serialize<S>(value: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_zoned_compat(&value).map_err(serde::de::Error::custom)
    }
}

pub(crate) mod option_zoned {
    use super::*;

    pub(crate) fn serialize<S>(value: &Option<Zoned>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(value) => serializer.serialize_some(&value.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Zoned>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| parse_zoned_compat(&value).map_err(serde::de::Error::custom))
            .transpose()
    }
}

fn parse_zoned_compat(value: &str) -> Result<Zoned, String> {
    if let Ok(zoned) = value.parse::<Zoned>() {
        return Ok(zoned);
    }

    let timestamp = value
        .parse::<Timestamp>()
        .map_err(|err| format!("failed to parse legacy timestamp '{value}': {err}"))?;
    Ok(timestamp.to_zoned(TimeZone::UTC))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_zoned_compat_accepts_current_zoned_format() {
        let zoned = parse_zoned_compat("2024-01-01T00:00:00+00:00[UTC]").expect("current format should parse");
        assert_eq!(zoned.time_zone().iana_name(), Some("UTC"));
    }

    #[test]
    fn parse_zoned_compat_accepts_legacy_rfc3339_format() {
        let zoned = parse_zoned_compat("2024-01-01T00:00:00+00:00").expect("legacy format should parse");
        assert_eq!(zoned.time_zone().iana_name(), Some("UTC"));
    }
}
