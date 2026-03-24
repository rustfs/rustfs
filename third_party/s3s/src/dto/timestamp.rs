//! timestamp

use std::io;
use std::num::ParseIntError;
use std::time::SystemTime;

use time::format_description::FormatItem;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(time::OffsetDateTime);

impl serde::Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let mut buf = Vec::new();
        self.format(TimestampFormat::DateTime, &mut buf).map_err(S::Error::custom)?;
        let s = std::str::from_utf8(&buf).map_err(S::Error::custom)?;
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        Self::parse(TimestampFormat::DateTime, &s).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampFormat {
    DateTime,
    HttpDate,
    EpochSeconds,
}

impl From<time::OffsetDateTime> for Timestamp {
    fn from(value: time::OffsetDateTime) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for time::OffsetDateTime {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl From<SystemTime> for Timestamp {
    fn from(value: SystemTime) -> Self {
        Self(time::OffsetDateTime::from(value))
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self(time::OffsetDateTime::UNIX_EPOCH)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTimestampError {
    #[error("time: {0}")]
    Time(#[from] time::error::Parse),
    #[error("int: {0}")]
    Int(#[from] ParseIntError),
    #[error("time overflow")]
    Overflow,
    #[error("component range: {0}")]
    ComponentRange(#[from] time::error::ComponentRange),
}

#[derive(Debug, thiserror::Error)]
pub enum FormatTimestampError {
    #[error("time: {0}")]
    Time(#[from] time::error::Format),
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// See <https://github.com/time-rs/time/issues/498>
const RFC1123: &[FormatItem<'_>] =
    format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");

/// See <https://github.com/minio/minio-java/issues/1419>
const RFC3339: &[FormatItem<'_>] = format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");

impl Timestamp {
    /// Parses `Timestamp` from string
    ///
    /// # Errors
    /// Returns an error if the string is invalid
    pub fn parse(format: TimestampFormat, s: &str) -> Result<Self, ParseTimestampError> {
        let ans = match format {
            TimestampFormat::DateTime => time::OffsetDateTime::parse(s, &Rfc3339)?,
            TimestampFormat::HttpDate => time::PrimitiveDateTime::parse(s, RFC1123)?.assume_utc(),
            TimestampFormat::EpochSeconds => match s.split_once('.') {
                Some((secs, frac)) => {
                    let secs: i64 = secs.parse()?;
                    let val: u32 = frac.parse::<u32>()?;
                    let mul: u32 = match frac.len() {
                        1 => 100_000_000,
                        2 => 10_000_000,
                        3 => 1_000_000,
                        4 => 100_000,
                        5 => 10000,
                        6 => 1000,
                        7 => 100,
                        8 => 10,
                        9 => 1,
                        _ => return Err(ParseTimestampError::Overflow),
                    };
                    let nanos_part = i128::from(val * mul);
                    // In Smithy epoch-seconds format, the fractional part is always positive.
                    // For example, "-1.5" means -1 seconds + 0.5 fractional = -0.5 seconds total.
                    let nanos = i128::from(secs) * 1_000_000_000 + nanos_part;
                    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)?
                }
                None => {
                    let secs: i64 = s.parse()?;
                    time::OffsetDateTime::from_unix_timestamp(secs)?
                }
            },
        };
        Ok(Self(ans))
    }

    /// Formats `Timestamp` into a writer
    ///
    /// # Errors
    /// Returns an error if the formatting fails
    pub fn format(&self, format: TimestampFormat, w: &mut impl io::Write) -> Result<(), FormatTimestampError> {
        match format {
            TimestampFormat::DateTime => {
                self.0.format_into(w, RFC3339)?;
            }
            TimestampFormat::HttpDate => {
                self.0.format_into(w, RFC1123)?;
            }
            TimestampFormat::EpochSeconds => {
                let val = self.0.unix_timestamp_nanos();

                #[allow(clippy::cast_precision_loss)] // FIXME: accurate conversion?
                {
                    let secs = (val / 1_000_000_000) as f64;
                    let nanos = (val % 1_000_000_000) as f64 / 1_000_000_000.0;
                    let ts = secs + nanos;
                    write!(w, "{ts}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unix_epoch() {
        let ts = Timestamp::default();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt, time::OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn text_repr() {
        let cases = [
            (TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z"),
            (TimestampFormat::HttpDate, "Tue, 29 Apr 2014 18:30:38 GMT"),
            (TimestampFormat::HttpDate, "Wed, 21 Oct 2015 07:28:00 GMT"),
            // (TimestampFormat::HttpDate, "Sun, 02 Jan 2000 20:34:56.000 GMT"), // FIXME: optional fractional seconds
            (TimestampFormat::EpochSeconds, "1515531081.1234"),
        ];

        for (fmt, expected) in cases {
            let time = Timestamp::parse(fmt, expected).unwrap();

            let mut buf = Vec::new();
            time.format(fmt, &mut buf).unwrap();
            let text = String::from_utf8(buf).unwrap();

            assert_eq!(expected, text);
        }
    }

    #[test]
    fn parse_epoch_seconds_integer() {
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "0").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt, time::OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn parse_epoch_seconds_negative() {
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "-1").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.unix_timestamp(), -1);
    }

    #[test]
    fn parse_epoch_seconds_fractional_lengths() {
        // 1 digit fractional
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.5").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.unix_timestamp(), 100);
        assert_eq!(dt.nanosecond(), 500_000_000);

        // 3 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.123").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_000_000);

        // 6 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.123456").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_456_000);

        // 9 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.123456789").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_456_789);
    }

    #[test]
    fn parse_epoch_seconds_overflow_fractional() {
        // 10 digits should fail
        let result = Timestamp::parse(TimestampFormat::EpochSeconds, "100.1234567890");
        assert!(result.is_err());
    }

    #[test]
    fn parse_datetime_invalid() {
        let result = Timestamp::parse(TimestampFormat::DateTime, "not-a-date");
        assert!(result.is_err());
    }

    #[test]
    fn parse_http_date_invalid() {
        let result = Timestamp::parse(TimestampFormat::HttpDate, "not-a-date");
        assert!(result.is_err());
    }

    #[test]
    fn parse_epoch_seconds_invalid() {
        let result = Timestamp::parse(TimestampFormat::EpochSeconds, "abc");
        assert!(result.is_err());
    }

    #[test]
    fn from_system_time() {
        let st = SystemTime::UNIX_EPOCH;
        let ts = Timestamp::from(st);
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt, time::OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn from_offset_datetime() {
        let odt = time::OffsetDateTime::UNIX_EPOCH;
        let ts = Timestamp::from(odt);
        let back: time::OffsetDateTime = ts.into();
        assert_eq!(back, time::OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn serde_roundtrip() {
        let ts = Timestamp::parse(TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z").unwrap();
        let json = serde_json::to_string(&ts).unwrap();
        assert!(json.contains("1985-04-12"));
        let parsed: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, parsed);
    }

    #[test]
    fn serde_deserialize_invalid() {
        let result: Result<Timestamp, _> = serde_json::from_str("\"not-a-date\"");
        assert!(result.is_err());
    }

    #[test]
    fn format_epoch_seconds() {
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "0").unwrap();
        let mut buf = Vec::new();
        ts.format(TimestampFormat::EpochSeconds, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text, "0");
    }

    #[test]
    fn timestamp_ord() {
        let ts1 = Timestamp::parse(TimestampFormat::EpochSeconds, "100").unwrap();
        let ts2 = Timestamp::parse(TimestampFormat::EpochSeconds, "200").unwrap();
        assert!(ts1 < ts2);
        assert_eq!(ts1, ts1.clone());
    }

    #[test]
    fn timestamp_hash() {
        use std::collections::HashSet;
        let ts1 = Timestamp::parse(TimestampFormat::EpochSeconds, "100").unwrap();
        let ts2 = Timestamp::parse(TimestampFormat::EpochSeconds, "100").unwrap();
        let mut set = HashSet::new();
        set.insert(ts1);
        set.insert(ts2);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn parse_epoch_seconds_all_frac_lengths() {
        // 2 digit fractional
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.12").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 120_000_000);

        // 4 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.1234").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_400_000);

        // 5 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.12345").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_450_000);

        // 7 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.1234567").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_456_700);

        // 8 digits
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "100.12345678").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        assert_eq!(dt.nanosecond(), 123_456_780);
    }

    #[test]
    fn parse_epoch_negative_with_frac() {
        // "-1.5" means -1 seconds + 0.5 fractional = -0.5 seconds
        let ts = Timestamp::parse(TimestampFormat::EpochSeconds, "-1.5").unwrap();
        let dt: time::OffsetDateTime = ts.into();
        let nanos = dt.unix_timestamp_nanos();
        assert_eq!(nanos, -500_000_000);
    }
}
