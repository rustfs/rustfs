use jiff::{Timestamp, ZonedDateTime, tz::TimeZone};

pub(crate) trait Rfc3339 {
    fn to_rfc3339(&self) -> String;
    fn format(&self, pattern: &str) -> String;
}

impl Rfc3339 for ZonedDateTime {
    fn to_rfc3339(&self) -> String {
        format_rfc3339(self)
    }

    fn format(&self, pattern: &str) -> String {
        self.strftime(pattern).to_string()
    }
}

pub(crate) fn parse_rfc3339(value: &str) -> Result<ZonedDateTime, jiff::Error> {
    if let Some(prefix) = value.strip_suffix('Z') {
        let normalized = format!("{prefix}+00:00");
        return ZonedDateTime::strptime("%Y-%m-%dT%H:%M:%S%.f%:z", normalized);
    }
    ZonedDateTime::strptime("%Y-%m-%dT%H:%M:%S%.f%:z", value)
}

pub(crate) fn format_rfc3339(value: &ZonedDateTime) -> String {
    value.strftime("%Y-%m-%dT%H:%M:%S%.f%:z").to_string()
}

pub(crate) fn utc_from_unix(seconds: i64) -> ZonedDateTime {
    Timestamp::new(seconds, 0)
        .expect("unix timestamp must fit jiff")
        .to_zoned_datetime(TimeZone::UTC)
}

pub(crate) fn offset_seconds(value: &ZonedDateTime) -> i32 {
    value.offset().seconds()
}
