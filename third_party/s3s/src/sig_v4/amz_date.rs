//! x-amz-date

use std::fmt::Write as _;

use arrayvec::ArrayString;

/// x-amz-date
#[derive(Debug, Clone)]
pub struct AmzDate {
    /// year
    year: u16,
    /// month
    month: u8,
    /// day
    day: u8,
    /// hour
    hour: u8,
    /// minute
    minute: u8,
    /// second
    second: u8,
}

/// [`AmzDate`]
#[derive(Debug, thiserror::Error)]
#[error("ParseAmzDateError")]
pub struct ParseAmzDateError(());

impl AmzDate {
    /// Parses `AmzDate` from header
    /// # Errors
    /// Returns an error if the header is invalid
    pub fn parse(header: &str) -> Result<Self, ParseAmzDateError> {
        self::parser::parse(header).map_err(|_| ParseAmzDateError(()))
    }

    /// `{YYYY}{MM}{DD}T{HH}{MM}{SS}Z`
    #[must_use]
    pub fn fmt_iso8601(&self) -> ArrayString<16> {
        let mut buf = <ArrayString<16>>::new();
        let (y, m, d, hh, mm, ss) = (self.year, self.month, self.day, self.hour, self.minute, self.second);
        write!(&mut buf, "{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z").unwrap();
        buf
    }

    /// `{YYYY}{MM}{DD}`
    #[must_use]
    pub fn fmt_date(&self) -> ArrayString<8> {
        let mut buf = <ArrayString<8>>::new();
        write!(&mut buf, "{:04}{:02}{:02}", self.year, self.month, self.day).unwrap();
        buf
    }

    pub fn to_time(&self) -> Option<time::OffsetDateTime> {
        let y = i32::from(self.year);
        let m: time::Month = self.month.try_into().ok()?;
        let d = self.day;

        let t = time::Date::from_calendar_date(y, m, d).ok()?;
        let t = t.with_hms(self.hour, self.minute, self.second).ok()?;
        Some(t.assume_utc())
    }
}

mod parser {
    use super::*;

    use crate::utils::parser::{Error, digit2, digit4};

    macro_rules! ensure {
        ($cond:expr) => {
            if !$cond {
                return Err(Error);
            }
        };
    }

    pub fn parse(input: &str) -> Result<AmzDate, Error> {
        let x = input.as_bytes();
        ensure!(x.len() == 16);

        let year = digit4([x[0], x[1], x[2], x[3]])?;
        let month = digit2([x[4], x[5]])?;
        let day = digit2([x[6], x[7]])?;
        ensure!(x[8] == b'T');

        let hour = digit2([x[9], x[10]])?;
        let minute = digit2([x[11], x[12]])?;
        let second = digit2([x[13], x[14]])?;
        ensure!(x[15] == b'Z');

        Ok(AmzDate {
            year,
            month,
            day,
            hour,
            minute,
            second,
        })
    }
}
