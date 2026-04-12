use crate::dto::{Timestamp, TimestampFormat};

use arrayvec::ArrayVec;

pub const fn fmt_boolean(val: bool) -> &'static str {
    if val { "true" } else { "false" }
}

pub fn fmt_integer<T>(val: i32, f: impl FnOnce(&str) -> T) -> T {
    let mut buf = itoa::Buffer::new();
    f(buf.format(val))
}

pub fn fmt_long<T>(val: i64, f: impl FnOnce(&str) -> T) -> T {
    let mut buf = itoa::Buffer::new();
    f(buf.format(val))
}

pub fn fmt_usize<T>(val: usize, f: impl FnOnce(&str) -> T) -> T {
    let mut buf = itoa::Buffer::new();
    f(buf.format(val))
}

pub fn fmt_timestamp<T>(val: &Timestamp, fmt: TimestampFormat, f: impl FnOnce(&[u8]) -> T) -> T {
    let mut buf = ArrayVec::<u8, 32>::new();
    val.format(fmt, &mut buf).unwrap();
    f(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_boolean() {
        assert_eq!(fmt_boolean(true), "true");
        assert_eq!(fmt_boolean(false), "false");
    }

    #[test]
    fn test_fmt_integer() {
        assert_eq!(fmt_integer(0, str::to_owned), "0");
        assert_eq!(fmt_integer(42, str::to_owned), "42");
        assert_eq!(fmt_integer(-1, str::to_owned), "-1");
        assert_eq!(fmt_integer(i32::MAX, str::to_owned), i32::MAX.to_string());
        assert_eq!(fmt_integer(i32::MIN, str::to_owned), i32::MIN.to_string());
    }

    #[test]
    fn test_fmt_long() {
        assert_eq!(fmt_long(0, str::to_owned), "0");
        assert_eq!(fmt_long(100, str::to_owned), "100");
        assert_eq!(fmt_long(-999, str::to_owned), "-999");
        assert_eq!(fmt_long(i64::MAX, str::to_owned), i64::MAX.to_string());
        assert_eq!(fmt_long(i64::MIN, str::to_owned), i64::MIN.to_string());
    }

    #[test]
    fn test_fmt_usize() {
        assert_eq!(fmt_usize(0, str::to_owned), "0");
        assert_eq!(fmt_usize(12345, str::to_owned), "12345");
        assert_eq!(fmt_usize(usize::MAX, str::to_owned), usize::MAX.to_string());
    }

    #[test]
    fn test_fmt_timestamp() {
        let ts = Timestamp::parse(TimestampFormat::DateTime, "1985-04-12T23:20:50.520Z").unwrap();
        let result = fmt_timestamp(&ts, TimestampFormat::DateTime, |b| std::str::from_utf8(b).unwrap().to_owned());
        assert_eq!(result, "1985-04-12T23:20:50.520Z");
    }
}
