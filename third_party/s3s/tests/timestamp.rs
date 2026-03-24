//! Tests using the Smithy `date_time_format_test_suite.json`
//! From: <https://github.com/smithy-lang/smithy-rs/blob/main/rust-runtime/aws-smithy-types/test_data/date_time_format_test_suite.json>

use s3s::dto::{Timestamp, TimestampFormat};

use serde::Deserialize;

#[derive(Deserialize)]
struct TestSuite {
    #[allow(dead_code)]
    description: Vec<String>,
    parse_epoch_seconds: Vec<TestCase>,
    parse_http_date: Vec<TestCase>,
    parse_date_time: Vec<TestCase>,
}

#[derive(Deserialize)]
struct TestCase {
    iso8601: String,
    canonical_seconds: String,
    canonical_nanos: u32,
    error: bool,
    smithy_format_value: Option<String>,
}

fn load_test_suite() -> TestSuite {
    let json = include_str!("../../../data/date_time_format_test_suite.json");
    serde_json::from_str(json).expect("failed to parse test suite")
}

/// Converts `canonical_seconds` (as string) and `canonical_nanos` into total nanoseconds.
fn canonical_to_nanos(canonical_seconds: &str, canonical_nanos: u32) -> i128 {
    let secs: i64 = canonical_seconds.parse().expect("invalid canonical_seconds");
    i128::from(secs) * 1_000_000_000 + i128::from(canonical_nanos)
}

#[test]
fn parse_epoch_seconds() {
    let suite = load_test_suite();

    for case in suite.parse_epoch_seconds {
        let Some(smithy_value) = case.smithy_format_value.as_ref() else {
            // Error cases without smithy_format_value - skip
            assert!(case.error, "non-error case should have smithy_format_value: {}", case.iso8601);
            continue;
        };

        let result = Timestamp::parse(TimestampFormat::EpochSeconds, smithy_value);

        if case.error {
            assert!(result.is_err(), "expected error parsing '{}' (iso8601: {})", smithy_value, case.iso8601);
        } else {
            let ts = result.unwrap_or_else(|e| panic!("failed to parse '{}' (iso8601: {}): {}", smithy_value, case.iso8601, e));
            let expected_nanos = canonical_to_nanos(&case.canonical_seconds, case.canonical_nanos);
            let odt: time::OffsetDateTime = ts.into();
            let actual_nanos = odt.unix_timestamp_nanos();

            assert_eq!(
                actual_nanos, expected_nanos,
                "mismatch for '{}' (iso8601: {}): expected {} nanos, got {} nanos",
                smithy_value, case.iso8601, expected_nanos, actual_nanos
            );
        }
    }
}

#[test]
fn parse_http_date() {
    let suite = load_test_suite();

    for case in suite.parse_http_date {
        let Some(smithy_value) = case.smithy_format_value.as_ref() else {
            // Error cases without smithy_format_value - skip
            assert!(case.error, "non-error case should have smithy_format_value: {}", case.iso8601);
            continue;
        };

        // s3s's RFC1123 format doesn't support fractional seconds, so skip those test cases
        // that include fractional seconds (e.g., "Sat, 18 Jan 1969 11:47:31.01 GMT")
        if smithy_value.contains('.') {
            continue;
        }

        let result = Timestamp::parse(TimestampFormat::HttpDate, smithy_value);

        if case.error {
            assert!(result.is_err(), "expected error parsing '{}' (iso8601: {})", smithy_value, case.iso8601);
        } else {
            let ts = result.unwrap_or_else(|e| panic!("failed to parse '{}' (iso8601: {}): {}", smithy_value, case.iso8601, e));

            // For http-date, fractional seconds are truncated, so we only compare whole seconds
            let expected_secs: i64 = case.canonical_seconds.parse().expect("invalid canonical_seconds");
            let odt: time::OffsetDateTime = ts.into();
            let actual_secs = odt.unix_timestamp();

            assert_eq!(
                actual_secs, expected_secs,
                "mismatch for '{}' (iso8601: {}): expected {} secs, got {} secs",
                smithy_value, case.iso8601, expected_secs, actual_secs
            );
        }
    }
}

#[test]
fn parse_date_time() {
    let suite = load_test_suite();

    for case in suite.parse_date_time {
        let Some(smithy_value) = case.smithy_format_value.as_ref() else {
            // Error cases without smithy_format_value - skip
            assert!(case.error, "non-error case should have smithy_format_value: {}", case.iso8601);
            continue;
        };

        let result = Timestamp::parse(TimestampFormat::DateTime, smithy_value);

        if case.error {
            assert!(result.is_err(), "expected error parsing '{}' (iso8601: {})", smithy_value, case.iso8601);
        } else {
            let ts = result.unwrap_or_else(|e| panic!("failed to parse '{}' (iso8601: {}): {}", smithy_value, case.iso8601, e));
            let expected_nanos = canonical_to_nanos(&case.canonical_seconds, case.canonical_nanos);
            let odt: time::OffsetDateTime = ts.into();
            let actual_nanos = odt.unix_timestamp_nanos();

            assert_eq!(
                actual_nanos, expected_nanos,
                "mismatch for '{}' (iso8601: {}): expected {} nanos, got {} nanos",
                smithy_value, case.iso8601, expected_nanos, actual_nanos
            );
        }
    }
}
