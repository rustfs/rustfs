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

use futures::Stream;
use hyper::http;
use std::{
    io::ErrorKind,
    pin::Pin,
    sync::LazyLock,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Interval, MissedTickBehavior, interval};

pub const MAX_RETRY: i64 = 10;
pub const MAX_JITTER: f64 = 1.0;
pub const NO_JITTER: f64 = 0.0;

pub const DEFAULT_RETRY_UNIT: Duration = Duration::from_millis(200);
pub const DEFAULT_RETRY_CAP: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct RetryTimer {
    base_sleep: Duration,
    max_sleep: Duration,
    jitter: f64,
    random: u64,
    max_retry: i64,
    rem: i64,
    timer: Option<Interval>,
}

impl RetryTimer {
    pub fn new(max_retry: i64, base_sleep: Duration, max_sleep: Duration, jitter: f64, random: u64) -> Self {
        Self {
            base_sleep,
            max_sleep,
            jitter,
            random,
            max_retry,
            rem: max_retry,
            timer: None,
        }
    }
}

impl Stream for RetryTimer {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        if self.rem == 0 {
            return Poll::Ready(None);
        }

        let jitter = self.jitter.clamp(NO_JITTER, MAX_JITTER);
        let attempt = self.max_retry - self.rem;
        let mut sleep = self.base_sleep * (1 << attempt);
        if sleep > self.max_sleep {
            sleep = self.max_sleep;
        }
        if (jitter - NO_JITTER).abs() > 1e-9 {
            let sleep_ms = sleep.as_millis();
            let reduction = ((sleep_ms as f64) * (self.random as f64 * jitter / 100_f64)).round() as u128;
            let jittered_ms = sleep_ms.saturating_sub(reduction);
            let clamped_ms = std::cmp::min(jittered_ms.max(1), u128::from(u64::MAX));
            sleep = Duration::from_millis(clamped_ms as u64);
        }
        //println!("sleep: {sleep:?}");

        if self.timer.is_none() {
            let mut timer = interval(sleep);
            timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
            self.timer = Some(timer);
        }

        let mut timer = self.timer.as_mut().expect("operation should succeed");
        match Pin::new(&mut timer).poll_tick(cx) {
            Poll::Ready(_) => {
                self.rem -= 1;
                if self.rem > 0 {
                    let mut new_timer = interval(sleep);
                    new_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    new_timer.reset();
                    self.timer = Some(new_timer);
                }
                Poll::Ready(Some(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Drives `operation` with capped, jittered exponential backoff, returning the
/// first success or the last error once `max_attempts` attempts are exhausted.
///
/// The sleep before retry `n` (1-based) is `min(base_delay * 2^(n-1), max_delay)`,
/// reduced by up to half through a cheap clock-derived jitter so concurrent
/// retriers decorrelate — the same backoff shape as [`RetryTimer`] without
/// needing a caller-supplied random seed or the Stream API. `max_attempts` is
/// clamped to at least 1.
pub async fn retry_with_backoff<F, Fut, T, E>(
    mut operation: F,
    max_attempts: usize,
    base_delay: Duration,
    max_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let max_attempts = max_attempts.max(1);
    let mut last_err = None;

    for attempt in 0..max_attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_err = Some(err);
                if attempt + 1 < max_attempts {
                    // Cap the shift so the multiplier cannot overflow; the cap
                    // below bounds the result anyway.
                    let exp = base_delay.saturating_mul(1u32 << attempt.min(16));
                    let mut sleep_duration = exp.min(max_delay);
                    // Up to 50% reduction, derived from the clock's sub-second
                    // nanoseconds — cheap decorrelation without a rand dependency.
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos())
                        .unwrap_or(0);
                    let reduction_percent = u64::from(nanos % 50);
                    let sleep_ms = sleep_duration.as_millis() as u64;
                    let jittered_ms = sleep_ms.saturating_sub(sleep_ms * reduction_percent / 100).max(1);
                    sleep_duration = Duration::from_millis(jittered_ms);
                    tokio::time::sleep(sleep_duration).await;
                }
            }
        }
    }

    Err(last_err.expect("max_attempts is clamped to at least 1, so at least one attempt ran"))
}

static RETRYABLE_S3CODES: LazyLock<Vec<String>> = LazyLock::new(|| {
    vec![
        "RequestError".to_string(),
        "RequestTimeout".to_string(),
        "Throttling".to_string(),
        "ThrottlingException".to_string(),
        "RequestLimitExceeded".to_string(),
        "RequestThrottled".to_string(),
        "InternalError".to_string(),
        "ExpiredToken".to_string(),
        "ExpiredTokenException".to_string(),
        "SlowDown".to_string(),
    ]
});

static RETRYABLE_HTTP_STATUSCODES: LazyLock<Vec<http::StatusCode>> = LazyLock::new(|| {
    vec![
        http::StatusCode::REQUEST_TIMEOUT,
        http::StatusCode::TOO_MANY_REQUESTS,
        //499,
        http::StatusCode::INTERNAL_SERVER_ERROR,
        http::StatusCode::BAD_GATEWAY,
        http::StatusCode::SERVICE_UNAVAILABLE,
        http::StatusCode::GATEWAY_TIMEOUT,
        //520,
    ]
});

pub fn is_s3code_retryable(s3code: &str) -> bool {
    RETRYABLE_S3CODES.contains(&s3code.to_string())
}

/// Like is_s3code_retryable but matches by substring containment on
/// the supplied message. Use this when only the rendered error string
/// is available (for example, inside protocol drivers that consume
/// StorageBackend::Error: Display) rather than a parsed S3 error code.
pub fn is_s3code_in_message_retryable(message: &str) -> bool {
    RETRYABLE_S3CODES.iter().any(|code| message.contains(code))
}

pub fn is_http_status_retryable(http_statuscode: &http::StatusCode) -> bool {
    RETRYABLE_HTTP_STATUSCODES.contains(http_statuscode)
}

pub fn is_request_error_retryable(_err: std::io::Error) -> bool {
    /*if err == Err::Canceled || err == Err::DeadlineExceeded {
        return err() == nil;
    }
    let uerr = err.(*url.Error);
    if uerr.is_ok() {
        let e = uerr.expect("operation should succeed");
        return match e.type {
            x509.UnknownAuthorityError => {
                false
            }
            _ => true,
        };
        return match e.error() {
            "http: server gave HTTP response to HTTPS client" => {
                false
            }
            _ => rue,
        };
    }
    true*/
    matches!(
        _err.kind(),
        ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::TimedOut
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::NotConnected
            | ErrorKind::UnexpectedEof
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn retry_timer_yields_expected_number_of_retries() {
        let max_retry = 3;
        let retry_timer = RetryTimer::new(max_retry, Duration::from_millis(1), Duration::from_millis(2), NO_JITTER, 0);

        let retries = timeout(Duration::from_secs(1), retry_timer.collect::<Vec<_>>())
            .await
            .expect("retry timer should complete")
            .len();

        assert_eq!(retries, max_retry as usize);
    }

    #[tokio::test]
    async fn retry_timer_finishes_immediately_when_retry_count_is_zero() {
        let mut retry_timer = RetryTimer::new(0, Duration::from_millis(1), Duration::from_millis(2), NO_JITTER, 0);

        assert_eq!(retry_timer.next().await, None);
    }

    #[test]
    fn is_s3code_in_message_retryable_matches_each_retryable_code() {
        for code in [
            "RequestError",
            "RequestTimeout",
            "Throttling",
            "ThrottlingException",
            "RequestLimitExceeded",
            "RequestThrottled",
            "InternalError",
            "ExpiredToken",
            "ExpiredTokenException",
            "SlowDown",
        ] {
            assert!(is_s3code_in_message_retryable(code), "bare code {code} must be classified retryable");
        }
    }

    #[test]
    fn is_s3code_in_message_retryable_matches_substring_in_longer_message() {
        assert!(is_s3code_in_message_retryable("S3Error: SlowDown please retry"));
        assert!(is_s3code_in_message_retryable("aws-sdk error code=Throttling status=503"));
    }

    #[test]
    fn is_s3code_in_message_retryable_rejects_terminal_codes() {
        assert!(!is_s3code_in_message_retryable("AccessDenied"));
        assert!(!is_s3code_in_message_retryable("NoSuchBucket: bucket-name"));
        assert!(!is_s3code_in_message_retryable("InvalidArgument: key"));
    }

    #[test]
    fn is_s3code_in_message_retryable_rejects_empty_string() {
        assert!(!is_s3code_in_message_retryable(""));
    }

    #[tokio::test]
    async fn retry_with_backoff_returns_first_success_without_retrying() {
        let mut calls = 0;
        let result: Result<i32, std::io::Error> = retry_with_backoff(
            || {
                calls += 1;
                async { Ok(42) }
            },
            3,
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .await;

        assert_eq!(result.expect("first attempt succeeds"), 42);
        assert_eq!(calls, 1, "a success must not trigger further attempts");
    }

    #[tokio::test]
    async fn retry_with_backoff_retries_until_success() {
        let mut calls = 0;
        let result: Result<i32, std::io::Error> = retry_with_backoff(
            || {
                calls += 1;
                let attempt = calls;
                async move {
                    if attempt < 3 {
                        Err(std::io::Error::other("transient"))
                    } else {
                        Ok(7)
                    }
                }
            },
            5,
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .await;

        assert_eq!(result.expect("third attempt succeeds"), 7);
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn retry_with_backoff_exhausts_attempts_and_returns_last_error() {
        let mut calls = 0;
        let result: Result<(), std::io::Error> = retry_with_backoff(
            || {
                calls += 1;
                let attempt = calls;
                async move { Err(std::io::Error::other(format!("attempt {attempt}"))) }
            },
            3,
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .await;

        let err = result.expect_err("all attempts fail");
        assert_eq!(err.to_string(), "attempt 3", "the LAST error must be returned");
        assert_eq!(calls, 3);
    }

    #[tokio::test]
    async fn retry_with_backoff_clamps_zero_attempts_to_one() {
        let mut calls = 0;
        let result: Result<(), std::io::Error> = retry_with_backoff(
            || {
                calls += 1;
                async { Err(std::io::Error::other("always")) }
            },
            0,
            Duration::from_millis(1),
            Duration::from_millis(2),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(calls, 1, "zero attempts clamps to a single attempt instead of panicking");
    }

    #[test]
    fn is_s3code_in_message_retryable_is_case_sensitive() {
        // Pin the contract: a backend that down-cases its error
        // strings would not be classified retryable. If a future
        // backend needs case-insensitive matching, change the helper
        // and update this test in the same change.
        assert!(!is_s3code_in_message_retryable("slowdown"));
        assert!(!is_s3code_in_message_retryable("THROTTLING"));
    }
}
