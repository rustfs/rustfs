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

        let mut timer = self.timer.as_mut().unwrap();
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

pub fn is_http_status_retryable(http_statuscode: &http::StatusCode) -> bool {
    RETRYABLE_HTTP_STATUSCODES.contains(http_statuscode)
}

pub fn is_request_error_retryable(_err: std::io::Error) -> bool {
    /*if err == Err::Canceled || err == Err::DeadlineExceeded {
        return err() == nil;
    }
    let uerr = err.(*url.Error);
    if uerr.is_ok() {
        let e = uerr.unwrap();
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
    todo!();
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use rand::RngExt;
    use std::time::UNIX_EPOCH;

    #[tokio::test]
    async fn test_retry() {
        let req_retry = 10;
        let random = rand::rng().random_range(0..=100);

        let mut retry_timer = RetryTimer::new(req_retry, DEFAULT_RETRY_UNIT, DEFAULT_RETRY_CAP, MAX_JITTER, random);
        println!("retry_timer: {retry_timer:?}");
        while retry_timer.next().await.is_some() {
            println!(
                "\ntime: {:?}",
                std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
            );
        }
    }
}
