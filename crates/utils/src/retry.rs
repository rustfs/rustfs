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
    task::{Context, Poll},
    time::{Duration, Instant},
    sync::LazyLock,
};
use tokio::time::interval;

pub const MAX_RETRY: i64 = 10;
pub const MAX_JITTER: f64 = 1.0;
pub const NO_JITTER: f64 = 0.0;

pub const DEFAULT_RETRY_UNIT: Duration = Duration::from_millis(200);
pub const DEFAULT_RETRY_CAP: Duration = Duration::from_secs(1);

pub struct RetryTimer {
    base_sleep: Duration,
    max_sleep: Duration,
    jitter: f64,
    random: u64,
    rem: i64,
}

impl RetryTimer {
    pub fn new(max_retry: i64, base_sleep: Duration, max_sleep: Duration, jitter: f64, random: u64) -> Self {
        Self {
            base_sleep,
            max_sleep,
            jitter,
            random,
            rem: max_retry,
        }
    }
}

impl Stream for RetryTimer {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let jitter = self.jitter.clamp(NO_JITTER, MAX_JITTER);

        let attempt = MAX_RETRY - self.rem;
        let mut sleep = self.base_sleep * (1 << attempt);
        if sleep > self.max_sleep {
            sleep = self.max_sleep;
        }
        if (jitter - NO_JITTER).abs() > 1e-9 {
            sleep -= sleep * self.random as u32 * jitter as u32;
        }

        if self.rem == 0 {
            return Poll::Ready(None);
        }

        self.rem -= 1;
        let mut t = interval(sleep);
        match t.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(())),
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
    /*if err == Err::Canceled) || err == Err::DeadlineExceeded) {
        return ctx.Err() == nil;
    }
    let ue = err.(*url.Error);
    if ue.is_ok() {
        let e = ue.Unwrap();
        switch e.(type) {
        case x509.UnknownAuthorityError:
            return false;
        }
        switch e.Error() {
        case "http: server gave HTTP response to HTTPS client":
            return false;
        }
    }
    true*/
    todo!();
}
