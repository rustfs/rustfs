use std::time::Duration;

// MaxRetry is the maximum number of retries before stopping.
pub const MAX_RETRY: i64 = 10;

// MaxJitter will randomize over the full exponential backoff time
pub const MAX_JITTER: f64 = 1.0;

// NoJitter disables the use of jitter for randomizing the exponential backoff time
pub const NO_JITTER: f64 = 0.0;

// DefaultRetryUnit - default unit multiplicative per retry.
// defaults to 200 * time.Millisecond
//const DefaultRetryUnit = 200 * time.Millisecond;

// DefaultRetryCap - Each retry attempt never waits no longer than
// this maximum time duration.
//const DefaultRetryCap = time.Second;

/*
struct Delay {
    when: Instant,
}

impl Future for Delay {
    type Output = &'static str;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<&'static str>
    {
        if Instant::now() >= self.when {
            println!("Hello world");
            Poll::Ready("done")
        } else {
            // Ignore this line for now.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct RetryTimer {
    rem: usize,
    delay: Delay,
}

impl RetryTimer {
    fn new() -> Self {
        Self {
            rem: 3,
            delay: Delay { when: Instant::now() }
        }
    }
}

impl Stream for RetryTimer {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Option<()>>
    {
        if self.rem == 0 {
            // No more delays
            return Poll::Ready(None);
        }

        match Pin::new(&mut self.delay).poll(cx) {
            Poll::Ready(_) => {
                let when = self.delay.when + Duration::from_millis(10);
                self.delay = Delay { when };
                self.rem -= 1;
                Poll::Ready(Some(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}*/

pub fn new_retry_timer(max_retry: i32, base_sleep: Duration, max_sleep: Duration, jitter: f64) -> Vec<i32> {
    /*attemptCh := make(chan int)

    exponentialBackoffWait := func(attempt int) time.Duration {
      // normalize jitter to the range [0, 1.0]
      if jitter < NoJitter {
        jitter = NoJitter
      }
      if jitter > MaxJitter {
        jitter = MaxJitter
      }

      // sleep = random_between(0, min(maxSleep, base * 2 ** attempt))
      sleep := baseSleep * time.Duration(1<<uint(attempt))
      if sleep > maxSleep {
        sleep = maxSleep
      }
      if jitter != NoJitter {
        sleep -= time.Duration(c.random.Float64() * float64(sleep) * jitter)
      }
      return sleep
    }

    go func() {
      defer close(attemptCh)
      for i := 0; i < maxRetry; i++ {
        select {
        case attemptCh <- i + 1:
        case <-ctx.Done():
          return
        }

        select {
        case <-time.After(exponentialBackoffWait(i)):
        case <-ctx.Done():
          return
        }
      }
    }()
    return attemptCh*/
    todo!();
}

/*var retryableS3Codes = map[string]struct{}{
  "RequestError":          {},
  "RequestTimeout":        {},
  "Throttling":            {},
  "ThrottlingException":   {},
  "RequestLimitExceeded":  {},
  "RequestThrottled":      {},
  "InternalError":         {},
  "ExpiredToken":          {},
  "ExpiredTokenException": {},
  "SlowDown":              {},
}

fn isS3CodeRetryable(s3Code string) (ok bool) {
  _, ok = retryableS3Codes[s3Code]
  return ok
}

var retryableHTTPStatusCodes = map[int]struct{}{
  http.StatusRequestTimeout:      {},
  429:                            {}, // http.StatusTooManyRequests is not part of the Go 1.5 library, yet
  499:                            {}, // client closed request, retry. A non-standard status code introduced by nginx.
  http.StatusInternalServerError: {},
  http.StatusBadGateway:          {},
  http.StatusServiceUnavailable:  {},
  http.StatusGatewayTimeout:      {},
  520:                            {}, // It is used by Cloudflare as a catch-all response for when the origin server sends something unexpected.
  // Add more HTTP status codes here.
}

fn isHTTPStatusRetryable(httpStatusCode int) (ok bool) {
  _, ok = retryableHTTPStatusCodes[httpStatusCode]
  return ok
}

fn isRequestErrorRetryable(ctx context.Context, err error) bool {
  if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
    // Retry if internal timeout in the HTTP call.
    return ctx.Err() == nil
  }
  if ue, ok := err.(*url.Error); ok {
    e := ue.Unwrap()
    switch e.(type) {
    // x509: certificate signed by unknown authority
    case x509.UnknownAuthorityError:
      return false
    }
    switch e.Error() {
    case "http: server gave HTTP response to HTTPS client":
      return false
    }
  }
  return true
}*/
