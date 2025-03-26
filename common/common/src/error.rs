use tracing_error::{SpanTrace, SpanTraceStatus};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Error {
    inner: Box<dyn std::error::Error + Send + Sync + 'static>,
    span_trace: SpanTrace,
}

impl Error {
    /// Create a new error from a `std::error::Error`.
    #[must_use]
    #[track_caller]
    pub fn new<T: std::error::Error + Send + Sync + 'static>(source: T) -> Self {
        Self::from_std_error(source.into())
    }

    /// Create a new error from a `std::error::Error`.
    #[must_use]
    #[track_caller]
    pub fn from_std_error(inner: StdError) -> Self {
        Self {
            inner,
            span_trace: SpanTrace::capture(),
        }
    }

    /// Create a new error from a string.
    #[must_use]
    #[track_caller]
    pub fn from_string(s: impl Into<String>) -> Self {
        Self::msg(s)
    }

    /// Create a new error from a string.
    #[must_use]
    #[track_caller]
    pub fn msg(s: impl Into<String>) -> Self {
        Self::from_std_error(s.into().into())
    }

    /// Returns `true` if the inner type is the same as `T`.
    #[inline]
    pub fn is<T: std::error::Error + 'static>(&self) -> bool {
        self.inner.is::<T>()
    }

    /// Returns some reference to the inner value if it is of type `T`, or
    /// `None` if it isn't.
    #[inline]
    pub fn downcast_ref<T: std::error::Error + 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref()
    }

    /// Returns some mutable reference to the inner value if it is of type `T`, or
    /// `None` if it isn't.
    #[inline]
    pub fn downcast_mut<T: std::error::Error + 'static>(&mut self) -> Option<&mut T> {
        self.inner.downcast_mut()
    }

    pub fn to_io_err(&self) -> Option<std::io::Error> {
        self.downcast_ref::<std::io::Error>()
            .map(|e| std::io::Error::new(e.kind(), e.to_string()))
    }
}

impl<T: std::error::Error + Send + Sync + 'static> From<T> for Error {
    fn from(e: T) -> Self {
        Self::new(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)?;

        if self.span_trace.status() != SpanTraceStatus::EMPTY {
            write!(f, "\nspan_trace:\n{}", self.span_trace)?;
        }

        Ok(())
    }
}
