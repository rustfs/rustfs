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

    pub fn inner_string(&self) -> String {
        self.inner.to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[derive(Debug)]
    struct CustomTestError {
        message: String,
    }

    impl std::fmt::Display for CustomTestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Custom test error: {}", self.message)
        }
    }

    impl std::error::Error for CustomTestError {}

    #[derive(Debug)]
    struct AnotherTestError;

    impl std::fmt::Display for AnotherTestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Another test error")
        }
    }

    impl std::error::Error for AnotherTestError {}

    #[test]
    fn test_error_new_from_std_error() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
        let error = Error::new(io_error);

        assert!(error.inner_string().contains("File not found"));
        assert!(error.is::<io::Error>());
    }

    #[test]
    fn test_error_from_std_error() {
        let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
        let boxed_error: StdError = Box::new(io_error);
        let error = Error::from_std_error(boxed_error);

        assert!(error.inner_string().contains("Permission denied"));
        assert!(error.is::<io::Error>());
    }

    #[test]
    fn test_error_from_string() {
        let error = Error::from_string("Test error message");
        assert_eq!(error.inner_string(), "Test error message");
    }

    #[test]
    fn test_error_msg() {
        let error = Error::msg("Another test message");
        assert_eq!(error.inner_string(), "Another test message");
    }

    #[test]
    fn test_error_msg_with_string() {
        let message = String::from("String message");
        let error = Error::msg(message);
        assert_eq!(error.inner_string(), "String message");
    }

    #[test]
    fn test_error_is_type_checking() {
        let io_error = io::Error::new(io::ErrorKind::InvalidInput, "Invalid input");
        let error = Error::new(io_error);

        assert!(error.is::<io::Error>());
        assert!(!error.is::<CustomTestError>());
    }

    #[test]
    fn test_error_downcast_ref() {
        let io_error = io::Error::new(io::ErrorKind::TimedOut, "Operation timed out");
        let error = Error::new(io_error);

        let downcast_io = error.downcast_ref::<io::Error>();
        assert!(downcast_io.is_some());
        assert_eq!(downcast_io.unwrap().kind(), io::ErrorKind::TimedOut);

        let downcast_custom = error.downcast_ref::<CustomTestError>();
        assert!(downcast_custom.is_none());
    }

    #[test]
    fn test_error_downcast_mut() {
        let io_error = io::Error::new(io::ErrorKind::Interrupted, "Operation interrupted");
        let mut error = Error::new(io_error);

        let downcast_io = error.downcast_mut::<io::Error>();
        assert!(downcast_io.is_some());
        assert_eq!(downcast_io.unwrap().kind(), io::ErrorKind::Interrupted);

        let downcast_custom = error.downcast_mut::<CustomTestError>();
        assert!(downcast_custom.is_none());
    }

    #[test]
    fn test_error_to_io_err() {
        // Test with IO error
        let original_io_error = io::Error::new(io::ErrorKind::BrokenPipe, "Broken pipe");
        let error = Error::new(original_io_error);

        let converted_io_error = error.to_io_err();
        assert!(converted_io_error.is_some());
        let io_err = converted_io_error.unwrap();
        assert_eq!(io_err.kind(), io::ErrorKind::BrokenPipe);
        assert!(io_err.to_string().contains("Broken pipe"));

        // Test with non-IO error
        let custom_error = CustomTestError {
            message: "Not an IO error".to_string(),
        };
        let error = Error::new(custom_error);

        let converted_io_error = error.to_io_err();
        assert!(converted_io_error.is_none());
    }

    #[test]
    fn test_error_inner_string() {
        let custom_error = CustomTestError {
            message: "Test message".to_string(),
        };
        let error = Error::new(custom_error);

        assert_eq!(error.inner_string(), "Custom test error: Test message");
    }

    #[test]
    fn test_error_from_trait() {
        let io_error = io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF");
        let error: Error = io_error.into();

        assert!(error.is::<io::Error>());
        assert!(error.inner_string().contains("Unexpected EOF"));
    }

    #[test]
    fn test_error_display() {
        let custom_error = CustomTestError {
            message: "Display test".to_string(),
        };
        let error = Error::new(custom_error);

        let display_string = format!("{}", error);
        assert!(display_string.contains("Custom test error: Display test"));
    }

    #[test]
    fn test_error_debug() {
        let error = Error::msg("Debug test");
        let debug_string = format!("{:?}", error);

        assert!(debug_string.contains("Error"));
        assert!(debug_string.contains("inner"));
        assert!(debug_string.contains("span_trace"));
    }

    #[test]
    fn test_multiple_error_types() {
        let errors = vec![
            Error::new(io::Error::new(io::ErrorKind::NotFound, "Not found")),
            Error::new(CustomTestError { message: "Custom".to_string() }),
            Error::new(AnotherTestError),
            Error::msg("String error"),
        ];

        assert!(errors[0].is::<io::Error>());
        assert!(errors[1].is::<CustomTestError>());
        assert!(errors[2].is::<AnotherTestError>());
        assert!(!errors[3].is::<io::Error>());
    }

    #[test]
    fn test_error_chain_compatibility() {
        // Test that our Error type works well with error chains
        let io_error = io::Error::new(io::ErrorKind::InvalidData, "Invalid data");
        let error = Error::new(io_error);

        // Should be able to convert back to Result
        let result: Result<(), Error> = Err(error);
        assert!(result.is_err());

        // Test the error from the result
        if let Err(err) = result {
            assert!(err.is::<io::Error>());
        }
    }

    #[test]
    fn test_result_type_alias() {
        // Test the Result type alias
        fn test_function() -> Result<String> {
            Ok("Success".to_string())
        }

        fn test_function_with_error() -> Result<String> {
            Err(Error::msg("Test error"))
        }

        let success_result = test_function();
        assert!(success_result.is_ok());
        assert_eq!(success_result.unwrap(), "Success");

        let error_result = test_function_with_error();
        assert!(error_result.is_err());
        assert_eq!(error_result.unwrap_err().inner_string(), "Test error");
    }

    #[test]
    fn test_error_with_empty_message() {
        let error = Error::msg("");
        assert_eq!(error.inner_string(), "");
    }

    #[test]
    fn test_error_with_unicode_message() {
        let unicode_message = "错误信息 🚨 Error message with émojis and ñon-ASCII";
        let error = Error::msg(unicode_message);
        assert_eq!(error.inner_string(), unicode_message);
    }

    #[test]
    fn test_error_with_very_long_message() {
        let long_message = "A".repeat(10000);
        let error = Error::msg(&long_message);
        assert_eq!(error.inner_string(), long_message);
    }

    #[test]
    fn test_span_trace_capture() {
        // Test that span trace is captured (though we can't easily test the content)
        let error = Error::msg("Span trace test");
        let display_string = format!("{}", error);

        // The error should at least contain the message
        assert!(display_string.contains("Span trace test"));
    }
}
