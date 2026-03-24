//! Validation API for S3 bucket names.
//!
//! The [`NameValidation`] trait lets users plug in a custom bucket name validator.
//! [`AwsNameValidation`] provides the standard AWS bucket naming rules.

/// Trait for validating S3 bucket names
///
/// Implementations should return `true` for valid names and `false` for invalid ones.
pub trait NameValidation: Send + Sync + 'static {
    /// Validate a bucket name
    fn validate_bucket_name(&self, name: &str) -> bool;
}

/// AWS-compliant name validation
#[derive(Debug, Clone, Default)]
pub struct AwsNameValidation {
    _priv: (),
}

impl AwsNameValidation {
    #[must_use]
    pub const fn new() -> Self {
        Self { _priv: () }
    }
}

impl NameValidation for AwsNameValidation {
    fn validate_bucket_name(&self, name: &str) -> bool {
        crate::path::check_bucket_name(name)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A name validation that allows any non-empty bucket name.
    /// This is for test only.
    #[derive(Debug, Clone, Default)]
    pub struct RelaxedNameValidation {
        _priv: (),
    }

    impl RelaxedNameValidation {
        #[must_use]
        pub const fn new() -> Self {
            Self { _priv: () }
        }
    }

    impl NameValidation for RelaxedNameValidation {
        fn validate_bucket_name(&self, name: &str) -> bool {
            !name.is_empty()
        }
    }

    #[test]
    fn test_default_validation() {
        let validator = AwsNameValidation::new();

        // Valid bucket names should pass
        assert!(validator.validate_bucket_name("valid-bucket"));
        assert!(validator.validate_bucket_name("my.example.bucket"));

        // Invalid bucket names should fail
        assert!(!validator.validate_bucket_name("InvalidBucket")); // Uppercase
        assert!(!validator.validate_bucket_name("invalid_bucket")); // Underscore
        assert!(!validator.validate_bucket_name("192.168.1.1")); // IP address

        assert!(!validator.validate_bucket_name("")); // Empty name should fail
    }

    #[test]
    fn test_relaxed_validation() {
        let validator = RelaxedNameValidation::new();

        // All bucket names should pass, even invalid ones
        assert!(validator.validate_bucket_name("valid-bucket"));
        assert!(validator.validate_bucket_name("InvalidBucket")); // Uppercase - allowed
        assert!(validator.validate_bucket_name("invalid_bucket")); // Underscore - allowed
        assert!(validator.validate_bucket_name("192.168.1.1")); // IP address - allowed
        assert!(validator.validate_bucket_name("xn--example")); // xn-- prefix - allowed
        assert!(validator.validate_bucket_name("ab")); // Too short - allowed
        assert!(validator.validate_bucket_name(&"a".repeat(100))); // Too long - allowed

        assert!(!validator.validate_bucket_name("")); // Empty name should fail
    }
}
