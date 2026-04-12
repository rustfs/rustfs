//! Virtual-host parsing for S3 request routing.
//!
//! This module provides the [`S3Host`] trait together with the built-in
//! implementations [`SingleDomain`] and [`MultiDomain`]. They parse the HTTP
//! `Host` header into a [`VirtualHost`] value that carries the base domain,
//! the bucket name (when the request uses virtual-hosted-style addressing),
//! and an optional region.

use crate::error::S3Result;

use std::borrow::Cow;

use stdx::default::default;

#[derive(Debug, Clone)]
pub struct VirtualHost<'a> {
    domain: Cow<'a, str>,
    bucket: Option<Cow<'a, str>>,
    region: Option<Cow<'a, str>>,
}

impl<'a> VirtualHost<'a> {
    pub fn new(domain: impl Into<Cow<'a, str>>) -> Self {
        Self {
            domain: domain.into(),
            bucket: None,
            region: None,
        }
    }

    /// Sets the bucket name for this virtual host.
    ///
    /// This method follows the builder pattern and returns `self` for method chaining.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3s::host::VirtualHost;
    ///
    /// let vh = VirtualHost::new("example.com")
    ///     .with_bucket("my-bucket");
    ///
    /// assert_eq!(vh.bucket(), Some("my-bucket"));
    /// ```
    #[must_use]
    pub fn with_bucket(mut self, bucket: impl Into<Cow<'a, str>>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    /// Sets the AWS region for this virtual host.
    ///
    /// This method follows the builder pattern and returns `self` for method chaining.
    /// The region represents the AWS region where the S3 bucket is located.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3s::host::VirtualHost;
    ///
    /// let vh = VirtualHost::new("example.com")
    ///     .with_bucket("my-bucket")
    ///     .with_region("us-west-2");
    ///
    /// assert_eq!(vh.region(), Some("us-west-2"));
    /// ```
    #[must_use]
    pub fn with_region(mut self, region: impl Into<Cow<'a, str>>) -> Self {
        self.region = Some(region.into());
        self
    }

    #[inline]
    #[must_use]
    pub fn domain(&self) -> &str {
        self.domain.as_ref()
    }

    #[inline]
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// Returns the AWS region associated with this virtual host, if set.
    ///
    /// # Returns
    ///
    /// - `Some(&str)` - The region name if it was set using `with_region()`
    /// - `None` - If no region was specified
    #[inline]
    #[must_use]
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }
}

pub trait S3Host: Send + Sync + 'static {
    /// Parses the `Host` header of the HTTP request.
    ///
    /// # Errors
    /// Returns an error if the `Host` is invalid for this service.
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("The domain is invalid")]
    InvalidDomain,

    #[error("Some subdomains overlap with each other")]
    OverlappingSubdomains,

    #[error("No base domains are specified")]
    ZeroDomains,
}

/// Naive check for a valid domain.
fn is_valid_domain(mut s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    if let Some((host, port)) = s.split_once(':') {
        if port.is_empty() {
            return false;
        }

        if port.parse::<u16>().is_err() {
            return false;
        }

        s = host;
    }

    for part in s.split('.') {
        if part.is_empty() {
            return false;
        }

        if part.as_bytes().iter().any(|&b| !b.is_ascii_alphanumeric() && b != b'-') {
            return false;
        }
    }

    true
}

fn parse_host_header<'a>(base_domain: &'a str, host: &'a str) -> Option<VirtualHost<'a>> {
    if host == base_domain {
        return Some(VirtualHost::new(base_domain));
    }

    if let Some(bucket) = host.strip_suffix(base_domain).and_then(|h| h.strip_suffix('.')) {
        return Some(VirtualHost::new(base_domain).with_bucket(bucket));
    }

    None
}

#[derive(Debug)]
pub struct SingleDomain {
    base_domain: String,
}

impl SingleDomain {
    /// Create a new `SingleDomain` with the base domain.
    ///
    /// # Errors
    /// Returns an error if the base domain is invalid.
    pub fn new(base_domain: &str) -> Result<Self, DomainError> {
        if !is_valid_domain(base_domain) {
            return Err(DomainError::InvalidDomain);
        }

        Ok(Self {
            base_domain: base_domain.into(),
        })
    }
}

impl S3Host for SingleDomain {
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>> {
        let base_domain = self.base_domain.as_str();

        if let Some(vh) = parse_host_header(base_domain, host) {
            return Ok(vh);
        }

        if is_valid_domain(host) {
            let bucket = host.to_ascii_lowercase();
            return Ok(VirtualHost::new(host).with_bucket(bucket));
        }

        Err(s3_error!(InvalidRequest, "Invalid host header"))
    }
}

#[derive(Debug)]
pub struct MultiDomain {
    base_domains: Vec<String>,
}

impl MultiDomain {
    /// Create a new `MultiDomain` with the base domains.
    ///
    /// # Errors
    /// Returns an error if
    /// + any of the base domains are invalid.
    /// + any of the base domains overlap with each other.
    /// + no base domains are specified.
    pub fn new<I>(base_domains: I) -> Result<Self, DomainError>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut v: Vec<String> = default();

        for domain in base_domains {
            let domain = domain.as_ref();

            if !is_valid_domain(domain) {
                return Err(DomainError::InvalidDomain);
            }

            for other in &v {
                if domain.ends_with(other) || other.ends_with(domain) {
                    return Err(DomainError::OverlappingSubdomains);
                }
            }

            v.push(domain.to_owned());
        }

        if v.is_empty() {
            return Err(DomainError::ZeroDomains);
        }

        Ok(Self { base_domains: v })
    }
}

impl S3Host for MultiDomain {
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>> {
        for base_domain in &self.base_domains {
            if let Some(vh) = parse_host_header(base_domain, host) {
                return Ok(vh);
            }
        }

        if is_valid_domain(host) {
            let bucket = host.to_ascii_lowercase();
            return Ok(VirtualHost::new(host).with_bucket(bucket));
        }

        Err(s3_error!(InvalidRequest, "Invalid host header"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::S3ErrorCode;

    #[test]
    fn single_domain_new() {
        let domain = "example.com";
        let result = SingleDomain::new(domain);
        let sd = result.unwrap();
        assert_eq!(sd.base_domain, domain);

        let domain = "example.com.org";
        let result = SingleDomain::new(domain);
        let sd = result.unwrap();
        assert_eq!(sd.base_domain, domain);

        let domain = "example.com.";
        let result = SingleDomain::new(domain);
        let err = result.unwrap_err();
        assert!(matches!(err, DomainError::InvalidDomain));

        let domain = "example.com:";
        let result = SingleDomain::new(domain);
        let err = result.unwrap_err();
        assert!(matches!(err, DomainError::InvalidDomain));

        let domain = "example.com:80";
        let result = SingleDomain::new(domain);
        assert!(result.is_ok());
    }

    #[test]
    fn multi_domain_new() {
        let domains = ["example.com", "example.org"];
        let result = MultiDomain::new(&domains);
        let md = result.unwrap();
        assert_eq!(md.base_domains, domains);

        let domains = ["example.com", "example.com"];
        let result = MultiDomain::new(&domains);
        let err = result.unwrap_err();
        assert!(matches!(err, DomainError::OverlappingSubdomains));

        let domains = ["example.com", "example.com.org"];
        let result = MultiDomain::new(&domains);
        let md = result.unwrap();
        assert_eq!(md.base_domains, domains);

        let domains: [&str; 0] = [];
        let result = MultiDomain::new(&domains);
        let err = result.unwrap_err();
        assert!(matches!(err, DomainError::ZeroDomains));
    }

    #[test]
    fn multi_domain_parse() {
        let domains = ["example.com", "example.org"];
        let md = MultiDomain::new(domains.iter().copied()).unwrap();

        let host = "example.com";
        let result = md.parse_host_header(host);
        let vh = result.unwrap();
        assert_eq!(vh.domain(), host);
        assert_eq!(vh.bucket(), None);

        let host = "example.org";
        let result = md.parse_host_header(host);
        let vh = result.unwrap();
        assert_eq!(vh.domain(), host);
        assert_eq!(vh.bucket(), None);

        let host = "example.com.org";
        let result = md.parse_host_header(host);
        let vh = result.unwrap();
        assert_eq!(vh.domain(), host);
        assert_eq!(vh.bucket(), Some("example.com.org"));

        let host = "example.com.org.";
        let result = md.parse_host_header(host);
        let err = result.unwrap_err();
        assert!(matches!(err.code(), S3ErrorCode::InvalidRequest));

        let host = "example.com.org.example.com";
        let result = md.parse_host_header(host);
        let vh = result.unwrap();
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), Some("example.com.org"));
    }

    #[test]
    fn virtual_host_builder() {
        // Test basic construction
        let vh = VirtualHost::new("example.com");
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), None);
        assert_eq!(vh.region(), None);

        // Test with_bucket builder
        let vh = VirtualHost::new("example.com").with_bucket("my-bucket");
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), Some("my-bucket"));
        assert_eq!(vh.region(), None);

        // Test with_region builder
        let vh = VirtualHost::new("example.com").with_region("us-west-2");
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), None);
        assert_eq!(vh.region(), Some("us-west-2"));

        // Test chaining with_bucket and with_region
        let vh = VirtualHost::new("example.com")
            .with_bucket("my-bucket")
            .with_region("us-east-1");
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), Some("my-bucket"));
        assert_eq!(vh.region(), Some("us-east-1"));

        // Test chaining with_region and with_bucket (reversed order)
        let vh = VirtualHost::new("example.com")
            .with_region("eu-west-1")
            .with_bucket("another-bucket");
        assert_eq!(vh.domain(), "example.com");
        assert_eq!(vh.bucket(), Some("another-bucket"));
        assert_eq!(vh.region(), Some("eu-west-1"));
    }
}
