//! AWS Signature Version 2 (legacy).
//!
//! Parses and verifies the older HMAC-SHA1-based request authentication scheme
//! used by Amazon S3 before Signature Version 4.
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html>
//!

mod authorization_v2;
pub use self::authorization_v2::*;

mod presigned_url_v2;
pub use self::presigned_url_v2::*;

mod post_signature_v2;
pub use self::post_signature_v2::*;

mod methods;
pub use self::methods::*;
