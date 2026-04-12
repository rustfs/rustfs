//! AWS Signature Version 4.
//!
//! Parses and verifies HMAC-SHA256-based request authentication for header-based
//! auth, presigned URLs, and multipart POST uploads.
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>
//!
//! See <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html>
//!

mod presigned_url_v4;
pub use self::presigned_url_v4::*;

mod authorization_v4;
pub use self::authorization_v4::*;

mod amz_content_sha256;
pub use self::amz_content_sha256::*;

mod amz_date;
pub use self::amz_date::*;

mod post_signature_v4;
pub use self::post_signature_v4::*;

mod upload_stream;
pub use self::upload_stream::*;

mod methods;
pub use self::methods::*;
