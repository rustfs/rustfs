use crate::S3Operation;
use crate::auth::Credentials;
use crate::path::S3Path;

use hyper::HeaderMap;
use hyper::Method;
use hyper::Uri;
use hyper::http::Extensions;

pub struct S3AccessContext<'a> {
    pub(crate) credentials: Option<&'a Credentials>,
    pub(crate) s3_path: &'a S3Path,
    pub(crate) s3_op: &'a S3Operation,

    pub(crate) method: &'a Method,
    pub(crate) uri: &'a Uri,
    pub(crate) headers: &'a HeaderMap,

    pub(crate) extensions: &'a mut Extensions,
}

impl S3AccessContext<'_> {
    /// Returns the credentials of current request.
    ///
    /// `None` means anonymous request.
    #[must_use]
    pub fn credentials(&self) -> Option<&Credentials> {
        self.credentials
    }

    /// Returns the S3 path of current request.
    ///
    /// An S3 path can be root, bucket, or object.
    #[must_use]
    pub fn s3_path(&self) -> &S3Path {
        self.s3_path
    }

    /// Returns the S3 operation of current request.
    #[must_use]
    pub fn s3_op(&self) -> &S3Operation {
        self.s3_op
    }

    #[must_use]
    pub fn method(&self) -> &Method {
        self.method
    }

    #[must_use]
    pub fn uri(&self) -> &Uri {
        self.uri
    }

    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        self.headers
    }

    /// Returns the extensions of current request.
    ///
    /// It is used to pass custom data between middlewares.
    #[must_use]
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        self.extensions
    }
}
