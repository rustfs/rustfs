use super::*;

// use crate::service::S3Service;

// use stdx::mem::output_size;

// #[test]
// #[ignore]
// fn track_future_size() {
//     macro_rules! future_size {
//         ($f:path, $v:expr) => {
//             (stringify!($f), output_size(&$f), $v)
//         };
//     }

//     #[rustfmt::skip]
//     let sizes = [
//         future_size!(S3Service::call,                           2704),
//         future_size!(call,                                      1512),
//         future_size!(prepare,                                   1440),
//         future_size!(SignatureContext::check,                   776),
//         future_size!(SignatureContext::v2_check,                296),
//         future_size!(SignatureContext::v2_check_presigned_url,  168),
//         future_size!(SignatureContext::v2_check_header_auth,    184),
//         future_size!(SignatureContext::v4_check,                752),
//         future_size!(SignatureContext::v4_check_post_signature, 368),
//         future_size!(SignatureContext::v4_check_presigned_url,  456),
//         future_size!(SignatureContext::v4_check_header_auth,    640),
//     ];

//     println!("{sizes:#?}");
//     for (name, size, expected) in sizes {
//         assert_eq!(size, expected, "{name:?} size changed: prev {expected}, now {size}");
//     }
// }

/// Verifies that when an anonymous (unauthenticated) request is processed, the `None`
/// branch of the credential match **explicitly clears** `region` and `service`.
///
/// This is a regression guard for the fix to Problem 1: previously only `credentials`
/// was cleared, leaving stale values if the fields had been pre-set.
#[tokio::test]
async fn anonymous_request_clears_region_and_service() {
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use std::sync::Arc;

    struct NoOpS3;
    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for NoOpS3 {}

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(NoOpS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());
    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: None,
        access: None,
        route: None,
        validation: None,
    };

    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/test-bucket/test-key")
            .body(Body::empty())
            .unwrap(),
    );

    // Pre-populate the fields to simulate hypothetical stale state and confirm
    // the explicit clearing in the None branch.
    req.s3ext.region = Some("leftover-region".parse().unwrap());
    req.s3ext.service = Some("leftover-service".into());

    // Auth processing (and thus field assignment) happens before route resolution,
    // so the fields are cleared regardless of whether prepare() succeeds overall.
    let _ = super::prepare(&mut req, &ccx).await;

    assert_eq!(req.s3ext.region, None, "anonymous request must clear region");
    assert_eq!(req.s3ext.service, None, "anonymous request must clear service");
}

/// Verifies that when the signature credential carries no region (`SigV2` or anonymous)
/// the region from `VirtualHost` (provided by `S3Host`) is used as a fallback.
///
/// Covers the `VirtualHost` region integration added in Problem 3.
#[tokio::test]
async fn vh_region_fallback_for_anonymous_request() {
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::error::S3Result;
    use crate::host::{S3Host, VirtualHost};
    use crate::http::{Body, Request};
    use std::sync::Arc;

    struct NoOpS3;
    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for NoOpS3 {}

    /// A test `S3Host` that always emits region "us-west-2" regardless of the Host value.
    struct RegionHost;
    impl S3Host for RegionHost {
        fn parse_host_header<'a>(&'a self, _host: &'a str) -> S3Result<VirtualHost<'a>> {
            Ok(VirtualHost::new("example.com").with_bucket("bucket").with_region("us-west-2"))
        }
    }

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(NoOpS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());
    let host = RegionHost;
    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: Some(&host),
        auth: None,
        access: None,
        route: None,
        validation: None,
    };

    // Virtual-hosted style request: Host header "bucket.example.com", path is the key.
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://bucket.example.com/test-key")
            .header(crate::header::HOST, "bucket.example.com")
            .body(Body::empty())
            .unwrap(),
    );

    let _ = super::prepare(&mut req, &ccx).await;

    assert_eq!(
        req.s3ext.region.as_ref().map(crate::region::Region::as_str),
        Some("us-west-2"),
        "S3Host region should be the fallback when credential provides no region"
    );
}

#[test]
fn error_custom_headers() {
    fn redirect307(location: &str) -> S3Error {
        let mut err = S3Error::new(S3ErrorCode::TemporaryRedirect);

        err.set_headers({
            let mut headers = HeaderMap::new();
            headers.insert(crate::header::LOCATION, location.parse().unwrap());
            headers
        });

        err
    }

    let res = serialize_error(redirect307("http://example.com"), false).unwrap();
    assert_eq!(res.status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(res.headers.get("location").unwrap(), "http://example.com");

    let body = res.body.bytes().unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert_eq!(
        body,
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<Error><Code>TemporaryRedirect</Code></Error>"
        )
    );
}

#[test]
fn extract_host_from_uri() {
    use crate::http::Request;
    use crate::ops::extract_host;

    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .version(::http::Version::HTTP_2)
            .uri("https://test.example.com:9001/rust.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251213T084305Z&X-Amz-SignedHeaders=host&X-Amz-Credential=rustfsadmin%2F20251213%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=3600&X-Amz-Signature=57133ee54dab71c00a10106c33cde2615b301bd2cf00e2439f3ddb4bc999ec66")
            .body(Body::empty())
            .unwrap(),
    );

    let host = extract_host(&req).unwrap();
    assert_eq!(host, Some("test.example.com:9001".to_string()));

    req.version = ::http::Version::HTTP_11;
    let host = extract_host(&req).unwrap();
    assert_eq!(host, None);

    req.version = ::http::Version::HTTP_3;
    let host = extract_host(&req).unwrap();
    assert_eq!(host, Some("test.example.com:9001".to_string()));

    let mut req = Request::from(
        hyper::Request::builder()
            .version(::http::Version::HTTP_10)
            .method(Method::GET)
            .uri("http://another.example.org/resource")
            .body(Body::empty())
            .unwrap(),
    );
    let host = extract_host(&req).unwrap();
    assert_eq!(host, None);

    req.version = ::http::Version::HTTP_2;
    let host = extract_host(&req).unwrap();
    assert_eq!(host, Some("another.example.org".to_string()));

    req.version = ::http::Version::HTTP_3;
    let host = extract_host(&req).unwrap();
    assert_eq!(host, Some("another.example.org".to_string()));

    let req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("/no/host/header")
            .header("Host", "header.example.com:8080")
            .body(Body::empty())
            .unwrap(),
    );
    let host = extract_host(&req).unwrap();
    assert_eq!(host, Some("header.example.com:8080".to_string()));

    let req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("/no/host/header")
            .body(Body::empty())
            .unwrap(),
    );
    let host = extract_host(&req).unwrap();
    assert_eq!(host, None);
}

#[tokio::test]
async fn presigned_url_expires_0_should_be_expired() {
    use crate::S3ErrorCode;
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, OrderedHeaders, OrderedQs};
    use crate::ops::signature::SignatureContext;
    use hyper::{Method, Uri};
    use std::sync::Arc;

    let qs = OrderedQs::parse(concat!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
        "&X-Amz-Date=20130524T000000Z",
        "&X-Amz-Expires=0",
        "&X-Amz-SignedHeaders=host",
        "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    ))
    .unwrap();

    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let method = Method::GET;
    let uri = Uri::from_static("https://s3.amazonaws.com/test.txt");
    let mut body = Body::empty();

    let mut cx = SignatureContext {
        auth: None,
        config: &config,
        req_version: ::http::Version::HTTP_11,
        req_method: &method,
        req_uri: &uri,
        req_body: &mut body,
        qs: Some(&qs),
        hs: OrderedHeaders::from_slice_unchecked(&[]),
        decoded_uri_path: "/test.txt".to_owned(),
        vh_bucket: None,
        content_length: None,
        mime: None,
        decoded_content_length: None,
        transformed_body: None,
        multipart: None,
        trailing_headers: None,
    };

    let result = cx.v4_check_presigned_url().await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn post_multipart_bucket_routes_to_post_object() {
    use crate::S3Request;
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::sig_v4;
    use bytes::Bytes;
    use hyper::Method;
    use hyper::header::HeaderValue;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestS3 {
        put_calls: AtomicUsize,
        post_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3 {
        async fn put_object(
            &self,
            _req: S3Request<crate::dto::PutObjectInput>,
        ) -> crate::error::S3Result<crate::protocol::S3Response<crate::dto::PutObjectOutput>> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::protocol::S3Response::new(crate::dto::PutObjectOutput::default()))
        }

        async fn post_object(
            &self,
            _req: S3Request<crate::dto::PostObjectInput>,
        ) -> crate::error::S3Result<crate::protocol::S3Response<crate::dto::PostObjectOutput>> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::protocol::S3Response::new(crate::dto::PostObjectOutput::default()))
        }
    }

    let test_s3 = Arc::new(TestS3 {
        put_calls: AtomicUsize::new(0),
        post_calls: AtomicUsize::new(0),
    });
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
    let auth = SimpleAuth::from_single(access_key, secret_key.clone());

    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: Some(&auth),
        access: None,
        route: None,
        validation: None,
    };

    // Build a minimal multipart/form-data POST object request.
    // Signature is validated by v4_check_post_signature using the policy blob.
    let boundary = "------------------------c634190ccaebbc34";
    let bucket = "mc-test-bucket-32569";
    let key = "mc-test-object-7658";
    let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAyMC0xMC0wM1QxMzoyNTo0Ny4yMThaIiwiY29uZGl0aW9ucyI6W1siZXEiLCIkYnVja2V0IiwibWMtdGVzdC1idWNrZXQtMzI1NjkiXSxbImVxIiwiJGtleSIsIm1jLXRlc3Qtb2JqZWN0LTc2NTgiXSxbImVxIiwiJHgtYW16LWRhdGUiLCIyMDIwMDkyNlQxMzI1NDdaIl0sWyJlcSIsIiR4LWFtei1hbGdvcml0aG0iLCJBV1M0LUhNQUMtU0hBMjU2Il0sWyJlcSIsIiR4LWFtei1jcmVkZW50aWFsIiwiQUtJQUlPU0ZPRE5ON0VYQU1QTEUvMjAyMDA5MjYvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJdXX0=";
    let algorithm = "AWS4-HMAC-SHA256";
    let credential = "AKIAIOSFODNN7EXAMPLE/20200926/us-east-1/s3/aws4_request";
    let amz_date = sig_v4::AmzDate::parse("20200926T132547Z").unwrap();
    let region = "us-east-1";
    let service = "s3";
    let signature = sig_v4::calculate_signature(policy_b64, &secret_key, &amz_date, region, service);

    let body = format!(
        concat!(
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"x-amz-signature\"\r\n\r\n",
            "{signature}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"bucket\"\r\n\r\n",
            "{bucket}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"policy\"\r\n\r\n",
            "{policy_b64}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"x-amz-algorithm\"\r\n\r\n",
            "{algorithm}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"x-amz-credential\"\r\n\r\n",
            "{credential}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n",
            "{amz_date}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"key\"\r\n\r\n",
            "{key}\r\n",
            "--{b}\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n",
            "Content-Type: text/plain\r\n\r\n",
            "hello\r\n",
            "--{b}--\r\n"
        ),
        amz_date = amz_date.fmt_iso8601(),
        b = boundary,
        signature = signature,
        bucket = bucket,
        policy_b64 = policy_b64,
        algorithm = algorithm,
        credential = credential,
        key = key,
    );

    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::POST)
            .uri(format!("http://localhost/{bucket}"))
            .header(crate::header::HOST, "localhost")
            .header(
                crate::header::CONTENT_TYPE,
                HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}")).unwrap(),
            )
            .body(Body::from(Bytes::from(body)))
            .unwrap(),
    );

    // POST Object with `policy` field now validates the policy.
    // The test policy has expired (2020-10-03), so we expect AccessDenied.
    let result = super::prepare(&mut req, &ccx).await;
    match result {
        Err(err) => assert_eq!(*err.code(), crate::error::S3ErrorCode::AccessDenied),
        Ok(_) => panic!("expected AccessDenied error for expired policy"),
    }
}

// Helper functions for POST policy resource exhaustion tests

/// Helper to create a test S3 service that tracks POST calls
mod post_policy_test_helpers {
    use std::fmt::Write;

    use crate::S3Request;
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3Config, S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::sig_v4;
    use bytes::Bytes;
    use hyper::Method;
    use hyper::header::HeaderValue;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct TestS3WithPostTracking {
        pub post_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3WithPostTracking {
        async fn post_object(
            &self,
            _req: S3Request<crate::dto::PostObjectInput>,
        ) -> crate::error::S3Result<crate::protocol::S3Response<crate::dto::PostObjectOutput>> {
            self.post_calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::protocol::S3Response::new(crate::dto::PostObjectOutput::default()))
        }
    }

    pub struct TestS3NoOp;

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3NoOp {}

    /// Create a test config with custom `post_object_max_file_size`
    pub fn create_test_config(post_object_max_file_size: u64) -> Arc<dyn S3ConfigProvider> {
        let config = S3Config {
            post_object_max_file_size,
            ..Default::default()
        };
        Arc::new(StaticConfigProvider::new(Arc::new(config)))
    }

    /// Create auth and `CallContext` for testing
    pub fn create_test_context<'a>(
        s3: &'a Arc<dyn crate::s3_trait::S3>,
        config: &'a Arc<dyn S3ConfigProvider>,
        auth: &'a SimpleAuth,
    ) -> CallContext<'a> {
        CallContext {
            s3,
            config,
            host: None,
            auth: Some(auth),
            access: None,
            route: None,
            validation: None,
        }
    }

    /// Create a `SimpleAuth` for testing
    pub fn create_test_auth() -> SimpleAuth {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
        SimpleAuth::from_single(access_key, secret_key)
    }

    /// Standard conditions that cover the form fields added by `build_post_object_request`.
    ///
    /// Per the S3 spec, each non-exempt form field must have a matching condition in the policy.
    /// The fields added by the helper are: bucket, key, x-amz-algorithm, x-amz-credential,
    /// x-amz-date.  (x-amz-signature, policy, and file are exempt.)
    pub const BASE_CONDITIONS: &str = r#"{"bucket":"test-bucket"},["eq","$key","test-key"],["starts-with","$x-amz-algorithm",""],["starts-with","$x-amz-credential",""],["starts-with","$x-amz-date",""]"#;

    pub fn build_multipart_fields(list: &[(&str, &str)], boundary: &str) -> String {
        let mut d = String::new();
        for (name, value) in list {
            write!(
                &mut d,
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .unwrap();
        }
        d
    }

    pub fn build_multipart_file_field(
        field_name: &str,
        filename: &str,
        content_type: &str,
        file_content: &str,
        boundary: &str,
    ) -> String {
        format!(
            concat!(
                "--{boundary}\r\n",
                "Content-Disposition: form-data; name=\"{field_name}\"; filename=\"{filename}\"\r\n",
                "Content-Type: {content_type}\r\n\r\n",
                "{file_content}\r\n",
                "--{boundary}--\r\n",
            ),
            boundary = boundary,
            field_name = field_name,
            filename = filename,
            content_type = content_type,
            file_content = file_content,
        )
    }

    /// Build a POST object request with a policy
    pub fn build_post_object_request(
        policy_json: &str,
        file_content: &str,
        secret_key: &SecretKey,
        with_content_type: bool,
    ) -> Request {
        let policy_b64 = base64_simd::STANDARD.encode_to_string(policy_json);

        let boundary = "------------------------test12345678";
        let bucket = "test-bucket";
        let key = "test-key";
        let amz_date = sig_v4::AmzDate::parse("20250101T000000Z").unwrap();
        let region = "us-east-1";
        let service = "s3";
        let content_type = "text/plain";
        let algorithm = "AWS4-HMAC-SHA256";
        let credential = "AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request";
        let signature = sig_v4::calculate_signature(&policy_b64, secret_key, &amz_date, region, service);
        let amz_date_str = amz_date.fmt_iso8601();

        let fields = {
            let mut f = vec![
                ("x-amz-signature", signature.as_str()),
                ("bucket", bucket),
                ("policy", policy_b64.as_str()),
                ("x-amz-algorithm", algorithm),
                ("x-amz-credential", credential),
                ("x-amz-date", amz_date_str.as_str()),
                ("key", key),
            ];
            if with_content_type {
                f.push(("Content-Type", content_type));
            }
            f
        };

        let body = build_multipart_fields(&fields, boundary)
            + build_multipart_file_field("file", "test.txt", content_type, file_content, boundary).as_str();

        Request::from(
            hyper::Request::builder()
                .method(Method::POST)
                .uri(format!("http://localhost/{bucket}"))
                .header(crate::header::HOST, "localhost")
                .header(
                    crate::header::CONTENT_TYPE,
                    HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}")).unwrap(),
                )
                .body(Body::from(Bytes::from(body)))
                .unwrap(),
        )
    }

    /// Build a POST object request whose body is split into many small chunks.
    ///
    /// This ensures that `aggregate_file_stream_limited` returns a `Vec<Bytes>`
    /// with multiple entries, so tests can distinguish between
    /// `vec_bytes.len()` (chunk count) and the total byte count.
    pub fn build_post_object_request_chunked(
        policy_json: &str,
        file_content: &str,
        secret_key: &SecretKey,
        chunk_size: usize,
    ) -> Request {
        let policy_b64 = base64_simd::STANDARD.encode_to_string(policy_json);

        let boundary = "------------------------test12345678";
        let bucket = "test-bucket";
        let key = "test-key";
        let amz_date = sig_v4::AmzDate::parse("20250101T000000Z").unwrap();
        let region = "us-east-1";
        let service = "s3";
        let content_type = "text/plain";
        let algorithm = "AWS4-HMAC-SHA256";
        let credential = "AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/s3/aws4_request";
        let signature = sig_v4::calculate_signature(&policy_b64, secret_key, &amz_date, region, service);

        let body = build_multipart_fields(
            &[
                ("x-amz-signature", &signature),
                ("bucket", bucket),
                ("policy", &policy_b64),
                ("x-amz-algorithm", algorithm),
                ("x-amz-credential", credential),
                ("x-amz-date", &amz_date.fmt_iso8601()),
                ("key", key),
            ],
            boundary,
        ) + &build_multipart_file_field("file", "test.txt", content_type, file_content, boundary);

        // Split the body into small chunks to simulate a multi-chunk stream
        let body_bytes: Vec<u8> = body.into_bytes();
        let chunks: Vec<Result<http_body::Frame<Bytes>, std::convert::Infallible>> = body_bytes
            .chunks(chunk_size)
            .map(|c| Ok(http_body::Frame::data(Bytes::copy_from_slice(c))))
            .collect();

        let stream_body = http_body_util::StreamBody::new(futures::stream::iter(chunks));

        Request::from(
            hyper::Request::builder()
                .method(Method::POST)
                .uri(format!("http://localhost/{bucket}"))
                .header(crate::header::HOST, "localhost")
                .header(
                    crate::header::CONTENT_TYPE,
                    HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}")).unwrap(),
                )
                .body(Body::http_body(stream_body))
                .unwrap(),
        )
    }
}

/// Test that policy max < config max results in using policy max for file size limit
#[tokio::test]
async fn post_object_policy_max_smaller_than_config_max() {
    use crate::auth::SecretKey;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    let test_s3 = Arc::new(post_policy_test_helpers::TestS3WithPostTracking {
        post_calls: AtomicUsize::new(0),
    });
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();

    // Set config max to 1MB
    let config = post_policy_test_helpers::create_test_config(1024 * 1024);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Create a policy with content-length-range max of 100 bytes (< config max of 1MB)
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",0,100],["eq","$Content-Type","text/plain"],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );
    let file_content = "a".repeat(50); // 50 bytes (within policy limit of 100 bytes)

    let mut req = post_policy_test_helpers::build_post_object_request(policy_json, &file_content, &secret_key, true);

    // This should succeed because file size (50 bytes) is within policy limit (100 bytes)
    // The important part is that the aggregation limit used is 100 bytes (policy max), not 1MB (config max)
    let result = super::prepare(&mut req, &ccx).await;
    assert!(result.is_ok(), "expected success for file within policy limit");
}

#[tokio::test]
async fn post_object_without_content_type_field_but_with_policy() {
    use crate::auth::SecretKey;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    let test_s3 = Arc::new(post_policy_test_helpers::TestS3WithPostTracking {
        post_calls: AtomicUsize::new(0),
    });
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();

    // Set config max to 1MB
    let config = post_policy_test_helpers::create_test_config(1024 * 1024);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Create a policy with content-length-range max of 100 bytes (< config max of 1MB)
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",0,100],["eq","$Content-Type","text/plain"],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );
    let file_content = "a".repeat(50); // 50 bytes (within policy limit of 100 bytes)

    let mut req = post_policy_test_helpers::build_post_object_request(policy_json, &file_content, &secret_key, false);

    // This should fail because the request omits the Content-Type form field required by the policy,
    // even though the file size (50 bytes) is within the policy's content-length-range limit (0–100 bytes).
    let result = super::prepare(&mut req, &ccx).await;

    // Assert that we get the specific policy error for the missing Content-Type field.
    let Err(err) = result else {
        panic!("expected error for missing Content-Type field required by policy")
    };
    assert_eq!(
        *err.code(),
        S3ErrorCode::InvalidPolicyDocument,
        "unexpected error code for missing Content-Type field required by policy"
    );

    // The error message (or debug representation) should indicate that the `eq` condition
    // on Content-Type failed because the field was missing or mismatched.
    let msg = format!("{err:?}");
    let msg_lower = msg.to_lowercase();
    assert!(
        msg_lower.contains("content-type") || msg_lower.contains("content type"),
        "error message should mention Content-Type requirement, got: {msg}"
    );
    assert!(
        msg_lower.contains("eq"),
        "error message should indicate failure of the `eq` condition, got: {msg}"
    );
}

/// Test that file exceeding policy max but under config max is rejected
#[tokio::test]
async fn post_object_file_exceeds_policy_max_but_under_config_max() {
    use crate::auth::SecretKey;
    use std::sync::Arc;

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(post_policy_test_helpers::TestS3NoOp);

    // Set config max to 10KB
    let config = post_policy_test_helpers::create_test_config(10 * 1024);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Create a policy with content-length-range max of 100 bytes
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",0,100],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );
    // Create a file with 150 bytes (exceeds policy max of 100 bytes, but under config max of 10KB)
    // This is the critical security test: file should be rejected before consuming memory
    let file_content = "a".repeat(150);

    let mut req = post_policy_test_helpers::build_post_object_request(policy_json, &file_content, &secret_key, false);

    // This should fail because file size (150 bytes) exceeds policy limit (100 bytes)
    // The key security improvement: file is rejected during aggregation (at 100 bytes limit),
    // not after reading the full 150 bytes (or potentially larger files)
    let result = super::prepare(&mut req, &ccx).await;
    assert!(result.is_err(), "expected error for file exceeding policy limit");

    // MultipartError::FileTooLarge is mapped to EntityTooLarge
    match result {
        Err(err) => {
            let code = err.code();
            assert!(
                matches!(code, crate::error::S3ErrorCode::EntityTooLarge),
                "expected EntityTooLarge error, got {code:?}",
            );
        }
        Ok(_) => panic!("expected error for file exceeding policy limit"),
    }
}

/// Test that policy max > config max results in using config max for file size limit
#[tokio::test]
async fn post_object_policy_max_larger_than_config_max() {
    use crate::auth::SecretKey;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    let test_s3 = Arc::new(post_policy_test_helpers::TestS3WithPostTracking {
        post_calls: AtomicUsize::new(0),
    });
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();

    // Set config max to 200 bytes (smaller than policy max)
    let config = post_policy_test_helpers::create_test_config(200);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Create a policy with content-length-range max of 10KB (> config max of 200 bytes)
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",0,10240],["eq","$Content-Type","text/plain"],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );
    // Create a file with 150 bytes (within config max of 200 bytes, within policy max of 10KB)
    let file_content = "a".repeat(150);

    let mut req = post_policy_test_helpers::build_post_object_request(policy_json, &file_content, &secret_key, true);

    // This should succeed because file size (150 bytes) is within config max (200 bytes)
    // The aggregation limit used is min(policy_max=10KB, config_max=200) = 200 bytes
    let result = super::prepare(&mut req, &ccx).await;
    assert!(result.is_ok(), "expected success for file within config limit");
}

/// Regression test for rustfs/rustfs#984:
/// POST Object with content-length-range [0, 10] should reject files larger than 10 bytes
/// with `EntityTooLarge` error code.
#[tokio::test]
async fn post_object_content_length_range_rejects_oversized_file() {
    use crate::auth::SecretKey;
    use std::sync::Arc;

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(post_policy_test_helpers::TestS3NoOp);

    let config = post_policy_test_helpers::create_test_config(5 * 1024 * 1024 * 1024);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Exact scenario from the issue: content-length-range [0, 10]
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",0,10],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );
    // File content is much larger than 10 bytes
    let file_content = "very long contents, longer than 10 bytes";

    let mut req = post_policy_test_helpers::build_post_object_request(policy_json, file_content, &secret_key, false);

    let result = super::prepare(&mut req, &ccx).await;
    assert!(result.is_err(), "expected error for file exceeding content-length-range");

    let Err(err) = result else {
        panic!("expected error for file exceeding content-length-range");
    };
    assert_eq!(
        *err.code(),
        crate::error::S3ErrorCode::EntityTooLarge,
        "expected EntityTooLarge error, got {:?}",
        err.code()
    );
}

// ========================================
// Access Control Tests
// ========================================

// Helper module for access control tests
mod access_control_test_helpers {
    use crate::S3Request;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct TestS3WithGetObject {
        pub get_object_calls: AtomicUsize,
    }

    impl TestS3WithGetObject {
        pub fn new() -> Self {
            Self {
                get_object_calls: AtomicUsize::new(0),
            }
        }

        pub fn get_call_count(&self) -> usize {
            self.get_object_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3WithGetObject {
        async fn get_object(
            &self,
            _req: S3Request<crate::dto::GetObjectInput>,
        ) -> crate::error::S3Result<crate::protocol::S3Response<crate::dto::GetObjectOutput>> {
            self.get_object_calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::protocol::S3Response::new(crate::dto::GetObjectOutput::default()))
        }
    }
}

/// Test S3 route denies anonymous access when auth is configured
#[tokio::test]
async fn test_s3_route_anonymous_access_denied() {
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use hyper::Method;
    use std::sync::Arc;

    let test_s3 = Arc::new(access_control_test_helpers::TestS3WithGetObject::new());
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
    let auth = SimpleAuth::from_single(access_key, secret_key);

    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: Some(&auth),
        access: None,
        route: None,
        validation: None,
    };

    // Create an anonymous GET object request (no auth headers or query params)
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/test-bucket/test-key.txt")
            .header(crate::header::HOST, "localhost")
            .body(Body::empty())
            .unwrap(),
    );

    // This should fail with AccessDenied because the request has no authentication.
    // Use `call` so that we exercise the full request lifecycle, and ensure that
    // access is denied before the S3 backend is invoked.
    let response = super::call(&mut req, &ccx).await.unwrap();

    // Verify that the response indicates access is denied
    assert_eq!(response.status, hyper::StatusCode::FORBIDDEN, "Anonymous request should have been denied");

    // Verify that the S3 service was never called (access was denied before dispatch)
    assert_eq!(test_s3.get_call_count(), 0);
}

/// Test S3 route with custom `S3Access` that allows anonymous access
#[tokio::test]
async fn test_s3_route_custom_access_allows_anonymous() {
    use crate::access::{S3Access, S3AccessContext};
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use hyper::Method;
    use std::sync::Arc;

    /// Custom `S3Access` that allows anonymous access
    struct AnonymousAccess;

    #[async_trait::async_trait]
    impl S3Access for AnonymousAccess {
        async fn check(&self, _cx: &mut S3AccessContext<'_>) -> crate::error::S3Result<()> {
            // Allow all access, including anonymous
            Ok(())
        }
    }

    let test_s3 = Arc::new(access_control_test_helpers::TestS3WithGetObject::new());
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
    let auth = SimpleAuth::from_single(access_key, secret_key);

    let anonymous_access = AnonymousAccess;

    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: Some(&auth),
        access: Some(&anonymous_access),
        route: None,
        validation: None,
    };

    // Create an anonymous GET object request
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/test-bucket/test-key.txt")
            .header(crate::header::HOST, "localhost")
            .body(Body::empty())
            .unwrap(),
    );

    // Call the full operation which should pass access control and invoke the handler
    let result = super::call(&mut req, &ccx).await;

    // Should succeed with a successful response
    match result {
        Ok(resp) => {
            // Should get a successful response (2xx status code)
            assert!(
                resp.status.is_success(),
                "Anonymous request should succeed when custom access control allows it, got status: {:?}",
                resp.status
            );
        }
        Err(err) => {
            panic!("Anonymous request should succeed when custom access control allows it, got error: {err:?}");
        }
    }

    // Verify that the S3 handler was actually invoked
    assert_eq!(test_s3.get_call_count(), 1, "S3 handler should have been invoked once");
}

/// Test custom route denies anonymous access by default
#[tokio::test]
async fn test_custom_route_anonymous_access_denied() {
    use crate::S3Request;
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::protocol::S3Response;
    use crate::route::S3Route;
    use hyper::header::HeaderValue;
    use hyper::http::Extensions;
    use hyper::{HeaderMap, Method, StatusCode, Uri};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestS3;

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3 {}

    /// Custom route that uses default `check_access` (requires authentication)
    #[derive(Debug, Clone)]
    struct TestCustomRoute {
        call_count: Arc<AtomicUsize>,
    }

    impl TestCustomRoute {
        fn new() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Route for TestCustomRoute {
        fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
            // Match POST requests to /custom-route
            method == Method::POST
                && uri.path() == "/custom-route"
                && headers
                    .get(hyper::header::CONTENT_TYPE)
                    .is_some_and(|v| v.as_bytes() == b"application/x-custom")
        }

        // Use default check_access which requires authentication

        async fn call(&self, _req: S3Request<Body>) -> crate::error::S3Result<S3Response<Body>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(S3Response::new(Body::from("Custom route response".to_string())))
        }
    }

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(TestS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
    let auth = SimpleAuth::from_single(access_key, secret_key);

    let custom_route = TestCustomRoute::new();

    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: Some(&auth),
        access: None,
        route: Some(&custom_route),
        validation: None,
    };

    // Create an anonymous request to the custom route
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::POST)
            .uri("http://localhost/custom-route")
            .header(crate::header::HOST, "localhost")
            .header(hyper::header::CONTENT_TYPE, HeaderValue::from_static("application/x-custom"))
            .body(Body::empty())
            .unwrap(),
    );

    // Call the operation (which will internally call prepare then check access on the custom route)
    let result = super::call(&mut req, &ccx).await;

    // call() serializes S3Errors into HTTP responses, so we check the status code
    match result {
        Ok(resp) => {
            // AccessDenied should result in a 403 Forbidden response
            assert_eq!(
                resp.status,
                StatusCode::FORBIDDEN,
                "Anonymous request to custom route should return 403 Forbidden"
            );
        }
        Err(err) => {
            // Shouldn't get here for normal S3 errors
            panic!("Unexpected error that wasn't serialized: {err:?}");
        }
    }

    // Verify that the custom route was never actually called (because access was denied)
    assert_eq!(custom_route.get_call_count(), 0);
}

/// Test custom route that overrides `check_access` to allow anonymous access
#[tokio::test]
async fn test_custom_route_anonymous_access_allowed_when_overridden() {
    use crate::S3Request;
    use crate::auth::{SecretKey, SimpleAuth};
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::protocol::S3Response;
    use crate::route::S3Route;
    use hyper::header::HeaderValue;
    use hyper::http::Extensions;
    use hyper::{HeaderMap, Method, Uri};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestS3;

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3 {}

    /// Custom route that allows anonymous access
    #[derive(Debug, Clone)]
    struct AnonymousCustomRoute {
        call_count: Arc<AtomicUsize>,
    }

    impl AnonymousCustomRoute {
        fn new() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Route for AnonymousCustomRoute {
        fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
            // Match GET requests to /public-route
            method == Method::GET
                && uri.path() == "/public-route"
                && headers
                    .get(hyper::header::CONTENT_TYPE)
                    .is_some_and(|v| v.as_bytes() == b"application/x-public")
        }

        async fn check_access(&self, _req: &mut S3Request<Body>) -> crate::error::S3Result<()> {
            // Allow anonymous access
            Ok(())
        }

        async fn call(&self, _req: S3Request<Body>) -> crate::error::S3Result<S3Response<Body>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(S3Response::new(Body::from("Public route response".to_string())))
        }
    }

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(TestS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();
    let auth = SimpleAuth::from_single(access_key, secret_key);

    // Use a custom route that allows anonymous access
    let anonymous_route = AnonymousCustomRoute::new();

    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: Some(&auth),
        access: None,
        route: Some(&anonymous_route),
        validation: None,
    };

    // Create an anonymous request to the public route
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/public-route")
            .header(crate::header::HOST, "localhost")
            .header(hyper::header::CONTENT_TYPE, HeaderValue::from_static("application/x-public"))
            .body(Body::empty())
            .unwrap(),
    );

    // Call the operation (which will internally call prepare then check access on the custom route)
    let result = super::call(&mut req, &ccx).await;

    // This should succeed because the custom route allows anonymous access
    match result {
        Ok(resp) => {
            // Should get a successful response (2xx status code)
            assert!(
                resp.status.is_success(),
                "Anonymous request should be allowed when custom route permits it, got status: {:?}",
                resp.status
            );
        }
        Err(err) => {
            panic!("Anonymous request should succeed on public route, got error: {err:?}");
        }
    }

    // Verify that the custom route was actually called
    assert_eq!(anonymous_route.get_call_count(), 1);
}

/// Test S3 route allows access when no auth provider is configured
///
/// When `CallContext.auth` is `None`, access checks are skipped for S3 operations,
/// allowing unsigned requests to succeed. This tests that behavior.
#[tokio::test]
async fn test_s3_route_no_auth_provider_allows_unsigned_requests() {
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use hyper::Method;
    use std::sync::Arc;

    let test_s3 = Arc::new(access_control_test_helpers::TestS3WithGetObject::new());
    let s3: Arc<dyn crate::s3_trait::S3> = test_s3.clone();
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    // No auth provider configured - access checks are skipped for S3 operations
    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: None,
        access: None,
        route: None,
        validation: None,
    };

    // Create an unsigned request
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/test-bucket/test-key.txt")
            .header(crate::header::HOST, "localhost")
            .body(Body::empty())
            .unwrap(),
    );

    // Call the full operation which should succeed when no auth provider is configured
    let result = super::call(&mut req, &ccx).await;

    // Should succeed with a successful response
    match result {
        Ok(resp) => {
            assert!(
                resp.status.is_success(),
                "Unsigned request should succeed when no auth provider is configured, got status: {:?}",
                resp.status
            );
        }
        Err(err) => {
            panic!("Unsigned request should succeed when no auth provider is configured, got error: {err:?}");
        }
    }

    // Verify that the S3 handler was invoked
    assert_eq!(test_s3.get_call_count(), 1, "S3 handler should have been invoked");
}

/// Test custom route with overridden `check_access` allows unsigned requests
///
/// Custom routes always call `check_access()`, even when no auth provider is configured.
/// This test verifies that a custom route can override `check_access` to allow access
/// without credentials, regardless of the auth provider configuration.
#[tokio::test]
async fn test_custom_route_override_check_access_allows_unsigned_requests() {
    use crate::S3Request;
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::protocol::S3Response;
    use crate::route::S3Route;
    use hyper::header::HeaderValue;
    use hyper::http::Extensions;
    use hyper::{HeaderMap, Method, Uri};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestS3;

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3 {}

    /// Custom route for testing
    #[derive(Debug, Clone)]
    struct TestRoute {
        call_count: Arc<AtomicUsize>,
    }

    impl TestRoute {
        fn new() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Route for TestRoute {
        fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
            method == Method::POST
                && uri.path() == "/test-route"
                && headers
                    .get(hyper::header::CONTENT_TYPE)
                    .is_some_and(|v| v.as_bytes() == b"application/x-test")
        }

        // Override check_access to allow access without credentials
        async fn check_access(&self, _req: &mut S3Request<Body>) -> crate::error::S3Result<()> {
            Ok(())
        }

        async fn call(&self, _req: S3Request<Body>) -> crate::error::S3Result<S3Response<Body>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(S3Response::new(Body::from("Test route response".to_string())))
        }
    }

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(TestS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let test_route = TestRoute::new();

    // Custom route's check_access is always called, even without an auth provider
    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: None,
        access: None,
        route: Some(&test_route),
        validation: None,
    };

    // Create an unsigned request to the custom route
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::POST)
            .uri("http://localhost/test-route")
            .header(crate::header::HOST, "localhost")
            .header(hyper::header::CONTENT_TYPE, HeaderValue::from_static("application/x-test"))
            .body(Body::empty())
            .unwrap(),
    );

    // Call the operation which should succeed because check_access is overridden to allow it
    let result = super::call(&mut req, &ccx).await;

    // Should succeed with a successful response
    match result {
        Ok(resp) => {
            assert!(
                resp.status.is_success(),
                "Unsigned request should succeed with overridden check_access, got status: {:?}",
                resp.status
            );
        }
        Err(err) => {
            panic!("Unsigned request should succeed with overridden check_access, got error: {err:?}");
        }
    }

    // Verify that the custom route was invoked
    assert_eq!(test_route.get_call_count(), 1, "Custom route should have been invoked");
}

/// Test custom route with default `check_access` denies unsigned requests even without auth provider
///
/// This test verifies a key difference between S3 operations and custom routes:
/// - S3 operations skip access checks when `CallContext.auth` is `None`
/// - Custom routes always call `check_access()`, and the default implementation denies
///   requests without credentials, even when no auth provider is configured
#[tokio::test]
async fn test_custom_route_default_check_access_denies_unsigned_without_auth_provider() {
    use crate::S3Request;
    use crate::config::{S3ConfigProvider, StaticConfigProvider};
    use crate::http::{Body, Request};
    use crate::ops::CallContext;
    use crate::protocol::S3Response;
    use crate::route::S3Route;
    use hyper::header::HeaderValue;
    use hyper::http::Extensions;
    use hyper::{HeaderMap, Method, StatusCode, Uri};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestS3;

    #[async_trait::async_trait]
    impl crate::s3_trait::S3 for TestS3 {}

    /// Custom route that uses default `check_access` (requires credentials)
    #[derive(Debug, Clone)]
    struct TestRoute {
        call_count: Arc<AtomicUsize>,
    }

    impl TestRoute {
        fn new() -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Route for TestRoute {
        fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
            method == Method::POST
                && uri.path() == "/auth-route"
                && headers
                    .get(hyper::header::CONTENT_TYPE)
                    .is_some_and(|v| v.as_bytes() == b"application/x-auth")
        }

        // Use default check_access (requires credentials)

        async fn call(&self, _req: S3Request<Body>) -> crate::error::S3Result<S3Response<Body>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(S3Response::new(Body::from("Auth route response".to_string())))
        }
    }

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(TestS3);
    let config: Arc<dyn S3ConfigProvider> = Arc::new(StaticConfigProvider::default());

    let test_route = TestRoute::new();

    // No auth provider configured - but custom routes still check access
    let ccx = CallContext {
        s3: &s3,
        config: &config,
        host: None,
        auth: None,
        access: None,
        route: Some(&test_route),
        validation: None,
    };

    // Create an unsigned request to the custom route
    let mut req = Request::from(
        hyper::Request::builder()
            .method(Method::POST)
            .uri("http://localhost/auth-route")
            .header(crate::header::HOST, "localhost")
            .header(hyper::header::CONTENT_TYPE, HeaderValue::from_static("application/x-auth"))
            .body(Body::empty())
            .unwrap(),
    );

    // Call the operation - should fail because default check_access requires credentials
    let result = super::call(&mut req, &ccx).await;

    // Should return 403 Forbidden
    match result {
        Ok(resp) => {
            assert_eq!(
                resp.status,
                StatusCode::FORBIDDEN,
                "Unsigned request should be denied by default check_access, got status: {:?}",
                resp.status
            );
        }
        Err(err) => {
            panic!("Expected 403 response, got error: {err:?}");
        }
    }

    // Verify that the custom route handler was never invoked (access denied before dispatch)
    assert_eq!(test_route.get_call_count(), 0, "Custom route handler should not have been invoked");
}

/// Regression test: `file_size` for post policy validation must be the total
/// byte count of all chunks, NOT the number of chunks in `Vec<Bytes>`.
///
/// Previously the code used `vec_bytes.len()` which returns the chunk count
/// instead of summing the byte lengths of all chunks.
/// This caused `content-length-range` policy validation to use a wrong value.
///
/// This test uses `build_post_object_request_chunked` to split the HTTP body
/// into many small chunks (1 KiB each). With a 30 KB file the multipart body
/// stream yields ~30 chunks, so `aggregate_file_stream_limited` returns a
/// `Vec<Bytes>` with ~30 entries. The buggy `vec_bytes.len()` would report
/// the file size as ~30, which is below the policy minimum of 100 and would
/// cause the request to be rejected. The correct code sums the byte lengths
/// and reports 30 000, which passes the `[100, 50000]` range check.
#[tokio::test]
async fn post_policy_file_size_is_total_bytes_not_chunk_count() {
    use crate::auth::SecretKey;
    use std::sync::Arc;

    let s3: Arc<dyn crate::s3_trait::S3> = Arc::new(post_policy_test_helpers::TestS3NoOp);

    // Set config max to 1MB to allow our test file
    let config = post_policy_test_helpers::create_test_config(1024 * 1024);

    let auth = post_policy_test_helpers::create_test_auth();
    let ccx = post_policy_test_helpers::create_test_context(&s3, &config, &auth);

    let secret_key: SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into();

    // Create a policy with content-length-range [100, 50000]
    // This will accept files between 100 and 50000 bytes
    let policy_json = &format!(
        r#"{{"expiration":"2030-01-01T00:00:00.000Z","conditions":[["content-length-range",100,50000],{}]}}"#,
        post_policy_test_helpers::BASE_CONDITIONS,
    );

    // Create a 30 KB file (30 000 bytes) within policy limits.
    // Use 1 KiB chunks so the body stream yields ~30 chunks for the file part.
    // With the buggy code (vec_bytes.len()), file_size would be ~30 (chunk count),
    // which is < 100 (policy minimum) and would incorrectly fail.
    let file_content = "a".repeat(30_000);
    let chunk_size = 1024;

    let mut req =
        post_policy_test_helpers::build_post_object_request_chunked(policy_json, &file_content, &secret_key, chunk_size);

    let result = super::prepare(&mut req, &ccx).await;

    // This must succeed: the file is 30 000 bytes, within [100, 50000].
    match result {
        Ok(_) => {}
        Err(err) => panic!("POST object with 30 KB file should pass content-length-range [100, 50000] validation, got: {err:?}"),
    }

    // Now test with a file that's too small (should fail)
    let small_file_content = "a".repeat(50); // 50 bytes, less than minimum of 100
    let mut req_small =
        post_policy_test_helpers::build_post_object_request_chunked(policy_json, &small_file_content, &secret_key, chunk_size);

    let result_small = super::prepare(&mut req_small, &ccx).await;
    match result_small {
        Err(err) => {
            assert_eq!(
                *err.code(),
                crate::error::S3ErrorCode::EntityTooSmall,
                "Expected EntityTooSmall error for content-length-range violation"
            );
            let msg = err.message().unwrap_or("");
            assert!(
                msg.contains("smaller than the minimum"),
                "Error message should mention file is too small, got: {msg}"
            );
        }
        Ok(_) => panic!("POST object with 50-byte file should fail content-length-range [100, 50000] validation"),
    }
}

#[test]
fn create_session_route_resolved() {
    use crate::http::{Body, OrderedQs};
    use crate::path::S3Path;

    let req = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/my-bucket?session")
            .body(Body::empty())
            .unwrap(),
    );

    let s3_path = S3Path::Bucket {
        bucket: "my-bucket".into(),
    };
    let qs = OrderedQs::parse("session").unwrap();
    let (op, needs_full_body) = generated::resolve_route(&req, &s3_path, Some(&qs)).unwrap();

    assert_eq!(op.name(), "CreateSession");
    assert!(!needs_full_body);
}

#[test]
fn create_session_deserialize_http() {
    use crate::http::Body;
    use crate::path::S3Path;

    let mut req = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/my-bucket?session")
            .header("x-amz-create-session-mode", "ReadWrite")
            .body(Body::empty())
            .unwrap(),
    );

    req.s3ext.s3_path = Some(S3Path::Bucket {
        bucket: "my-bucket".into(),
    });

    let input = generated::CreateSession::deserialize_http(&mut req).unwrap();

    assert_eq!(input.bucket, "my-bucket");
    assert_eq!(input.session_mode.as_ref().map(crate::dto::SessionMode::as_str), Some("ReadWrite"));
    assert!(input.server_side_encryption.is_none());
    assert!(input.ssekms_key_id.is_none());
    assert!(input.ssekms_encryption_context.is_none());
    assert!(input.bucket_key_enabled.is_none());
}

#[test]
fn create_session_serialize_http() {
    use crate::dto::{CreateSessionOutput, SessionCredentials, Timestamp, TimestampFormat};

    let creds = SessionCredentials {
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
        expiration: Timestamp::parse(TimestampFormat::DateTime, "2024-01-01T00:05:00.000Z").unwrap(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
        session_token: "FwoGZXIvYXdzEBYaDHqa0A".to_owned(),
    };

    let output = CreateSessionOutput {
        credentials: creds,
        ..Default::default()
    };

    let resp = generated::CreateSession::serialize_http(output).unwrap();
    assert_eq!(resp.status, hyper::StatusCode::OK);
}

#[test]
fn list_directory_buckets_route_resolved() {
    use crate::http::{Body, OrderedQs};
    use crate::path::S3Path;

    let req = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/?x-id=ListDirectoryBuckets")
            .body(Body::empty())
            .unwrap(),
    );

    let s3_path = S3Path::Root;
    let qs = OrderedQs::parse("x-id=ListDirectoryBuckets").unwrap();
    let (op, needs_full_body) = generated::resolve_route(&req, &s3_path, Some(&qs)).unwrap();

    assert_eq!(op.name(), "ListDirectoryBuckets");
    assert!(!needs_full_body);
}

#[test]
fn list_buckets_route_still_default() {
    use crate::http::{Body, OrderedQs};
    use crate::path::S3Path;

    // With x-id=ListBuckets
    let req = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/?x-id=ListBuckets")
            .body(Body::empty())
            .unwrap(),
    );

    let s3_path = S3Path::Root;
    let qs = OrderedQs::parse("x-id=ListBuckets").unwrap();
    let (op, _) = generated::resolve_route(&req, &s3_path, Some(&qs)).unwrap();
    assert_eq!(op.name(), "ListBuckets");

    // Without any query string
    let req2 = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/")
            .body(Body::empty())
            .unwrap(),
    );
    let (op2, _) = generated::resolve_route(&req2, &s3_path, None).unwrap();
    assert_eq!(op2.name(), "ListBuckets");
}

#[test]
fn list_directory_buckets_deserialize_http() {
    use crate::http::{Body, OrderedQs};

    let mut req = crate::http::Request::from(
        hyper::Request::builder()
            .method(Method::GET)
            .uri("http://localhost/?continuation-token=abc123&max-directory-buckets=10")
            .body(Body::empty())
            .unwrap(),
    );

    req.s3ext.s3_path = Some(crate::path::S3Path::Root);
    req.s3ext.qs = Some(OrderedQs::parse("continuation-token=abc123&max-directory-buckets=10").unwrap());

    let input = generated::ListDirectoryBuckets::deserialize_http(&mut req).unwrap();

    assert_eq!(input.continuation_token.as_deref(), Some("abc123"));
    assert_eq!(input.max_directory_buckets, Some(10));
}

#[test]
fn list_directory_buckets_serialize_http() {
    use crate::dto::ListDirectoryBucketsOutput;

    let output = ListDirectoryBucketsOutput { ..Default::default() };

    let resp = generated::ListDirectoryBuckets::serialize_http(output).unwrap();
    assert_eq!(resp.status, hyper::StatusCode::OK);
}
