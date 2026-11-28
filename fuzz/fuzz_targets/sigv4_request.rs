#![no_main]

//! Fuzz target for public SigV4 request canonicalization and signing APIs
//! (`try_sign_v4`, `try_pre_sign_v4`).
//!
//! This harness exercises canonicalization and error handling using fixed test
//! credentials and a fixed signing timestamp with no network I/O. No-panic
//! signing alone does not prove server-side signature-verification rejection;
//! presigned header rejection is verified separately via unit tests and
//! rejection-branch mutation checks.

use arbitrary::{Arbitrary, Unstructured};
use http::header::{HeaderName, HeaderValue};
use http::{Method, Request, Uri};
use libfuzzer_sys::fuzz_target;
use rustfs_signer::{try_pre_sign_v4, try_sign_v4};
use time::OffsetDateTime;
use time::macros::datetime;

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_HEADERS: usize = 32;
const MAX_QUERY_PARAMS: usize = 32;
const MAX_FIELD_BYTES: usize = 1024;
const MAX_REGION_BYTES: usize = 64;

const FIXED_ACCESS_KEY: &str = "FUZZTESTACCESSKEY0001";
const FIXED_SECRET_KEY: &str = "FUZZTESTSECRETKEY_DO_NOT_LEAK_0001";
const FIXED_REGION: &str = "us-east-1";
const FIXED_TIME: OffsetDateTime = datetime!(2026-09-01 00:00:00 UTC);

#[derive(Arbitrary, Debug)]
struct FuzzSigV4Request {
    method_selector: u8,
    custom_method: String,
    include_uri_host: bool,
    host: String,
    path: String,
    use_raw_query: bool,
    raw_query: String,
    query_params: Vec<(String, String)>,
    headers: Vec<(String, Vec<u8>)>,
    session_token: String,
    region: String,
    expires: i64,
    content_len: i64,
}

fn clamp_str(input: &str, max_bytes: usize) -> &str {
    if input.len() <= max_bytes {
        return input;
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    &input[..end]
}

fn sanitize_host(host: &str) -> String {
    let filtered: String = clamp_str(host, 128)
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | ':'))
        .collect();
    if filtered.is_empty() {
        "bucket.example.com".to_string()
    } else {
        filtered
    }
}

fn is_ignored_header(name: &str) -> bool {
    matches!(name, "accept-encoding" | "authorization" | "user-agent")
}

fn has_invalid_canonical_header_value(req: &Request<()>, skip_x_amz_date: bool, skip_security_token: bool) -> bool {
    for name in req.headers().keys() {
        let lower = name.as_str();
        if is_ignored_header(lower)
            || (skip_x_amz_date && lower == "x-amz-date")
            || (skip_security_token && lower == "x-amz-security-token")
        {
            continue;
        }
        for value in req.headers().get_all(name) {
            let Ok(text) = std::str::from_utf8(value.as_bytes()) else {
                return true;
            };
            if text.chars().any(|ch| !ch.is_ascii() && ch.is_whitespace()) {
                return true;
            }
        }
    }
    false
}

fn check_signing_invariants(req: Request<()>, session_token: &str, region: &str, expires: i64, content_len: i64) {
    let missing_host = req.uri().host().is_none() && req.headers().get("host").is_none();
    let invalid_presign_header = has_invalid_canonical_header_value(&req, false, false);
    let invalid_sign_header = has_invalid_canonical_header_value(&req, true, !session_token.is_empty());

    let presign_req = clone_empty_request(&req);
    let presign_result = try_pre_sign_v4(
        presign_req,
        FIXED_ACCESS_KEY,
        FIXED_SECRET_KEY,
        session_token,
        region,
        expires,
        FIXED_TIME,
    );
    match &presign_result {
        Ok(signed) => {
            assert!(
                !missing_host,
                "try_pre_sign_v4 accepted a request with neither URI host nor Host header: uri={}",
                req.uri()
            );
            assert!(
                !invalid_presign_header,
                "try_pre_sign_v4 accepted invalid non-UTF8 or non-ASCII-whitespace header bytes"
            );
            let query = signed.uri().query().unwrap_or_default();
            assert!(
                query.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"),
                "presigned query missing SigV4 algorithm: {query}"
            );
            assert!(
                query.contains("X-Amz-SignedHeaders="),
                "presigned query missing X-Amz-SignedHeaders: {query}"
            );
            assert!(query.contains("X-Amz-Signature="), "presigned query missing X-Amz-Signature: {query}");
            assert!(!query.contains(FIXED_SECRET_KEY), "presigned query leaked raw secret access key");
        }
        Err(_) => {}
    }
    if missing_host {
        assert!(
            presign_result.is_err(),
            "try_pre_sign_v4 must reject a request without URI host or Host header"
        );
    }

    let sign_result = try_sign_v4(req, content_len, FIXED_ACCESS_KEY, FIXED_SECRET_KEY, session_token, region);
    match &sign_result {
        Ok(signed) => {
            assert!(!missing_host, "try_sign_v4 accepted a request with neither URI host nor Host header");
            assert!(
                !invalid_sign_header,
                "try_sign_v4 accepted invalid non-UTF8 or non-ASCII-whitespace header bytes"
            );
            let auth = signed
                .headers()
                .get("authorization")
                .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
                .unwrap_or_default();
            assert!(
                auth.starts_with("AWS4-HMAC-SHA256 "),
                "Authorization header missing AWS4-HMAC-SHA256 prefix"
            );
            assert!(auth.contains("Signature="), "Authorization header missing Signature field");
            assert!(!auth.contains(FIXED_SECRET_KEY), "Authorization header leaked raw secret access key");
        }
        Err(_) => {}
    }
    if missing_host {
        assert!(sign_result.is_err(), "try_sign_v4 must reject a request without URI host or Host header");
    }
}

fn clone_empty_request(req: &Request<()>) -> Request<()> {
    let mut builder = Request::builder()
        .method(req.method().clone())
        .uri(req.uri().clone())
        .version(req.version());
    if let Some(headers) = builder.headers_mut() {
        for (name, value) in req.headers() {
            headers.append(name.clone(), value.clone());
        }
    }
    builder.body(()).expect("cloning valid http::Request<()> cannot fail")
}

fn exercise_raw_corpus(data: &[u8]) {
    let mut lines = data.split(|&byte| byte == b'\n');
    let Some(first_line) = lines.next() else {
        return;
    };
    let Ok(request_line) = std::str::from_utf8(first_line) else {
        return;
    };
    let request_line = request_line.trim_end_matches('\r');
    let Some((method_str, uri_str)) = request_line.split_once(' ') else {
        return;
    };
    let Ok(method) = Method::from_bytes(method_str.as_bytes()) else {
        return;
    };
    let Ok(uri) = uri_str.parse::<Uri>() else {
        return;
    };

    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(headers) = builder.headers_mut() {
        for raw_line in lines.take(MAX_HEADERS) {
            let trimmed = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            let Some(colon) = trimmed.iter().position(|&b| b == b':') else {
                continue;
            };
            let raw_name = &trimmed[..colon];
            let mut raw_value = &trimmed[colon + 1..];
            if raw_value.starts_with(b" ") {
                raw_value = &raw_value[1..];
            }
            if raw_value.len() > MAX_FIELD_BYTES * 8 {
                raw_value = &raw_value[..MAX_FIELD_BYTES * 8];
            }
            if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(raw_name), HeaderValue::from_bytes(raw_value)) {
                headers.append(name, value);
            }
        }
    }
    if let Ok(req) = builder.body(()) {
        check_signing_invariants(req, "", FIXED_REGION, 900, 0);
    }
}

fn exercise_structured(input: FuzzSigV4Request) {
    let method = match input.method_selector % 7 {
        0 => Method::GET,
        1 => Method::PUT,
        2 => Method::POST,
        3 => Method::DELETE,
        4 => Method::HEAD,
        5 => Method::OPTIONS,
        _ => Method::from_bytes(clamp_str(&input.custom_method, 16).as_bytes()).unwrap_or(Method::GET),
    };

    let raw_path = clamp_str(&input.path, MAX_FIELD_BYTES);
    let path = if raw_path.starts_with('/') {
        raw_path.to_string()
    } else {
        format!("/{raw_path}")
    };

    let query = if input.use_raw_query {
        clamp_str(&input.raw_query, MAX_FIELD_BYTES).to_string()
    } else {
        let mut pairs = Vec::new();
        for (k, v) in input.query_params.into_iter().take(MAX_QUERY_PARAMS) {
            let k = clamp_str(&k, 128);
            let v = clamp_str(&v, 256);
            pairs.push(format!("{k}={v}"));
        }
        pairs.join("&")
    };

    let uri_string = if input.include_uri_host {
        let host = sanitize_host(&input.host);
        if query.is_empty() {
            format!("https://{host}{path}")
        } else {
            format!("https://{host}{path}?{query}")
        }
    } else if query.is_empty() {
        path
    } else {
        format!("{path}?{query}")
    };

    let Ok(uri) = uri_string.parse::<Uri>() else {
        return;
    };

    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(headers) = builder.headers_mut() {
        for (raw_name, raw_value) in input.headers.into_iter().take(MAX_HEADERS) {
            let name_str = clamp_str(&raw_name, 128);
            let val_slice = &raw_value[..raw_value.len().min(MAX_FIELD_BYTES)];
            if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(name_str.as_bytes()), HeaderValue::from_bytes(val_slice)) {
                headers.append(name, value);
            }
        }
    }

    let Ok(req) = builder.body(()) else {
        return;
    };
    let region = clamp_str(&input.region, MAX_REGION_BYTES);
    let region = if region.is_empty() { FIXED_REGION } else { region };
    let session_token = clamp_str(&input.session_token, 256);
    check_signing_invariants(req, session_token, region, input.expires, input.content_len);
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    exercise_raw_corpus(data);

    let mut unstructured = Unstructured::new(data);
    if let Ok(structured) = FuzzSigV4Request::arbitrary(&mut unstructured) {
        exercise_structured(structured);
    }
});
