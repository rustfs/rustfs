//! Signature v4 methods

use super::AmzDate;

use crate::auth::SecretKey;
use crate::http::OrderedHeaders;
use crate::utils::crypto::{hex, hex_sha256, hex_sha256_chunk, hmac_sha256};
use crate::utils::stable_sort_by_first;

use hyper::Method;
use hyper::body::Bytes;
use smallvec::SmallVec;
use stdx::str::StrExt;
use zeroize::Zeroize;

/// custom uri encode
#[allow(clippy::indexing_slicing, clippy::inline_always, clippy::unwrap_used)]
fn uri_encode(output: &mut String, input: &str, encode_slash: bool) {
    /// hex uppercase
    #[inline(always)] // perf
    fn to_hex(x: u8) -> u8 {
        b"0123456789ABCDEF"[usize::from(x)]
    }

    let mut buf: SmallVec<[u8; 512]> = SmallVec::with_capacity(input.len());

    for &byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'~' | b'.' => buf.push(byte),
            b'/' => {
                if encode_slash {
                    buf.push(b'%');
                    buf.push(b'2');
                    buf.push(b'F');
                } else {
                    buf.push(byte);
                }
            }
            _ => {
                buf.push(b'%');
                buf.push(to_hex(byte.wrapping_shr(4)));
                buf.push(to_hex(byte & 15));
            }
        }
    }

    let s = str::from_ascii_simd(buf.as_ref()).unwrap();
    output.push_str(s);
}

/// custom uri encode
fn uri_encode_string(input: &str, encode_slash: bool) -> String {
    let mut output = String::with_capacity(input.len());
    uri_encode(&mut output, input, encode_slash);
    output
}

/// is skipped header
fn is_skipped_header(header: &str) -> bool {
    header == "authorization"
}

/// is skipped query string
fn is_skipped_query_string(name: &str) -> bool {
    name == "X-Amz-Signature"
}

/// Normalize header value according to AWS `SigV4` specification:
/// Trim leading and trailing whitespace and replace sequential whitespace with a single space.
///
/// Reference: <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
fn normalize_header_value(ans: &mut String, value: &str) {
    let trimmed = value.trim();

    // Fast path: if no internal whitespace, append as-is
    if !trimmed.chars().any(char::is_whitespace) {
        ans.push_str(trimmed);
        return;
    }

    // Split on any whitespace and rejoin with single spaces
    let mut first = true;
    for word in trimmed.split_whitespace() {
        if first {
            first = false;
        } else {
            ans.push(' ');
        }
        ans.push_str(word);
    }
}

/// sha256 hash of an empty string
const EMPTY_STRING_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Payload
#[derive(Debug, Clone, Copy)]
pub enum Payload<'a> {
    /// unsigned
    Unsigned,
    /// single chunk
    SingleChunk(&'a str),
    /// multiple chunks
    MultipleChunks,
    /// multiple chunks with trailing headers
    MultipleChunksWithTrailer,
    /// unsigned streaming with trailing headers
    UnsignedMultipleChunksWithTrailer,
}

#[cfg(test)]
impl Payload<'_> {
    pub fn empty() -> Self {
        Payload::SingleChunk(EMPTY_STRING_SHA256_HASH)
    }
}

/// create canonical request
#[must_use]
pub fn create_canonical_request(
    method: &Method,
    uri_path: &str,
    decoded_query_strings: &[(impl AsRef<str>, impl AsRef<str>)],
    signed_headers: &OrderedHeaders<'_>,
    payload: Payload<'_>,
) -> String {
    let mut ans = String::with_capacity(256);

    {
        // <HTTPMethod>\n
        ans.push_str(method.as_str());
        ans.push('\n');
    }

    {
        // <CanonicalURI>\n
        uri_encode(&mut ans, uri_path, false);
        ans.push('\n');
    }

    {
        // <CanonicalQueryString>\n
        let encoded_query_strings = {
            let mut qs = <SmallVec<[(String, String); 16]>>::new();
            for (n, v) in decoded_query_strings {
                let name = uri_encode_string(n.as_ref(), true);
                let value = uri_encode_string(v.as_ref(), true);
                qs.push((name, value));
            }
            stable_sort_by_first(&mut qs);
            qs
        };

        if let Some((first, remain)) = encoded_query_strings.split_first() {
            {
                let (name, value) = first;
                ans.push_str(name);
                ans.push('=');
                ans.push_str(value);
            }
            for (name, value) in remain {
                ans.push('&');
                ans.push_str(name);
                ans.push('=');
                ans.push_str(value);
            }
        }

        ans.push('\n');
    }

    {
        // <CanonicalHeaders>\n
        // According to AWS SigV4 spec, multiple headers with the same name should be combined with comma-separated values.
        // Reference: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html

        // FIXME: check HOST, Content-Type, x-amz-security-token, x-amz-content-sha256

        let headers_slice = signed_headers.as_ref();
        let mut i = 0;
        while i < headers_slice.len() {
            let (name, value) = headers_slice[i];
            if is_skipped_header(name) {
                i += 1;
                continue;
            }

            ans.push_str(name);
            ans.push(':');
            normalize_header_value(&mut ans, value);

            // Combine values for headers with the same name (comma-separated)
            let mut j = i + 1;
            while j < headers_slice.len() && headers_slice[j].0 == name {
                ans.push(',');
                normalize_header_value(&mut ans, headers_slice[j].1);
                j += 1;
            }

            ans.push('\n');
            i = j;
        }
        ans.push('\n');
    }

    {
        // <SignedHeaders>\n
        // Each header name should only appear once, even if the header has multiple values
        let headers_slice = signed_headers.as_ref();
        let mut first_flag = true;
        let mut i = 0;
        while i < headers_slice.len() {
            let (name, _) = headers_slice[i];
            if is_skipped_header(name) {
                i += 1;
                continue;
            }

            if first_flag {
                first_flag = false;
            } else {
                ans.push(';');
            }
            ans.push_str(name);

            // Skip duplicate header names
            while i < headers_slice.len() && headers_slice[i].0 == name {
                i += 1;
            }
        }

        ans.push('\n');
    }

    {
        // <HashedPayload>
        match payload {
            Payload::Unsigned => ans.push_str("UNSIGNED-PAYLOAD"),
            Payload::SingleChunk(checksum) => ans.push_str(checksum),
            Payload::MultipleChunks => ans.push_str("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            Payload::MultipleChunksWithTrailer => ans.push_str("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
            Payload::UnsignedMultipleChunksWithTrailer => ans.push_str("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        }
    }

    ans
}

/// create string to sign
#[must_use]
pub fn create_string_to_sign(canonical_request: &str, amz_date: &AmzDate, region: &str, service: &str) -> String {
    let mut ans = String::with_capacity(256);

    {
        // <Algorithm>\n
        ans.push_str("AWS4-HMAC-SHA256\n");
    }

    {
        // <RequestDateTime>\n
        ans.push_str(&amz_date.fmt_iso8601());
        ans.push('\n');
    }

    {
        // <CredentialScope>\n
        ans.push_str(&amz_date.fmt_date());
        ans.push('/');
        ans.push_str(region);
        ans.push('/');
        ans.push_str(service);
        ans.push_str("/aws4_request\n");
    }

    {
        // <HashedCanonicalRequest>
        hex_sha256(canonical_request.as_bytes(), |s| ans.push_str(s));
    }

    ans
}

/// create `string_to_sign` of a chunk
pub fn create_chunk_string_to_sign(
    amz_date: &AmzDate,
    region: &str,
    service: &str,
    prev_signature: &str,
    chunk_data: &[Bytes],
) -> String {
    let mut ans = String::with_capacity(256);

    {
        ans.push_str("AWS4-HMAC-SHA256-PAYLOAD\n");
    }
    {
        ans.push_str(&amz_date.fmt_iso8601());
        ans.push('\n');
    }
    {
        ans.push_str(&amz_date.fmt_date());
        ans.push('/');
        ans.push_str(region);
        ans.push('/');
        ans.push_str(service);
        ans.push_str("/aws4_request\n");
    }
    {
        ans.push_str(prev_signature);
        ans.push('\n');
    }
    {
        ans.push_str(EMPTY_STRING_SHA256_HASH);
        ans.push('\n');
    }
    {
        if chunk_data.is_empty() {
            ans.push_str(EMPTY_STRING_SHA256_HASH);
        } else {
            hex_sha256_chunk(chunk_data, |s| ans.push_str(s));
        }
    }

    ans
}

/// create `string_to_sign` of the final trailer block (0-chunk with trailing headers)
pub fn create_trailer_string_to_sign(
    amz_date: &AmzDate,
    region: &str,
    service: &str,
    prev_signature: &str,
    canonical_trailers: &[u8],
) -> String {
    let mut ans = String::with_capacity(256);

    {
        ans.push_str("AWS4-HMAC-SHA256-TRAILER\n");
    }
    {
        ans.push_str(&amz_date.fmt_iso8601());
        ans.push('\n');
    }
    {
        ans.push_str(&amz_date.fmt_date());
        ans.push('/');
        ans.push_str(region);
        ans.push('/');
        ans.push_str(service);
        ans.push_str("/aws4_request\n");
    }
    {
        ans.push_str(prev_signature);
        ans.push('\n');
    }
    {
        // hash of canonicalized trailing headers
        hex_sha256(canonical_trailers, |s| ans.push_str(s));
    }

    ans
}

/// calculate signature
#[must_use]
pub fn calculate_signature(
    string_to_sign: &str,
    secret_key: &SecretKey,
    amz_date: &AmzDate,
    region: &str,
    service: &str,
) -> String {
    let mut secret = {
        let secret_key = secret_key.expose();
        let mut buf = <SmallVec<[u8; 128]>>::with_capacity(secret_key.len().saturating_add(4));
        buf.extend_from_slice(b"AWS4");
        buf.extend_from_slice(secret_key.as_bytes());
        buf
    };

    // DateKey
    let date = amz_date.fmt_date();
    let date_key = hmac_sha256(secret.as_slice(), date.as_bytes());

    secret.zeroize();
    drop(secret);

    // DateRegionKey
    let date_region_key = hmac_sha256(date_key, region);

    // DateRegionServiceKey
    let date_region_service_key = hmac_sha256(date_region_key, service);

    // SigningKey
    let signing_key = hmac_sha256(date_region_service_key, "aws4_request");

    // Signature
    hex(hmac_sha256(signing_key, string_to_sign))
}

/// create presigned canonical request
pub fn create_presigned_canonical_request(
    method: &Method,
    uri_path: &str,
    decoded_query_strings: &[(impl AsRef<str>, impl AsRef<str>)],
    signed_headers: &OrderedHeaders<'_>,
) -> String {
    let mut ans = String::with_capacity(256);
    {
        // <HTTPMethod>\n
        ans.push_str(method.as_str());
        ans.push('\n');
    }
    {
        // <CanonicalURI>\n
        uri_encode(&mut ans, uri_path, false);
        ans.push('\n');
    }
    {
        // <CanonicalQueryString>\n
        let encoded_query_strings = {
            let mut qs = <SmallVec<[(String, String); 16]>>::new();
            for (n, v) in decoded_query_strings {
                if is_skipped_query_string(n.as_ref()) {
                    continue;
                }
                let name = uri_encode_string(n.as_ref(), true);
                let value = uri_encode_string(v.as_ref(), true);
                qs.push((name, value));
            }
            stable_sort_by_first(&mut qs);
            qs
        };

        if let Some((first, remain)) = encoded_query_strings.split_first() {
            {
                let (name, value) = first;
                ans.push_str(name);
                ans.push('=');
                ans.push_str(value);
            }
            for (name, value) in remain {
                ans.push('&');
                ans.push_str(name);
                ans.push('=');
                ans.push_str(value);
            }
        }

        ans.push('\n');
    }
    {
        // <CanonicalHeaders>\n
        // According to AWS SigV4 spec, multiple headers with the same name should be
        // combined into a single header with values separated by commas.

        let headers_slice = signed_headers.as_ref();
        let mut i = 0;
        while i < headers_slice.len() {
            let (name, value) = headers_slice[i];
            if is_skipped_header(name) {
                i += 1;
                continue;
            }

            ans.push_str(name);
            ans.push(':');
            normalize_header_value(&mut ans, value);

            // Combine values for headers with the same name (comma-separated)
            let mut j = i + 1;
            while j < headers_slice.len() && headers_slice[j].0 == name {
                ans.push(',');
                normalize_header_value(&mut ans, headers_slice[j].1);
                j += 1;
            }

            ans.push('\n');
            i = j;
        }
        ans.push('\n');
    }
    {
        // <SignedHeaders>\n
        // Each header name should only appear once, even if the header has multiple values
        let headers_slice = signed_headers.as_ref();
        let mut first_flag = true;
        let mut i = 0;
        while i < headers_slice.len() {
            let (name, _) = headers_slice[i];
            if is_skipped_header(name) {
                i += 1;
                continue;
            }

            if first_flag {
                first_flag = false;
            } else {
                ans.push(';');
            }
            ans.push_str(name);

            // Skip duplicate header names
            while i < headers_slice.len() && headers_slice[i].0 == name {
                i += 1;
            }
        }

        ans.push('\n');
    }
    {
        // <Payload>
        ans.push_str("UNSIGNED-PAYLOAD");
    }
    ans
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http::OrderedQs;
    use crate::sig_v4::PresignedUrlV4;
    use crate::utils::crypto::hex_sha256_string;

    #[test]
    fn example_get_object() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/test.txt";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ]);

        let method = Method::GET;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, path, qs, &headers, Payload::empty());

        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/test.txt\n",
                "\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "range:bytes=0-9\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:20130524T000000Z\n",
                "\n",
                "host;range;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");
    }

    #[test]
    fn example_put_object_single_chunk() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/test$file.text";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ]);

        let method = Method::PUT;
        let payload_checksum = &hex_sha256_string("Welcome to Amazon S3.".as_bytes());
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, path, qs, &headers, Payload::SingleChunk(payload_checksum));

        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/test%24file.text\n",
                "\n",
                "date:Fri, 24 May 2013 00:00:00 GMT\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n",
                "x-amz-date:20130524T000000Z\n",
                "x-amz-storage-class:REDUCED_REDUNDANCY\n",
                "\n",
                "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n",
                "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd");
    }

    #[test]
    fn example_put_object_multiple_chunks_seed_signature() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/examplebucket/chunkObject.txt";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("content-encoding", "aws-chunked"),
            ("content-length", "66824"),
            ("host", "s3.amazonaws.com"),
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-decoded-content-length", "66560"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ]);

        let method = Method::PUT;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, path, qs, &headers, Payload::MultipleChunks);

        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/examplebucket/chunkObject.txt\n",
                "\n",
                "content-encoding:aws-chunked\n",
                "content-length:66824\n",
                "host:s3.amazonaws.com\n",
                "x-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD\n",
                "x-amz-date:20130524T000000Z\n",
                "x-amz-decoded-content-length:66560\n",
                "x-amz-storage-class:REDUCED_REDUNDANCY\n",
                "\n",
                "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class\n",
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "cee3fed04b70f867d036f722359b0b1f2f0e5dc0efadbc082b76c4c60e316455",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",);
    }

    #[test]
    fn example_put_object_multiple_chunks_chunk_signature() {
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        let region = "us-east-1";
        let service = "s3";
        let date = AmzDate::parse(timestamp).unwrap();

        let seed_signature = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9";

        let chunk1_string_to_sign =
            create_chunk_string_to_sign(&date, region, service, seed_signature, &[Bytes::from(vec![b'a'; 64 * 1024])]);
        assert_eq!(
            chunk1_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
            )
        );

        let chunk1_signature = calculate_signature(&chunk1_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk1_signature, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648");

        let chunk2_string_to_sign =
            create_chunk_string_to_sign(&date, region, service, &chunk1_signature, &[Bytes::from(vec![b'a'; 1024])]);
        assert_eq!(
            chunk2_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "2edc986847e209b4016e141a6dc8716d3207350f416969382d431539bf292e4a",
            )
        );

        let chunk2_signature = calculate_signature(&chunk2_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk2_signature, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497");

        let chunk3_string_to_sign = create_chunk_string_to_sign(&date, region, service, &chunk2_signature, &[]);
        assert_eq!(
            chunk3_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let chunk3_signature = calculate_signature(&chunk3_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk3_signature, "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9");
    }

    #[test]
    fn example_put_object_multiple_chunks_with_trailer_seed_signature() {
        // Example from AWS docs: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        let region = "us-east-1";
        let service = "s3";
        let path = "/examplebucket/chunkObject.txt";

        // Match canonical header order in the doc example
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("content-encoding", "aws-chunked"),
            ("host", "s3.amazonaws.com"),
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-decoded-content-length", "66560"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ("x-amz-trailer", "x-amz-checksum-crc32c"),
        ]);

        let method = Method::PUT;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, path, qs, &headers, Payload::MultipleChunksWithTrailer);

        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/examplebucket/chunkObject.txt\n",
                "\n",
                "content-encoding:aws-chunked\n",
                "host:s3.amazonaws.com\n",
                "x-amz-content-sha256:STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER\n",
                "x-amz-date:20130524T000000Z\n",
                "x-amz-decoded-content-length:66560\n",
                "x-amz-storage-class:REDUCED_REDUNDANCY\n",
                "x-amz-trailer:x-amz-checksum-crc32c\n",
                "\n",
                "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer\n",
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "44d48b8c2f70eae815a0198cc73d7a546a73a93359c070abbaa5e6c7de112559",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e");
    }

    #[test]
    fn example_put_object_unsigned_multiple_chunks_with_trailer_seed_signature() {
        // Hypothetical canonical request for STREAMING-UNSIGNED-PAYLOAD-TRAILER.
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        let region = "us-east-1";
        let service = "s3";
        let path = "/examplebucket/chunkObject.txt";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("content-encoding", "aws-chunked"),
            ("host", "s3.amazonaws.com"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-decoded-content-length", "66560"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            ("x-amz-trailer", "x-amz-checksum-crc32c"),
        ]);

        let method = Method::PUT;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, path, qs, &headers, Payload::UnsignedMultipleChunksWithTrailer);

        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/examplebucket/chunkObject.txt\n",
                "\n",
                "content-encoding:aws-chunked\n",
                "host:s3.amazonaws.com\n",
                "x-amz-content-sha256:STREAMING-UNSIGNED-PAYLOAD-TRAILER\n",
                "x-amz-date:20130524T000000Z\n",
                "x-amz-decoded-content-length:66560\n",
                "x-amz-storage-class:REDUCED_REDUNDANCY\n",
                "x-amz-trailer:x-amz-checksum-crc32c\n",
                "\n",
                "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer\n",
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);

        // We don't assert the exact signature value here; just ensure it can be computed.
        let _signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
    }

    #[test]
    fn example_put_object_multiple_chunks_with_trailer_chunk_signature() {
        // Example from AWS docs: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        let region = "us-east-1";
        let service = "s3";
        let date = AmzDate::parse(timestamp).unwrap();

        let seed_signature = "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e";

        // Chunk 1: 65536 bytes of 'a'
        let chunk1_string_to_sign =
            create_chunk_string_to_sign(&date, region, service, seed_signature, &[Bytes::from(vec![b'a'; 64 * 1024])]);
        assert_eq!(
            chunk1_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
            )
        );

        let chunk1_signature = calculate_signature(&chunk1_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk1_signature, "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2");

        // Chunk 2: 1024 bytes of 'a'
        let chunk2_string_to_sign =
            create_chunk_string_to_sign(&date, region, service, &chunk1_signature, &[Bytes::from(vec![b'a'; 1024])]);
        assert_eq!(
            chunk2_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "2edc986847e209b4016e141a6dc8716d3207350f416969382d431539bf292e4a",
            )
        );

        let chunk2_signature = calculate_signature(&chunk2_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk2_signature, "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7");

        // Chunk 3: 0 bytes
        let chunk3_string_to_sign = create_chunk_string_to_sign(&date, region, service, &chunk2_signature, &[]);
        assert_eq!(
            chunk3_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-PAYLOAD\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let chunk3_signature = calculate_signature(&chunk3_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(chunk3_signature, "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992");
    }

    #[test]
    fn example_put_object_multiple_chunks_with_trailer_trailer_signature() {
        // Example from AWS docs: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        let region = "us-east-1";
        let service = "s3";
        let date = AmzDate::parse(timestamp).unwrap();

        let prev_signature = "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"; // chunk3 signature
        let canonical_trailers = b"x-amz-checksum-crc32c:sOO8/Q==\n";

        let trailer_string_to_sign = create_trailer_string_to_sign(&date, region, service, prev_signature, canonical_trailers);

        assert_eq!(
            trailer_string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256-TRAILER\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\n",
                "1e376db7e1a34a8ef1c4bcee131a2d60a1cb62503747488624e10995f448d774",
            )
        );

        let trailer_signature = calculate_signature(&trailer_string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(trailer_signature, "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435");
    }

    #[test]
    fn example_get_bucket_lifecycle_configuration() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ]);

        let query_strings = &[("lifecycle", "")];

        let method = Method::GET;

        let canonical_request = create_canonical_request(&method, path, query_strings, &headers, Payload::empty());
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/\n",
                "lifecycle=\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:20130524T000000Z\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543");
    }

    #[test]
    fn example_list_objects() {
        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let timestamp = "20130524T000000Z";
        // let bucket = "examplebucket";
        let region = "us-east-1";
        let service = "s3";
        let path = "/";

        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ]);

        let query_strings = &[("max-keys", "2"), ("prefix", "J")];

        let method = Method::GET;

        let canonical_request = create_canonical_request(&method, path, query_strings, &headers, Payload::empty());

        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/\n",
                "max-keys=2&prefix=J\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:20130524T000000Z\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
        );

        let date = AmzDate::parse(timestamp).unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7",
            )
        );

        let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
        assert_eq!(signature, "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7");
    }

    #[test]
    fn example_presigned_url() {
        use hyper::Uri;

        // let access_key_id = "AKIAIOSFODNN7EXAMPLE";
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        let method = Method::GET;

        let uri = Uri::from_static(concat!(
            "https://s3.amazonaws.com/test.txt",
            "?X-Amz-Algorithm=AWS4-HMAC-SHA256",
            "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request",
            "&X-Amz-Date=20130524T000000Z",
            "&X-Amz-Expires=86400",
            "&X-Amz-SignedHeaders=host",
            "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        ));

        let headers = OrderedHeaders::from_slice_unchecked(&[("host", "examplebucket.s3.amazonaws.com")]);

        let query_strings = &[
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-Expires", "86400"),
            ("X-Amz-SignedHeaders", "host"),
            ("X-Amz-Signature", "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"),
        ];

        let qs = OrderedQs::from_vec_unchecked(query_strings.iter().map(|&(n, v)| (n.to_owned(), v.to_owned())).collect());

        let info = PresignedUrlV4::parse(&qs).unwrap();

        let canonical_request = create_presigned_canonical_request(&method, uri.path(), query_strings, &headers);

        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/test.txt\n",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );

        let string_to_sign = create_string_to_sign(
            &canonical_request,
            &info.amz_date,
            info.credential.aws_region,
            info.credential.aws_service,
        );
        assert_eq!(
            string_to_sign,
            concat!(
                "AWS4-HMAC-SHA256\n",
                "20130524T000000Z\n",
                "20130524/us-east-1/s3/aws4_request\n",
                "3bfa292879f6447bbcda7001decf97f4a54dc650c8942174ae0a9121cf58ad04",
            )
        );

        let signature = calculate_signature(
            &string_to_sign,
            &secret_access_key,
            &info.amz_date,
            info.credential.aws_region,
            info.credential.aws_service,
        );
        assert_eq!(signature, "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");
        assert_eq!(signature, info.signature);
    }

    #[test]
    fn special_20230204() {
        use hyper::header::HeaderName;
        use hyper::header::HeaderValue;

        let mut req = http::Request::<hyper::body::Bytes>::default();

        *req.method_mut() = Method::GET;
        *req.uri_mut() = hyper::Uri::from_static(
            "http://localhost:8014/minio-java-test-1gqr1v4?prefix=prefix&suffix=suffix&events=s3%3AObjectCreated%3A%2A&events=s3%3AObjectAccessed%3A%2A",
        );

        let x_amz_date = "20230204T155111Z";
        let headers = [
            ("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg=="),
            ("host", "localhost:8014"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", x_amz_date),
        ];
        for (name, value) in &headers {
            req.headers_mut()
                .insert(HeaderName::from_static(name), HeaderValue::from_static(value));
        }

        let signed_header_names = &["content-md5", "host", "x-amz-content-sha256", "x-amz-date"];

        let payload = Payload::empty();
        let date = AmzDate::parse(x_amz_date).unwrap();
        let region = "us-east-1";
        let service = "s3";

        let secret_access_key = SecretKey::from("minioadmin");

        {
            let uri_path = req.uri().path();
            let qs = req.uri().query().map(|q| OrderedQs::parse(q).unwrap());
            let query_strings: &[_] = qs.as_ref().map_or(&[], |x| x.as_ref());

            let signed_headers = OrderedHeaders::from_headers(req.headers())
                .unwrap()
                .find_multiple_with_on_missing(signed_header_names, |_| None);

            let canonical_request = create_canonical_request(req.method(), uri_path, query_strings, &signed_headers, payload);

            let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);

            let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
            assert_eq!(signature, "96ad058ca27352e0fc2bd4efd8973792077570667bdaf749655f42e204bc649c");
        }
    }

    #[test]
    fn special_20250414() {
        use hyper::header::HeaderName;
        use hyper::header::HeaderValue;

        let mut req = http::Request::<hyper::body::Bytes>::default();

        *req.method_mut() = Method::POST;
        *req.uri_mut() = hyper::Uri::from_static("https://play.rustfs.com:7000/");

        let x_amz_date = "20250414T022518Z";
        let headers = [
            ("amz-sdk-invocation-id", "8c875471-a10a-45ad-b37e-e67e82de17d6"),
            ("amz-sdk-request", "attempt=1; max=3"),
            ("content-length", "130"),
            ("content-type", "application/x-www-form-urlencoded"),
            ("x-amz-content-sha256", "8e333076fe36da6633c89f2e38100cecb8e7587a1e0d6ce31838b6f68262b949"),
            ("x-amz-date", x_amz_date),
            (
                "x-amz-user-agent",
                "aws-sdk-js/3.777.0 ua/2.1 os/macOS#10.15.7 lang/js md/browser#Chrome_135.0.0.0 api/sts#3.777.0 m/N,E,e",
            ),
        ];
        for (name, value) in &headers {
            req.headers_mut()
                .insert(HeaderName::from_static(name), HeaderValue::from_static(value));
        }

        {
            let authorithy = HeaderValue::from_str(req.uri().authority().unwrap().as_str()).unwrap();
            req.headers_mut().insert(HeaderName::from_static("host"), authorithy);
        }

        let signed_header_names = &[
            "amz-sdk-invocation-id",
            "amz-sdk-request",
            "content-length",
            "content-type",
            "host",
            "x-amz-content-sha256",
            "x-amz-date",
            "x-amz-user-agent",
        ];

        let body = b"RoleArn=arn%3Aaws%3Aiam%3A%3A%2A%3Arole%2FAdmin&RoleSessionName=console&DurationSeconds=43200&Action=AssumeRole&Version=2011-06-15";
        let payload_checksum = hex_sha256_string(body);
        let date = AmzDate::parse(x_amz_date).unwrap();
        let region = "cn-east-1";
        let service = "sts";

        let secret_access_key = SecretKey::from("rustfsadmin");

        {
            let uri_path = req.uri().path();
            let qs = req.uri().query().map(|q| OrderedQs::parse(q).unwrap());
            let query_strings: &[_] = qs.as_ref().map_or(&[], |x| x.as_ref());

            let signed_headers = OrderedHeaders::from_headers(req.headers())
                .unwrap()
                .find_multiple_with_on_missing(signed_header_names, |_| panic!());

            let canonical_request = create_canonical_request(
                req.method(),
                uri_path,
                query_strings,
                &signed_headers,
                Payload::SingleChunk(&payload_checksum),
            );

            let string_to_sign = create_string_to_sign(&canonical_request, &date, region, service);

            let signature = calculate_signature(&string_to_sign, &secret_access_key, &date, region, service);
            assert_eq!(signature, "7ed3ea6c69ed841068bbdd3cc1eb92a9ae5a4b1b0635267066bd676f6edc0189");
        }
    }

    #[test]
    fn normalize_header_value_no_internal_whitespace() {
        let mut ans = String::new();
        // leading/trailing spaces should be trimmed, no internal whitespace => fast path
        normalize_header_value(&mut ans, "  value  ");
        assert_eq!(ans, "value");
    }

    #[test]
    fn normalize_header_value_collapse_whitespace() {
        let mut ans = String::new();
        // multiple spaces, tabs and newlines should collapse into single spaces
        normalize_header_value(&mut ans, "  foo   bar\tbaz\nqux  ");
        assert_eq!(ans, "foo bar baz qux");
    }

    #[test]
    fn normalize_header_value_only_spaces() {
        let mut ans = String::new();
        // value with only whitespace becomes empty string after trimming
        normalize_header_value(&mut ans, "    ");
        assert_eq!(ans, "");
    }

    #[test]
    fn example_put_presigned_url() {
        // Test PUT presigned URL signing - similar to GET but with PUT method
        // This is used for uploading files to S3 using presigned URLs
        // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let method = Method::PUT;
        let headers = OrderedHeaders::from_slice_unchecked(&[("host", "examplebucket.s3.amazonaws.com")]);

        // Query strings for signing (without signature - signature is computed from these)
        let query_strings_for_signing = &[
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-Expires", "86400"),
            ("X-Amz-SignedHeaders", "host"),
        ];

        let canonical_request = create_presigned_canonical_request(&method, "/test.txt", query_strings_for_signing, &headers);

        // Canonical request for PUT should be similar to GET, just with PUT method
        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/test.txt\n",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );

        let amz_date = AmzDate::parse("20130524T000000Z").unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &amz_date, "us-east-1", "s3");
        let signature = calculate_signature(&string_to_sign, &secret_access_key, &amz_date, "us-east-1", "s3");

        // Signature value derived from the above test inputs (not from official AWS test vectors)
        assert_eq!(signature, "f4db56459304dafaa603a99a23c6bea8821890259a65c18ff503a4a72a80efd9");
    }

    #[test]
    fn example_put_presigned_url_with_content_type() {
        // Test PUT presigned URL with content-type signed header
        // When content-type is in signed headers, it must match the request header exactly
        let secret_access_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        let method = Method::PUT;

        // Headers include content-type which is signed
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("content-type", "application/octet-stream"),
            ("host", "examplebucket.s3.amazonaws.com"),
        ]);

        let query_strings_for_signing = &[
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
            ("X-Amz-Date", "20130524T000000Z"),
            ("X-Amz-Expires", "86400"),
            ("X-Amz-SignedHeaders", "content-type;host"),
        ];

        let canonical_request = create_presigned_canonical_request(&method, "/test.txt", query_strings_for_signing, &headers);

        // Canonical request should include content-type header
        assert_eq!(
            canonical_request,
            concat!(
                "PUT\n",
                "/test.txt\n",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=content-type%3Bhost\n",
                "content-type:application/octet-stream\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "content-type;host\n",
                "UNSIGNED-PAYLOAD",
            )
        );

        let amz_date = AmzDate::parse("20130524T000000Z").unwrap();
        let string_to_sign = create_string_to_sign(&canonical_request, &amz_date, "us-east-1", "s3");
        let signature = calculate_signature(&string_to_sign, &secret_access_key, &amz_date, "us-east-1", "s3");

        // Signature value derived from the test inputs above; not from official AWS test vectors.
        assert_eq!(signature, "fd31b71961609f4b313497cb07ab0aedd268863bd547cc198db23cf04b8f663d");
    }

    #[test]
    fn multi_value_headers_combined_with_comma() {
        // Test that multiple headers with the same name are combined into a single line
        // with values separated by commas, as per AWS SigV4 spec.
        // This matches the behavior of AWS SDK Go which sends multiple x-amz-object-attributes headers.
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "127.0.0.1:9001"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20251205T145918Z"),
            ("x-amz-object-attributes", "ETag"),
            ("x-amz-object-attributes", "ObjectSize"),
            ("x-amz-object-attributes", "StorageClass"),
        ]);

        let method = Method::GET;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, "/bucket/key", qs, &headers, Payload::empty());

        // According to AWS SigV4 spec:
        // - Multiple headers with the same name should be combined with comma-separated values
        // - Each header name should appear only once in SignedHeaders
        assert_eq!(
            canonical_request,
            concat!(
                "GET\n",
                "/bucket/key\n",
                "\n",
                "host:127.0.0.1:9001\n",
                "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n",
                "x-amz-date:20251205T145918Z\n",
                "x-amz-object-attributes:ETag,ObjectSize,StorageClass\n",
                "\n",
                "host;x-amz-content-sha256;x-amz-date;x-amz-object-attributes\n",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
        );
    }

    #[test]
    fn multi_value_headers_presigned_url() {
        // Test that presigned URL canonical request also handles multi-value headers correctly
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "s3.amazonaws.com"),
            ("x-amz-object-attributes", "ETag"),
            ("x-amz-object-attributes", "ObjectSize"),
        ]);

        let method = Method::GET;
        let qs: &[(String, String)] = &[
            ("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string()),
            (
                "X-Amz-Credential".to_string(),
                "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
            ),
        ];

        let canonical_request = create_presigned_canonical_request(&method, "/bucket/key", qs, &headers);

        // Verify that x-amz-object-attributes values are comma-separated
        // and the header name appears only once in SignedHeaders
        assert!(canonical_request.contains("x-amz-object-attributes:ETag,ObjectSize\n"));
        assert!(canonical_request.contains(";x-amz-object-attributes\n"));
        // Make sure the header name doesn't appear twice in SignedHeaders
        let signed_headers_line = canonical_request.lines().find(|l| l.contains(';')).unwrap();
        let count = signed_headers_line.matches("x-amz-object-attributes").count();
        assert_eq!(count, 1, "x-amz-object-attributes should appear only once in SignedHeaders");
    }

    #[test]
    fn multi_value_headers_with_whitespace_normalization() {
        // Test that multi-value headers also have their values normalized (whitespace trimmed/collapsed)
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("host", "s3.amazonaws.com"),
            ("x-amz-meta-custom", "  value1  "),
            ("x-amz-meta-custom", "value2   with   spaces"),
        ]);

        let method = Method::GET;
        let qs: &[(String, String)] = &[];

        let canonical_request = create_canonical_request(&method, "/bucket/key", qs, &headers, Payload::empty());

        // Both values should be normalized and combined with comma
        assert!(canonical_request.contains("x-amz-meta-custom:value1,value2 with spaces\n"));
    }
}
