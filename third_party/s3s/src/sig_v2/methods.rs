use crate::auth::SecretKey;
use crate::http::OrderedHeaders;
use crate::http::OrderedQs;
use crate::utils::crypto::hmac_sha1;

use std::ops::Not;

use hyper::Method;

fn base64(data: impl AsRef<[u8]>) -> String {
    base64_simd::STANDARD.encode_to_string(data)
}

pub fn calculate_signature(secret_key: &SecretKey, string_to_sign: &str) -> String {
    base64(hmac_sha1(secret_key.expose(), string_to_sign))
}

const INCLUDED_QUERY: &[&str] = &[
    "accelerate",
    "acl",
    "analytics",
    "cors",
    "defaultObjectAcl",
    "delete",
    "inventory",
    "lifecycle",
    "location",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "partNumber",
    "policy",
    "replication",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "restore",
    "select",
    "select-type",
    "storageClass",
    "tagging",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
];

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    HeaderAuth,
    PresignedUrl,
}

pub fn create_string_to_sign(
    mode: Mode,
    method: &Method,
    uri_path: &str,
    qs: Option<&OrderedQs>,
    headers: &OrderedHeaders<'_>,
    virtual_host_bucket: Option<&str>,
) -> String {
    let mut ans = String::with_capacity(256);

    {
        // {HTTP-Verb}\n
        ans.push_str(method.as_str());
        ans.push('\n');
    }

    {
        // {Content-MD5}\n
        if let Some(v) = headers.get_unique("content-md5") {
            ans.push_str(v);
        }
        ans.push('\n');
    }

    {
        // {Content-Type}\n
        if let Some(v) = headers.get_unique("content-type") {
            ans.push_str(v);
        }
        ans.push('\n');
    }

    match mode {
        // {Date}\n
        Mode::HeaderAuth => {
            //  "if you include the x-amz-date header, use the empty string
            //      for the Date when constructing the StringToSign."
            let mut date = headers.get_unique("date").unwrap_or_default();
            if headers.get_unique("x-amz-date").is_some() {
                date = "";
            }
            ans.push_str(date);
            ans.push('\n');
        }
        // {Expires}\n
        Mode::PresignedUrl => {
            let expires = qs.and_then(|qs| qs.get_unique("Expires")).unwrap_or_default();
            ans.push_str(expires);
            ans.push('\n');
        }
    }

    {
        // {CanonicalizedAmzHeaders}
        let mut last = "";
        for &(name, _) in headers.as_ref() {
            if name.starts_with("x-amz-").not() {
                continue;
            }
            if name == last {
                continue;
            }
            last = name;

            ans.push_str(name);
            ans.push(':');

            let mut iter = headers.get_all(name);
            let first = iter.next().unwrap();
            ans.push_str(first.trim());

            for value in iter {
                ans.push(',');
                ans.push_str(value.trim());
            }

            ans.push('\n');
        }
    }

    {
        // {CanonicalizedResource}

        if let Some(bucket) = virtual_host_bucket {
            ans.push('/');
            ans.push_str(bucket);
        }

        ans.push_str(uri_path);

        if let Some(qs) = qs {
            let mut is_first = true;
            for q in INCLUDED_QUERY {
                if let Some(v) = qs.get_unique(q) {
                    if is_first {
                        ans.push('?');
                        is_first = false;
                    } else {
                        ans.push('&');
                    }
                    ans.push_str(q);
                    if v.is_empty().not() {
                        ans.push('=');
                        ans.push_str(v);
                    }
                }
            }
        }
    }

    ans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted() {
        for w in INCLUDED_QUERY.windows(2) {
            assert!(w[0] < w[1], "{w:?}");
        }
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn examples() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        {
            // Object GET
            let method = &Method::GET;
            let uri_path = "/photos/puppy.jpg";
            let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Tue, 27 Mar 2007 19:36:42 +0000")]);
            let qs = None;
            let vh_bucket = Some("awsexamplebucket1");

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "Tue, 27 Mar 2007 19:36:42 +0000\n",
                    "/awsexamplebucket1/photos/puppy.jpg",
                )
            );

            assert_eq!(signature, "qgk2+6Sv9/oM7G3qLEjTH1a1l1g=");
        }

        {
            // Object PUT
            let method = &Method::PUT;
            let uri_path = "/photos/puppy.jpg";
            let headers = OrderedHeaders::from_slice_unchecked(&[
                ("content-type", "image/jpeg"),
                ("date", "Tue, 27 Mar 2007 21:15:45 +0000"),
            ]);
            let qs = None;
            let vh_bucket = Some("awsexamplebucket1");

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "PUT\n",
                    "\n",
                    "image/jpeg\n",
                    "Tue, 27 Mar 2007 21:15:45 +0000\n",
                    "/awsexamplebucket1/photos/puppy.jpg",
                )
            );

            assert_eq!(signature, "iqRzw+ileNPu1fhspnRs8nOjjIA=");
        }

        {
            // List
            let method = &Method::GET;
            let uri_path = "/";
            let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Tue, 27 Mar 2007 19:42:41 +0000")]);
            let qs = None;
            let vh_bucket = Some("awsexamplebucket1");

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "Tue, 27 Mar 2007 19:42:41 +0000\n", //
                    "/awsexamplebucket1/",
                )
            );

            assert_eq!(signature, "m0WP8eCtspQl5Ahe6L1SozdX9YA=");
        }

        {
            // Fetch
            let method = &Method::GET;
            let uri_path = "/";
            let qs = OrderedQs::from_vec_unchecked(vec![("acl".into(), String::new())]);
            let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Tue, 27 Mar 2007 19:44:46 +0000")]);
            let vh_bucket = Some("awsexamplebucket1");

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, Some(&qs), &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "Tue, 27 Mar 2007 19:44:46 +0000\n", //
                    "/awsexamplebucket1/?acl",
                )
            );

            assert_eq!(signature, "82ZHiFIjc+WbcwFKGUVEQspPn+0=");
        }

        {
            // Delete
            let method = &Method::DELETE;
            let uri_path = "/awsexamplebucket1/photos/puppy.jpg";
            let headers = OrderedHeaders::from_slice_unchecked(&[
                ("date", "Tue, 27 Mar 2007 21:20:27 +0000"),
                ("x-amz-date", "Tue, 27 Mar 2007 21:20:26 +0000"),
            ]);
            let qs = None;
            let vh_bucket = None;

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "DELETE\n",
                    "\n",
                    "\n",
                    "\n",
                    "x-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n",
                    "/awsexamplebucket1/photos/puppy.jpg",
                )
            );

            // FIXME: The example is wrong?
            // assert_eq!(signature, "XbyTlbQdu9Xw5o8P4iMwPktxQd8=");
            assert_eq!(signature, "Ri1hpB1zpS9pGqR7y8kuNFCl4sE=");
        }

        {
            // Upload
            let method = &Method::PUT;
            let uri_path = "/db-backup.dat.gz";
            let headers = OrderedHeaders::from_slice_unchecked(&[
                ("date", "Tue, 27 Mar 2007 21:06:08 +0000"),
                ("x-amz-acl", "public-read"),
                ("content-type", "application/x-download"),
                ("content-md5", "4gJE4saaMU4BqNR0kLY+lw=="),
                ("x-amz-meta-reviewedby", "joe@example.com"),
                ("x-amz-meta-reviewedby", "jane@example.com"),
                ("x-amz-meta-filechecksum", "0x02661779"),
                ("x-amz-meta-checksumalgorithm", "crc32"),
                ("content-disposition", "attachment; filename=database.dat"),
                ("content-encoding", "gzip"),
                ("content-length", "5913339"),
            ]);
            let qs = None;
            let vh_bucket = Some("static.example.com");

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "PUT\n",
                    "4gJE4saaMU4BqNR0kLY+lw==\n",
                    "application/x-download\n",
                    "Tue, 27 Mar 2007 21:06:08 +0000\n",
                    "x-amz-acl:public-read\n",
                    "x-amz-meta-checksumalgorithm:crc32\n",
                    "x-amz-meta-filechecksum:0x02661779\n",
                    "x-amz-meta-reviewedby:joe@example.com,jane@example.com\n",
                    "/static.example.com/db-backup.dat.gz",
                )
            );

            // assert_eq!(signature, "dKZcB+bz2EPXgSdXZp9ozGeOM4I="); // The example is wrong?
            assert_eq!(signature, "jtBQa0Aq+DkULFI8qrpwIjGEx0E=");
        }

        {
            // List all my buckets
            let method = &Method::GET;
            let uri_path = "/";
            let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Wed, 28 Mar 2007 01:29:59 +0000")]);
            let qs = None;
            let vh_bucket = None;

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "Wed, 28 Mar 2007 01:29:59 +0000\n", //
                    "/",
                )
            );

            assert_eq!(signature, "qGdzdERIC03wnaRNKh6OqZehG9s=");
        }

        {
            // Unicode keys
            let method = &Method::GET;
            let uri_path = "/dictionary/fran%C3%A7ais/pr%c3%a9f%c3%a8re";
            let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Wed, 28 Mar 2007 01:49:49 +0000")]);
            let qs = None;
            let vh_bucket = None;

            let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "Wed, 28 Mar 2007 01:49:49 +0000\n",
                    "/dictionary/fran%C3%A7ais/pr%c3%a9f%c3%a8re",
                )
            );

            assert_eq!(signature, "DNEZGsoieTZ92F3bUfSPQcbGmlM=");
        }

        {
            // Query string request authentication
            let method = &Method::GET;
            let uri_path = "/photos/puppy.jpg";
            let headers = OrderedHeaders::default();
            let qs = OrderedQs::parse(concat!(
                "AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE",
                // "&Signature=NpgCjnDzrM%2BWFzoENXmpNDUsSn8%3D", // The example is wrong?
                "&Signature=1No4mq5ETf02z8aet9voy6gui6E%3D",
                "&Expires=1175139620",
            ))
            .unwrap();
            let vh_bucket = Some("awsexamplebucket1");

            let presigned_url = super::super::PresignedUrlV2::parse(&qs).unwrap();
            assert_eq!(presigned_url.access_key, access_key);

            let string_to_sign = create_string_to_sign(Mode::PresignedUrl, method, uri_path, Some(&qs), &headers, vh_bucket);
            let signature = calculate_signature(&secret_key, &string_to_sign);

            assert_eq!(
                string_to_sign,
                concat!(
                    "GET\n",
                    "\n",
                    "\n",
                    "1175139620\n", //
                    "/awsexamplebucket1/photos/puppy.jpg",
                )
            );

            assert_eq!(signature, presigned_url.signature);
        }
    }

    /// Regression test for <https://github.com/s3s-project/s3s/issues/137>
    ///
    /// When `x-amz-date` is present:
    /// - The Date field in the string-to-sign must be empty
    /// - `x-amz-date` must be included in `CanonicalizedAmzHeaders`
    /// - The URI path is used directly (not parsed into `S3Path`)
    #[test]
    fn regression_sig_v2_x_amz_date() {
        let secret_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        // Path-style request with x-amz-date: date field must be empty,
        // and x-amz-date must appear in canonicalized headers
        let method = &Method::GET;
        let uri_path = "/mybucket/myobject";
        let headers = OrderedHeaders::from_slice_unchecked(&[
            ("date", "Thu, 14 Mar 2024 12:00:00 +0000"),
            ("x-amz-date", "Thu, 14 Mar 2024 12:00:00 +0000"),
        ]);
        let qs = None;
        let vh_bucket = None;

        let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);

        // Date field must be empty when x-amz-date is present
        assert_eq!(
            string_to_sign,
            concat!(
                "GET\n",
                "\n",
                "\n",
                "\n", // empty date because x-amz-date is present
                "x-amz-date:Thu, 14 Mar 2024 12:00:00 +0000\n",
                "/mybucket/myobject",
            )
        );

        // Sanity-check: a non-empty signature is produced for this input
        let sig = calculate_signature(&secret_key, &string_to_sign);
        assert!(!sig.is_empty());
    }

    /// Regression test for <https://github.com/s3s-project/s3s/issues/137>
    ///
    /// Virtual-hosted-style: bucket must be prepended to the canonicalized resource
    #[test]
    fn regression_sig_v2_virtual_hosted_bucket() {
        let secret_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        // Virtual-hosted-style: URI path is "/key" and bucket is prepended
        let method = &Method::GET;
        let uri_path = "/myobject";
        let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Thu, 14 Mar 2024 12:00:00 +0000")]);
        let qs = None;
        let vh_bucket = Some("mybucket");

        let string_to_sign = create_string_to_sign(Mode::HeaderAuth, method, uri_path, qs, &headers, vh_bucket);

        assert_eq!(
            string_to_sign,
            concat!("GET\n", "\n", "\n", "Thu, 14 Mar 2024 12:00:00 +0000\n", "/mybucket/myobject",)
        );

        // Sanity-check: a non-empty signature is produced for this input
        let sig = calculate_signature(&secret_key, &string_to_sign);
        assert!(!sig.is_empty());
    }

    /// Regression test for <https://github.com/s3s-project/s3s/issues/137>
    ///
    /// `PresignedUrl` mode: Expires field must come from query string
    #[test]
    fn regression_sig_v2_presigned_expires() {
        let secret_key = SecretKey::from("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        let method = &Method::GET;
        let uri_path = "/myobject";
        let headers = OrderedHeaders::from_slice_unchecked(&[("date", "Thu, 14 Mar 2024 12:00:00 +0000")]);
        let qs = OrderedQs::from_vec_unchecked(vec![("Expires".into(), "1710417600".into())]);
        let vh_bucket = Some("mybucket");

        let string_to_sign = create_string_to_sign(Mode::PresignedUrl, method, uri_path, Some(&qs), &headers, vh_bucket);

        // In presigned URL mode, the date line should be the Expires value from the query string
        assert_eq!(
            string_to_sign,
            concat!(
                "GET\n",
                "\n",
                "\n",
                "1710417600\n", // Expires from query string, not Date header
                "/mybucket/myobject",
            )
        );

        // Sanity-check: a non-empty signature is produced for this input
        let sig = calculate_signature(&secret_key, &string_to_sign);
        assert!(!sig.is_empty());
    }
}
