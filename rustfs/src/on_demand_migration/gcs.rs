// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native Google Cloud Storage source backend.
//!
//! The `gcs` provider already reaches GCS through its S3 interoperability API,
//! which needs an HMAC key pair. This backend is the other half: it authorizes
//! with a service-account key, the credential most GCS projects actually issue,
//! by minting OAuth tokens through the shared `google-cloud-auth` credential
//! machinery the tier layer already uses.
//!
//! Two GCS surfaces are involved, each for the half it describes best. The read
//! path uses the XML API (`/{bucket}/{object}`), whose responses carry
//! `x-goog-meta-*` user metadata and the `x-goog-hash` digest in one round trip.
//! Listing uses the JSON API (`objects.list`), whose `pageToken` maps directly
//! onto the shared page cursor and whose `prefixes` are the delimiter roll-up.
//! Both accept the same bearer token.
//!
//! Every call this backend makes needs only `storage.objects.get` and
//! `storage.objects.list`, the two permissions of the `objectViewer` role, so a
//! key scoped to exactly the migration's needs works.
//!
//! `x-goog-hash` carries a base64 MD5 for every non-composite object; it is
//! converted to hex and becomes the head's ETag, so a pulled object is checked
//! against the digest GCS itself computed. A composite object has no MD5, and
//! its ETag is then marked opaque rather than checked.

use super::native_http::{
    NativeHeadFields, NativeHttp, base64_md5_to_hex, header, native_source_head, parse_http_timestamp, read_text, response_body,
};
use super::source_client::{
    GcsSourceSpec, SourceBackend, SourceError, SourceGet, SourceHead, SourceListRequest, SourceObject, SourcePage,
    SourceTimeouts, range_header_value,
};
use super::storage_api::HTTPRangeSpec;
use super::storage_api::remote_s3_client::RemoteS3ClientError;
use google_cloud_auth::credentials::service_account::{AccessSpecifier, Builder as ServiceAccountBuilder};
use google_cloud_auth::credentials::{CacheableResource, Credentials};
use http::{HeaderMap, HeaderValue, Method};
use serde::Deserialize;
use std::collections::HashMap;
use url::Url;

/// Read-only object scope: this backend never writes to the source.
const READ_ONLY_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_only";
const METADATA_PREFIX: &str = "x-goog-meta-";
/// One `objects.list` page is small; refuse an unbounded document.
const MAX_JSON_BYTES: usize = 8 * 1024 * 1024;

pub struct GcsNativeSourceBackend {
    http: NativeHttp,
    bucket: String,
    credentials: Credentials,
}

impl GcsNativeSourceBackend {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        spec: &GcsSourceSpec,
        timeouts: SourceTimeouts,
        skip_tls_verify: bool,
        ca_cert_pem: Option<&str>,
    ) -> Result<Self, RemoteS3ClientError> {
        let key: serde_json::Value = serde_json::from_str(&spec.service_account_json)
            .map_err(|_| RemoteS3ClientError::Credentials("gcs service account key is not valid JSON"))?;
        let credentials = ServiceAccountBuilder::new(key)
            .with_access_specifier(AccessSpecifier::from_scopes([READ_ONLY_SCOPE]))
            .build()
            .map_err(|_| RemoteS3ClientError::Credentials("gcs service account key is not usable"))?;
        Ok(Self {
            http: NativeHttp::new(endpoint, timeouts, skip_tls_verify, ca_cert_pem)?,
            bucket: bucket.to_string(),
            credentials,
        })
    }

    /// Authorization headers for one request. A credential failure is reported
    /// as `AccessDenied` with no message: the renderer of a credential error
    /// has the key material in scope, and the class is what callers act on.
    async fn auth_headers(&self) -> Result<HeaderMap, SourceError> {
        match self.credentials.headers(http::Extensions::new()).await {
            Ok(CacheableResource::New { data, .. }) => Ok(data),
            // Only returned when the caller passes an entity tag, which this
            // backend never does; an empty set is still the honest answer.
            Ok(CacheableResource::NotModified) => Ok(HeaderMap::new()),
            Err(_) => Err(SourceError::AccessDenied),
        }
    }

    /// XML API URL of one object; `/` in the key stay path separators.
    fn object_url(&self, key: &str) -> Result<Url, SourceError> {
        self.http.url(std::iter::once(self.bucket.as_str()).chain(key.split('/')))
    }

    /// JSON API URL of the bucket's object collection.
    fn objects_url(&self) -> Result<Url, SourceError> {
        self.http.url(["storage", "v1", "b", self.bucket.as_str(), "o"])
    }

    async fn request(&self, method: Method, url: Url, mut headers: HeaderMap) -> Result<reqwest::Request, SourceError> {
        for (name, value) in self.auth_headers().await? {
            if let Some(name) = name {
                headers.insert(name, value);
            }
        }
        let mut request = reqwest::Request::new(method, url);
        *request.headers_mut() = headers;
        Ok(request)
    }

    async fn send_object(&self, request: reqwest::Request) -> Result<reqwest::Response, SourceError> {
        match self.http.send_object(request, None).await {
            Err(SourceError::NotFound) => {
                // An XML object URL also returns 404 when its bucket is gone.
                // Reuse the read-only listing probe before caching a key miss.
                self.probe().await?;
                Err(SourceError::NotFound)
            }
            result => result,
        }
    }

    /// Shared mapping for the XML API's HEAD and GET responses.
    fn head_from_response(headers: &HeaderMap) -> Result<SourceHead, SourceError> {
        if header(headers, "x-goog-encryption-key-sha256").is_some() {
            return Err(SourceError::Unsupported(
                "source object uses a customer-supplied encryption key; customer-key sources are not supported".to_string(),
            ));
        }
        // `x-goog-hash` lists digests as `name=base64`, comma separated, and may
        // repeat across header lines. Only the MD5 describes the whole object.
        let md5 = headers
            .get_all("x-goog-hash")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .filter_map(|digest| digest.trim().strip_prefix("md5="))
            .find_map(base64_md5_to_hex);

        let (etag, etag_is_opaque) = match md5 {
            Some(md5) => (Some(md5), false),
            // A composite object has no MD5; its ETag describes the composition
            // rather than the bytes, so it is provenance only.
            None => (header(headers, "etag").map(str::to_string), true),
        };
        native_source_head(
            headers,
            METADATA_PREFIX,
            NativeHeadFields {
                etag,
                etag_is_opaque,
                version_id: header(headers, "x-goog-generation").map(str::to_string),
                storage_class: header(headers, "x-goog-storage-class").map(str::to_string),
            },
        )
    }
}

#[async_trait::async_trait]
impl SourceBackend for GcsNativeSourceBackend {
    async fn head(&self, key: &str) -> Result<SourceHead, SourceError> {
        let request = self.request(Method::HEAD, self.object_url(key)?, HeaderMap::new()).await?;
        let response = self.send_object(request).await?;
        Self::head_from_response(response.headers())
    }

    async fn get(&self, key: &str, range: Option<&HTTPRangeSpec>) -> Result<SourceGet, SourceError> {
        let mut headers = HeaderMap::new();
        if let Some(range) = range.map(range_header_value).transpose()? {
            headers.insert(
                http::header::RANGE,
                HeaderValue::from_str(&range).map_err(|_| SourceError::Other("invalid range header".to_string()))?,
            );
        }
        let request = self.request(Method::GET, self.object_url(key)?, headers).await?;
        let response = self.send_object(request).await?;
        let head = Self::head_from_response(response.headers())?;
        let content_range = header(response.headers(), "content-range").map(str::to_string);
        Ok(SourceGet {
            head,
            body: response_body(response),
            content_range,
        })
    }

    async fn list(&self, request: &SourceListRequest<'_>) -> Result<SourcePage, SourceError> {
        // `objects.list` offers `startOffset`, which is inclusive, so it cannot
        // express "resume after this key" without silently repeating it.
        if request.start_after.is_some() {
            return Err(SourceError::Unsupported(
                "gcs sources cannot resume a listing from a key; use the continuation token".to_string(),
            ));
        }
        let mut url = self.objects_url()?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(prefix) = request.prefix.filter(|prefix| !prefix.is_empty()) {
                query.append_pair("prefix", prefix);
            }
            if let Some(delimiter) = request.delimiter.filter(|delimiter| !delimiter.is_empty()) {
                query.append_pair("delimiter", delimiter);
            }
            if let Some(token) = request.continuation_token.filter(|token| !token.is_empty()) {
                query.append_pair("pageToken", token);
            }
            if request.max_keys > 0 {
                query.append_pair("maxResults", &request.max_keys.to_string());
            }
        }

        let request = self.request(Method::GET, url, HeaderMap::new()).await?;
        let response = self.http.send(request, None).await?;
        let body = read_text(response, MAX_JSON_BYTES).await?;
        parse_objects_list(&body)
    }

    /// GCS has no object tagging API; user metadata is already carried by the
    /// head mapping. An empty map keeps `policy.copy_tags` from failing a pull
    /// over a concept the provider does not have.
    async fn tagging(&self, _key: &str) -> Result<HashMap<String, String>, SourceError> {
        Ok(HashMap::new())
    }

    /// A one-object listing, not `buckets.get`: the migration pipeline only
    /// ever needs `storage.objects.list` and `storage.objects.get`, and a key
    /// scoped to exactly those (the `objectViewer` role) cannot read the bucket
    /// resource. Probing with `buckets.get` would reject a correct key.
    async fn probe(&self) -> Result<(), SourceError> {
        let mut url = self.objects_url()?;
        url.query_pairs_mut().append_pair("maxResults", "1");
        let request = self.request(Method::GET, url, HeaderMap::new()).await?;
        let response = self.http.send(request, None).await?;
        read_text(response, MAX_JSON_BYTES)
            .await
            .and_then(|body| parse_objects_list(&body))?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObjectsList {
    #[serde(default)]
    items: Vec<ListedObject>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedObject {
    name: String,
    /// GCS renders the size as a decimal string, not a JSON number.
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    updated: Option<String>,
    #[serde(default)]
    md5_hash: Option<String>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    storage_class: Option<String>,
}

fn parse_objects_list(body: &str) -> Result<SourcePage, SourceError> {
    let listing: ObjectsList =
        serde_json::from_str(body).map_err(|err| SourceError::Other(format!("source listing is not valid JSON: {err}")))?;
    let next_continuation_token = listing.next_page_token.filter(|token| !token.is_empty());
    let objects = listing
        .items
        .into_iter()
        .map(|item| {
            if item.name.is_empty() {
                return Err(SourceError::Other("source listing object has no name".to_string()));
            }
            let size = item
                .size
                .and_then(|size| size.parse::<u64>().ok())
                .ok_or_else(|| SourceError::Other("source listing object has no valid size".to_string()))?;
            let etag = item
                .md5_hash
                .as_deref()
                .and_then(base64_md5_to_hex)
                .or_else(|| item.etag.map(|etag| etag.trim_matches('"').to_string()))
                .filter(|etag| !etag.is_empty());
            Ok(SourceObject {
                key: item.name,
                etag,
                size,
                last_modified: item.updated.as_deref().and_then(parse_http_timestamp),
                storage_class: item.storage_class,
                // GCS never encodes a part count in a digest or an ETag.
                is_multipart_etag: false,
            })
        })
        .collect::<Result<_, SourceError>>()?;

    Ok(SourcePage {
        objects,
        common_prefixes: listing.prefixes,
        is_truncated: next_continuation_token.is_some(),
        next_continuation_token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::on_demand_migration::backend_contract::{BackendCapabilities, assert_backend_contract};
    use crate::on_demand_migration::test_http_fixture::{ScriptedResponse, assert_requests, scripted_server};
    use google_cloud_auth::credentials::anonymous::Builder as AnonymousBuilder;

    const LIST_PAGE_ONE: &str = r#"{
      "kind": "storage#objects",
      "nextPageToken": "cursor-1",
      "prefixes": ["dir/sub/"],
      "items": [
        {
          "name": "dir/a.txt",
          "size": "5",
          "updated": "2015-10-21T07:28:00.000Z",
          "md5Hash": "XUFAKrxLKna5cZ2REBfFkg==",
          "etag": "CJizy9Wq0McCEAE=",
          "storageClass": "STANDARD"
        }
      ]
    }"#;

    const LIST_PAGE_TWO: &str = r#"{
      "kind": "storage#objects",
      "items": [
        {
          "name": "dir/b.txt",
          "size": "7",
          "updated": "2015-10-21T07:28:00.000Z",
          "etag": "\"CJizy9Wq0McCEAI=\""
        }
      ]
    }"#;

    fn backend(endpoint: &Url) -> GcsNativeSourceBackend {
        GcsNativeSourceBackend {
            http: NativeHttp::for_test(endpoint.clone()),
            bucket: "legacy".to_string(),
            // Anonymous credentials add no headers, so the fixture sees exactly
            // the request this backend builds.
            credentials: AnonymousBuilder::new().build(),
        }
    }

    fn object_headers() -> Vec<(&'static str, String)> {
        vec![
            ("Content-Type", "text/plain".to_string()),
            ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT".to_string()),
            ("ETag", "\"CJizy9Wq0McCEAE=\"".to_string()),
            ("x-goog-hash", "crc32c=AAAAAA==,md5=XUFAKrxLKna5cZ2REBfFkg==".to_string()),
            ("x-goog-meta-owner", "alice".to_string()),
            ("x-goog-storage-class", "STANDARD".to_string()),
            ("x-goog-generation", "1445412480000000".to_string()),
        ]
    }

    #[test]
    fn objects_list_maps_items_prefixes_and_the_page_token() {
        let page = parse_objects_list(LIST_PAGE_ONE).expect("page should parse");
        assert_eq!(page.common_prefixes, vec!["dir/sub/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("cursor-1"));
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "dir/a.txt");
        assert_eq!(page.objects[0].size, 5, "the string size is parsed");
        assert_eq!(
            page.objects[0].etag.as_deref(),
            Some("5d41402abc4b2a76b9719d911017c592"),
            "the base64 md5Hash becomes a hex ETag"
        );
        assert_eq!(page.objects[0].storage_class.as_deref(), Some("STANDARD"));
        assert!(page.objects[0].last_modified.is_some(), "RFC 3339 `updated` is parsed");

        let page = parse_objects_list(LIST_PAGE_TWO).expect("page should parse");
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());
        assert_eq!(
            page.objects[0].etag.as_deref(),
            Some("CJizy9Wq0McCEAI="),
            "without md5Hash the raw etag is carried"
        );

        assert!(parse_objects_list("not json").is_err());
    }

    #[tokio::test]
    async fn head_prefers_the_goog_hash_md5_over_the_etag() {
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, object_headers(), String::new())]).await;
        let head = backend(&endpoint).head("dir/a b.txt").await.expect("HEAD should map");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert_eq!(recorded[0].method, "HEAD");
        assert_eq!(recorded[0].target, "/legacy/dir/a%20b.txt", "the XML API addresses the object by path");
        assert_eq!(
            head.etag.as_deref(),
            Some("5d41402abc4b2a76b9719d911017c592"),
            "the x-goog-hash md5 is the content digest"
        );
        assert!(!head.etag_is_opaque, "a GCS md5 may be checked against the pulled bytes");
        assert_eq!(head.user_metadata, HashMap::from([("owner".to_string(), "alice".to_string())]));
        assert_eq!(head.version_id.as_deref(), Some("1445412480000000"));
        assert_eq!(head.storage_class.as_deref(), Some("STANDARD"));
    }

    #[tokio::test]
    async fn a_composite_object_without_an_md5_keeps_an_opaque_etag() {
        let headers = object_headers()
            .into_iter()
            .map(|(name, value)| {
                if name == "x-goog-hash" {
                    (name, "crc32c=AAAAAA==".to_string())
                } else {
                    (name, value)
                }
            })
            .collect();
        let (endpoint, _) = scripted_server(vec![ScriptedResponse::new(200, headers, String::new())]).await;
        let head = backend(&endpoint).head("composed").await.expect("HEAD should map");
        assert_eq!(head.etag.as_deref(), Some("CJizy9Wq0McCEAE="));
        assert!(head.etag_is_opaque, "a composite ETag describes the composition, not the bytes");
    }

    #[tokio::test]
    async fn customer_supplied_key_objects_are_refused() {
        let mut headers = object_headers();
        headers.push(("x-goog-encryption-key-sha256", "abc".to_string()));
        let (endpoint, _) = scripted_server(vec![ScriptedResponse::new(200, headers, String::new())]).await;
        let err = backend(&endpoint)
            .head("a.txt")
            .await
            .expect_err("CSEK objects are unsupported");
        assert!(matches!(err, SourceError::Unsupported(_)), "{err:?}");
    }

    #[tokio::test]
    async fn list_and_probe_address_the_json_api() {
        let (endpoint, recorded) = scripted_server(vec![
            ScriptedResponse::new(200, Vec::new(), LIST_PAGE_ONE.to_string()),
            ScriptedResponse::new(200, Vec::new(), "{}".to_string()),
        ])
        .await;
        let backend = backend(&endpoint);

        backend
            .list(&SourceListRequest {
                prefix: Some("dir/"),
                delimiter: Some("/"),
                continuation_token: Some("cursor-0"),
                max_keys: 2,
                ..Default::default()
            })
            .await
            .expect("listing should succeed");
        backend.probe().await.expect("probe should succeed");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert!(recorded[0].target.starts_with("/storage/v1/b/legacy/o?"), "{}", recorded[0].target);
        assert!(recorded[0].target.contains("prefix=dir%2F"), "{}", recorded[0].target);
        assert!(recorded[0].target.contains("delimiter=%2F"), "{}", recorded[0].target);
        assert!(recorded[0].target.contains("pageToken=cursor-0"), "{}", recorded[0].target);
        assert!(recorded[0].target.contains("maxResults=2"), "{}", recorded[0].target);
        assert_eq!(
            recorded[1].target, "/storage/v1/b/legacy/o?maxResults=1",
            "the probe uses the listing permission the pipeline already needs"
        );
    }

    #[tokio::test]
    async fn gcs_native_backend_satisfies_the_shared_backend_contract() {
        let mut ranged = object_headers();
        ranged.push(("Content-Range", "bytes 1-3/5".to_string()));
        // A HEAD reports the object size with no body, exactly as GCS does.
        let mut head_only = object_headers();
        head_only.push(("Content-Length", "5".to_string()));
        let (endpoint, _) = scripted_server(vec![
            ScriptedResponse::new(200, head_only, String::new()),
            ScriptedResponse::new(200, object_headers(), "hello".to_string()),
            ScriptedResponse::new(206, ranged, "ell".to_string()),
            ScriptedResponse::new(200, Vec::new(), LIST_PAGE_ONE.to_string()),
            ScriptedResponse::new(200, Vec::new(), LIST_PAGE_TWO.to_string()),
            // GCS has no tagging call, so the contract's tag step issues no
            // request; the probe is the next one on the wire.
            ScriptedResponse::new(200, Vec::new(), "{}".to_string()),
            ScriptedResponse::new(404, Vec::new(), String::new()),
            ScriptedResponse::new(200, Vec::new(), "{}".to_string()),
            ScriptedResponse::new(403, Vec::new(), String::new()),
        ])
        .await;

        assert_backend_contract(
            &backend(&endpoint),
            BackendCapabilities {
                etag_is_opaque: false,
                supports_start_after: false,
                // GCS objects have no tags; the contract's tag step is skipped.
                supports_tagging: false,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn listing_404_is_not_an_object_not_found() {
        let (endpoint, _) = scripted_server(vec![ScriptedResponse::new(404, Vec::new(), String::new())]).await;
        let err = backend(&endpoint)
            .list(&SourceListRequest {
                max_keys: 1,
                ..Default::default()
            })
            .await
            .expect_err("a failed bucket listing is not a per-object miss");
        assert_eq!(err.class_label(), "other", "{err:?}");
    }

    #[tokio::test]
    async fn object_404_requires_a_readable_source_bucket() {
        for method in [Method::HEAD, Method::GET] {
            for (probe_status, expected_class) in [
                (200, "not_found"),
                (404, "other"),
                (403, "access_denied"),
                (503, "throttled"),
                (500, "server_error"),
            ] {
                let (endpoint, recorded) = scripted_server(vec![
                    ScriptedResponse::new(404, Vec::new(), String::new()),
                    ScriptedResponse::new(probe_status, Vec::new(), "{}".to_string()),
                ])
                .await;
                let backend = backend(&endpoint);
                let result = if method == Method::HEAD {
                    backend.head("missing").await.map(|_| ())
                } else {
                    backend.get("missing", None).await.map(|_| ())
                };
                let error = result.expect_err("the object 404 must remain an error");
                assert_eq!(error.class_label(), expected_class, "{method} with probe HTTP {probe_status}: {error:?}");
                let recorded = recorded.lock().expect("recorder lock");
                assert_eq!(recorded.len(), 2, "one bounded read-only probe per ambiguous object miss");
                assert_eq!(recorded[0].method, method.as_str());
                assert_eq!(recorded[1].method, "GET");
                assert_eq!(recorded[1].target, "/storage/v1/b/legacy/o?maxResults=1");
            }
        }
    }

    #[tokio::test]
    async fn native_listing_rejects_missing_or_invalid_required_object_fields() {
        for entry in [
            r#"{"size":"1"}"#,
            r#"{"name":"","size":"1"}"#,
            r#"{"name":"broken"}"#,
            r#"{"name":"broken","size":null}"#,
            r#"{"name":"broken","size":""}"#,
            r#"{"name":"broken","size":"-1"}"#,
            r#"{"name":"broken","size":"18446744073709551616"}"#,
            r#"{"name":"broken","size":"not-a-size"}"#,
            r#"{"name":"broken","size":1}"#,
        ] {
            let body = format!(r#"{{"items":[{{"name":"valid","size":"1"}},{entry}],"nextPageToken":"next"}}"#);
            let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, Vec::new(), body)]).await;
            let err = backend(&endpoint)
                .list(&SourceListRequest {
                    prefix: Some("dir/"),
                    delimiter: Some("/"),
                    continuation_token: Some("opaque+/="),
                    max_keys: 2,
                    ..Default::default()
                })
                .await
                .expect_err("malformed object must reject the complete native page");
            assert!(matches!(err, SourceError::Other(_)), "{entry}: {err:?}");
            assert!(!err.is_retryable());
            assert_requests(
                &recorded,
                &[(
                    "GET",
                    "/storage/v1/b/legacy/o?prefix=dir%2F&delimiter=%2F&pageToken=opaque%2B%2F%3D&maxResults=2",
                )],
            );
        }
    }

    #[tokio::test]
    async fn native_listing_preserves_zero_size_unicode_prefixes_and_opaque_cursors() {
        let body = r#"{"items":[{"name":"目录/空 & file","size":"0"}],"prefixes":["目录/子/"],"nextPageToken":"opaque+/="}"#;
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, Vec::new(), body.to_string())]).await;
        let page = backend(&endpoint)
            .list(&SourceListRequest {
                max_keys: 2,
                ..Default::default()
            })
            .await
            .expect("valid native page");
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "目录/空 & file");
        assert_eq!(page.objects[0].size, 0);
        assert_eq!(page.common_prefixes, ["目录/子/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("opaque+/="));
        assert_requests(&recorded, &[("GET", "/storage/v1/b/legacy/o?maxResults=2")]);
    }

    #[tokio::test]
    async fn missing_object_head_requires_one_successful_bucket_probe() {
        for (status, body, expected, retryable) in [
            (200, "{}", "not_found", false),
            (403, "", "access_denied", false),
            (404, "", "other", false),
            (429, "", "throttled", true),
            (500, "", "server_error", true),
            (503, "", "throttled", true),
            (200, "not JSON", "other", false),
        ] {
            let (endpoint, recorded) = scripted_server(vec![
                ScriptedResponse::new(404, Vec::new(), String::new()),
                ScriptedResponse::new(status, Vec::new(), body.to_string()),
            ])
            .await;
            let err = backend(&endpoint).head("missing").await.expect_err("missing HEAD must fail");
            assert_eq!(err.class_label(), expected, "probe {status} {body:?}: {err:?}");
            assert_eq!(err.is_retryable(), retryable, "probe {status} {body:?}: {err:?}");
            if status == 500 {
                assert!(matches!(err, SourceError::ServerError(500)));
            }
            assert_requests(&recorded, &[("HEAD", "/legacy/missing"), ("GET", "/storage/v1/b/legacy/o?maxResults=1")]);
        }
    }

    #[tokio::test]
    async fn denied_object_reads_do_not_probe_or_become_object_absence() {
        for method in [Method::HEAD, Method::GET] {
            let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(
                403,
                vec![("x-goog-unused-error-code", "NoSuchKey".to_string())],
                "untrusted-error-body".to_string(),
            )])
            .await;
            let backend = backend(&endpoint);
            let result = if method == Method::HEAD {
                backend.head("missing").await.map(|_| ())
            } else {
                backend.get("missing", None).await.map(|_| ())
            };
            let err = result.expect_err("denied object read must remain a failure");
            assert_eq!(err.class_label(), "access_denied");
            assert!(!err.is_retryable());
            assert!(!err.to_string().contains("untrusted-error-body"));
            assert_requests(&recorded, &[(method.as_str(), "/legacy/missing")]);
        }
    }
    #[tokio::test]
    async fn non_object_errors_ignore_untrusted_error_code_headers() {
        for probe in [false, true] {
            for (status, expected, retryable) in [(403, "access_denied", false), (500, "server_error", true)] {
                let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(
                    status,
                    vec![("x-goog-unused-error-code", "NoSuchKey".to_string())],
                    "untrusted-error-body".to_string(),
                )])
                .await;
                let backend = backend(&endpoint);
                let result = if probe {
                    backend.probe().await
                } else {
                    backend
                        .list(&SourceListRequest {
                            max_keys: 2,
                            ..Default::default()
                        })
                        .await
                        .map(|_| ())
                };
                let err = result.expect_err("a synthetic provider header cannot change the source status");
                assert_eq!(err.class_label(), expected, "probe={probe} status={status}: {err:?}");
                assert_eq!(err.is_retryable(), retryable);
                assert!(!err.to_string().contains("untrusted-error-body"));
                if status == 500 {
                    assert!(matches!(err, SourceError::ServerError(500)));
                }
                assert_requests(
                    &recorded,
                    &[(
                        "GET",
                        if probe {
                            "/storage/v1/b/legacy/o?maxResults=1"
                        } else {
                            "/storage/v1/b/legacy/o?maxResults=2"
                        },
                    )],
                );
            }
        }
    }
}
