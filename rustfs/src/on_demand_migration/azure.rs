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

//! Native Azure Blob source backend.
//!
//! Azure has no S3 API, so this backend speaks the Blob REST service directly:
//! Get Blob / Get Blob Properties for the read path, List Blobs for the listing,
//! Get Blob Tags for the tags and Get Container Properties for the probe. The
//! local key is the blob name inside the container named by `source.bucket`.
//!
//! Two authorization schemes are supported, matching the two forms an operator
//! can hold: a storage-account key signed per request with Shared Key, and a SAS
//! token appended to the request query.
//!
//! Azure's ETag is a concurrency token, not a digest of the bytes, so every head
//! this backend produces is marked [`SourceHead::etag_is_opaque`]: the write-back
//! path records the value for provenance and refuses to check content against it.
//! `Content-MD5` is the only Azure digest, it is optional per blob, and it is not
//! mapped onto the ETag slot precisely so that the two never get confused.
//!
//! The anti-loop `source-proxy-request` markers the S3 backend sends are omitted:
//! they mean something only to a RustFS or MinIO source, and Azure would have to
//! carry them through Shared Key canonicalization for no gain.

use super::native_http::{
    NativeHeadFields, NativeHttp, header, native_source_head, parse_http_timestamp, read_text, response_body,
};
use super::source_client::{
    AzureAuth, AzureSourceSpec, SourceBackend, SourceError, SourceGet, SourceHead, SourceListRequest, SourceObject, SourcePage,
    SourceTimeouts, range_header_value,
};
use super::storage_api::HTTPRangeSpec;
use super::storage_api::remote_s3_client::RemoteS3ClientError;
use hmac::{Hmac, Mac, digest::KeyInit};
use http::{HeaderMap, HeaderValue, Method};
use quick_xml::Reader;
use quick_xml::events::Event;
use sha2::Sha256;
use std::collections::{BTreeMap, HashMap};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

/// Blob REST version this backend pins. Every response field it reads exists
/// from this version on, including blob tags and blob versioning.
const API_VERSION: &str = "2021-08-06";
const HEADER_VERSION: &str = "x-ms-version";
const HEADER_DATE: &str = "x-ms-date";
const HEADER_ERROR_CODE: &str = "x-ms-error-code";
const METADATA_PREFIX: &str = "x-ms-meta-";
/// A List Blobs or Get Blob Tags response is small; refuse a source that
/// streams an unbounded document at us instead of buffering it.
const MAX_XML_BYTES: usize = 8 * 1024 * 1024;

/// `Sun, 06 Nov 1994 08:49:37 GMT`, the only `x-ms-date` form Azure accepts.
const HTTP_DATE: &[time::format_description::BorrowedFormatItem<'static>] =
    time::macros::format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");

enum Credential {
    /// Decoded storage-account key.
    SharedKey(Vec<u8>),
    /// SAS parameters, decoded once so re-encoding cannot double-escape them.
    Sas(Vec<(String, String)>),
}

pub struct AzureSourceBackend {
    http: NativeHttp,
    account: String,
    container: String,
    credential: Credential,
}

impl AzureSourceBackend {
    pub fn new(
        endpoint: &str,
        container: &str,
        spec: &AzureSourceSpec,
        timeouts: SourceTimeouts,
        skip_tls_verify: bool,
        ca_cert_pem: Option<&str>,
    ) -> Result<Self, RemoteS3ClientError> {
        let credential = match &spec.auth {
            AzureAuth::SharedKey(key) => {
                let key = base64_simd::STANDARD
                    .decode_to_vec(key.as_bytes())
                    .map_err(|_| RemoteS3ClientError::Credentials("azure account key is not base64"))?;
                // HMAC accepts a zero-length key, so an absent one would sign
                // every request with nothing rather than fail here.
                if key.is_empty() {
                    return Err(RemoteS3ClientError::Credentials("azure account key is empty"));
                }
                Credential::SharedKey(key)
            }
            AzureAuth::Sas(sas) => {
                let pairs: Vec<(String, String)> = url::form_urlencoded::parse(sas.trim_start_matches('?').as_bytes())
                    .into_owned()
                    .collect();
                if pairs.is_empty() {
                    return Err(RemoteS3ClientError::Credentials("azure sas token has no parameters"));
                }
                Credential::Sas(pairs)
            }
        };
        Ok(Self {
            http: NativeHttp::new(endpoint, timeouts, skip_tls_verify, ca_cert_pem)?,
            account: spec.account.clone(),
            container: container.to_string(),
            credential,
        })
    }

    /// URL of one blob in the container. The key is split so its `/` stay path
    /// separators while every other character is percent-encoded.
    fn blob_url(&self, key: &str) -> Result<Url, SourceError> {
        self.http.url(std::iter::once(self.container.as_str()).chain(key.split('/')))
    }

    fn container_url(&self) -> Result<Url, SourceError> {
        self.http.url(std::iter::once(self.container.as_str()))
    }

    /// Builds a signed (or SAS-carrying) request. `headers` holds the
    /// operation's own headers; the service headers and authorization are
    /// added here so every request is authorized the same way.
    fn request(&self, method: Method, mut url: Url, mut headers: HeaderMap) -> Result<reqwest::Request, SourceError> {
        headers.insert(HEADER_VERSION, HeaderValue::from_static(API_VERSION));
        let now = time::OffsetDateTime::now_utc()
            .format(HTTP_DATE)
            .map_err(|err| SourceError::Other(format!("cannot render the request date: {err}")))?;
        headers.insert(
            HEADER_DATE,
            HeaderValue::from_str(&now).map_err(|_| SourceError::Other("cannot render the request date".to_string()))?,
        );

        match &self.credential {
            Credential::SharedKey(key) => {
                let signature = shared_key_signature(key, &self.account, method.as_str(), &url, &headers)?;
                headers.insert(
                    http::header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("SharedKey {}:{signature}", self.account))
                        .map_err(|_| SourceError::Other("cannot render the authorization header".to_string()))?,
                );
            }
            Credential::Sas(pairs) => {
                url.query_pairs_mut().extend_pairs(pairs.iter().map(|(k, v)| (k, v)));
            }
        }

        let mut request = reqwest::Request::new(method, url);
        *request.headers_mut() = headers;
        Ok(request)
    }

    /// A missing blob is distinct from a missing container or version. Only
    /// object reads may use BlobNotFound as positive evidence of absence.
    async fn send_object_request(&self, request: reqwest::Request) -> Result<reqwest::Response, SourceError> {
        let is_head = request.method() == Method::HEAD;
        let versioned = request
            .url()
            .query_pairs()
            .any(|(name, _)| name.eq_ignore_ascii_case("versionid") || name.eq_ignore_ascii_case("snapshot"));
        let response = self.http.execute(request).await?;
        if response.status() == http::StatusCode::NOT_FOUND && !versioned {
            match header(response.headers(), HEADER_ERROR_CODE) {
                Some("BlobNotFound") => return Err(SourceError::NotFound),
                None | Some("ResourceNotFound") if is_head => {
                    // HEAD may omit an error code. One successful container
                    // probe proves key absence; a failed probe keeps its error.
                    // These are two independently timed requests, not one deadline.
                    drop(response);
                    self.probe().await?;
                    return Err(SourceError::NotFound);
                }
                _ => {}
            }
        }
        NativeHttp::check_response(response, Some(HEADER_ERROR_CODE))
    }

    /// Shared mapping for Get Blob and Get Blob Properties.
    fn head_from_response(headers: &HeaderMap) -> Result<SourceHead, SourceError> {
        // A customer-provided key means the service holds ciphertext it cannot
        // decrypt for us; the same rule the S3 path applies to SSE-C.
        if header(headers, "x-ms-encryption-key-sha256").is_some() {
            return Err(SourceError::Unsupported(
                "source blob uses a customer-provided encryption key; customer-key sources are not supported".to_string(),
            ));
        }
        native_source_head(
            headers,
            METADATA_PREFIX,
            NativeHeadFields {
                etag: header(headers, "etag").map(str::to_string),
                etag_is_opaque: true,
                version_id: header(headers, "x-ms-version-id").map(str::to_string),
                storage_class: header(headers, "x-ms-access-tier").map(str::to_string),
            },
        )
    }
}

#[async_trait::async_trait]
impl SourceBackend for AzureSourceBackend {
    async fn head(&self, key: &str) -> Result<SourceHead, SourceError> {
        let request = self.request(Method::HEAD, self.blob_url(key)?, HeaderMap::new())?;
        let response = self.send_object_request(request).await?;
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
        let request = self.request(Method::GET, self.blob_url(key)?, headers)?;
        let response = self.send_object_request(request).await?;
        let head = Self::head_from_response(response.headers())?;
        let content_range = header(response.headers(), "content-range").map(str::to_string);
        Ok(SourceGet {
            head,
            body: response_body(response),
            content_range,
        })
    }

    async fn list(&self, request: &SourceListRequest<'_>) -> Result<SourcePage, SourceError> {
        // Azure paginates with an opaque marker and has no "start after this
        // key" form. Refuse rather than silently listing from the beginning.
        if request.start_after.is_some() {
            return Err(SourceError::Unsupported(
                "azure sources cannot resume a listing from a key; use the continuation token".to_string(),
            ));
        }
        let mut url = self.container_url()?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("restype", "container");
            query.append_pair("comp", "list");
            if let Some(prefix) = request.prefix.filter(|prefix| !prefix.is_empty()) {
                query.append_pair("prefix", prefix);
            }
            if let Some(delimiter) = request.delimiter.filter(|delimiter| !delimiter.is_empty()) {
                query.append_pair("delimiter", delimiter);
            }
            if let Some(marker) = request.continuation_token.filter(|marker| !marker.is_empty()) {
                query.append_pair("marker", marker);
            }
            if request.max_keys > 0 {
                query.append_pair("maxresults", &request.max_keys.to_string());
            }
        }

        let request = self.request(Method::GET, url, HeaderMap::new())?;
        let response = self.http.send(request, Some(HEADER_ERROR_CODE)).await?;
        let body = read_text(response, MAX_XML_BYTES).await?;
        let listing = parse_list_blobs(&body)?;

        Ok(SourcePage {
            objects: listing.objects,
            common_prefixes: listing.prefixes,
            is_truncated: listing.next_marker.is_some(),
            next_continuation_token: listing.next_marker,
        })
    }

    async fn tagging(&self, key: &str) -> Result<HashMap<String, String>, SourceError> {
        let mut url = self.blob_url(key)?;
        url.query_pairs_mut().append_pair("comp", "tags");
        let request = self.request(Method::GET, url, HeaderMap::new())?;
        let response = self.http.send(request, Some(HEADER_ERROR_CODE)).await?;
        let body = read_text(response, MAX_XML_BYTES).await?;
        parse_blob_tags(&body)
    }

    async fn probe(&self) -> Result<(), SourceError> {
        let mut url = self.container_url()?;
        url.query_pairs_mut().append_pair("restype", "container");
        let request = self.request(Method::HEAD, url, HeaderMap::new())?;
        self.http.send(request, Some(HEADER_ERROR_CODE)).await?;
        Ok(())
    }
}

/// Shared Key signature over the canonical request. Only the fields this
/// backend ever sets are non-empty: `Range`, the `x-ms-*` headers and the
/// canonicalized resource. GET and HEAD carry no body, so every `Content-*`
/// slot stays empty.
fn shared_key_signature(key: &[u8], account: &str, method: &str, url: &Url, headers: &HeaderMap) -> Result<String, SourceError> {
    let mut string_to_sign = String::with_capacity(256);
    string_to_sign.push_str(method);
    string_to_sign.push('\n');
    // Content-Encoding, Content-Language, Content-Length, Content-MD5,
    // Content-Type, Date, If-Modified-Since, If-Match, If-None-Match,
    // If-Unmodified-Since: all empty. `Date` stays empty because `x-ms-date`
    // carries the timestamp and Azure then ignores this slot.
    for _ in 0..10 {
        string_to_sign.push('\n');
    }
    string_to_sign.push_str(header(headers, "range").unwrap_or_default());
    string_to_sign.push('\n');

    // Canonicalized headers: every `x-ms-*` header, lowercased and sorted.
    let mut canonical_headers = BTreeMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if let Some(rest) = name.strip_prefix("x-ms-")
            && !rest.is_empty()
            && let Ok(value) = value.to_str()
        {
            canonical_headers.insert(name.to_string(), value.trim().to_string());
        }
    }
    for (name, value) in &canonical_headers {
        string_to_sign.push_str(name);
        string_to_sign.push(':');
        string_to_sign.push_str(value);
        string_to_sign.push('\n');
    }

    // Canonicalized resource: the account, the encoded path, then every query
    // parameter lowercased and sorted, with repeated values joined by commas.
    string_to_sign.push('/');
    string_to_sign.push_str(account);
    string_to_sign.push_str(url.path());
    let mut canonical_query: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in url.query_pairs() {
        canonical_query
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.into_owned());
    }
    for (name, mut values) in canonical_query {
        values.sort();
        string_to_sign.push('\n');
        string_to_sign.push_str(&name);
        string_to_sign.push(':');
        string_to_sign.push_str(&values.join(","));
    }

    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| SourceError::Other("azure account key has an unusable length".to_string()))?;
    mac.update(string_to_sign.as_bytes());
    Ok(base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes()))
}

#[derive(Debug)]
struct AzureListing {
    objects: Vec<SourceObject>,
    prefixes: Vec<String>,
    /// `None` when the listing is complete; Azure marks the end with an empty
    /// `NextMarker`.
    next_marker: Option<String>,
}

#[derive(Default)]
struct BlobEntry {
    name: Option<String>,
    etag: Option<String>,
    size: Option<u64>,
    last_modified: Option<std::time::SystemTime>,
    access_tier: Option<String>,
}

/// Parses one `List Blobs` page.
fn parse_list_blobs(xml: &str) -> Result<AzureListing, SourceError> {
    let mut reader = xml_reader(xml);
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    let mut next_marker = None;
    let mut blob: Option<BlobEntry> = None;
    let mut in_blob_prefix = false;
    let mut blob_prefix: Option<String> = None;
    // Open container elements. quick-xml reports a truncated document as a
    // plain end of input, so a non-zero depth at EOF is the only signal that
    // the page was cut short and must not be read as a complete listing.
    let mut depth = 0_usize;

    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) => {
                let name = local_name(start.name().as_ref());
                match name.as_str() {
                    "blob" => {
                        depth += 1;
                        blob = Some(BlobEntry::default());
                    }
                    "blobprefix" => {
                        depth += 1;
                        in_blob_prefix = true;
                    }
                    "properties" | "blobs" | "enumerationresults" => depth += 1,
                    _ => {
                        let end = start.to_end().into_owned();
                        let text = leaf_text(&mut reader, end.name())?;
                        apply_list_field(&name, text, &mut blob, &mut blob_prefix, &mut next_marker, in_blob_prefix)?;
                    }
                }
            }
            Ok(Event::Empty(empty)) => {
                let name = local_name(empty.name().as_ref());
                if matches!(name.as_str(), "blob" | "blobprefix") {
                    return Err(SourceError::Other("source listing entry has no name".to_string()));
                }
                apply_list_field(&name, String::new(), &mut blob, &mut blob_prefix, &mut next_marker, in_blob_prefix)?;
            }
            Ok(Event::End(end)) => match local_name(end.name().as_ref()).as_str() {
                "blob" => {
                    depth = depth.saturating_sub(1);
                    if let Some(entry) = blob.take() {
                        objects.push(SourceObject {
                            key: entry
                                .name
                                .filter(|name| !name.is_empty())
                                .ok_or_else(|| SourceError::Other("source listing object has no name".to_string()))?,
                            etag: entry.etag,
                            size: entry
                                .size
                                .ok_or_else(|| SourceError::Other("source listing object has no valid size".to_string()))?,
                            last_modified: entry.last_modified,
                            storage_class: entry.access_tier,
                            // Azure ETags carry no part count; the listing
                            // never describes a composed object.
                            is_multipart_etag: false,
                        });
                    }
                }
                "blobprefix" => {
                    depth = depth.saturating_sub(1);
                    in_blob_prefix = false;
                    prefixes.push(
                        blob_prefix
                            .take()
                            .filter(|name| !name.is_empty())
                            .ok_or_else(|| SourceError::Other("source listing prefix has no name".to_string()))?,
                    );
                }
                "properties" | "blobs" | "enumerationresults" => depth = depth.saturating_sub(1),
                _ => {}
            },
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => return Err(SourceError::Other(format!("source listing is not valid XML: {err}"))),
        }
    }
    if depth != 0 {
        return Err(SourceError::Other("source listing ended before every element was closed".to_string()));
    }

    Ok(AzureListing {
        objects,
        prefixes,
        next_marker: next_marker.filter(|marker| !marker.is_empty()),
    })
}

fn apply_list_field(
    name: &str,
    text: String,
    blob: &mut Option<BlobEntry>,
    blob_prefix: &mut Option<String>,
    next_marker: &mut Option<String>,
    in_blob_prefix: bool,
) -> Result<(), SourceError> {
    match name {
        "name" => {
            if in_blob_prefix {
                *blob_prefix = Some(text);
            } else if let Some(entry) = blob.as_mut() {
                entry.name = Some(text);
            }
        }
        "nextmarker" => *next_marker = Some(text),
        "etag" => {
            if let Some(entry) = blob.as_mut() {
                entry.etag = Some(text.trim().trim_matches('"').to_string()).filter(|etag| !etag.is_empty());
            }
        }
        "content-length" => {
            if let Some(entry) = blob.as_mut() {
                entry.size = Some(
                    text.trim()
                        .parse()
                        .map_err(|_| SourceError::Other("source listing object has no valid size".to_string()))?,
                );
            }
        }
        "last-modified" => {
            if let Some(entry) = blob.as_mut() {
                entry.last_modified = parse_http_timestamp(text.trim());
            }
        }
        "accesstier" => {
            if let Some(entry) = blob.as_mut() {
                entry.access_tier = Some(text).filter(|tier| !tier.is_empty());
            }
        }
        _ => {}
    }
    Ok(())
}

/// Parses a `Get Blob Tags` response.
fn parse_blob_tags(xml: &str) -> Result<HashMap<String, String>, SourceError> {
    let mut reader = xml_reader(xml);
    let mut tags = HashMap::new();
    let mut key = None;
    let mut value = None;
    let mut depth = 0_usize;

    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) => {
                let name = local_name(start.name().as_ref());
                match name.as_str() {
                    "tags" | "tagset" | "tag" => depth += 1,
                    _ => {
                        let end = start.to_end().into_owned();
                        let text = leaf_text(&mut reader, end.name())?;
                        match name.as_str() {
                            "key" => key = Some(text),
                            "value" => value = Some(text),
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::Empty(empty)) => match local_name(empty.name().as_ref()).as_str() {
                "key" => key = Some(String::new()),
                "value" => value = Some(String::new()),
                _ => {}
            },
            Ok(Event::End(end)) => {
                let name = local_name(end.name().as_ref());
                if matches!(name.as_str(), "tags" | "tagset" | "tag") {
                    depth = depth.saturating_sub(1);
                }
                if name == "tag"
                    && let (Some(key), Some(value)) = (key.take(), value.take())
                {
                    tags.insert(key, value);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => return Err(SourceError::Other(format!("source tags are not valid XML: {err}"))),
        }
    }
    if depth != 0 {
        return Err(SourceError::Other("source tags ended before every element was closed".to_string()));
    }

    Ok(tags)
}

fn xml_reader(xml: &str) -> Reader<&[u8]> {
    let mut reader = Reader::from_str(xml);
    let config = reader.config_mut();
    config.trim_text_start = true;
    config.trim_text_end = true;
    reader
}

/// Lowercased element name without its namespace prefix.
fn local_name(raw: &str) -> String {
    raw.rsplit(':').next().unwrap_or(raw).to_ascii_lowercase()
}

/// Text of a leaf element, consuming through its end tag.
fn leaf_text(reader: &mut Reader<&[u8]>, end: quick_xml::name::QName<'_>) -> Result<String, SourceError> {
    let raw = reader
        .read_text(end)
        .map_err(|err| format!("source response is not valid XML: {err}"))
        .and_then(|text| {
            quick_xml::escape::unescape(text.as_ref())
                .map(|text| text.into_owned())
                .map_err(|err| format!("source response has invalid XML escapes: {err}"))
        });
    raw.map_err(SourceError::Other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::on_demand_migration::backend_contract::{BackendCapabilities, assert_backend_contract};
    use crate::on_demand_migration::source_client::SourceError;
    use crate::on_demand_migration::test_http_fixture::{ScriptedResponse, assert_requests, scripted_server};

    const LIST_PAGE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" ContainerName="legacy">
  <Prefix>photos/</Prefix>
  <Delimiter>/</Delimiter>
  <MaxResults>2</MaxResults>
  <Blobs>
    <Blob>
      <Name>photos/a &amp; b.jpg</Name>
      <Properties>
        <Last-Modified>Wed, 21 Oct 2015 07:28:00 GMT</Last-Modified>
        <Etag>0x8D2F1B0A1B2C3D4</Etag>
        <Content-Length>42</Content-Length>
        <Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5>
        <BlobType>BlockBlob</BlobType>
        <AccessTier>Hot</AccessTier>
      </Properties>
    </Blob>
    <Blob>
      <Name>photos/b.jpg</Name>
      <Properties>
        <Content-Length>7</Content-Length>
      </Properties>
    </Blob>
    <BlobPrefix>
      <Name>photos/raw/</Name>
    </BlobPrefix>
  </Blobs>
  <NextMarker>2!76!MDAwMDI0</NextMarker>
</EnumerationResults>"#;

    const LAST_PAGE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults><Blobs><Blob><Name>only.txt</Name><Properties><Content-Length>1</Content-Length></Properties></Blob></Blobs><NextMarker /></EnumerationResults>"#;

    const TAGS: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Tags><TagSet>
  <Tag><Key>env</Key><Value>prod</Value></Tag>
  <Tag><Key>team</Key><Value>storage &amp; co</Value></Tag>
</TagSet></Tags>"#;

    #[test]
    fn list_blobs_maps_entries_prefixes_and_the_marker() {
        let listing = parse_list_blobs(LIST_PAGE).expect("page should parse");
        assert_eq!(listing.prefixes, vec!["photos/raw/"]);
        assert_eq!(listing.next_marker.as_deref(), Some("2!76!MDAwMDI0"));
        assert_eq!(listing.objects.len(), 2);
        let first = &listing.objects[0];
        assert_eq!(first.key, "photos/a & b.jpg", "XML entities in a blob name are decoded");
        assert_eq!(first.etag.as_deref(), Some("0x8D2F1B0A1B2C3D4"));
        assert_eq!(first.size, 42);
        assert_eq!(
            first.last_modified,
            Some(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_445_412_480))
        );
        assert_eq!(first.storage_class.as_deref(), Some("Hot"));
        assert!(!first.is_multipart_etag);
        assert_eq!(listing.objects[1].key, "photos/b.jpg");
        assert_eq!(listing.objects[1].size, 7);
        assert!(listing.objects[1].etag.is_none());
    }

    #[test]
    fn an_empty_next_marker_ends_the_listing() {
        let listing = parse_list_blobs(LAST_PAGE).expect("page should parse");
        assert_eq!(listing.objects.len(), 1);
        assert!(listing.next_marker.is_none(), "an empty NextMarker is not a cursor");
    }

    #[test]
    fn malformed_listing_xml_is_an_error() {
        for bad in [
            "<EnumerationResults><Blobs>",
            "<EnumerationResults><Blobs><Blob><Name>a</Name></Blobs></EnumerationResults>",
            "not xml at <all",
        ] {
            let err = parse_list_blobs(bad).expect_err("{bad} must fail");
            assert!(matches!(err, SourceError::Other(_)), "{bad}: {err:?}");
        }
        assert!(parse_blob_tags("<Tags><TagSet>").is_err(), "a truncated tag set must fail");
    }

    #[tokio::test]
    async fn native_listing_rejects_missing_or_invalid_required_object_fields() {
        for entry in [
            "<Blob />",
            "<Blob><Properties><Content-Length>1</Content-Length></Properties></Blob>",
            "<Blob><Name /><Properties><Content-Length>1</Content-Length></Properties></Blob>",
            "<Blob><Name>broken</Name></Blob>",
            "<Blob><Name>broken</Name><Properties><Content-Length /></Properties></Blob>",
            "<Blob><Name>broken</Name><Properties><Content-Length>-1</Content-Length></Properties></Blob>",
            "<Blob><Name>broken</Name><Properties><Content-Length>18446744073709551616</Content-Length></Properties></Blob>",
            "<Blob><Name>broken</Name><Properties><Content-Length>not-a-size</Content-Length></Properties></Blob>",
            "<BlobPrefix />",
            "<BlobPrefix><Name /></BlobPrefix>",
            "<BlobPrefix></BlobPrefix>",
        ] {
            // Reject the entire page even if a valid object precedes the bad
            // entry, so callers cannot expose partial data or advance its cursor.
            let body = format!(
                "<EnumerationResults><Blobs><Blob><Name>valid</Name><Properties><Content-Length>1</Content-Length></Properties></Blob>{entry}</Blobs><NextMarker>next</NextMarker></EnumerationResults>"
            );
            let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, Vec::new(), body)]).await;
            let err = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]))
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
                    "/legacy?restype=container&comp=list&prefix=dir%2F&delimiter=%2F&marker=opaque%2B%2F%3D&maxresults=2",
                )],
            );
        }
    }

    #[tokio::test]
    async fn native_listing_preserves_zero_size_unicode_prefixes_and_opaque_cursors() {
        let body = "<EnumerationResults><Blobs><Blob><Name>目录/空 &amp; file</Name><Properties><Content-Length>0</Content-Length></Properties></Blob><BlobPrefix><Name>目录/子/</Name></BlobPrefix></Blobs><NextMarker>opaque+/=</NextMarker></EnumerationResults>";
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, Vec::new(), body.to_string())]).await;
        let page = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]))
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
        assert_requests(&recorded, &[("GET", "/legacy?restype=container&comp=list&maxresults=2")]);
    }

    #[test]
    fn blob_tags_parse_into_the_shared_tag_map() {
        let tags = parse_blob_tags(TAGS).expect("tags should parse");
        assert_eq!(
            tags,
            HashMap::from([
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "storage & co".to_string())
            ])
        );
        assert!(parse_blob_tags("<Tags><TagSet /></Tags>").expect("empty tag set").is_empty());
    }

    /// Signature fixture from a request this backend really builds: it pins the
    /// canonical form so a change to the header set or the query canonicalization
    /// cannot silently start producing signatures Azure rejects.
    #[test]
    fn shared_key_signs_the_canonical_request() {
        let key = base64_simd::STANDARD.decode_to_vec(b"c2VjcmV0LWtleQ==").expect("test key");
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_VERSION, HeaderValue::from_static(API_VERSION));
        headers.insert(HEADER_DATE, HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT"));
        headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=10-14"));
        let url = Url::parse("https://acct.blob.core.windows.net/legacy/photos/a.jpg").expect("url");

        let signature = shared_key_signature(&key, "acct", "GET", &url, &headers).expect("signature");
        let expected = {
            let string_to_sign = concat!(
                "GET\n\n\n\n\n\n\n\n\n\n\n",
                "bytes=10-14\n",
                "x-ms-date:Sun, 06 Nov 1994 08:49:37 GMT\n",
                "x-ms-version:2021-08-06\n",
                "/acct/legacy/photos/a.jpg"
            );
            let mut mac = HmacSha256::new_from_slice(&key).expect("hmac");
            mac.update(string_to_sign.as_bytes());
            base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes())
        };
        assert_eq!(signature, expected);
    }

    #[test]
    fn shared_key_canonicalizes_query_parameters() {
        let key = vec![1_u8; 32];
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_VERSION, HeaderValue::from_static(API_VERSION));
        // Query order must not change the signature: Azure canonicalizes by
        // lowercased parameter name.
        let a = Url::parse("https://acct.blob.core.windows.net/legacy?restype=container&comp=list&prefix=p%2F").expect("url");
        let b = Url::parse("https://acct.blob.core.windows.net/legacy?prefix=p%2F&COMP=list&restype=container").expect("url");
        assert_eq!(
            shared_key_signature(&key, "acct", "GET", &a, &headers).expect("a"),
            shared_key_signature(&key, "acct", "GET", &b, &headers).expect("b")
        );
    }

    fn backend(endpoint: &Url, credential: Credential) -> AzureSourceBackend {
        AzureSourceBackend {
            http: NativeHttp::for_test(endpoint.clone()),
            account: "acct".to_string(),
            container: "legacy".to_string(),
            credential,
        }
    }

    fn blob_headers() -> Vec<(&'static str, String)> {
        vec![
            ("ETag", "\"0x8D2F1B0A1B2C3D4\"".to_string()),
            ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT".to_string()),
            ("Content-Type", "image/jpeg".to_string()),
            ("Content-MD5", "1B2M2Y8AsgTpgAmY7PhCfg==".to_string()),
            ("x-ms-meta-owner", "alice".to_string()),
            ("x-ms-access-tier", "Cool".to_string()),
            ("x-ms-version-id", "2026-01-01T00:00:00.0000000Z".to_string()),
            ("x-ms-blob-type", "BlockBlob".to_string()),
        ]
    }

    #[test]
    fn an_absent_or_malformed_account_key_is_refused_before_any_request() {
        for key in ["", "not base64!"] {
            let spec = AzureSourceSpec {
                account: "acct".to_string(),
                auth: AzureAuth::SharedKey(key.to_string()),
            };
            let built = AzureSourceBackend::new(
                "https://acct.blob.core.windows.net",
                "legacy",
                &spec,
                SourceTimeouts::default(),
                false,
                None,
            );
            assert!(
                matches!(built, Err(RemoteS3ClientError::Credentials(_))),
                "{key:?} must not build a client"
            );
        }
        let spec = AzureSourceSpec {
            account: "acct".to_string(),
            auth: AzureAuth::Sas(String::new()),
        };
        assert!(
            AzureSourceBackend::new(
                "https://acct.blob.core.windows.net",
                "legacy",
                &spec,
                SourceTimeouts::default(),
                false,
                None
            )
            .is_err(),
            "an empty SAS token carries no parameters"
        );
    }

    #[tokio::test]
    async fn head_signs_the_request_and_maps_azure_metadata() {
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, blob_headers(), String::new())]).await;
        let backend = backend(&endpoint, Credential::SharedKey(b"0123456789abcdef0123456789abcdef".to_vec()));

        let head = backend.head("photos/a b.jpg").await.expect("HEAD should map");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].method, "HEAD");
        assert_eq!(recorded[0].target, "/legacy/photos/a%20b.jpg", "the blob name is path-encoded");
        assert_eq!(recorded[0].header("x-ms-version"), Some(API_VERSION));
        assert!(recorded[0].header("x-ms-date").is_some(), "a signed request must carry x-ms-date");
        assert!(
            recorded[0]
                .header("authorization")
                .is_some_and(|value| value.starts_with("SharedKey acct:")),
            "{:?}",
            recorded[0].header("authorization")
        );

        assert_eq!(head.etag.as_deref(), Some("0x8D2F1B0A1B2C3D4"));
        assert!(head.etag_is_opaque, "an Azure ETag is never a content digest");
        assert!(!head.is_multipart_etag);
        assert_eq!(head.size, 0);
        assert_eq!(head.content_type.as_deref(), Some("image/jpeg"));
        assert_eq!(head.storage_class.as_deref(), Some("Cool"));
        assert_eq!(head.version_id.as_deref(), Some("2026-01-01T00:00:00.0000000Z"));
        assert_eq!(head.user_metadata, HashMap::from([("owner".to_string(), "alice".to_string())]));
        assert!(head.sse.is_none());
    }

    #[tokio::test]
    async fn sas_credentials_travel_in_the_query_and_never_sign() {
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(200, blob_headers(), String::new())]).await;
        let backend = backend(
            &endpoint,
            Credential::Sas(vec![
                ("sv".to_string(), "2021-08-06".to_string()),
                ("sig".to_string(), "a+b/c=".to_string()),
            ]),
        );

        backend.head("a.txt").await.expect("HEAD should map");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert!(recorded[0].header("authorization").is_none(), "a SAS request must not be signed");
        assert!(recorded[0].target.contains("sv=2021-08-06"), "{}", recorded[0].target);
        assert!(
            recorded[0].target.contains("sig=a%2Bb%2Fc%3D"),
            "the SAS signature must be re-encoded exactly once: {}",
            recorded[0].target
        );
    }

    #[tokio::test]
    async fn get_passes_the_range_through_and_streams_the_body() {
        let mut headers = blob_headers();
        headers.push(("Content-Range", "bytes 10-14/100".to_string()));
        let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(206, headers, "hello".to_string())]).await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 10,
            end: 14,
        };
        let got = backend.get("a.txt", Some(&range)).await.expect("ranged GET should succeed");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert_eq!(recorded[0].method, "GET");
        assert_eq!(recorded[0].header("range"), Some("bytes=10-14"));
        assert_eq!(got.content_range.as_deref(), Some("bytes 10-14/100"));
        assert_eq!(got.head.size, 5);
        let body = got.body.collect().await.expect("body should stream").into_bytes();
        assert_eq!(body.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn customer_key_blobs_are_refused() {
        let mut headers = blob_headers();
        headers.push(("x-ms-encryption-key-sha256", "abc".to_string()));
        let (endpoint, _) = scripted_server(vec![ScriptedResponse::new(200, headers, String::new())]).await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        let err = backend.head("a.txt").await.expect_err("customer-key blobs are unsupported");
        assert!(matches!(err, SourceError::Unsupported(_)), "{err:?}");
        assert_eq!(err.class_label(), "unsupported");
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn list_requests_the_container_and_pages_with_the_marker() {
        let (endpoint, recorded) = scripted_server(vec![
            ScriptedResponse::new(200, Vec::new(), LIST_PAGE.to_string()),
            ScriptedResponse::new(200, Vec::new(), LAST_PAGE.to_string()),
        ])
        .await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        let page = backend
            .list(&SourceListRequest {
                prefix: Some("photos/"),
                delimiter: Some("/"),
                max_keys: 2,
                ..Default::default()
            })
            .await
            .expect("first page should list");
        assert!(page.is_truncated);
        assert_eq!(page.next_continuation_token.as_deref(), Some("2!76!MDAwMDI0"));
        assert_eq!(page.common_prefixes, vec!["photos/raw/"]);

        let page = backend
            .list(&SourceListRequest {
                prefix: Some("photos/"),
                continuation_token: page.next_continuation_token.as_deref(),
                max_keys: 2,
                ..Default::default()
            })
            .await
            .expect("second page should list");
        assert!(!page.is_truncated);
        assert!(page.next_continuation_token.is_none());

        let recorded = recorded.lock().expect("recorder lock").clone();
        for request in &recorded {
            assert!(request.target.starts_with("/legacy?"), "{}", request.target);
            assert!(request.target.contains("restype=container"), "{}", request.target);
            assert!(request.target.contains("comp=list"), "{}", request.target);
            assert!(request.target.contains("prefix=photos%2F"), "{}", request.target);
            assert!(request.target.contains("maxresults=2"), "{}", request.target);
        }
        assert!(!recorded[0].target.contains("marker="), "{}", recorded[0].target);
        assert!(recorded[1].target.contains("marker=2%2176%21MDAwMDI0"), "{}", recorded[1].target);
    }

    #[tokio::test]
    async fn list_refuses_a_start_after_cursor_before_sending() {
        let (endpoint, recorded) = scripted_server(Vec::new()).await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        let err = backend
            .list(&SourceListRequest {
                start_after: Some("a"),
                max_keys: 1,
                ..Default::default()
            })
            .await
            .expect_err("azure has no start-after form");
        assert!(matches!(err, SourceError::Unsupported(_)), "{err:?}");
        assert!(
            recorded.lock().expect("recorder lock").is_empty(),
            "an unsupported request must never reach the source"
        );
    }

    #[tokio::test]
    async fn tagging_and_probe_address_the_right_resources() {
        let (endpoint, recorded) = scripted_server(vec![
            ScriptedResponse::new(200, Vec::new(), TAGS.to_string()),
            ScriptedResponse::new(200, Vec::new(), String::new()),
        ])
        .await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        let tags = backend.tagging("a.txt").await.expect("tags should parse");
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        backend.probe().await.expect("probe should succeed");

        let recorded = recorded.lock().expect("recorder lock").clone();
        assert_eq!(recorded[0].target, "/legacy/a.txt?comp=tags");
        assert_eq!(recorded[1].method, "HEAD");
        assert_eq!(recorded[1].target, "/legacy?restype=container");
    }

    #[tokio::test]
    async fn azure_statuses_map_onto_the_shared_error_classes() {
        for (status, code, expected, retryable) in [
            (404_u16, Some("BlobNotFound"), "not_found", false),
            (403, Some("AuthorizationPermissionMismatch"), "access_denied", false),
            (401, None, "access_denied", false),
            (429, None, "throttled", true),
            (503, Some("ServerBusy"), "throttled", true),
            (500, None, "server_error", true),
        ] {
            let headers = code
                .map(|code| vec![(HEADER_ERROR_CODE, code.to_string())])
                .unwrap_or_default();
            let (endpoint, _) = scripted_server(vec![ScriptedResponse::new(status, headers, String::new())]).await;
            let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));
            let err = backend.head("a.txt").await.expect_err("{status} must fail");
            assert_eq!(err.class_label(), expected, "status {status} -> {err:?}");
            assert_eq!(err.is_retryable(), retryable, "status {status} -> {err:?}");
        }
    }

    const CONTRACT_LIST_PAGE_ONE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="legacy">
  <Blobs>
    <Blob>
      <Name>dir/a.txt</Name>
      <Properties>
        <Last-Modified>Wed, 21 Oct 2015 07:28:00 GMT</Last-Modified>
        <Etag>0x8D2F1B0A1B2C3D4</Etag>
        <Content-Length>5</Content-Length>
        <AccessTier>Hot</AccessTier>
      </Properties>
    </Blob>
    <BlobPrefix><Name>dir/sub/</Name></BlobPrefix>
  </Blobs>
  <NextMarker>cursor-1</NextMarker>
</EnumerationResults>"#;

    const CONTRACT_LIST_PAGE_TWO: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="legacy">
  <Blobs>
    <Blob>
      <Name>dir/b.txt</Name>
      <Properties>
        <Last-Modified>Wed, 21 Oct 2015 07:28:00 GMT</Last-Modified>
        <Content-Length>7</Content-Length>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>"#;

    const CONTRACT_TAGS: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<Tags><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tags>"#;

    fn contract_blob_headers() -> Vec<(&'static str, String)> {
        vec![
            ("ETag", "\"0x8D2F1B0A1B2C3D4\"".to_string()),
            ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT".to_string()),
            ("Content-Type", "text/plain".to_string()),
            ("x-ms-meta-owner", "alice".to_string()),
            ("x-ms-access-tier", "Hot".to_string()),
            ("x-ms-blob-type", "BlockBlob".to_string()),
        ]
    }

    #[tokio::test]
    async fn object_not_found_requires_provider_evidence_or_one_successful_head_probe() {
        for method in [Method::HEAD, Method::GET] {
            for (status, code, expected) in [
                (404, Some("BlobNotFound"), "not_found"),
                (403, Some("BlobNotFound"), "access_denied"),
                (404, Some("ContainerNotFound"), "other"),
                (404, Some("BlobVersionNotFound"), "other"),
                (404, Some("UnrecognizedError"), "other"),
                (404, None, if method == Method::HEAD { "not_found" } else { "other" }),
                (404, Some("ResourceNotFound"), if method == Method::HEAD { "not_found" } else { "other" }),
            ] {
                let probes = method == Method::HEAD && status == 404 && matches!(code, None | Some("ResourceNotFound"));
                let headers = code
                    .map(|value| vec![(HEADER_ERROR_CODE, value.to_string())])
                    .unwrap_or_default();
                let mut responses = vec![ScriptedResponse::new(status, headers, "untrusted-error-body".to_string())];
                if probes {
                    responses.push(ScriptedResponse::new(200, Vec::new(), String::new()));
                }
                let (endpoint, recorded) = scripted_server(responses).await;
                let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));
                let result = if method == Method::HEAD {
                    backend.head("missing").await.map(|_| ())
                } else {
                    backend.get("missing", None).await.map(|_| ())
                };
                let err = result.expect_err("object error must remain an error");
                assert_eq!(err.class_label(), expected, "{method} {status} {code:?}: {err:?}");
                assert!(!err.is_retryable(), "{err:?}");
                assert!(!err.to_string().contains("untrusted-error-body"));
                let mut requests = vec![(method.as_str(), "/legacy/missing")];
                if probes {
                    requests.push(("HEAD", "/legacy?restype=container"));
                }
                assert_requests(&recorded, &requests);
            }
        }
    }

    #[tokio::test]
    async fn ambiguous_head_preserves_the_container_probe_failure() {
        for (status, expected, retryable) in [
            (403, "access_denied", false),
            (404, "other", false),
            (429, "throttled", true),
            (500, "server_error", true),
            (503, "throttled", true),
        ] {
            let (endpoint, recorded) = scripted_server(vec![
                ScriptedResponse::new(404, Vec::new(), String::new()),
                // A BlobNotFound header on a container request cannot prove
                // that the object is missing, regardless of this status.
                ScriptedResponse::new(status, vec![(HEADER_ERROR_CODE, "BlobNotFound".to_string())], String::new()),
            ])
            .await;
            let err = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]))
                .head("missing")
                .await
                .expect_err("failed probe must not become object absence");
            assert_eq!(err.class_label(), expected, "probe {status}: {err:?}");
            assert_eq!(err.is_retryable(), retryable, "probe {status}: {err:?}");
            if status == 500 {
                assert!(matches!(err, SourceError::ServerError(500)));
            }
            assert_requests(&recorded, &[("HEAD", "/legacy/missing"), ("HEAD", "/legacy?restype=container")]);
        }
    }

    #[tokio::test]
    async fn version_and_snapshot_absence_are_not_missing_current_blobs() {
        for selector in ["versionid", "snapshot"] {
            for code in [None, Some("BlobNotFound"), Some("ResourceNotFound")] {
                for method in [Method::HEAD, Method::GET] {
                    let headers = code
                        .map(|value| vec![(HEADER_ERROR_CODE, value.to_string())])
                        .unwrap_or_default();
                    let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(404, headers, String::new())]).await;
                    let backend = backend(&endpoint, Credential::Sas(vec![(selector.to_string(), "old-version".to_string())]));
                    let result = if method == Method::HEAD {
                        backend.head("object").await.map(|_| ())
                    } else {
                        backend.get("object", None).await.map(|_| ())
                    };
                    let err = result.expect_err("missing selected version must remain a source error");
                    assert!(matches!(err, SourceError::Other(_)), "{method} {selector} {code:?}: {err:?}");
                    assert_requests(&recorded, &[(method.as_str(), &format!("/legacy/object?{selector}=old-version"))]);
                }
            }
        }
    }

    #[tokio::test]
    async fn blob_not_found_header_is_not_object_absence_for_list_or_tags() {
        for tags in [false, true] {
            let (endpoint, recorded) = scripted_server(vec![ScriptedResponse::new(
                404,
                vec![(HEADER_ERROR_CODE, "BlobNotFound".to_string())],
                String::new(),
            )])
            .await;
            let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));
            let result = if tags {
                backend.tagging("missing").await.map(|_| ())
            } else {
                backend.list(&SourceListRequest::default()).await.map(|_| ())
            };
            assert!(matches!(result, Err(SourceError::Other(_))), "tags={tags}: {result:?}");
            assert_requests(
                &recorded,
                &[(
                    "GET",
                    if tags {
                        "/legacy/missing?comp=tags"
                    } else {
                        "/legacy?restype=container&comp=list"
                    },
                )],
            );
        }
    }

    #[tokio::test]
    async fn azure_backend_satisfies_the_shared_backend_contract() {
        let mut ranged = contract_blob_headers();
        ranged.push(("Content-Range", "bytes 1-3/5".to_string()));
        // A HEAD reports the object size with no body, exactly as Azure does.
        let mut head_only = contract_blob_headers();
        head_only.push(("Content-Length", "5".to_string()));
        let (endpoint, recorded) = scripted_server(vec![
            ScriptedResponse::new(200, head_only, String::new()),
            ScriptedResponse::new(200, contract_blob_headers(), "hello".to_string()),
            ScriptedResponse::new(206, ranged, "ell".to_string()),
            ScriptedResponse::new(200, Vec::new(), CONTRACT_LIST_PAGE_ONE.to_string()),
            ScriptedResponse::new(200, Vec::new(), CONTRACT_LIST_PAGE_TWO.to_string()),
            ScriptedResponse::new(200, Vec::new(), CONTRACT_TAGS.to_string()),
            ScriptedResponse::new(200, Vec::new(), String::new()),
            ScriptedResponse::new(404, vec![(HEADER_ERROR_CODE, "BlobNotFound".to_string())], String::new()),
            ScriptedResponse::new(
                403,
                vec![(HEADER_ERROR_CODE, "AuthorizationPermissionMismatch".to_string())],
                String::new(),
            ),
        ])
        .await;
        let backend = backend(&endpoint, Credential::SharedKey(vec![7_u8; 32]));

        assert_backend_contract(
            &backend,
            BackendCapabilities {
                // Azure's ETag is a concurrency token; the contract requires it
                // to be carried but never read as a digest.
                etag_is_opaque: true,
                // Azure paginates only with an opaque marker.
                supports_start_after: false,
                supports_tagging: true,
            },
        )
        .await;
        assert_requests(
            &recorded,
            &[
                ("HEAD", "/legacy/dir/a.txt"),
                ("GET", "/legacy/dir/a.txt"),
                ("GET", "/legacy/dir/a.txt"),
                ("GET", "/legacy?restype=container&comp=list&prefix=dir%2F&delimiter=%2F&maxresults=2"),
                (
                    "GET",
                    "/legacy?restype=container&comp=list&prefix=dir%2F&delimiter=%2F&marker=cursor-1&maxresults=2",
                ),
                ("GET", "/legacy/dir/a.txt?comp=tags"),
                ("HEAD", "/legacy?restype=container"),
                ("HEAD", "/legacy/missing"),
                ("HEAD", "/legacy/secret"),
            ],
        );
    }

    #[tokio::test]
    async fn transport_failures_never_render_the_request_url() {
        // Nothing is listening on the reserved port, so the connect fails and
        // the error must not carry the SAS-bearing URL.
        let backend = backend(
            &Url::parse("http://127.0.0.1:1").expect("endpoint"),
            Credential::Sas(vec![("sig".to_string(), "top-secret-signature".to_string())]),
        );
        let err = backend.head("a.txt").await.expect_err("a closed port must fail");
        let rendered = err.to_string();
        assert!(!rendered.contains("top-secret-signature"), "{rendered}");
        assert!(!rendered.contains("127.0.0.1"), "{rendered}");
    }
}
