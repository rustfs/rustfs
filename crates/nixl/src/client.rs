//! Passive client windows and the control exchange that owns their publication.

use crate::mem::PassiveBuffer;
use crate::{
    NIXL_AGENT_HEADER, NIXL_BUFFER_HEADER, NIXL_METADATA_HEADER, NixlConfig, NixlError, NixlServer,
    RDMA_BYTES_TRANSFERRED_HEADER, RDMA_REPLY_HEADER, StagingRegistration, nixl_err,
};
use aws_credential_types::Credentials;
use aws_sigv4::{
    http_request::{
        PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings, UriPathNormalizationMode, sign,
    },
    sign::v4,
};
use reqwest::{
    Method, Request, RequestBuilder, Response, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{fs::File, sync::Arc};

struct ClientRegistration {
    memory: StagingRegistration<PassiveBuffer>,
    name: HeaderValue,
    metadata: HeaderValue,
}

/// One disjoint range of registered host memory. Local I/O uses the kernel;
/// callers never borrow a Rust slice backed by pages accessible to a peer.
pub struct RegisteredClientWindow {
    registration: Arc<ClientRegistration>,
    offset: usize,
    len: usize,
    quarantine_on_drop: bool,
}

impl RegisteredClientWindow {
    /// Register one allocation with a native progress thread and return its complete windows.
    pub fn register(config: &NixlConfig) -> Result<Vec<Self>, NixlError> {
        if !config.progress_thread {
            return Err(NixlError::Token(
                "registered client windows require the native progress thread".to_owned(),
            ));
        }
        let memory = NixlServer::register_staging(config, PassiveBuffer::new)?;
        let metadata = memory.agent.get_local_md().map_err(nixl_err("local metadata"))?;
        let count = memory.buffer.len() / config.chunk_bytes;
        let registration = Arc::new(ClientRegistration {
            memory,
            name: sensitive_header(&config.agent_name)?,
            metadata: sensitive_header(&base64_simd::STANDARD.encode_to_string(&metadata))?,
        });
        Ok((0..count)
            .map(|i| Self {
                registration: Arc::clone(&registration),
                offset: i * config.chunk_bytes,
                len: config.chunk_bytes,
                quarantine_on_drop: false,
            })
            .collect())
    }

    fn offset(&self, offset: usize, len: usize) -> Result<usize, NixlError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(NixlError::Token("range outside client window".to_owned()));
        }
        Ok(self.offset + offset)
    }

    /// Copy from the idle window into caller-owned bytes.
    pub fn read_at(&self, offset: usize, output: &mut [u8]) -> Result<(), NixlError> {
        self.registration
            .memory
            .buffer
            .read_at(self.offset(offset, output.len())?, output)
    }

    /// Copy caller-owned bytes into the idle window.
    pub fn write_at(&mut self, offset: usize, input: &[u8]) -> Result<(), NixlError> {
        self.registration
            .memory
            .buffer
            .write_at(self.offset(offset, input.len())?, input)
    }

    /// Fill an idle window directly from a file without a userspace staging copy.
    pub fn read_file_at(&mut self, file: &File, file_offset: u64, window_offset: usize, len: usize) -> Result<(), NixlError> {
        self.registration
            .memory
            .buffer
            .read_file_at(file, file_offset, self.offset(window_offset, len)?, len)
    }

    /// Write directly to a file without a userspace staging copy. Open the
    /// destination without append mode and keep its flags stable during the write.
    pub fn write_file_at(&self, file: &File, file_offset: u64, window_offset: usize, len: usize) -> Result<(), NixlError> {
        self.registration
            .memory
            .buffer
            .write_file_at(file, file_offset, self.offset(window_offset, len)?, len)
    }

    /// Consume the idle window before preparing one control request for a trusted peer.
    /// An empty GET still advertises a one-byte descriptor.
    ///
    /// ```compile_fail,E0382
    /// use rustfs_nixl::{NixlError, RegisteredClientWindow};
    /// fn prepare(mut idle: RegisteredClientWindow) -> Result<(), NixlError> {
    ///     let transfer = idle.begin(1)?;
    ///     idle.write_at(0, &[42])?;
    ///     drop(transfer);
    ///     Ok(())
    /// }
    /// ```
    pub fn begin(self, len: usize) -> Result<ClientWindowTransfer, NixlError> {
        self.offset(0, len.max(1))?;
        Ok(ClientWindowTransfer {
            window: self,
            expected: len,
        })
    }
}

impl Drop for RegisteredClientWindow {
    fn drop(&mut self) {
        if self.quarantine_on_drop {
            // A missing receipt cannot bound remote access. Pin the registration,
            // agent and mapping for the process lifetime, including on task cancellation.
            std::mem::forget(Arc::clone(&self.registration));
        }
    }
}

/// HTTP connection pool for one-shot transfers to trusted RustFS endpoints.
#[derive(Clone)]
pub struct ClientWindowHttp(reqwest::Client);

impl ClientWindowHttp {
    /// Retain the caller's TLS, timeout and proxy settings. A registered request
    /// may be sent only once and only to the intended endpoint.
    pub fn new(builder: reqwest::ClientBuilder) -> Result<Self, NixlError> {
        builder
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connection_verbose(false)
            .build()
            .map(Self)
            .map_err(control_error)
    }
}

/// An exclusively owned window prepared for transfer. Its registration and
/// data address cannot be extracted, and its pages cannot be borrowed.
pub struct ClientWindowTransfer {
    window: RegisteredClientWindow,
    expected: usize,
}

/// Headers received from the actual control exchange. A window is returned only
/// after confirmed completion or a verified rejection before DMA.
pub struct ClientWindowResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub window: Option<RegisteredClientWindow>,
    pub rejected_before_dma: bool,
}

impl ClientWindowTransfer {
    fn prepare(&self, request: &mut Request, credentials: &Credentials, region: &str) -> Result<StatusCode, NixlError> {
        let expected_status = match *request.method() {
            Method::GET if request.headers().contains_key("range") => StatusCode::PARTIAL_CONTENT,
            Method::GET => StatusCode::OK,
            Method::PUT if self.expected > 0 => StatusCode::OK,
            _ => return Err(NixlError::Token("unsupported registered transfer request".to_owned())),
        };
        let headers = request.headers_mut();
        if [
            NIXL_AGENT_HEADER,
            NIXL_METADATA_HEADER,
            NIXL_BUFFER_HEADER,
            "authorization",
            "x-amz-security-token",
            "x-amz-date",
        ]
        .iter()
        .any(|name| headers.contains_key(*name))
        {
            return Err(NixlError::Token("control request overrides a reserved header".to_owned()));
        }
        let registration = &self.window.registration;
        let address = registration.memory.buffer.addr() + self.window.offset;
        let descriptor = format!("dram:0:{address}:{}", self.expected.max(1));
        headers.insert(NIXL_AGENT_HEADER, registration.name.clone());
        headers.insert(NIXL_METADATA_HEADER, registration.metadata.clone());
        headers.insert(NIXL_BUFFER_HEADER, sensitive_header(&descriptor)?);
        sign_control(request, credentials, region)?;
        Ok(expected_status)
    }

    /// Sign and send exactly one request to the trusted RustFS endpoint. The
    /// peer must honor the advertised bounds and stop using them before acknowledging.
    /// The SDK checks the response on the same exchange. Cancellation after
    /// publication keeps the registration alive for the rest of the process.
    pub async fn exchange(
        mut self,
        http: &ClientWindowHttp,
        builder: RequestBuilder,
        credentials: &Credentials,
        region: &str,
    ) -> Result<ClientWindowResponse, NixlError> {
        let mut request = builder.build().map_err(control_error)?;
        let expected_status = self.prepare(&mut request, credentials, region)?;
        let url = request.url().clone();
        let download = request.method() == Method::GET;
        let conditional = download && request.headers().contains_key("if-match");
        self.window.quarantine_on_drop = true;
        let mut response = http.0.execute(request).await.map_err(control_error)?;
        let status = response.status();
        let same_url = response.url() == &url;
        let headers = std::mem::take(response.headers_mut());
        let completed = same_url
            && status == expected_status
            && completion_headers(status, &headers, self.expected)
            && (!download || empty_control_body(&headers, &mut response).await);
        let rejected_before_dma =
            same_url && conditional && precondition_headers(status, &headers) && precondition_body(response).await;
        let window = if completed || rejected_before_dma {
            self.window.quarantine_on_drop = false;
            Some(self.window)
        } else {
            None
        };
        Ok(ClientWindowResponse {
            status,
            headers,
            window,
            rejected_before_dma,
        })
    }
}

// Sign after installing the capability so neither the registration metadata
// nor a replayable signed request is returned to the caller.
fn sign_control(request: &mut Request, credentials: &Credentials, region: &str) -> Result<(), NixlError> {
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let identity = credentials.clone().into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("s3")
        .time(std::time::SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|_| NixlError::Token("invalid S3 signing parameters".to_owned()))?;
    let mut fields = Vec::with_capacity(request.headers().len());
    for (name, value) in request.headers() {
        fields.push((
            name.as_str(),
            value
                .to_str()
                .map_err(|_| NixlError::Token("invalid S3 control header".to_owned()))?,
        ));
    }
    let signable = SignableRequest::new(
        request.method().as_str(),
        request.url().as_str(),
        fields.into_iter(),
        SignableBody::UnsignedPayload,
    )
    .map_err(|_| NixlError::Token("invalid S3 control request".to_owned()))?;
    // The SigV4 TRACE event renders custom header values from the canonical
    // request. Its redaction list does not include NIXL capabilities.
    let (instructions, _) = tracing::dispatcher::with_default(&tracing::Dispatch::none(), || sign(signable, &params.into()))
        .map_err(|_| NixlError::Token("could not sign S3 control request".to_owned()))?
        .into_parts();
    for (name, value) in instructions.headers() {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| NixlError::Token("invalid signed header name".to_owned()))?;
        let mut value = HeaderValue::from_str(value).map_err(|_| NixlError::Token("invalid signed header value".to_owned()))?;
        value.set_sensitive(true);
        request.headers_mut().insert(name, value);
    }
    Ok(())
}

fn sensitive_header(text: &str) -> Result<HeaderValue, NixlError> {
    let mut value = HeaderValue::from_str(text).map_err(|_| NixlError::Token("invalid registered window header".to_owned()))?;
    value.set_sensitive(true);
    Ok(value)
}

fn single<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(first)
}

fn control_error(error: reqwest::Error) -> NixlError {
    NixlError::Control(error.without_url())
}

// RustFS rejects a failed conditional GET before transferring data. A generic
// HTTP 412 (including one sent by a proxy) does not establish that outcome.
fn precondition_headers(status: StatusCode, headers: &HeaderMap) -> bool {
    status == StatusCode::PRECONDITION_FAILED
        && !headers
            .keys()
            .any(|key| key.as_str().starts_with("x-amz-rdma-") || key.as_str().starts_with("x-rustfs-nixl-"))
        && (!headers.contains_key("x-amz-error-code") || single(headers, "x-amz-error-code") == Some("PreconditionFailed"))
}

// A registered GET puts its payload in the window. An HTTP entity body is a
// contradictory receipt, including when the server omits Content-Length.
async fn empty_control_body(headers: &HeaderMap, response: &mut Response) -> bool {
    if headers.contains_key("content-length") && single(headers, "content-length") != Some("0") {
        return false;
    }
    loop {
        match response.chunk().await {
            Ok(None) => return true,
            Ok(Some(chunk)) if chunk.is_empty() => continue,
            _ => return false,
        }
    }
}

async fn precondition_body(mut response: Response) -> bool {
    const LIMIT: usize = 16 * 1024;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if chunk.len() <= LIMIT - body.len() => body.extend_from_slice(&chunk),
            Ok(None) => break,
            _ => return false,
        }
    }
    precondition_xml(&body)
}

fn precondition_xml(body: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    enum Document {
        Error {
            #[serde(rename = "Code")]
            code: String,
        },
    }
    let mut xml = quick_xml::de::Deserializer::from_reader(body.trim_ascii_end());
    matches!(serde::Deserialize::deserialize(&mut xml), Ok(Document::Error {code}) if code == "PreconditionFailed")
        && xml.is_empty()
}

fn completion_headers(status: StatusCode, headers: &HeaderMap, expected: usize) -> bool {
    single(headers, RDMA_REPLY_HEADER) == Some(status.as_str())
        && match single(headers, RDMA_BYTES_TRANSFERRED_HEADER) {
            Some(count) => count.parse::<usize>().ok() == Some(expected),
            None => !headers.contains_key(RDMA_BYTES_TRANSFERRED_HEADER) && expected == 0,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn passive_windows_require_native_progress() {
        let config = NixlConfig {
            progress_thread: false,
            ..NixlConfig::default()
        };
        assert!(matches!(RegisteredClientWindow::register(&config),
            Err(NixlError::Token(message)) if message == "registered client windows require the native progress thread"));
    }

    #[test]
    fn releases_only_an_unambiguous_s3_precondition_rejection() {
        let mut headers = HeaderMap::new();
        assert!(precondition_headers(StatusCode::PRECONDITION_FAILED, &headers));
        assert!(!precondition_headers(StatusCode::BAD_GATEWAY, &headers));
        headers.insert("x-amz-error-code", HeaderValue::from_static("PreconditionFailed"));
        headers.append("x-amz-error-code", HeaderValue::from_static("PreconditionFailed"));
        assert!(!precondition_headers(StatusCode::PRECONDITION_FAILED, &headers));
        headers.remove("x-amz-error-code");
        headers.insert(RDMA_BYTES_TRANSFERRED_HEADER, HeaderValue::from_static("0"));
        assert!(!precondition_headers(StatusCode::PRECONDITION_FAILED, &headers));
        assert!(precondition_xml(b"<Error><Code>PreconditionFailed</Code></Error>"));
        assert!(!precondition_xml(b"<Error><Code>AccessDenied</Code></Error>"));
        assert!(!precondition_xml(b"<Error><Code>PreconditionFailed</Code></Error><Other/>"));
        assert!(!precondition_xml(b"<html>PreconditionFailed</html>"));
    }

    #[test]
    fn receipt_requires_an_unambiguous_status_and_byte_count() {
        let mut headers = HeaderMap::new();
        headers.insert(RDMA_REPLY_HEADER, HeaderValue::from_static("206"));
        assert!(completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 0));
        assert!(!completion_headers(StatusCode::OK, &headers, 0));
        assert!(!completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 1));
        headers.insert(RDMA_BYTES_TRANSFERRED_HEADER, HeaderValue::from_static("8"));
        assert!(completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 8));
        assert!(!completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 9));
        headers.append(RDMA_BYTES_TRANSFERRED_HEADER, HeaderValue::from_static("8"));
        assert!(!completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 8));
        assert!(!completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 0));
        headers.remove(RDMA_BYTES_TRANSFERRED_HEADER);
        headers.append(RDMA_REPLY_HEADER, HeaderValue::from_static("206"));
        assert!(!completion_headers(StatusCode::PARTIAL_CONTENT, &headers, 0));
    }
}
