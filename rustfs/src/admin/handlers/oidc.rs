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

use super::sts::create_oidc_sts_credentials;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use http::StatusCode;
use hyper::Method;
use matchit::Params;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use time::OffsetDateTime;
use tracing::{error, info, warn};
use url::Url;

const OIDC_PATH_PREFIX: &str = "/rustfs/admin/v3/oidc";

/// Validate that a provider ID contains only safe characters (alphanumeric, underscore, hyphen).
fn is_valid_provider_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Validate that a redirect_after path is a safe relative path (starts with `/`, no scheme).
fn is_safe_redirect_path(path: &str) -> bool {
    path.starts_with('/') && !path.starts_with("//") && !path.contains("://")
}

/// Validate that a scheme is either "http" or "https".
fn is_valid_scheme(scheme: &str) -> bool {
    scheme == "http" || scheme == "https"
}

/// Register OIDC routes on the admin router.
pub fn register_oidc_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/providers"),
        AdminOperation(&ListOidcProvidersHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/authorize/{{provider_id}}"),
        AdminOperation(&OidcAuthorizeHandler {}),
    )?;
    r.insert(
        Method::GET,
        &format!("{ADMIN_PREFIX}/v3/oidc/callback/{{provider_id}}"),
        AdminOperation(&OidcCallbackHandler {}),
    )?;

    Ok(())
}

/// Returns true if the given path is an OIDC endpoint (requires unauthenticated access).
pub fn is_oidc_path(path: &str) -> bool {
    path.starts_with(OIDC_PATH_PREFIX)
}

/// Handler: GET /rustfs/admin/v3/oidc/providers
/// Returns list of configured OIDC providers for the login page.
pub struct ListOidcProvidersHandler {}

#[async_trait::async_trait]
impl Operation for ListOidcProvidersHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let oidc_sys = rustfs_iam::get_oidc().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let providers = oidc_sys.list_providers();
        let json_body = serde_json::to_vec(&providers)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize error: {e}")))?;

        let mut resp = S3Response::new((StatusCode::OK, Body::from(json_body)));
        resp.headers
            .insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
        Ok(resp)
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/authorize/:provider_id
/// Generates PKCE challenge, stores state, and returns 302 redirect to IdP.
pub struct OidcAuthorizeHandler {}

#[async_trait::async_trait]
impl Operation for OidcAuthorizeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        let oidc_sys = rustfs_iam::get_oidc().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        // Derive the callback redirect URI from the request
        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        // Optional: redirect_after query parameter (must be a safe relative path)
        let redirect_after = extract_safe_redirect_after(&req.uri)?;

        let auth_url = oidc_sys
            .authorize_url(provider_id, &redirect_uri, redirect_after)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("authorize failed: {e}")))?;

        info!("OIDC authorize redirect for provider '{}' to IdP", provider_id);

        // Return 302 redirect
        let mut resp = S3Response::new((StatusCode::FOUND, Body::empty()));
        resp.headers.insert(
            http::header::LOCATION,
            auth_url
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct authorization URL"))?,
        );
        Ok(resp)
    }
}

/// Handler: GET /rustfs/admin/v3/oidc/callback/:provider_id?code=...&state=...
/// Exchanges authorization code for tokens, maps claims, issues STS credentials.
pub struct OidcCallbackHandler {}

#[async_trait::async_trait]
impl Operation for OidcCallbackHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let provider_id = params
            .get("provider_id")
            .ok_or_else(|| s3_error!(InvalidRequest, "missing provider_id"))?;

        if !is_valid_provider_id(provider_id) {
            return Err(s3_error!(InvalidRequest, "invalid provider_id"));
        }

        // Extract code and state from query parameters
        let code =
            extract_query_param(&req.uri, "code").ok_or_else(|| s3_error!(InvalidRequest, "missing 'code' query parameter"))?;
        let state =
            extract_query_param(&req.uri, "state").ok_or_else(|| s3_error!(InvalidRequest, "missing 'state' query parameter"))?;

        // Check for error response from IdP
        if let Some(error) = extract_query_param(&req.uri, "error") {
            let desc = extract_query_param(&req.uri, "error_description").unwrap_or_default();
            warn!("OIDC callback received error from IdP: {} - {}", error, desc);
            return Err(S3Error::with_message(
                S3ErrorCode::AccessDenied,
                format!("OIDC authentication failed: {error} - {desc}"),
            ));
        }

        let oidc_sys = rustfs_iam::get_oidc().ok_or_else(|| s3_error!(InternalError, "OIDC not initialized"))?;

        let redirect_uri = derive_callback_uri(&req, provider_id)?;

        // Exchange authorization code for tokens and extract claims
        let (claims, actual_provider_id, session) = oidc_sys.exchange_code(&state, &code, &redirect_uri).await.map_err(|e| {
            error!("OIDC code exchange failed: {}", e);
            S3Error::with_message(S3ErrorCode::AccessDenied, format!("code exchange failed: {e}"))
        })?;

        info!(
            "OIDC login successful: username='{}', email='{}', sub='{}' (provider: {})",
            claims.username, claims.email, claims.sub, actual_provider_id
        );

        // Map claims to policies and groups
        let (policies, groups) = oidc_sys.map_claims_to_policies(&actual_provider_id, &claims);

        info!(
            "OIDC claim mapping: user='{}', policies={:?}, groups={:?}",
            claims.username, policies, groups
        );

        // Generate STS credentials using the shared helper.
        // Console/OIDC sessions use a fixed 1-hour duration as a security/UX choice.
        // Longer-lived credentials (15 min to 12 hours) can be requested via CLI/SDK
        // through AssumeRoleWithWebIdentity.
        let new_cred = create_oidc_sts_credentials(&claims, &actual_provider_id, &policies, &groups, 3600, None).await?;

        // Build redirect URL to console with credentials in the fragment
        let console_redirect = build_console_redirect(
            &req,
            &new_cred.access_key,
            &new_cred.secret_key,
            &new_cred.session_token,
            new_cred.expiration,
            session.redirect_after.as_deref(),
        )?;

        let mut resp = S3Response::new((StatusCode::FOUND, Body::empty()));
        resp.headers.insert(
            http::header::LOCATION,
            console_redirect
                .parse()
                .map_err(|_| s3_error!(InternalError, "failed to construct console redirect URL"))?,
        );
        Ok(resp)
    }
}

/// Derive the OIDC callback URI.
/// Uses the provider's configured redirect_uri if set, otherwise derives dynamically
/// from request headers. For production deployments behind a reverse proxy, configuring
/// an explicit redirect_uri is recommended to prevent header manipulation.
fn derive_callback_uri(req: &S3Request<Body>, provider_id: &str) -> S3Result<String> {
    // Use explicitly configured redirect_uri if available
    if let Some(oidc_sys) = rustfs_iam::get_oidc()
        && let Some(config) = oidc_sys.get_provider_config(provider_id)
    {
        if let Some(ref uri) = config.redirect_uri {
            let parsed = Url::parse(uri).map_err(|_| s3_error!(InvalidRequest, "invalid configured redirect_uri"))?;
            if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
                return Err(s3_error!(InvalidRequest, "configured redirect_uri must be absolute http/https URL"));
            }
            return Ok(uri.clone());
        }

        if !config.redirect_uri_dynamic {
            return Err(s3_error!(
                InvalidRequest,
                "provider requires explicit redirect_uri because redirect_uri_dynamic is disabled"
            ));
        }
    }

    let scheme = extract_request_scheme(req)?;
    let host = extract_request_host(req)?;

    Ok(format!("{scheme}://{host}/rustfs/admin/v3/oidc/callback/{provider_id}"))
}

/// Extract a query parameter from the URI.
fn extract_query_param(uri: &http::Uri, key: &str) -> Option<String> {
    uri.query().and_then(|q| {
        // Parse query string manually without external dependency
        q.split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let k = parts.next()?;
                let v = parts.next().unwrap_or("");
                if k == key {
                    Some(urlencoding::decode(v).unwrap_or_default().into_owned())
                } else {
                    None
                }
            })
            .next()
    })
}

fn extract_safe_redirect_after(uri: &http::Uri) -> S3Result<Option<String>> {
    let redirect_after = extract_query_param(uri, "redirect_after");
    match redirect_after {
        Some(value) if !is_safe_redirect_path(&value) => Err(s3_error!(InvalidRequest, "invalid redirect_after")),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

/// Build the console redirect URL with STS credentials in the hash fragment.
fn build_console_redirect(
    req: &S3Request<Body>,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    expiration: Option<OffsetDateTime>,
    redirect_after: Option<&str>,
) -> S3Result<String> {
    let scheme = extract_request_scheme(req)?;
    let host = extract_request_host(req)?;

    let console_prefix = "/rustfs/console";
    let page = redirect_after.filter(|p| is_safe_redirect_path(p)).unwrap_or("/");

    let exp_str = expiration
        .map(|e| e.format(&time::format_description::well_known::Rfc3339).unwrap_or_default())
        .unwrap_or_default();

    let fragment = format!(
        "accessKey={}&secretKey={}&sessionToken={}&expiration={}&redirect={}",
        urlencoding::encode(access_key),
        urlencoding::encode(secret_key),
        urlencoding::encode(session_token),
        urlencoding::encode(&exp_str),
        urlencoding::encode(page),
    );

    Ok(format!("{scheme}://{host}{console_prefix}/auth/oidc-callback/#{fragment}"))
}

fn extract_request_scheme(req: &S3Request<Body>) -> S3Result<String> {
    let raw_scheme = req
        .headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').next().map(str::trim).unwrap_or(""))
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| req.uri.scheme_str().map(str::to_owned))
        .unwrap_or_else(|| "http".to_owned())
        .to_ascii_lowercase();

    if !is_valid_scheme(&raw_scheme) {
        return Err(s3_error!(InvalidRequest, "invalid scheme in request"));
    }

    Ok(raw_scheme)
}

fn extract_request_host(req: &S3Request<Body>) -> S3Result<String> {
    let host = req
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let v = v.trim();
            if v.is_empty() { None } else { Some(v.to_owned()) }
        })
        .or_else(|| req.uri.authority().map(|a| a.as_str().to_owned()))
        .ok_or_else(|| s3_error!(InvalidRequest, "cannot determine host for redirect URI"))?;

    parse_host_authority(&host)
}

fn parse_host_authority(raw_host: &str) -> S3Result<String> {
    let host = raw_host.trim();
    if host.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }

    // Parse as authority to normalize and validate, while rejecting URL-style
    // constructions (userinfo, query, fragment, and explicit paths).
    let parsed = Url::parse(&format!("http://{host}")).map_err(|_| s3_error!(InvalidRequest, "invalid host header"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid host header"));
    }
    if parsed.path() != "/" {
        return Err(s3_error!(InvalidRequest, "invalid host"));
    }

    Ok(parsed.authority().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_oidc_path() {
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/providers"));
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/authorize/okta"));
        assert!(is_oidc_path("/rustfs/admin/v3/oidc/callback/okta"));
        assert!(!is_oidc_path("/rustfs/admin/v3/users"));
        assert!(!is_oidc_path("/health"));
    }

    #[test]
    fn test_extract_query_param() {
        let uri: http::Uri = "http://localhost/callback?code=abc123&state=xyz789".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "code"), Some("abc123".to_string()));
        assert_eq!(extract_query_param(&uri, "state"), Some("xyz789".to_string()));
        assert_eq!(extract_query_param(&uri, "missing"), None);
    }

    #[test]
    fn test_extract_query_param_empty() {
        let uri: http::Uri = "http://localhost/callback".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "code"), None);
    }

    #[test]
    fn test_extract_query_param_encoded() {
        let uri: http::Uri = "http://localhost/callback?redirect_after=%2Fdashboard".parse().unwrap();
        assert_eq!(extract_query_param(&uri, "redirect_after"), Some("/dashboard".to_string()));
    }

    #[test]
    fn test_parse_host_authority_rejects_userinfo() {
        assert!(parse_host_authority("evil.com@victim.com").is_err());
    }

    #[test]
    fn test_parse_host_authority_rejects_query_fragment() {
        assert!(parse_host_authority("example.com?x=y").is_err());
        assert!(parse_host_authority("example.com#fragment").is_err());
    }

    #[test]
    fn test_parse_host_authority_rejects_path() {
        assert!(parse_host_authority("example.com/path").is_err());
    }

    #[test]
    fn test_parse_host_authority_accepts_valid_host_with_port() {
        assert_eq!(
            parse_host_authority("example.com:8443").expect("valid host should pass"),
            "example.com:8443"
        );
    }

    #[test]
    fn test_extract_safe_redirect_after() {
        let uri: http::Uri = "http://localhost/callback?redirect_after=%2Fdashboard".parse().unwrap();
        assert_eq!(
            extract_safe_redirect_after(&uri).expect("valid redirect should pass"),
            Some("/dashboard".to_string())
        );

        let uri: http::Uri = "http://localhost/callback?redirect_after=javascript:alert(1)"
            .parse()
            .unwrap();
        assert!(extract_safe_redirect_after(&uri).is_err());
    }

    #[test]
    fn test_is_valid_provider_id() {
        assert!(is_valid_provider_id("AUTHENTIK"));
        assert!(is_valid_provider_id("my-provider"));
        assert!(is_valid_provider_id("okta_prod"));
        assert!(is_valid_provider_id("Azure123"));
        assert!(!is_valid_provider_id(""));
        assert!(!is_valid_provider_id("../evil"));
        assert!(!is_valid_provider_id("foo bar"));
        assert!(!is_valid_provider_id("foo/bar"));
        assert!(!is_valid_provider_id("provider;drop"));
    }

    #[test]
    fn test_is_safe_redirect_path() {
        assert!(is_safe_redirect_path("/"));
        assert!(is_safe_redirect_path("/dashboard"));
        assert!(is_safe_redirect_path("/buckets/my-bucket"));
        assert!(!is_safe_redirect_path("https://evil.com"));
        assert!(!is_safe_redirect_path("javascript:alert(1)"));
        assert!(!is_safe_redirect_path("//evil.com/path"));
        assert!(!is_safe_redirect_path("relative/path"));
        assert!(!is_safe_redirect_path(""));
    }

    #[test]
    fn test_is_valid_scheme() {
        assert!(is_valid_scheme("http"));
        assert!(is_valid_scheme("https"));
        assert!(!is_valid_scheme("ftp"));
        assert!(!is_valid_scheme("javascript"));
        assert!(!is_valid_scheme(""));
    }
}
