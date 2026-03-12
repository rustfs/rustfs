// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Static Website Hosting for Swift Containers
//!
//! This module implements static website hosting, allowing Swift containers
//! to serve static websites directly without requiring an external web server.
//!
//! # Features
//!
//! - **Index Documents**: Serve default index.html for directory requests
//! - **Error Documents**: Custom 404 error pages
//! - **Directory Listings**: Auto-generated HTML listings (optional)
//! - **Custom CSS**: Style directory listings with custom CSS
//!
//! # Configuration
//!
//! Static website hosting is configured via container metadata:
//!
//! - `X-Container-Meta-Web-Index` - Index document name (e.g., "index.html")
//! - `X-Container-Meta-Web-Error` - Error document name (e.g., "404.html")
//! - `X-Container-Meta-Web-Listings` - Enable directory listings ("true"/"false")
//! - `X-Container-Meta-Web-Listings-CSS` - CSS file path for listings styling
//!
//! # Examples
//!
//! ```bash
//! # Enable static website hosting
//! swift post my-website \
//!   -m "web-index:index.html" \
//!   -m "web-error:404.html" \
//!   -m "web-listings:true"
//!
//! # Upload website files
//! swift upload my-website index.html
//! swift upload my-website 404.html
//! swift upload my-website style.css
//!
//! # Access website
//! curl http://swift.example.com/v1/AUTH_account/my-website/
//! # Returns: index.html
//! ```
//!
//! # Path Resolution
//!
//! 1. **Root or directory path** (ends with `/`):
//!    - Serve index document if configured
//!    - Otherwise, generate directory listing if enabled
//!    - Otherwise, return 404
//!
//! 2. **File path**:
//!    - Serve the file if it exists
//!    - If not found and error document configured, serve error document
//!    - Otherwise, return standard 404
//!
//! 3. **Directory without trailing slash**:
//!    - Redirect to path with trailing slash (301)

use super::{SwiftError, SwiftResult, container, object};
use axum::http::{Response, StatusCode};
use rustfs_credentials::Credentials;
use s3s::Body;
use tracing::debug;

/// Static website configuration for a container
#[derive(Debug, Clone, Default)]
pub struct StaticWebConfig {
    /// Index document name (e.g., "index.html")
    pub index: Option<String>,

    /// Error document name (e.g., "404.html")
    pub error: Option<String>,

    /// Enable directory listings
    pub listings: bool,

    /// CSS file path for directory listings
    pub listings_css: Option<String>,
}

impl StaticWebConfig {
    /// Check if static web is enabled (has index document configured)
    pub fn is_enabled(&self) -> bool {
        self.index.is_some()
    }

    /// Get index document name
    pub fn index_document(&self) -> Option<&str> {
        self.index.as_deref()
    }

    /// Get error document name
    pub fn error_document(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Check if directory listings are enabled
    pub fn listings_enabled(&self) -> bool {
        self.listings
    }

    /// Get listings CSS path
    pub fn listings_css_path(&self) -> Option<&str> {
        self.listings_css.as_deref()
    }
}

/// Load static website configuration from container metadata
pub async fn load_config(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<StaticWebConfig> {
    let metadata = container::get_container_metadata(account, container, credentials).await?;

    let index = metadata.custom_metadata.get("web-index").cloned();
    let error = metadata.custom_metadata.get("web-error").cloned();
    let listings = metadata
        .custom_metadata
        .get("web-listings")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false);
    let listings_css = metadata.custom_metadata.get("web-listings-css").cloned();

    Ok(StaticWebConfig {
        index,
        error,
        listings,
        listings_css,
    })
}

/// Check if static website hosting is enabled for a container
pub async fn is_enabled(account: &str, container: &str, credentials: &Credentials) -> SwiftResult<bool> {
    let config = load_config(account, container, credentials).await?;
    Ok(config.is_enabled())
}

/// Detect MIME type from file extension
pub fn detect_content_type(path: &str) -> &'static str {
    let extension = path.rsplit('.').next().unwrap_or("");

    match extension.to_lowercase().as_str() {
        // HTML/XML
        "html" | "htm" => "text/html; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "xhtml" => "application/xhtml+xml; charset=utf-8",

        // CSS/JavaScript
        "css" => "text/css; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "mjs" => "application/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",

        // Images
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        "ico" => "image/x-icon",

        // Fonts
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "eot" => "application/vnd.ms-fontobject",

        // Documents
        "pdf" => "application/pdf",
        "txt" => "text/plain; charset=utf-8",
        "md" => "text/markdown; charset=utf-8",

        // Media
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",

        // Archives
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",

        // Default
        _ => "application/octet-stream",
    }
}

/// Normalize path for static web serving
///
/// - Remove leading slash
/// - Treat empty path as root "/"
/// - Preserve trailing slash
pub fn normalize_path(path: &str) -> String {
    let path = path.trim_start_matches('/');
    if path.is_empty() { String::new() } else { path.to_string() }
}

/// Check if path represents a directory (ends with /)
pub fn is_directory_path(path: &str) -> bool {
    path.is_empty() || path.ends_with('/')
}

/// Resolve the actual object path to serve
///
/// Returns (object_path, is_index, is_listing)
pub fn resolve_path(path: &str, config: &StaticWebConfig) -> (String, bool, bool) {
    let normalized = normalize_path(path);

    if is_directory_path(&normalized) {
        // Directory path
        if let Some(index) = config.index_document() {
            // Serve index document
            let index_path = if normalized.is_empty() {
                index.to_string()
            } else {
                format!("{}{}", normalized, index)
            };
            (index_path, true, false)
        } else if config.listings_enabled() {
            // Generate directory listing
            (normalized, false, true)
        } else {
            // No index, no listings - 404
            (normalized, false, false)
        }
    } else {
        // File path - serve directly
        (normalized, false, false)
    }
}

/// Generate breadcrumb navigation HTML
fn generate_breadcrumbs(path: &str, container: &str) -> String {
    let mut html = String::from("<div class=\"breadcrumbs\">\n");
    html.push_str(&format!("  <a href=\"/\">/{}</a>\n", container));

    if !path.is_empty() {
        let parts: Vec<&str> = path.trim_end_matches('/').split('/').collect();
        let mut current_path = String::new();

        for (i, part) in parts.iter().enumerate() {
            current_path.push_str(part);
            if i < parts.len() - 1 {
                current_path.push('/');
                html.push_str(&format!("  / <a href=\"/{}\">{}</a>\n", current_path, part));
            } else {
                html.push_str(&format!("  / <strong>{}</strong>\n", part));
            }
        }
    }

    html.push_str("</div>\n");
    html
}

/// Generate HTML directory listing
pub fn generate_directory_listing(
    container: &str,
    path: &str,
    objects: &[super::types::Object],
    css_path: Option<&str>,
) -> String {
    let mut html = String::from("<!DOCTYPE html>\n<html>\n<head>\n");
    html.push_str("  <meta charset=\"utf-8\">\n");
    html.push_str(&format!("  <title>Index of /{}</title>\n", path));

    if let Some(css) = css_path {
        html.push_str(&format!("  <link rel=\"stylesheet\" href=\"/{}\">\n", css));
    } else {
        // Default inline CSS
        html.push_str("  <style>\n");
        html.push_str("    body { font-family: sans-serif; margin: 2em; }\n");
        html.push_str("    h1 { border-bottom: 1px solid #ccc; padding-bottom: 0.5em; }\n");
        html.push_str("    .breadcrumbs { margin: 1em 0; color: #666; }\n");
        html.push_str("    .breadcrumbs a { color: #0066cc; text-decoration: none; }\n");
        html.push_str("    .breadcrumbs a:hover { text-decoration: underline; }\n");
        html.push_str("    table { border-collapse: collapse; width: 100%; }\n");
        html.push_str("    th { text-align: left; padding: 0.5em; background: #f0f0f0; border-bottom: 2px solid #ccc; }\n");
        html.push_str("    td { padding: 0.5em; border-bottom: 1px solid #eee; }\n");
        html.push_str("    tr:hover { background: #f9f9f9; }\n");
        html.push_str("    a { color: #0066cc; text-decoration: none; }\n");
        html.push_str("    a:hover { text-decoration: underline; }\n");
        html.push_str("    .size { text-align: right; }\n");
        html.push_str("    .date { color: #666; }\n");
        html.push_str("  </style>\n");
    }

    html.push_str("</head>\n<body>\n");
    html.push_str(&format!("  <h1>Index of /{}</h1>\n", path));
    html.push_str(&generate_breadcrumbs(path, container));

    html.push_str("  <table>\n");
    html.push_str("    <thead>\n");
    html.push_str("      <tr>\n");
    html.push_str("        <th>Name</th>\n");
    html.push_str("        <th class=\"size\">Size</th>\n");
    html.push_str("        <th class=\"date\">Last Modified</th>\n");
    html.push_str("      </tr>\n");
    html.push_str("    </thead>\n");
    html.push_str("    <tbody>\n");

    // Add parent directory link if not at root
    if !path.is_empty() {
        let parent_path = if path.contains('/') {
            let parts: Vec<&str> = path.trim_end_matches('/').split('/').collect();
            parts[..parts.len() - 1].join("/") + "/"
        } else {
            String::from("")
        };

        html.push_str("      <tr>\n");
        html.push_str(&format!("        <td><a href=\"/{}\">..</a></td>\n", parent_path));
        html.push_str("        <td class=\"size\">-</td>\n");
        html.push_str("        <td class=\"date\">-</td>\n");
        html.push_str("      </tr>\n");
    }

    // List objects
    for obj in objects {
        let name = if let Some(stripped) = obj.name.strip_prefix(path) {
            stripped
        } else {
            &obj.name
        };

        // Format size
        let size = if obj.bytes < 1024 {
            format!("{} B", obj.bytes)
        } else if obj.bytes < 1024 * 1024 {
            format!("{:.1} KB", obj.bytes as f64 / 1024.0)
        } else if obj.bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", obj.bytes as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1} GB", obj.bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        };

        // Format date - last_modified is a String already
        let date = &obj.last_modified;

        html.push_str("      <tr>\n");
        html.push_str(&format!("        <td><a href=\"/{}\">{}</a></td>\n", obj.name, name));
        html.push_str(&format!("        <td class=\"size\">{}</td>\n", size));
        html.push_str(&format!("        <td class=\"date\">{}</td>\n", date));
        html.push_str("      </tr>\n");
    }

    html.push_str("    </tbody>\n");
    html.push_str("  </table>\n");
    html.push_str("</body>\n</html>\n");

    html
}

/// Handle static website GET request
pub async fn handle_static_web_get(
    account: &str,
    container: &str,
    path: &str,
    credentials: &Credentials,
) -> SwiftResult<Response<Body>> {
    // Load configuration
    let config = load_config(account, container, credentials).await?;

    if !config.is_enabled() {
        return Err(SwiftError::InternalServerError("Static web not enabled for this container".to_string()));
    }

    debug!("Static web request: container={}, path={}, config={:?}", container, path, config);

    // Resolve path
    let (object_path, _is_index, is_listing) = resolve_path(path, &config);

    if is_listing {
        // Generate directory listing
        debug!("Generating directory listing for path: {}", object_path);

        // List objects with prefix
        let prefix = if object_path.is_empty() {
            None
        } else {
            Some(object_path.to_string())
        };

        let objects = container::list_objects(
            account,
            container,
            credentials,
            None, // limit (i32)
            None, // marker
            prefix,
            None, // delimiter
        )
        .await?;

        // Generate HTML
        let html = generate_directory_listing(container, &object_path, &objects, config.listings_css_path());

        // Build response
        let trans_id = super::handler::generate_trans_id();
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/html; charset=utf-8")
            .header("content-length", html.len().to_string())
            .header("x-trans-id", trans_id.clone())
            .header("x-openstack-request-id", trans_id)
            .body(Body::from(html))
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)));
    }

    // Try to serve the object
    debug!("Attempting to serve object: {}", object_path);

    match object::get_object(account, container, &object_path, credentials, None).await {
        Ok(reader) => {
            // Object exists - serve it
            let content_type = detect_content_type(&object_path);

            let trans_id = super::handler::generate_trans_id();
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", content_type)
                .header("x-trans-id", trans_id.clone())
                .header("x-openstack-request-id", trans_id);

            // Convert reader to body
            use tokio_util::io::ReaderStream;
            let stream = ReaderStream::new(reader.stream);
            let axum_body = axum::body::Body::from_stream(stream);
            let body = Body::http_body_unsync(axum_body);

            response
                .body(body)
                .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
        }
        Err(SwiftError::NotFound(_)) => {
            // Object not found - try to serve error document
            if let Some(error_doc) = config.error_document() {
                debug!("Serving error document: {}", error_doc);

                match object::get_object(account, container, error_doc, credentials, None).await {
                    Ok(reader) => {
                        let content_type = detect_content_type(error_doc);

                        let trans_id = super::handler::generate_trans_id();
                        let response = Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .header("content-type", content_type)
                            .header("x-trans-id", trans_id.clone())
                            .header("x-openstack-request-id", trans_id);

                        use tokio_util::io::ReaderStream;
                        let stream = ReaderStream::new(reader.stream);
                        let axum_body = axum::body::Body::from_stream(stream);
                        let body = Body::http_body_unsync(axum_body);

                        return response
                            .body(body)
                            .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)));
                    }
                    Err(_) => {
                        // Error document also not found - return standard 404
                        debug!("Error document not found, returning standard 404");
                    }
                }
            }

            // Return standard 404
            let trans_id = super::handler::generate_trans_id();
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "text/plain; charset=utf-8")
                .header("x-trans-id", trans_id.clone())
                .header("x-openstack-request-id", trans_id)
                .body(Body::from("404 Not Found\n".to_string()))
                .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_web_config_enabled() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        assert!(config.is_enabled());
        assert_eq!(config.index_document(), Some("index.html"));
        assert_eq!(config.error_document(), None);
        assert!(!config.listings_enabled());
    }

    #[test]
    fn test_static_web_config_disabled() {
        let config = StaticWebConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_detect_content_type() {
        assert_eq!(detect_content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(detect_content_type("style.css"), "text/css; charset=utf-8");
        assert_eq!(detect_content_type("app.js"), "application/javascript; charset=utf-8");
        assert_eq!(detect_content_type("image.png"), "image/png");
        assert_eq!(detect_content_type("image.jpg"), "image/jpeg");
        assert_eq!(detect_content_type("font.woff2"), "font/woff2");
        assert_eq!(detect_content_type("data.json"), "application/json; charset=utf-8");
        assert_eq!(detect_content_type("unknown.xyz"), "application/octet-stream");
    }

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path(""), "");
        assert_eq!(normalize_path("/"), "");
        assert_eq!(normalize_path("/index.html"), "index.html");
        assert_eq!(normalize_path("index.html"), "index.html");
        assert_eq!(normalize_path("/docs/"), "docs/");
        assert_eq!(normalize_path("docs/"), "docs/");
    }

    #[test]
    fn test_is_directory_path() {
        assert!(is_directory_path(""));
        assert!(is_directory_path("/"));
        assert!(is_directory_path("docs/"));
        assert!(is_directory_path("/docs/"));
        assert!(!is_directory_path("index.html"));
        assert!(!is_directory_path("/index.html"));
        assert!(!is_directory_path("docs"));
    }

    #[test]
    fn test_resolve_path_root_with_index() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("", &config);
        assert_eq!(path, "index.html");
        assert!(is_index);
        assert!(!is_listing);

        let (path, is_index, is_listing) = resolve_path("/", &config);
        assert_eq!(path, "index.html");
        assert!(is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_resolve_path_directory_with_index() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("docs/", &config);
        assert_eq!(path, "docs/index.html");
        assert!(is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_resolve_path_file() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("style.css", &config);
        assert_eq!(path, "style.css");
        assert!(!is_index);
        assert!(!is_listing);

        let (path, is_index, is_listing) = resolve_path("/docs/readme.txt", &config);
        assert_eq!(path, "docs/readme.txt");
        assert!(!is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_resolve_path_directory_with_listings() {
        let config = StaticWebConfig {
            index: None,
            error: None,
            listings: true,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("docs/", &config);
        assert_eq!(path, "docs/");
        assert!(!is_index);
        assert!(is_listing);
    }

    #[test]
    fn test_resolve_path_directory_no_index_no_listings() {
        let config = StaticWebConfig {
            index: None,
            error: None,
            listings: false,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("docs/", &config);
        assert_eq!(path, "docs/");
        assert!(!is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_generate_breadcrumbs() {
        let html = generate_breadcrumbs("", "my-website");
        assert!(html.contains("/my-website"));

        let html = generate_breadcrumbs("docs/", "my-website");
        assert!(html.contains("/my-website"));
        assert!(html.contains("<strong>docs</strong>"));

        let html = generate_breadcrumbs("docs/api/", "my-website");
        assert!(html.contains("/my-website"));
        assert!(html.contains("docs"));
        assert!(html.contains("<strong>api</strong>"));
    }

    // Additional comprehensive tests

    #[test]
    fn test_content_type_comprehensive() {
        // Text formats
        assert_eq!(detect_content_type("file.txt"), "text/plain; charset=utf-8");
        assert_eq!(detect_content_type("README.md"), "text/markdown; charset=utf-8");
        assert_eq!(detect_content_type("data.xml"), "application/xml; charset=utf-8");

        // Web formats
        assert_eq!(detect_content_type("page.xhtml"), "application/xhtml+xml; charset=utf-8");
        assert_eq!(detect_content_type("module.mjs"), "application/javascript; charset=utf-8");

        // Images
        assert_eq!(detect_content_type("logo.svg"), "image/svg+xml");
        assert_eq!(detect_content_type("photo.webp"), "image/webp");
        assert_eq!(detect_content_type("favicon.ico"), "image/x-icon");

        // Fonts
        assert_eq!(detect_content_type("font.ttf"), "font/ttf");
        assert_eq!(detect_content_type("font.otf"), "font/otf");
        assert_eq!(detect_content_type("font.eot"), "application/vnd.ms-fontobject");

        // Media
        assert_eq!(detect_content_type("video.webm"), "video/webm");
        assert_eq!(detect_content_type("audio.ogg"), "audio/ogg");
        assert_eq!(detect_content_type("audio.wav"), "audio/wav");

        // Archives
        assert_eq!(detect_content_type("archive.gz"), "application/gzip");
        assert_eq!(detect_content_type("backup.tar"), "application/x-tar");
    }

    #[test]
    fn test_path_normalization_edge_cases() {
        assert_eq!(normalize_path("///"), "");
        assert_eq!(normalize_path("///index.html"), "index.html");
        assert_eq!(normalize_path("/a/b/c/"), "a/b/c/");
    }

    #[test]
    fn test_directory_path_detection() {
        // Directories
        assert!(is_directory_path("a/"));
        assert!(is_directory_path("a/b/"));
        assert!(is_directory_path("a/b/c/"));

        // Files
        assert!(!is_directory_path("a"));
        assert!(!is_directory_path("a/b"));
        assert!(!is_directory_path("a/b/file.html"));
    }

    #[test]
    fn test_resolve_path_nested_directories() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        // Nested directory with index
        let (path, is_index, is_listing) = resolve_path("docs/api/v1/", &config);
        assert_eq!(path, "docs/api/v1/index.html");
        assert!(is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_resolve_path_with_custom_index() {
        let config = StaticWebConfig {
            index: Some("default.htm".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("/", &config);
        assert_eq!(path, "default.htm");
        assert!(is_index);
        assert!(!is_listing);
    }

    #[test]
    fn test_config_with_all_features() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: Some("404.html".to_string()),
            listings: true,
            listings_css: Some("style.css".to_string()),
        };

        assert!(config.is_enabled());
        assert_eq!(config.index_document(), Some("index.html"));
        assert_eq!(config.error_document(), Some("404.html"));
        assert!(config.listings_enabled());
        assert_eq!(config.listings_css_path(), Some("style.css"));
    }

    #[test]
    fn test_config_minimal() {
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: false,
            listings_css: None,
        };

        assert!(config.is_enabled());
        assert!(config.error_document().is_none());
        assert!(!config.listings_enabled());
        assert!(config.listings_css_path().is_none());
    }

    #[test]
    fn test_breadcrumbs_root() {
        let html = generate_breadcrumbs("", "container");
        assert!(html.contains("breadcrumbs"));
        assert!(html.contains("/container"));
        assert!(!html.contains("<strong>")); // No subdirectories
    }

    #[test]
    fn test_breadcrumbs_single_level() {
        let html = generate_breadcrumbs("docs/", "my-site");
        assert!(html.contains("/my-site"));
        assert!(html.contains("<strong>docs</strong>"));
    }

    #[test]
    fn test_breadcrumbs_multiple_levels() {
        let html = generate_breadcrumbs("a/b/c/", "container");
        assert!(html.contains("/container"));
        assert!(html.contains("href=\"/a/\""));
        assert!(html.contains("href=\"/a/b/\""));
        assert!(html.contains("<strong>c</strong>"));
    }

    #[test]
    fn test_directory_listing_structure() {
        use super::super::types::Object;

        let objects = vec![
            Object {
                name: "docs/file1.txt".to_string(),
                hash: "abc".to_string(),
                bytes: 1024,
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-01T00:00:00Z".to_string(),
            },
            Object {
                name: "docs/file2.txt".to_string(),
                hash: "def".to_string(),
                bytes: 2048,
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-02T00:00:00Z".to_string(),
            },
        ];

        let html = generate_directory_listing("container", "docs/", &objects, None);

        // Check structure
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("<table>"));
        assert!(html.contains("<thead>"));
        assert!(html.contains("<tbody>"));

        // Check content
        assert!(html.contains("file1.txt"));
        assert!(html.contains("file2.txt"));
        assert!(html.contains("1.0 KB"));
        assert!(html.contains("2.0 KB"));
    }

    #[test]
    fn test_directory_listing_with_custom_css() {
        let objects = vec![];
        let html = generate_directory_listing("container", "", &objects, Some("custom.css"));

        assert!(html.contains("custom.css"));
        assert!(html.contains("<link rel=\"stylesheet\""));
    }

    #[test]
    fn test_directory_listing_size_formatting() {
        use super::super::types::Object;

        let objects = vec![
            Object {
                name: "small.txt".to_string(),
                hash: "a".to_string(),
                bytes: 500, // 500 B
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-01T00:00:00Z".to_string(),
            },
            Object {
                name: "medium.txt".to_string(),
                hash: "b".to_string(),
                bytes: 5120, // 5 KB
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-01T00:00:00Z".to_string(),
            },
            Object {
                name: "large.txt".to_string(),
                hash: "c".to_string(),
                bytes: 5242880, // 5 MB
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-01T00:00:00Z".to_string(),
            },
            Object {
                name: "huge.txt".to_string(),
                hash: "d".to_string(),
                bytes: 5368709120, // 5 GB
                content_type: "text/plain".to_string(),
                last_modified: "2024-01-01T00:00:00Z".to_string(),
            },
        ];

        let html = generate_directory_listing("container", "", &objects, None);

        // Check size formatting
        assert!(html.contains("500 B"));
        assert!(html.contains("KB"));
        assert!(html.contains("MB"));
        assert!(html.contains("GB"));
    }

    #[test]
    fn test_directory_listing_parent_link() {
        let objects = vec![];

        // Root directory - no parent link
        let html = generate_directory_listing("container", "", &objects, None);
        let parent_count = html.matches("..").count();
        assert_eq!(parent_count, 0, "Root should not have parent link");

        // Subdirectory - has parent link
        let html = generate_directory_listing("container", "docs/", &objects, None);
        assert!(html.contains(".."));
    }

    #[test]
    fn test_resolve_path_priority() {
        // Index takes priority over listings
        let config = StaticWebConfig {
            index: Some("index.html".to_string()),
            error: None,
            listings: true,
            listings_css: None,
        };

        let (path, is_index, is_listing) = resolve_path("/", &config);
        assert_eq!(path, "index.html");
        assert!(is_index);
        assert!(!is_listing); // Index wins over listings
    }

    #[test]
    fn test_content_type_case_insensitive() {
        assert_eq!(detect_content_type("FILE.HTML"), "text/html; charset=utf-8");
        assert_eq!(detect_content_type("FILE.CSS"), "text/css; charset=utf-8");
        assert_eq!(detect_content_type("FILE.JS"), "application/javascript; charset=utf-8");
        assert_eq!(detect_content_type("FILE.PNG"), "image/png");
    }
}
