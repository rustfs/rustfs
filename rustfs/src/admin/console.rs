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

use crate::config::build;
use crate::license::get_license;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum_extra::extract::Host;
use http::{HeaderMap, HeaderName, StatusCode, Uri};
use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;
// use axum::response::Redirect;
// use axum::routing::get;
// use axum::{
//     body::Body,
//     http::{Response, StatusCode},
//     response::IntoResponse,
//     Router,
// };
// use axum_extra::extract::Host;
// use axum_server::tls_rustls::RustlsConfig;
// use http::{header, HeaderMap, HeaderName, Uri};
// use io::Error;
// use mime_guess::from_path;
// use rust_embed::RustEmbed;
// use rustfs_config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
// use rustfs_utils::parse_and_resolve_address;
// use serde::Serialize;
// use std::io;
// use std::net::{IpAddr, SocketAddr};
// use std::sync::OnceLock;
// use std::time::Duration;
// use tokio::signal;
// use tower_http::cors::{Any, CorsLayer};
// use tower_http::trace::TraceLayer;
use tracing::{error, instrument};

// shadow!(build);

const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/static"]
struct StaticFiles;

/// Static file handler
pub(crate) async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        path = "index.html"
    }
    if let Some(file) = StaticFiles::get(path) {
        let mime_type = from_path(path).first_or_octet_stream();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.to_string())
            .body(Body::from(file.data))
            .unwrap()
    } else if let Some(file) = StaticFiles::get("index.html") {
        let mime_type = from_path("index.html").first_or_octet_stream();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.to_string())
            .body(Body::from(file.data))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("404 Not Found"))
            .unwrap()
    }
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct Config {
    #[serde(skip)]
    port: u16,
    api: Api,
    s3: S3,
    release: Release,
    license: License,
    doc: String,
}

impl Config {
    fn new(local_ip: IpAddr, port: u16, version: &str, date: &str) -> Self {
        Config {
            port,
            api: Api {
                base_url: format!("http://{local_ip}:{port}/{RUSTFS_ADMIN_PREFIX}"),
            },
            s3: S3 {
                endpoint: format!("http://{local_ip}:{port}"),
                region: "cn-east-1".to_owned(),
            },
            release: Release {
                version: version.to_string(),
                date: date.to_string(),
            },
            license: License {
                name: "Apache-2.0".to_string(),
                url: "https://www.apache.org/licenses/LICENSE-2.0".to_string(),
            },
            doc: "https://rustfs.com/docs/".to_string(),
        }
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn version_info(&self) -> String {
        format!(
            "RELEASE.{}@{} (rust {} {})",
            self.release.date.clone(),
            self.release.version.clone().trim_start_matches('@'),
            build::RUST_VERSION,
            build::BUILD_TARGET
        )
    }

    #[allow(dead_code)]
    pub(crate) fn version(&self) -> String {
        self.release.version.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn license(&self) -> String {
        format!("{} {}", self.license.name.clone(), self.license.url.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn doc(&self) -> String {
        self.doc.clone()
    }
}

#[derive(Debug, Serialize, Clone)]
struct Api {
    #[serde(rename = "baseURL")]
    base_url: String,
}

#[derive(Debug, Serialize, Clone)]
struct S3 {
    endpoint: String,
    region: String,
}

#[derive(Debug, Serialize, Clone)]
struct Release {
    version: String,
    date: String,
}

#[derive(Debug, Serialize, Clone)]
struct License {
    name: String,
    url: String,
}

pub(crate) static CONSOLE_CONFIG: OnceLock<Config> = OnceLock::new();

#[allow(clippy::const_is_empty)]
pub(crate) fn init_console_cfg(local_ip: IpAddr, port: u16) {
    CONSOLE_CONFIG.get_or_init(|| {
        let ver = {
            if !build::TAG.is_empty() {
                build::TAG.to_string()
            } else if !build::SHORT_COMMIT.is_empty() {
                format!("@{}", build::SHORT_COMMIT)
            } else {
                build::PKG_VERSION.to_string()
            }
        };

        Config::new(local_ip, port, ver.as_str(), build::COMMIT_DATE_3339)
    });
}

// fn is_socket_addr_or_ip_addr(host: &str) -> bool {
//     host.parse::<SocketAddr>().is_ok() || host.parse::<IpAddr>().is_ok()
// }

#[allow(dead_code)]
pub async fn license_handler() -> impl IntoResponse {
    let license = get_license().unwrap_or_default();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(serde_json::to_string(&license).unwrap_or_default()))
        .unwrap()
}

fn _is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            // 10.0.0.0/8
            octets[0] == 10 ||
                // 172.16.0.0/12
                (octets[0] == 172 && (octets[1] >= 16 && octets[1] <= 31)) ||
                // 192.168.0.0/16
                (octets[0] == 192 && octets[1] == 168)
        }
        IpAddr::V6(_) => false,
    }
}

#[allow(clippy::const_is_empty)]
#[allow(dead_code)]
#[instrument(fields(host))]
pub async fn config_handler(uri: Uri, Host(host): Host, headers: HeaderMap) -> impl IntoResponse {
    // Get the scheme from the headers or use the URI scheme
    let scheme = headers
        .get(HeaderName::from_static("x-forwarded-proto"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| uri.scheme().map(|s| s.as_str()).unwrap_or("http"));

    let raw_host = uri.host().unwrap_or(host.as_str());
    let host_for_url = if let Ok(socket_addr) = raw_host.parse::<SocketAddr>() {
        // Successfully parsed, it's in IP:Port format.
        // For IPv6, we need to enclose it in brackets to form a valid URL.
        let ip = socket_addr.ip();
        if ip.is_ipv6() { format!("[{ip}]") } else { format!("{ip}") }
    } else if let Ok(ip) = raw_host.parse::<IpAddr>() {
        // Pure IP (no ports)
        if ip.is_ipv6() { format!("[{ip}]") } else { ip.to_string() }
    } else {
        // The domain name may not be able to resolve directly to IP, remove the port
        raw_host.split(':').next().unwrap_or(raw_host).to_string()
    };

    // Make a copy of the current configuration
    let mut cfg = match CONSOLE_CONFIG.get() {
        Some(cfg) => cfg.clone(),
        None => {
            error!("Console configuration not initialized");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("Console configuration not initialized"))
                .unwrap();
        }
    };

    let url = format!("{}://{}:{}", scheme, host_for_url, cfg.port);
    cfg.api.base_url = format!("{url}{RUSTFS_ADMIN_PREFIX}");
    cfg.s3.endpoint = url;

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg.to_json()))
        .unwrap()
}

// pub fn register_router() -> Router {
//     Router::new()
//         .route("/license", get(license_handler))
//         .route("/config.json", get(config_handler))
//         .fallback_service(get(static_handler))
// }
//
// #[allow(dead_code)]
// pub async fn start_static_file_server(addrs: &str, tls_path: Option<String>) {
//     // Configure CORS
//     let cors = CorsLayer::new()
//         .allow_origin(Any) // In the production environment, we recommend that you specify a specific domain name
//         .allow_methods([http::Method::GET, http::Method::POST])
//         .allow_headers([header::CONTENT_TYPE]);
//
//     // Create a route
//     let app = register_router()
//         .layer(cors)
//         .layer(tower_http::compression::CompressionLayer::new().gzip(true).deflate(true))
//         .layer(TraceLayer::new_for_http());
//
//     // Check and start the HTTPS/HTTP server
//     match start_server(addrs, tls_path, app).await {
//         Ok(_) => info!("Console Server shutdown gracefully"),
//         Err(e) => error!("Console Server error: {}", e),
//     }
// }
//
// async fn start_server(addrs: &str, tls_path: Option<String>, app: Router) -> io::Result<()> {
//     let server_addr = parse_and_resolve_address(addrs).expect("Console Failed to parse socket address");
//     let server_port = server_addr.port();
//     let server_address = server_addr.to_string();
//
//     info!("Console WebUI: http://{} http://127.0.0.1:{} ", server_address, server_port);
//
//     let tls_path = tls_path.unwrap_or_default();
//     let key_path = format!("{tls_path}/{RUSTFS_TLS_KEY}");
//     let cert_path = format!("{tls_path}/{RUSTFS_TLS_CERT}");
//     let handle = axum_server::Handle::new();
//     // create a signal off listening task
//     let handle_clone = handle.clone();
//     tokio::spawn(async move {
//         shutdown_signal().await;
//         info!("Console Initiating graceful shutdown...");
//         handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
//     });
//
//     let has_tls_certs = tokio::try_join!(tokio::fs::metadata(&key_path), tokio::fs::metadata(&cert_path)).is_ok();
//     info!("Console TLS certs: {:?}", has_tls_certs);
//     if has_tls_certs {
//         info!("Console Found TLS certificates, starting with HTTPS");
//         match RustlsConfig::from_pem_file(cert_path, key_path).await {
//             Ok(config) => {
//                 info!("Console Starting HTTPS server...");
//                 axum_server::bind_rustls(server_addr, config)
//                     .handle(handle.clone())
//                     .serve(app.into_make_service())
//                     .await
//                     .map_err(Error::other)?;
//
//                 info!("Console HTTPS server running on https://{}", server_addr);
//
//                 Ok(())
//             }
//             Err(e) => {
//                 error!("Console Failed to create TLS config: {}", e);
//                 start_http_server(server_addr, app, handle).await
//             }
//         }
//     } else {
//         info!("Console TLS certificates not found at {} and {}", key_path, cert_path);
//         start_http_server(server_addr, app, handle).await
//     }
// }
//
// #[allow(dead_code)]
// /// 308 redirect for HTTP to HTTPS
// fn redirect_to_https(https_port: u16) -> Router {
//     Router::new().route(
//         "/*path",
//         get({
//             move |uri: Uri, req: http::Request<Body>| async move {
//                 let host = req
//                     .headers()
//                     .get("host")
//                     .map_or("localhost", |h| h.to_str().unwrap_or("localhost"));
//                 let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("");
//                 let https_url = format!("https://{host}:{https_port}{path}");
//                 Redirect::permanent(&https_url)
//             }
//         }),
//     )
// }
//
// async fn start_http_server(addr: SocketAddr, app: Router, handle: axum_server::Handle) -> io::Result<()> {
//     info!("Console Starting HTTP server... {}", addr.to_string());
//     axum_server::bind(addr)
//         .handle(handle)
//         .serve(app.into_make_service())
//         .await
//         .map_err(Error::other)
// }
//
// async fn shutdown_signal() {
//     let ctrl_c = async {
//         signal::ctrl_c().await.expect("Console failed to install Ctrl+C handler");
//     };
//
//     #[cfg(unix)]
//     let terminate = async {
//         signal::unix::signal(signal::unix::SignalKind::terminate())
//             .expect("Console failed to install signal handler")
//             .recv()
//             .await;
//     };
//
//     #[cfg(not(unix))]
//     let terminate = std::future::pending::<()>();
//
//     tokio::select! {
//         _ = ctrl_c => {
//             info!("Console shutdown_signal ctrl_c")
//         },
//         _ = terminate => {
//             info!("Console shutdown_signal terminate")
//         },
//     }
// }
