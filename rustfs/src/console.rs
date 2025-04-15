use crate::config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use crate::license::get_license;
use axum::{
    body::Body,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_extra::extract::Host;
use std::io;

use axum::response::Redirect;
use axum_server::tls_rustls::RustlsConfig;
use http::{header, Uri};
use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use shadow_rs::shadow;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::signal;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, instrument};

shadow!(build);

const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/static"]
struct StaticFiles;

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
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
    fn new(local_ip: Ipv4Addr, port: u16, version: &str, date: &str) -> Self {
        Config {
            port,
            api: Api {
                base_url: format!("http://{}:{}/{}", local_ip, port, RUSTFS_ADMIN_PREFIX),
            },
            s3: S3 {
                endpoint: format!("http://{}:{}", local_ip, port),
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

    pub(crate) fn version(&self) -> String {
        format!(
            "RELEASE.{} (rust {} {})",
            self.release.date.clone(),
            build::RUST_VERSION,
            build::BUILD_TARGET
        )
    }

    pub(crate) fn license(&self) -> String {
        format!("{} {}", self.license.name.clone(), self.license.url.clone())
    }

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
pub(crate) fn init_console_cfg(local_ip: Ipv4Addr, port: u16) {
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

async fn license_handler() -> impl IntoResponse {
    let license = get_license().unwrap_or_default();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(serde_json::to_string(&license).unwrap_or_default()))
        .unwrap()
}

fn _is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            let octets = ip.octets();
            // 10.0.0.0/8
            octets[0] == 10 ||
                // 172.16.0.0/12
                (octets[0] == 172 && (octets[1] >= 16 && octets[1] <= 31)) ||
                // 192.168.0.0/16
                (octets[0] == 192 && octets[1] == 168)
        }
        std::net::IpAddr::V6(_) => false,
    }
}

#[allow(clippy::const_is_empty)]
#[instrument(fields(host))]
async fn config_handler(uri: Uri, Host(host): Host) -> impl IntoResponse {
    let scheme = uri.scheme().map(|s| s.as_str()).unwrap_or("http");

    // 从 uri 中获取 host，如果没有则使用 Host extractor 的值
    let host = uri.host().unwrap_or(host.as_str());

    let host = if host.contains(':') {
        let (host, _) = host.split_once(':').unwrap_or((host, "80"));
        host
    } else {
        host
    };

    // 将当前配置复制一份
    let mut cfg = CONSOLE_CONFIG.get().unwrap().clone();

    let url = format!("{}://{}:{}", scheme, host, cfg.port);

    // // 如果指定入口，直接使用
    // let url = if let Some(endpoint) = &config::get_config().console_fs_endpoint {
    //     debug!("axum Using rustfs endpoint address: {}", endpoint);
    //     endpoint.clone()
    // } else {
    //     let host_with_port = if host.contains(':') {
    //         host.clone()
    //     } else {
    //         format!("{}:80", host)
    //     };
    //     // 尝试解析为 socket address，但不强制要求一定要是 IP 地址
    //     let socket_addr = host_with_port.to_socket_addrs().ok().and_then(|mut addrs| addrs.next());
    //     debug!("axum Using host with port: {}, Socket address: {:?}", host_with_port, socket_addr);
    //     match socket_addr {
    //         Some(addr) if addr.ip().is_ipv4() => {
    //             let ipv4 = addr.ip().to_string();
    //             // 如果是私有 IP、环回地址或未指定地址，保留原始域名
    //             if is_private_ip(addr.ip()) || addr.ip().is_loopback() || addr.ip().is_unspecified() {
    //                 let (host, _) = host_with_port.split_once(':').unwrap_or((&host, "80"));
    //                 debug!("axum Using private IPv4 address: {}", host);
    //                 format!("http://{}:{}", host, cfg.port)
    //             } else {
    //                 debug!("axum Using public IPv4 address");
    //                 format!("http://{}:{}", ipv4, cfg.port)
    //             }
    //         }
    //         _ => {
    //             // 如果不是有效的 IPv4 地址，保留原始域名
    //             let (host, _) = host_with_port.split_once(':').unwrap_or((&host, "80"));
    //             debug!("axum Using domain address: {}", host);
    //             format!("http://{}:{}", host, cfg.port)
    //         }
    //     }
    // };

    cfg.api.base_url = format!("{}{}", url, RUSTFS_ADMIN_PREFIX);
    cfg.s3.endpoint = url;

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg.to_json()))
        .unwrap()
}

pub async fn start_static_file_server(
    addrs: &str,
    local_ip: Ipv4Addr,
    access_key: &str,
    secret_key: &str,
    tls_path: Option<String>,
) {
    // 配置 CORS
    let cors = CorsLayer::new()
        .allow_origin(Any) // 生产环境建议指定具体域名
        .allow_methods([http::Method::GET, http::Method::POST])
        .allow_headers([header::CONTENT_TYPE]);
    // Create a route
    let app = Router::new()
        .route("/license", get(license_handler))
        .route("/config.json", get(config_handler))
        .fallback_service(get(static_handler))
        .layer(cors)
        .layer(tower_http::compression::CompressionLayer::new())
        // .layer(tower_http::limit::RequestBodyLimitLayer::new(1024 * 1024)) // 开启 limit feature
        .layer(TraceLayer::new_for_http());
    let local_addr: SocketAddr = addrs.parse().expect("Failed to parse socket address");
    info!("WebUI: http://{}:{} http://127.0.0.1:{}", local_ip, local_addr.port(), local_addr.port());
    info!("   RootUser: {}", access_key);
    info!("   RootPass: {}", secret_key);

    // Check and start the HTTPS/HTTP server
    match start_server(addrs, local_addr, tls_path, app.clone()).await {
        Ok(_) => info!("Server shutdown gracefully"),
        Err(e) => error!("Server error: {}", e),
    }
}
async fn start_server(addrs: &str, local_addr: SocketAddr, tls_path: Option<String>, app: Router) -> io::Result<()> {
    let tls_path = tls_path.unwrap_or_default();
    let key_path = format!("{}/{}", tls_path, RUSTFS_TLS_KEY);
    let cert_path = format!("{}/{}", tls_path, RUSTFS_TLS_CERT);

    let addr = addrs
        .parse::<SocketAddr>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid address: {}", e)))?;

    let handle = axum_server::Handle::new();
    // create a signal off listening task
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Initiating graceful shutdown...");
        handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
    });

    let has_tls_certs = tokio::try_join!(tokio::fs::metadata(&key_path), tokio::fs::metadata(&cert_path)).is_ok();
    debug!("Console TLS certs: {:?}", has_tls_certs);
    if has_tls_certs {
        debug!("Found TLS certificates, starting with HTTPS");
        match RustlsConfig::from_pem_file(cert_path, key_path).await {
            Ok(config) => {
                info!("Starting HTTPS server...");
                let https_future = axum_server::bind_rustls(local_addr, config)
                    .handle(handle.clone())
                    .serve(app.into_make_service());
                // 启动 HTTP 重定向服务器
                let redirect_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), addr.port());
                let redirect_future = axum_server::bind(redirect_addr).serve(redirect_to_https(addr.port()).into_make_service());

                info!("HTTPS server running on https://{}", addr);
                info!("HTTP redirect server running on http://{}", redirect_addr);

                // 并发运行 HTTPS 和 HTTP 重定向
                tokio::try_join!(https_future, redirect_future)?;
                Ok(())
            }
            Err(e) => {
                error!("Failed to create TLS config: {}", e);
                start_http_server(addr, app, handle).await
            }
        }
    } else {
        debug!("TLS certificates not found at {} and {}", key_path, cert_path);
        start_http_server(addr, app, handle).await
    }
}

// HTTP 到 HTTPS 的 301 重定向
fn redirect_to_https(https_port: u16) -> Router {
    Router::new().route(
        "/*path",
        get({
            move |uri: Uri, req: http::Request<Body>| async move {
                let host = req
                    .headers()
                    .get("host")
                    .map_or("localhost", |h| h.to_str().unwrap_or("localhost"));
                let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("");
                let https_url = format!("https://{}:{}{}", host, https_port, path);
                Redirect::permanent(&https_url)
            }
        }),
    )
}

async fn start_http_server(addr: SocketAddr, app: Router, handle: axum_server::Handle) -> io::Result<()> {
    debug!("Starting HTTP server...");
    axum_server::bind(addr)
        .handle(handle)
        .serve(app.into_make_service())
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("shutdown_signal ctrl_c")
        },
        _ = terminate => {
            info!("shutdown_signal terminate")
        },
    }
}
