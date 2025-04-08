use crate::license::get_license;
use axum::{
    body::Body,
    extract::Host,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use axum_server::tls_rustls::RustlsConfig;
use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use shadow_rs::shadow;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::signal;
use tracing::{debug, error, info};

shadow!(build);

const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

const RUSTFS_CONSOLE_TLS_KEY: &str = "rustfs_console_tls_key.pem";
const RUSTFS_CONSOLE_TLS_CERT: &str = "rustfs_console_tls_cert.pem";

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

#[allow(clippy::const_is_empty)]
async fn config_handler(Host(host): Host) -> impl IntoResponse {
    let host_with_port = if host.contains(':') { host } else { format!("{}:80", host) };

    let is_addr = host_with_port
        .to_socket_addrs()
        .map(|addrs| {
            addrs.into_iter().find(|v| {
                if let SocketAddr::V4(ipv4) = v {
                    !ipv4.ip().is_private() && !ipv4.ip().is_loopback() && !ipv4.ip().is_unspecified()
                } else {
                    false
                }
            })
        })
        .unwrap_or_default();

    let mut cfg = CONSOLE_CONFIG.get().unwrap().clone();

    let url = if let Some(addr) = is_addr {
        format!("http://{}:{}", addr.ip(), cfg.port)
    } else {
        let (host, _) = host_with_port.split_once(':').unwrap_or_default();
        format!("http://{}:{}", host, cfg.port)
    };

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
    // Create a route
    let app = Router::new()
        .route("/license", get(license_handler))
        .route("/config.json", get(config_handler))
        .nest_service("/", get(static_handler));
    let local_addr: SocketAddr = addrs.parse().expect("Failed to parse socket address");
    info!("WebUI: http://{}:{} http://127.0.0.1:{}", local_ip, local_addr.port(), local_addr.port());
    info!("   RootUser: {}", access_key);
    info!("   RootPass: {}", secret_key);

    let tls_path = tls_path.unwrap_or_default();
    let key_path = format!("{}/{}", tls_path, RUSTFS_CONSOLE_TLS_KEY);
    let cert_path = format!("{}/{}", tls_path, RUSTFS_CONSOLE_TLS_CERT);
    // Check and start the HTTPS/HTTP server
    match start_server(addrs, local_addr, &key_path, &cert_path, app.clone()).await {
        Ok(_) => info!("Server shutdown gracefully"),
        Err(e) => error!("Server error: {}", e),
    }
}
async fn start_server(addrs: &str, local_addr: SocketAddr, key_path: &str, cert_path: &str, app: Router) -> std::io::Result<()> {
    let has_tls_certs = tokio::try_join!(tokio::fs::metadata(key_path), tokio::fs::metadata(cert_path)).is_ok();

    if has_tls_certs {
        debug!("Found TLS certificates, starting with HTTPS");
        match tokio::try_join!(tokio::fs::read(key_path), tokio::fs::read(cert_path)) {
            Ok((key_data, cert_data)) => {
                match RustlsConfig::from_pem(cert_data, key_data).await {
                    Ok(config) => {
                        let handle = axum_server::Handle::new();
                        // create a signal off listening task
                        let handle_clone = handle.clone();
                        tokio::spawn(async move {
                            shutdown_signal().await;
                            handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
                        });
                        debug!("Starting HTTPS server...");
                        axum_server::bind_rustls(local_addr, config)
                            .handle(handle.clone())
                            .serve(app.into_make_service())
                            .await
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to create TLS config: {}", e);
                        start_http_server(addrs, app).await
                    }
                }
            }
            Err(e) => {
                error!("Failed to read TLS certificates: {}", e);
                start_http_server(addrs, app).await
            }
        }
    } else {
        debug!("TLS certificates not found at {} and {}", key_path, cert_path);
        start_http_server(addrs, app).await
    }
}

async fn start_http_server(addrs: &str, app: Router) -> std::io::Result<()> {
    debug!("Starting HTTP server...");
    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
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
