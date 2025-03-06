use axum::{
    body::Body,
    extract::Host,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use shadow_rs::shadow;
use std::net::{Ipv4Addr, ToSocketAddrs};
use std::sync::OnceLock;
use tracing::info;

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

#[allow(clippy::const_is_empty)]
async fn config_handler(Host(host): Host) -> impl IntoResponse {
    let host_with_port = if host.contains(':') { host } else { format!("{}:80", host) };

    let is_addr = host_with_port
        .to_socket_addrs()
        .map(|addrs| addrs.into_iter().find(|v| v.is_ipv4()))
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

pub async fn start_static_file_server(addrs: &str, local_ip: Ipv4Addr, access_key: &str, secret_key: &str) {
    // 创建路由
    let app = Router::new()
        .route("/config.json", get(config_handler))
        .nest_service("/", get(static_handler));

    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    info!("WebUI: http://{}:{} http://127.0.0.1:{}", local_ip, local_addr.port(), local_addr.port());
    info!("   RootUser: {}", access_key);
    info!("   RootPass: {}", secret_key);

    axum::serve(listener, app).await.unwrap();
}
