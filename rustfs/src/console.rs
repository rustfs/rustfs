use axum::{
    body::Body,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use mime_guess::from_path;
use rust_embed::RustEmbed;
use serde::Serialize;
use shadow_rs::shadow;
use tracing::info;
use std::net::Ipv4Addr;
use std::sync::OnceLock;

shadow!(build);

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

#[derive(Debug, Serialize)]
pub(crate) struct Config {
    api: Api,
    s3: S3,
    release: Release,
    license: License,
    doc: String
}

impl Config {
    fn new(url: &str, version: &str, date: &str) -> Self {
        Config {
            api: Api {
                base_url: format!("{}/rustfs/admin/v3", url),
            },
            s3: S3 {
                endpoint: url.to_owned(),
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
            doc: "https://rustfs.com/docs/".to_string()
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

#[derive(Debug, Serialize)]
struct Api {
    #[serde(rename = "baseURL")]
    base_url: String,
}

#[derive(Debug, Serialize)]
struct S3 {
    endpoint: String,
    region: String,
}

#[derive(Debug, Serialize)]
struct Release {
    version: String,
    date: String,
}

#[derive(Debug, Serialize)]
struct License {
    name: String,
    url: String,
}

pub(crate) static CONSOLE_CONFIG: OnceLock<Config> = OnceLock::new();

pub(crate) fn init_console_cfg(fs_addr: &str) {
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

        Config::new(fs_addr, ver.as_str(), build::COMMIT_DATE_3339)
    });
}

#[allow(clippy::const_is_empty)]
async fn config_handler() -> impl IntoResponse {
    let cfg = CONSOLE_CONFIG.get().unwrap().to_json();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg))
        .unwrap()
}

pub async fn start_static_file_server(addrs: &str, local_ip: Ipv4Addr, access_key: &str, secret_key: &str) {
    // 创建路由
    let app = Router::new()
        .route("/config.json", get(config_handler))
        .nest_service("/", get(static_handler));

    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    info!("WebUI: http://{}:{} http://127.0.0.1:{}", 
          local_ip, local_addr.port(), local_addr.port());
    info!("   RootUser: {}", access_key);
    info!("   RootPass: {}", secret_key);
    
    axum::serve(listener, app).await.unwrap();
}
