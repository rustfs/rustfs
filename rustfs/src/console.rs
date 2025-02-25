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
struct Config {
    api: Api,
    s3: S3,
    release: Release,
    license: License,
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
        }
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
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

async fn config_handler(axum::extract::Extension(fs_addr): axum::extract::Extension<String>) -> impl IntoResponse {
    let cfg = Config::new(&fs_addr, "v0.0.1", "2025-01-01").to_json();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg))
        .unwrap()
}

pub async fn start_static_file_server(addrs: &str, fs_addr: &str) {
    // 创建路由
    let app = Router::new()
        .route("/config.json", get(config_handler).layer(axum::extract::Extension(fs_addr.to_owned())))
        .nest_service("/", get(static_handler));

    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();

    println!("console running on: http://{} with s3 api {}", listener.local_addr().unwrap(), fs_addr);

    axum::serve(listener, app).await.unwrap();
}
