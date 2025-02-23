use axum::{
    body::Body,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use include_dir::{include_dir, Dir};
use serde::Serialize;

static STATIC_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/static");

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    if let Some(file) = STATIC_DIR.get_file(path) {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(file.contents()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("404 Not Found"))
            .unwrap()
    }
}

#[derive(Debug, Clone, Serialize)]
struct Config {
    fs_addr: String,
}

async fn config_handler(axum::extract::Extension(fs_addr): axum::extract::Extension<String>) -> impl IntoResponse {
    let cfg = serde_json::to_string(&Config { fs_addr }).unwrap_or_default();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg))
        .unwrap()
}

pub async fn start_static_file_server(addrs: &str, fs_addr: &str) {
    // 创建路由
    let app = Router::new()
        .route("/config.json", get(config_handler).layer(axum::extract::Extension(fs_addr.to_string())))
        .route("/*file", get(static_handler));

    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();

    println!("console listening on: {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}
