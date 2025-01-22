use axum::{
    body::Body,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use include_dir::{include_dir, Dir};

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

pub async fn start_static_file_server(addrs: &str) {
    // 创建路由
    let app = Router::new().route("/*file", get(static_handler));

    let listener = tokio::net::TcpListener::bind(addrs).await.unwrap();

    println!("console listening on: {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}
