use axum::{extract::Json, http::StatusCode, routing::post, Router};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    // 构建应用
    let app = Router::new().route("/webhook", post(receive_webhook));
    // 启动服务器
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn receive_webhook(Json(payload): Json<Value>) -> StatusCode {
    println!("收到 webhook 请求 time: {}，内容：{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string(), serde_json::to_string_pretty(&payload).unwrap());
    StatusCode::OK
}
