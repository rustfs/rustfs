use axum::routing::get;
use axum::{extract::Json, http::StatusCode, routing::post, Router};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() {
    // 构建应用
    let app = Router::new()
        .route("/webhook", post(receive_webhook))
        .route("/webhook", get(receive_webhook));
    // 启动服务器
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3020").await.unwrap();
    println!("Server running on http://0.0.0.0:3020");

    // 创建关闭信号处理
    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                eprintln!("Server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down server...");
        }
    }
}

async fn receive_webhook(Json(payload): Json<Value>) -> StatusCode {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");

    // get the number of seconds since the unix era
    let seconds = since_the_epoch.as_secs();

    // Manually calculate year, month, day, hour, minute, and second
    let (year, month, day, hour, minute, second) = convert_seconds_to_date(seconds);

    // output result
    println!("current time:{:04}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, minute, second);
    println!(
        "received a webhook request time:{} content:\n {}",
        seconds,
        serde_json::to_string_pretty(&payload).unwrap()
    );
    StatusCode::OK
}

fn convert_seconds_to_date(seconds: u64) -> (u32, u32, u32, u32, u32, u32) {
    // assume that the time zone is utc
    let seconds_per_minute = 60;
    let seconds_per_hour = 3600;
    let seconds_per_day = 86400;

    // Calculate the year, month, day, hour, minute, and second corresponding to the number of seconds
    let mut total_seconds = seconds;
    let mut year = 1970;
    let mut month = 1;
    let mut day = 1;
    let mut hour = 0;
    let mut minute = 0;
    let mut second = 0;

    // calculate year
    while total_seconds >= 31536000 {
        year += 1;
        total_seconds -= 31536000; // simplified processing no leap year considered
    }

    // calculate month
    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in &days_in_month {
        if total_seconds >= m * seconds_per_day {
            month += 1;
            total_seconds -= m * seconds_per_day;
        } else {
            break;
        }
    }

    // calculate the number of days
    day += total_seconds / seconds_per_day;
    total_seconds %= seconds_per_day;

    // calculate hours
    hour += total_seconds / seconds_per_hour;
    total_seconds %= seconds_per_hour;

    // calculate minutes
    minute += total_seconds / seconds_per_minute;
    total_seconds %= seconds_per_minute;

    // calculate the number of seconds
    second += total_seconds;

    (year as u32, month as u32, day as u32, hour as u32, minute as u32, second as u32)
}
