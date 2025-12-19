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

use axum::{
    Router,
    extract::Json,
    extract::Query,
    http::{HeaderMap, Response, StatusCode},
    routing::{get, post},
};
use rustfs_utils::parse_and_resolve_address;
use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct ResetParams {
    reason: Option<String>,
}

// Define a global variable and count the number of data received

static WEBHOOK_COUNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() {
    // Build an application
    let app = Router::new()
        .route("/webhook", post(receive_webhook))
        .route("/webhook/reset/{reason}", get(reset_webhook_count_with_path))
        .route("/webhook/reset", get(reset_webhook_count))
        .route("/webhook", get(receive_webhook));
    // Start the server
    // let addr = "[0.0.0.0.0.0.0.0]:3020";
    let server_addr = match parse_and_resolve_address(":3020") {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Failed to parse address: {e}");
            return;
        }
    };
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Server running on {server_addr}");

    // Self-checking after the service is started
    tokio::spawn(async move {
        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        match is_service_active(server_addr).await {
            Ok(true) => println!("Service health check: Successful - Service is running normally"),
            Ok(false) => eprintln!("Service Health Check: Failed - Service Not Responded"),
            Err(e) => eprintln!("Service health check errors:{e}"),
        }
    });

    // Create a shutdown signal processing
    tokio::select! {
        result = axum::serve(listener, app) => {
            if let Err(e) = result {
                eprintln!("Server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down server...");
        }
    }
}

/// Create a method to reset the value of WEBHOOK_COUNT
async fn reset_webhook_count_with_path(axum::extract::Path(reason): axum::extract::Path<String>) -> Response<String> {
    // Output the value of the current counter
    let current_count = WEBHOOK_COUNT.load(Ordering::SeqCst);
    println!("Current webhook count: {current_count}");

    println!("Reset webhook count, reason: {reason}");
    // Reset the counter to 0
    WEBHOOK_COUNT.store(0, Ordering::SeqCst);
    println!("Webhook count has been reset to 0.");

    Response::builder()
        .header("Foo", "Bar")
        .status(StatusCode::OK)
        .body(format!(
            "Webhook count reset successfully. Previous count: {current_count}. Reason: {reason}"
        ))
        .unwrap()
}

/// Create a method to reset the value of WEBHOOK_COUNT
/// You can reset the counter by calling this method
async fn reset_webhook_count(Query(params): Query<ResetParams>, headers: HeaderMap) -> Response<String> {
    // Output the value of the current counter
    let current_count = WEBHOOK_COUNT.load(Ordering::SeqCst);
    println!("Current webhook count: {current_count}");

    let reason = params.reason.unwrap_or_else(|| "Reason not provided".to_string());
    println!("Reset webhook count, reason: {reason}");
    let time_now = chrono::offset::Utc::now().to_string();
    for header in headers {
        let (key, value) = header;
        println!("Header: {key:?}: {value:?}, time: {time_now}");
    }

    println!("Reset webhook count printed headers");
    // Reset the counter to 0
    WEBHOOK_COUNT.store(0, Ordering::SeqCst);
    println!("Webhook count has been reset to 0.");
    let time_now = chrono::offset::Utc::now().to_string();
    Response::builder()
        .header("Foo", "Bar")
        .status(StatusCode::OK)
        .body(format!("Webhook count reset successfully current_count:{current_count},time: {time_now}"))
        .unwrap()
}

async fn is_service_active(addr: SocketAddr) -> Result<bool, String> {
    let socket_addr = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| format!("Unable to resolve host:{e}"))?
        .next()
        .ok_or_else(|| "Address not found".to_string())?;

    println!("Checking service status:{socket_addr}");

    match tokio::time::timeout(std::time::Duration::from_secs(5), tokio::net::TcpStream::connect(socket_addr)).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) => {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                Ok(false)
            } else {
                Err(format!("Connection failed:{e}"))
            }
        }
        Err(_) => Err("Connection timeout".to_string()),
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
    println!("current time:{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}");
    println!(
        "received a webhook request time:{} content:\n {}",
        seconds,
        serde_json::to_string_pretty(&payload).unwrap()
    );
    WEBHOOK_COUNT.fetch_add(1, Ordering::SeqCst);
    println!(
        "Total webhook requests received: {} , Time: {}",
        WEBHOOK_COUNT.load(Ordering::SeqCst),
        chrono::offset::Utc::now()
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
