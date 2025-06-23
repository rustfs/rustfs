use axum::routing::get;
use axum::{
    extract::Json,
    http::{HeaderMap, Response, StatusCode},
    routing::post,
    Router,
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Query;
use serde::Deserialize;

#[derive(Deserialize)]
struct ResetParams {
    reason: Option<String>,
}

// Define a global variable and count the number of data received
use std::sync::atomic::{AtomicU64, Ordering};

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
    let addr = "0.0.0.0:3020";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("Server running on {}", addr);

    // Self-checking after the service is started
    tokio::spawn(async move {
        // Give the server some time to start
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        match is_service_active(addr).await {
            Ok(true) => println!("Service health check: Successful - Service is running normally"),
            Ok(false) => eprintln!("Service Health Check: Failed - Service Not Responded"),
            Err(e) => eprintln!("Service health check errors:{}", e),
        }
    });

    // Create a shutdown signal processing
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

/// Create a method to reset the value of WEBHOOK_COUNT
async fn reset_webhook_count_with_path(axum::extract::Path(reason): axum::extract::Path<String>) -> Response<String> {
    // Output the value of the current counter
    let current_count = WEBHOOK_COUNT.load(Ordering::SeqCst);
    println!("Current webhook count: {}", current_count);

    println!("Reset webhook count, reason: {}", reason);
    // Reset the counter to 0
    WEBHOOK_COUNT.store(0, Ordering::SeqCst);
    println!("Webhook count has been reset to 0.");

    Response::builder()
        .header("Foo", "Bar")
        .status(StatusCode::OK)
        .body(format!(
            "Webhook count reset successfully. Previous count: {}. Reason: {}",
            current_count, reason
        ))
        .unwrap()
}

/// Create a method to reset the value of WEBHOOK_COUNT
/// You can reset the counter by calling this method
async fn reset_webhook_count(Query(params): Query<ResetParams>, headers: HeaderMap) -> Response<String> {
    // Output the value of the current counter
    let current_count = WEBHOOK_COUNT.load(Ordering::SeqCst);
    println!("Current webhook count: {}", current_count);

    let reason = params.reason.unwrap_or_else(|| "Reason not provided".to_string());
    println!("Reset webhook count, reason: {}", reason);

    for header in headers {
        let (key, value) = header;
        println!("Header: {:?}: {:?}", key, value);
    }

    println!("Reset webhook count printed headers");
    // Reset the counter to 0
    WEBHOOK_COUNT.store(0, Ordering::SeqCst);
    println!("Webhook count has been reset to 0.");
    Response::builder()
        .header("Foo", "Bar")
        .status(StatusCode::OK)
        .body(format!("Webhook count reset successfully current_count:{}", current_count))
        .unwrap()
}

async fn is_service_active(addr: &str) -> Result<bool, String> {
    let socket_addr = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| format!("Unable to resolve host:{}", e))?
        .next()
        .ok_or_else(|| "Address not found".to_string())?;

    println!("Checking service status:{}", socket_addr);

    match tokio::time::timeout(std::time::Duration::from_secs(5), tokio::net::TcpStream::connect(socket_addr)).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) => {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                Ok(false)
            } else {
                Err(format!("Connection failed:{}", e))
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
    println!("current time:{:04}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, minute, second);
    println!(
        "received a webhook request time:{} content:\n {}",
        seconds,
        serde_json::to_string_pretty(&payload).unwrap()
    );
    WEBHOOK_COUNT.fetch_add(1, Ordering::SeqCst);
    println!("Total webhook requests received: {}", WEBHOOK_COUNT.load(Ordering::SeqCst));
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
