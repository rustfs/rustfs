[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS S3Select API - SQL Query Interface

<p align="center">
  <strong>AWS S3 Select compatible SQL query API for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS S3Select API** provides AWS S3 Select compatible SQL query capabilities for the [RustFS](https://rustfs.com) distributed object storage system. It enables clients to retrieve subsets of data from objects using SQL expressions, reducing data transfer and improving query performance through server-side filtering.

> **Note:** This is a high-performance submodule of RustFS that provides essential SQL query capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📊 SQL Query Support

- **Standard SQL**: Support for SELECT, WHERE, GROUP BY, ORDER BY clauses
- **Data Types**: Support for strings, numbers, booleans, timestamps
- **Functions**: Built-in SQL functions (aggregation, string, date functions)
- **Complex Expressions**: Nested queries and complex conditional logic

### 📁 Format Support

- **CSV Files**: Comma-separated values with customizable delimiters
- **JSON Documents**: JSON objects and arrays with path expressions
- **Parquet Files**: Columnar format with schema evolution
- **Apache Arrow**: High-performance columnar data format

### 🚀 Performance Features

- **Streaming Processing**: Process large files without loading into memory
- **Parallel Execution**: Multi-threaded query execution
- **Predicate Pushdown**: Push filters down to storage layer
- **Columnar Processing**: Efficient columnar data processing with Apache DataFusion

### 🔧 S3 Compatibility

- **S3 Select API**: Full compatibility with AWS S3 Select API
- **Request Formats**: Support for JSON and XML request formats
- **Response Streaming**: Streaming query results back to clients
- **Error Handling**: AWS-compatible error responses

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-s3select-api = "0.1.0"
```

## 🔧 Usage

### Basic S3 Select Query

```rust
use rustfs_s3select_api::{S3SelectService, SelectRequest, InputSerialization, OutputSerialization};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create S3 Select service
    let s3select = S3SelectService::new().await?;

    // Configure input format (CSV)
    let input_serialization = InputSerialization::CSV {
        file_header_info: "USE".to_string(),
        record_delimiter: "\n".to_string(),
        field_delimiter: ",".to_string(),
        quote_character: "\"".to_string(),
        quote_escape_character: "\"".to_string(),
        comments: "#".to_string(),
    };

    // Configure output format
    let output_serialization = OutputSerialization::JSON {
        record_delimiter: "\n".to_string(),
    };

    // Create select request
    let select_request = SelectRequest {
        bucket: "sales-data".to_string(),
        key: "2024/sales.csv".to_string(),
        expression: "SELECT name, revenue FROM S3Object WHERE revenue > 10000".to_string(),
        expression_type: "SQL".to_string(),
        input_serialization,
        output_serialization,
        request_progress: false,
    };

    // Execute query
    let mut result_stream = s3select.select_object_content(select_request).await?;

    // Process streaming results
    while let Some(event) = result_stream.next().await {
        match event? {
            SelectEvent::Records(data) => {
                println!("Query result: {}", String::from_utf8(data)?);
            }
            SelectEvent::Stats(stats) => {
                println!("Bytes scanned: {}", stats.bytes_scanned);
                println!("Bytes processed: {}", stats.bytes_processed);
                println!("Bytes returned: {}", stats.bytes_returned);
            }
            SelectEvent::Progress(progress) => {
                println!("Progress: {}%", progress.details.bytes_processed_percent);
            }
            SelectEvent::End => {
                println!("Query completed");
                break;
            }
        }
    }

    Ok(())
}
```

### CSV Data Processing

```rust
use rustfs_s3select_api::{S3SelectService, CSVInputSerialization};

async fn csv_processing_example() -> Result<(), Box<dyn std::error::Error>> {
    let s3select = S3SelectService::new().await?;

    // Configure CSV input with custom settings
    let csv_input = CSVInputSerialization {
        file_header_info: "USE".to_string(),
        record_delimiter: "\r\n".to_string(),
        field_delimiter: "|".to_string(),
        quote_character: "'".to_string(),
        quote_escape_character: "\\".to_string(),
        comments: "//".to_string(),
        allow_quoted_record_delimiter: false,
    };

    // Query with aggregation
    let select_request = SelectRequest {
        bucket: "analytics".to_string(),
        key: "user-events.csv".to_string(),
        expression: r#"
            SELECT
                event_type,
                COUNT(*) as event_count,
                AVG(CAST(duration as DECIMAL)) as avg_duration
            FROM S3Object
            WHERE timestamp >= '2024-01-01'
            GROUP BY event_type
            ORDER BY event_count DESC
        "#.to_string(),
        expression_type: "SQL".to_string(),
        input_serialization: InputSerialization::CSV(csv_input),
        output_serialization: OutputSerialization::JSON {
            record_delimiter: "\n".to_string(),
        },
        request_progress: true,
    };

    let mut result_stream = s3select.select_object_content(select_request).await?;

    let mut total_events = 0;
    while let Some(event) = result_stream.next().await {
        match event? {
            SelectEvent::Records(data) => {
                let result: serde_json::Value = serde_json::from_slice(&data)?;
                println!("Event type: {}, Count: {}, Avg duration: {}",
                    result["event_type"], result["event_count"], result["avg_duration"]);
                total_events += result["event_count"].as_u64().unwrap_or(0);
            }
            SelectEvent::Progress(progress) => {
                println!("Processing: {}%", progress.details.bytes_processed_percent);
            }
            _ => {}
        }
    }

    println!("Total events processed: {}", total_events);
    Ok(())
}
```

### JSON Data Querying

```rust
use rustfs_s3select_api::{JSONInputSerialization, JSONType};

async fn json_querying_example() -> Result<(), Box<dyn std::error::Error>> {
    let s3select = S3SelectService::new().await?;

    // Configure JSON input
    let json_input = JSONInputSerialization {
        json_type: JSONType::Lines, // JSON Lines format
    };

    // Query nested JSON data
    let select_request = SelectRequest {
        bucket: "logs".to_string(),
        key: "application.jsonl".to_string(),
        expression: r#"
            SELECT
                s.timestamp,
                s.level,
                s.message,
                s.metadata.user_id,
                s.metadata.request_id
            FROM S3Object[*] s
            WHERE s.level = 'ERROR'
            AND s.metadata.user_id IS NOT NULL
            ORDER BY s.timestamp DESC
        "#.to_string(),
        expression_type: "SQL".to_string(),
        input_serialization: InputSerialization::JSON(json_input),
        output_serialization: OutputSerialization::JSON {
            record_delimiter: "\n".to_string(),
        },
        request_progress: false,
    };

    let mut result_stream = s3select.select_object_content(select_request).await?;

    while let Some(event) = result_stream.next().await {
        if let SelectEvent::Records(data) = event? {
            let log_entry: serde_json::Value = serde_json::from_slice(&data)?;
            println!("Error at {}: {} (User: {}, Request: {})",
                log_entry["timestamp"],
                log_entry["message"],
                log_entry["user_id"],
                log_entry["request_id"]
            );
        }
    }

    Ok(())
}
```

### Parquet File Analysis

```rust
use rustfs_s3select_api::{ParquetInputSerialization};

async fn parquet_analysis_example() -> Result<(), Box<dyn std::error::Error>> {
    let s3select = S3SelectService::new().await?;

    // Parquet files don't need serialization configuration
    let parquet_input = ParquetInputSerialization {};

    // Complex analytical query
    let select_request = SelectRequest {
        bucket: "data-warehouse".to_string(),
        key: "sales/2024/q1/sales_data.parquet".to_string(),
        expression: r#"
            SELECT
                region,
                product_category,
                SUM(amount) as total_sales,
                COUNT(*) as transaction_count,
                AVG(amount) as avg_transaction,
                MIN(amount) as min_sale,
                MAX(amount) as max_sale
            FROM S3Object
            WHERE sale_date >= '2024-01-01'
            AND sale_date < '2024-04-01'
            AND amount > 0
            GROUP BY region, product_category
            HAVING SUM(amount) > 50000
            ORDER BY total_sales DESC
            LIMIT 20
        "#.to_string(),
        expression_type: "SQL".to_string(),
        input_serialization: InputSerialization::Parquet(parquet_input),
        output_serialization: OutputSerialization::JSON {
            record_delimiter: "\n".to_string(),
        },
        request_progress: true,
    };

    let mut result_stream = s3select.select_object_content(select_request).await?;

    while let Some(event) = result_stream.next().await {
        match event? {
            SelectEvent::Records(data) => {
                let sales_data: serde_json::Value = serde_json::from_slice(&data)?;
                println!("Region: {}, Category: {}, Total Sales: ${:.2}",
                    sales_data["region"],
                    sales_data["product_category"],
                    sales_data["total_sales"]
                );
            }
            SelectEvent::Stats(stats) => {
                println!("Query statistics:");
                println!("  Bytes scanned: {}", stats.bytes_scanned);
                println!("  Bytes processed: {}", stats.bytes_processed);
                println!("  Bytes returned: {}", stats.bytes_returned);
            }
            _ => {}
        }
    }

    Ok(())
}
```

### Advanced SQL Functions

```rust
async fn advanced_sql_functions_example() -> Result<(), Box<dyn std::error::Error>> {
    let s3select = S3SelectService::new().await?;

    // Query with various SQL functions
    let select_request = SelectRequest {
        bucket: "analytics".to_string(),
        key: "user_data.csv".to_string(),
        expression: r#"
            SELECT
                -- String functions
                UPPER(name) as name_upper,
                SUBSTRING(email, 1, POSITION('@' IN email) - 1) as username,
                LENGTH(description) as desc_length,

                -- Date functions
                EXTRACT(YEAR FROM registration_date) as reg_year,
                DATE_DIFF('day', registration_date, last_login) as days_since_reg,

                -- Numeric functions
                ROUND(score, 2) as rounded_score,
                CASE
                    WHEN score >= 90 THEN 'Excellent'
                    WHEN score >= 70 THEN 'Good'
                    WHEN score >= 50 THEN 'Average'
                    ELSE 'Poor'
                END as score_category,

                -- Conditional logic
                COALESCE(nickname, SUBSTRING(name, 1, POSITION(' ' IN name) - 1)) as display_name

            FROM S3Object
            WHERE registration_date IS NOT NULL
            AND score IS NOT NULL
            ORDER BY score DESC
        "#.to_string(),
        expression_type: "SQL".to_string(),
        input_serialization: InputSerialization::CSV {
            file_header_info: "USE".to_string(),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            quote_character: "\"".to_string(),
            quote_escape_character: "\"".to_string(),
            comments: "#".to_string(),
        },
        output_serialization: OutputSerialization::JSON {
            record_delimiter: "\n".to_string(),
        },
        request_progress: false,
    };

    let mut result_stream = s3select.select_object_content(select_request).await?;

    while let Some(event) = result_stream.next().await {
        if let SelectEvent::Records(data) = event? {
            let user: serde_json::Value = serde_json::from_slice(&data)?;
            println!("User: {} ({}) - Score: {} ({})",
                user["display_name"],
                user["username"],
                user["rounded_score"],
                user["score_category"]
            );
        }
    }

    Ok(())
}
```

### Streaming Large Datasets

```rust
use rustfs_s3select_api::{SelectObjectContentStream, ProgressDetails};

async fn streaming_large_datasets() -> Result<(), Box<dyn std::error::Error>> {
    let s3select = S3SelectService::new().await?;

    let select_request = SelectRequest {
        bucket: "big-data".to_string(),
        key: "large_dataset.csv".to_string(),
        expression: "SELECT * FROM S3Object WHERE status = 'active'".to_string(),
        expression_type: "SQL".to_string(),
        input_serialization: InputSerialization::CSV {
            file_header_info: "USE".to_string(),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            quote_character: "\"".to_string(),
            quote_escape_character: "\"".to_string(),
            comments: "".to_string(),
        },
        output_serialization: OutputSerialization::JSON {
            record_delimiter: "\n".to_string(),
        },
        request_progress: true,
    };

    let mut result_stream = s3select.select_object_content(select_request).await?;

    let mut processed_count = 0;
    let mut output_file = tokio::fs::File::create("filtered_results.jsonl").await?;

    while let Some(event) = result_stream.next().await {
        match event? {
            SelectEvent::Records(data) => {
                // Write results to file
                output_file.write_all(&data).await?;
                processed_count += 1;

                if processed_count % 1000 == 0 {
                    println!("Processed {} records", processed_count);
                }
            }
            SelectEvent::Progress(progress) => {
                println!("Progress: {:.1}% ({} bytes processed)",
                    progress.details.bytes_processed_percent,
                    progress.details.bytes_processed
                );
            }
            SelectEvent::Stats(stats) => {
                println!("Final statistics:");
                println!("  Total bytes scanned: {}", stats.bytes_scanned);
                println!("  Total bytes processed: {}", stats.bytes_processed);
                println!("  Total bytes returned: {}", stats.bytes_returned);
                println!("  Processing efficiency: {:.2}%",
                    (stats.bytes_returned as f64 / stats.bytes_scanned as f64) * 100.0
                );
            }
            SelectEvent::End => {
                println!("Streaming completed. Total records: {}", processed_count);
                break;
            }
        }
    }

    output_file.flush().await?;
    Ok(())
}
```

### HTTP API Integration

```rust
use rustfs_s3select_api::{S3SelectHandler, SelectRequestXML};
use axum::{Router, Json, extract::{Path, Query}};

async fn setup_s3select_http_api() -> Router {
    let s3select_handler = S3SelectHandler::new().await.unwrap();

    Router::new()
        .route("/buckets/:bucket/objects/:key/select",
               axum::routing::post(handle_select_object_content))
        .layer(Extension(s3select_handler))
}

async fn handle_select_object_content(
    Path((bucket, key)): Path<(String, String)>,
    Extension(handler): Extension<S3SelectHandler>,
    body: String,
) -> Result<impl axum::response::IntoResponse, Box<dyn std::error::Error>> {
    // Parse S3 Select request (XML or JSON)
    let select_request = handler.parse_request(&body, &bucket, &key).await?;

    // Execute query
    let result_stream = handler.execute_select(select_request).await?;

    // Return streaming response
    let response = axum::response::Response::builder()
        .header("content-type", "application/xml")
        .header("x-amz-request-id", "12345")
        .body(axum::body::Body::from_stream(result_stream))?;

    Ok(response)
}
```

## 🏗️ Architecture

### S3Select API Architecture

```
S3Select API Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    S3 Select HTTP API                       │
├─────────────────────────────────────────────────────────────┤
│   Request      │   Response     │   Streaming  │   Error    │
│   Parsing      │   Formatting   │   Results    │   Handling │
├─────────────────────────────────────────────────────────────┤
│              Query Engine (DataFusion)                      │
├─────────────────────────────────────────────────────────────┤
│   SQL Parser   │   Optimizer    │   Execution  │   Streaming│
├─────────────────────────────────────────────────────────────┤
│              Storage Integration                            │
└─────────────────────────────────────────────────────────────┘
```

### Supported Data Formats

| Format | Features | Use Cases |
|--------|----------|-----------|
| CSV | Custom delimiters, headers, quotes | Log files, exports |
| JSON | Objects, arrays, nested data | APIs, documents |
| JSON Lines | Streaming JSON records | Event logs, analytics |
| Parquet | Columnar, schema evolution | Data warehousing |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test SQL parsing
cargo test sql_parsing

# Test format support
cargo test format_support

# Test streaming
cargo test streaming

# Integration tests
cargo test --test integration

# Performance tests
cargo test --test performance --release
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Apache DataFusion, Arrow
- **Memory**: Sufficient RAM for query processing

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS S3Select Query](../s3select-query) - Query engine implementation
- [RustFS ECStore](../ecstore) - Storage backend

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [S3Select API Reference](https://docs.rustfs.com/s3select-api/)
- [SQL Reference](https://docs.rustfs.com/sql/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details.

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with 📊 by the RustFS Team
</p>
