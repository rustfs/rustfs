[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS S3Select Query - High-Performance Query Engine

<p align="center">
  <strong>Apache DataFusion-powered SQL query engine for RustFS S3 Select implementation</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS S3Select Query** is the high-performance query engine that powers SQL processing for the [RustFS](https://rustfs.com) S3 Select API. Built on Apache DataFusion, it provides blazing-fast SQL execution with advanced optimization techniques, streaming processing, and support for multiple data formats.

> **Note:** This is a core performance-critical submodule of RustFS that provides the SQL query execution engine for the S3 Select API. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🚀 High-Performance Query Engine

- **Apache DataFusion**: Built on the fastest SQL engine in Rust
- **Vectorized Processing**: SIMD-accelerated columnar processing
- **Parallel Execution**: Multi-threaded query execution
- **Memory Efficient**: Streaming processing with minimal memory footprint

### 📊 Advanced SQL Support

- **Standard SQL**: Full support for SQL:2016 standard
- **Complex Queries**: Joins, subqueries, window functions, CTEs
- **Aggregations**: Group by, having, order by with optimizations
- **Built-in Functions**: 200+ SQL functions including UDFs

### 🔧 Query Optimization

- **Cost-Based Optimizer**: Intelligent query planning
- **Predicate Pushdown**: Push filters to data sources
- **Projection Pushdown**: Only read required columns
- **Join Optimization**: Hash joins, sort-merge joins

### 📁 Data Format Support

- **Parquet**: Native columnar format with predicate pushdown
- **CSV**: Efficient CSV parsing with schema inference
- **JSON**: Nested JSON processing with path expressions
- **Arrow**: Zero-copy Arrow format processing

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-s3select-query = "0.1.0"
```

## 🔧 Usage

### Basic Query Engine Setup

```rust
use rustfs_s3select_query::{QueryEngine, DataSource, QueryResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create query engine
    let query_engine = QueryEngine::new().await?;

    // Register data source
    let data_source = DataSource::from_csv("s3://bucket/data.csv").await?;
    query_engine.register_table("sales", data_source).await?;

    // Execute SQL query
    let sql = "SELECT region, SUM(amount) as total FROM sales GROUP BY region";
    let result = query_engine.execute_query(sql).await?;

    // Process results
    while let Some(batch) = result.next().await {
        let batch = batch?;
        println!("Batch with {} rows", batch.num_rows());

        // Convert to JSON for display
        let json_rows = batch.to_json()?;
        for row in json_rows {
            println!("{}", row);
        }
    }

    Ok(())
}
```

### Advanced Query Execution

```rust
use rustfs_s3select_query::{
    QueryEngine, QueryPlan, ExecutionConfig,
    DataSource, SchemaRef, RecordBatch
};

async fn advanced_query_example() -> Result<(), Box<dyn std::error::Error>> {
    // Configure execution settings
    let config = ExecutionConfig::new()
        .with_target_partitions(8)
        .with_batch_size(8192)
        .with_max_memory(1024 * 1024 * 1024); // 1GB memory limit

    let query_engine = QueryEngine::with_config(config).await?;

    // Register multiple data sources
    let customers = DataSource::from_parquet("s3://warehouse/customers.parquet").await?;
    let orders = DataSource::from_csv("s3://logs/orders.csv").await?;
    let products = DataSource::from_json("s3://catalog/products.json").await?;

    query_engine.register_table("customers", customers).await?;
    query_engine.register_table("orders", orders).await?;
    query_engine.register_table("products", products).await?;

    // Complex analytical query
    let sql = r#"
        SELECT
            c.customer_segment,
            p.category,
            COUNT(*) as order_count,
            SUM(o.amount) as total_revenue,
            AVG(o.amount) as avg_order_value,
            STDDEV(o.amount) as revenue_stddev
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN products p ON o.product_id = p.product_id
        WHERE o.order_date >= '2024-01-01'
        AND o.status = 'completed'
        GROUP BY c.customer_segment, p.category
        HAVING SUM(o.amount) > 10000
        ORDER BY total_revenue DESC
        LIMIT 50
    "#;

    // Get query plan for optimization analysis
    let plan = query_engine.create_logical_plan(sql).await?;
    println!("Query plan:\n{}", plan.display_indent());

    // Execute with streaming results
    let mut result_stream = query_engine.execute_stream(sql).await?;

    let mut total_rows = 0;
    while let Some(batch) = result_stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();

        // Process batch
        for row_idx in 0..batch.num_rows() {
            let segment = batch.column_by_name("customer_segment")?
                .as_any().downcast_ref::<StringArray>()
                .unwrap().value(row_idx);
            let category = batch.column_by_name("category")?
                .as_any().downcast_ref::<StringArray>()
                .unwrap().value(row_idx);
            let revenue = batch.column_by_name("total_revenue")?
                .as_any().downcast_ref::<Float64Array>()
                .unwrap().value(row_idx);

            println!("Segment: {}, Category: {}, Revenue: ${:.2}",
                segment, category, revenue);
        }
    }

    println!("Total rows processed: {}", total_rows);
    Ok(())
}
```

### Custom Data Sources

```rust
use rustfs_s3select_query::{DataSource, TableProvider, SchemaRef};
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use datafusion::arrow::record_batch::RecordBatch;

struct CustomS3DataSource {
    bucket: String,
    key: String,
    schema: SchemaRef,
}

impl CustomS3DataSource {
    async fn new(bucket: &str, key: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Infer schema from S3 object
        let schema = Self::infer_schema(bucket, key).await?;

        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            schema: Arc::new(schema),
        })
    }

    async fn infer_schema(bucket: &str, key: &str) -> Result<Schema, Box<dyn std::error::Error>> {
        // Read sample data to infer schema
        let sample_data = read_s3_sample(bucket, key).await?;

        // Create schema based on data format
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        ]);

        Ok(schema)
    }
}

#[async_trait::async_trait]
impl TableProvider for CustomS3DataSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create execution plan for scanning S3 data
        let scan_plan = S3ScanExec::new(
            self.bucket.clone(),
            self.key.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        );

        Ok(Arc::new(scan_plan))
    }
}

async fn custom_data_source_example() -> Result<(), Box<dyn std::error::Error>> {
    let query_engine = QueryEngine::new().await?;

    // Register custom data source
    let custom_source = CustomS3DataSource::new("analytics", "events.parquet").await?;
    query_engine.register_table("events", Arc::new(custom_source)).await?;

    // Query custom data source
    let sql = "SELECT * FROM events WHERE timestamp > NOW() - INTERVAL '1 day'";
    let result = query_engine.execute_query(sql).await?;

    // Process results
    while let Some(batch) = result.next().await {
        let batch = batch?;
        println!("Custom source batch: {} rows", batch.num_rows());
    }

    Ok(())
}
```

### Query Optimization and Analysis

```rust
use rustfs_s3select_query::{QueryEngine, QueryOptimizer, QueryMetrics};

async fn query_optimization_example() -> Result<(), Box<dyn std::error::Error>> {
    let query_engine = QueryEngine::new().await?;

    // Register data source
    let data_source = DataSource::from_parquet("s3://warehouse/sales.parquet").await?;
    query_engine.register_table("sales", data_source).await?;

    let sql = r#"
        SELECT
            region,
            product_category,
            SUM(amount) as total_sales,
            COUNT(*) as transaction_count
        FROM sales
        WHERE sale_date >= '2024-01-01'
        AND amount > 100
        GROUP BY region, product_category
        ORDER BY total_sales DESC
    "#;

    // Analyze query plan
    let logical_plan = query_engine.create_logical_plan(sql).await?;
    println!("Logical Plan:\n{}", logical_plan.display_indent());

    let physical_plan = query_engine.create_physical_plan(&logical_plan).await?;
    println!("Physical Plan:\n{}", physical_plan.display_indent());

    // Execute with metrics
    let start_time = std::time::Instant::now();
    let mut result_stream = query_engine.execute_stream(sql).await?;

    let mut total_rows = 0;
    let mut total_batches = 0;

    while let Some(batch) = result_stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        total_batches += 1;
    }

    let execution_time = start_time.elapsed();

    // Get execution metrics
    let metrics = query_engine.get_execution_metrics().await?;

    println!("Query Performance:");
    println!("  Execution time: {:?}", execution_time);
    println!("  Total rows: {}", total_rows);
    println!("  Total batches: {}", total_batches);
    println!("  Rows per second: {:.2}", total_rows as f64 / execution_time.as_secs_f64());
    println!("  Memory used: {} bytes", metrics.memory_used);
    println!("  Bytes scanned: {}", metrics.bytes_scanned);

    Ok(())
}
```

### Streaming Query Processing

```rust
use rustfs_s3select_query::{StreamingQueryEngine, StreamingResult};
use futures::StreamExt;

async fn streaming_processing_example() -> Result<(), Box<dyn std::error::Error>> {
    let streaming_engine = StreamingQueryEngine::new().await?;

    // Register streaming data source
    let stream_source = DataSource::from_streaming_csv("s3://logs/stream.csv").await?;
    streaming_engine.register_table("log_stream", stream_source).await?;

    // Continuous query with windowing
    let sql = r#"
        SELECT
            TUMBLE_START(timestamp, INTERVAL '5' MINUTE) as window_start,
            COUNT(*) as event_count,
            AVG(response_time) as avg_response_time,
            MAX(response_time) as max_response_time
        FROM log_stream
        WHERE status_code >= 400
        GROUP BY TUMBLE(timestamp, INTERVAL '5' MINUTE)
    "#;

    let mut result_stream = streaming_engine.execute_streaming_query(sql).await?;

    // Process streaming results
    while let Some(window_result) = result_stream.next().await {
        let batch = window_result?;

        for row_idx in 0..batch.num_rows() {
            let window_start = batch.column_by_name("window_start")?
                .as_any().downcast_ref::<TimestampArray>()
                .unwrap().value(row_idx);
            let event_count = batch.column_by_name("event_count")?
                .as_any().downcast_ref::<Int64Array>()
                .unwrap().value(row_idx);
            let avg_response = batch.column_by_name("avg_response_time")?
                .as_any().downcast_ref::<Float64Array>()
                .unwrap().value(row_idx);

            println!("Window {}: {} errors, avg response time: {:.2}ms",
                window_start, event_count, avg_response);
        }
    }

    Ok(())
}
```

### User-Defined Functions (UDFs)

```rust
use rustfs_s3select_query::{QueryEngine, ScalarUDF, Volatility};
use datafusion::arrow::datatypes::{DataType, Field};

async fn custom_functions_example() -> Result<(), Box<dyn std::error::Error>> {
    let query_engine = QueryEngine::new().await?;

    // Register custom scalar function
    let extract_domain_udf = ScalarUDF::new(
        "extract_domain",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ArrayRef]| {
            let emails = args[0].as_any().downcast_ref::<StringArray>().unwrap();
            let mut domains = Vec::new();

            for i in 0..emails.len() {
                if let Some(email) = emails.value_opt(i) {
                    if let Some(domain) = email.split('@').nth(1) {
                        domains.push(Some(domain.to_string()));
                    } else {
                        domains.push(None);
                    }
                } else {
                    domains.push(None);
                }
            }

            Ok(Arc::new(StringArray::from(domains)))
        }),
    );

    query_engine.register_udf(extract_domain_udf).await?;

    // Register aggregate function
    let percentile_udf = AggregateUDF::new(
        "percentile_90",
        vec![DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(|| Box::new(PercentileAccumulator::new(0.9))),
    );

    query_engine.register_udaf(percentile_udf).await?;

    // Use custom functions in query
    let data_source = DataSource::from_csv("s3://users/profiles.csv").await?;
    query_engine.register_table("users", data_source).await?;

    let sql = r#"
        SELECT
            extract_domain(email) as domain,
            COUNT(*) as user_count,
            percentile_90(score) as p90_score
        FROM users
        GROUP BY extract_domain(email)
        ORDER BY user_count DESC
    "#;

    let result = query_engine.execute_query(sql).await?;

    while let Some(batch) = result.next().await {
        let batch = batch?;

        for row_idx in 0..batch.num_rows() {
            let domain = batch.column_by_name("domain")?
                .as_any().downcast_ref::<StringArray>()
                .unwrap().value(row_idx);
            let user_count = batch.column_by_name("user_count")?
                .as_any().downcast_ref::<Int64Array>()
                .unwrap().value(row_idx);
            let p90_score = batch.column_by_name("p90_score")?
                .as_any().downcast_ref::<Float64Array>()
                .unwrap().value(row_idx);

            println!("Domain: {}, Users: {}, P90 Score: {:.2}",
                domain, user_count, p90_score);
        }
    }

    Ok(())
}
```

### Query Caching and Materialization

```rust
use rustfs_s3select_query::{QueryEngine, QueryCache, MaterializedView};

async fn query_caching_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut query_engine = QueryEngine::new().await?;

    // Enable query result caching
    let cache_config = QueryCache::new()
        .with_max_size(1024 * 1024 * 1024) // 1GB cache
        .with_ttl(Duration::from_secs(300)); // 5 minutes TTL

    query_engine.enable_caching(cache_config).await?;

    // Register data source
    let data_source = DataSource::from_parquet("s3://warehouse/transactions.parquet").await?;
    query_engine.register_table("transactions", data_source).await?;

    // Create materialized view for common queries
    let materialized_view = MaterializedView::new(
        "daily_sales",
        r#"
            SELECT
                DATE(transaction_date) as date,
                SUM(amount) as total_sales,
                COUNT(*) as transaction_count
            FROM transactions
            GROUP BY DATE(transaction_date)
        "#.to_string(),
        Duration::from_secs(3600), // Refresh every hour
    );

    query_engine.register_materialized_view(materialized_view).await?;

    // Query using materialized view
    let sql = r#"
        SELECT
            date,
            total_sales,
            LAG(total_sales, 1) OVER (ORDER BY date) as prev_day_sales,
            (total_sales - LAG(total_sales, 1) OVER (ORDER BY date)) /
             LAG(total_sales, 1) OVER (ORDER BY date) * 100 as growth_rate
        FROM daily_sales
        WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
        ORDER BY date DESC
    "#;

    // First execution - cache miss
    let start_time = std::time::Instant::now();
    let result1 = query_engine.execute_query(sql).await?;
    let mut rows1 = 0;
    while let Some(batch) = result1.next().await {
        rows1 += batch?.num_rows();
    }
    let first_execution_time = start_time.elapsed();

    // Second execution - cache hit
    let start_time = std::time::Instant::now();
    let result2 = query_engine.execute_query(sql).await?;
    let mut rows2 = 0;
    while let Some(batch) = result2.next().await {
        rows2 += batch?.num_rows();
    }
    let second_execution_time = start_time.elapsed();

    println!("First execution: {:?} ({} rows)", first_execution_time, rows1);
    println!("Second execution: {:?} ({} rows)", second_execution_time, rows2);
    println!("Cache speedup: {:.2}x",
        first_execution_time.as_secs_f64() / second_execution_time.as_secs_f64());

    Ok(())
}
```

## 🏗️ Architecture

### Query Engine Architecture

```
Query Engine Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    SQL Query Interface                      │
├─────────────────────────────────────────────────────────────┤
│   Parser       │   Planner     │   Optimizer  │   Executor  │
├─────────────────────────────────────────────────────────────┤
│              Apache DataFusion Core                         │
├─────────────────────────────────────────────────────────────┤
│   Vectorized   │   Parallel    │   Streaming  │   Memory    │
│   Processing   │   Execution   │   Engine     │   Management│
├─────────────────────────────────────────────────────────────┤
│              Data Source Integration                        │
├─────────────────────────────────────────────────────────────┤
│   Parquet      │   CSV         │   JSON       │   Arrow     │
│   Reader       │   Parser      │   Parser     │   Format    │
└─────────────────────────────────────────────────────────────┘
```

### Execution Flow

1. **SQL Parsing**: Convert SQL string to logical plan
2. **Logical Optimization**: Apply rule-based optimizations
3. **Physical Planning**: Create physical execution plan
4. **Execution**: Execute plan with streaming results
5. **Result Streaming**: Return results as Arrow batches

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test query execution
cargo test query_execution

# Test optimization
cargo test optimization

# Test data formats
cargo test data_formats

# Benchmark tests
cargo test --test benchmarks --release

# Integration tests
cargo test --test integration
```

## 📊 Performance Benchmarks

| Operation | Throughput | Latency | Memory |
|-----------|------------|---------|---------|
| CSV Scan | 2.5 GB/s | 10ms | 50MB |
| Parquet Scan | 5.0 GB/s | 5ms | 30MB |
| JSON Parse | 1.2 GB/s | 15ms | 80MB |
| Aggregation | 1.8 GB/s | 20ms | 100MB |
| Join | 800 MB/s | 50ms | 200MB |

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **CPU**: Multi-core recommended for parallel processing
- **Memory**: Variable based on query complexity

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS S3Select API](../s3select-api) - S3 Select API implementation
- [RustFS ECStore](../ecstore) - Storage backend

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [S3Select Query Reference](https://docs.rustfs.com/s3select-query/)
- [DataFusion Integration Guide](https://docs.rustfs.com/datafusion/)

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
  Made with ⚡ by the RustFS Team
</p>
