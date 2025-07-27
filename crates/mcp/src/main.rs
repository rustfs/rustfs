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

use anyhow::{Context, Result};
use clap::Parser;
use rmcp::ServiceExt;
use rustfs_mcp::{Config, RustfsMcpServer};
use std::env;
use tokio::io::{stdin, stdout};
use tracing::{Level, error, info};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    init_tracing(&config)?;

    info!("Starting RustFS MCP Server v{}", env!("CARGO_PKG_VERSION"));

    if let Err(e) = config.validate() {
        error!("Configuration validation failed: {}", e);
        print_usage_help();
        std::process::exit(1);
    }

    config.log_configuration();

    if let Err(e) = run_server(config).await {
        error!("Server error: {}", e);
        std::process::exit(1);
    }

    info!("RustFS MCP Server shutdown complete");
    Ok(())
}

async fn run_server(config: Config) -> Result<()> {
    info!("Initializing RustFS MCP Server");

    let server = RustfsMcpServer::new(config).await?;

    info!("Starting MCP server with stdio transport");

    server
        .serve((stdin(), stdout()))
        .await
        .context("Failed to serve MCP server")?
        .waiting()
        .await
        .context("Error while waiting for server shutdown")?;

    Ok(())
}

fn init_tracing(config: &Config) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.log_level))
        .context("Failed to create log filter")?;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_writer(std::io::stderr) // Force logs to stderr to avoid interfering with MCP protocol on stdout
        .finish();

    tracing::subscriber::set_global_default(subscriber).context("Failed to set global tracing subscriber")?;

    Ok(())
}

fn print_usage_help() {
    eprintln!();
    eprintln!("RustFS MCP Server - Model Context Protocol server for S3 operations");
    eprintln!();
    eprintln!("For more help, run: rustfs-mcp --help");
    eprintln!();
    eprintln!("QUICK START:");
    eprintln!("  # Using command-line arguments");
    eprintln!("  rustfs-mcp --access-key-id YOUR_KEY --secret-access-key YOUR_SECRET");
    eprintln!();
    eprintln!("  # Using environment variables");
    eprintln!("  export AWS_ACCESS_KEY_ID=YOUR_KEY");
    eprintln!("  export AWS_SECRET_ACCESS_KEY=YOUR_SECRET");
    eprintln!("  rustfs-mcp");
    eprintln!();
    eprintln!("  # For local development with RustFS");
    eprintln!("  rustfs-mcp --access-key-id minioadmin --secret-access-key minioadmin --endpoint-url http://localhost:9000");
    eprintln!();
}
