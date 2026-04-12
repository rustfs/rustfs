//! HTTPS server example for s3s
//!
//! This example demonstrates how to run an S3 service over HTTPS using TLS.
//! It uses tokio-rustls for TLS support and loads certificates from PEM files.
//!
//! # Generating Test Certificates
//!
//! You can generate self-signed certificates for testing using OpenSSL:
//!
//! ```bash
//! openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
//!     -days 365 -subj "/CN=localhost"
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Generate test certificates first (see above)
//! cargo run --example https -- --cert cert.pem --key key.pem
//! ```
//!
//! Then you can access the server at <https://localhost:8014>. You'll need to accept
//! the self-signed certificate warning in your browser or S3 client.
//!
//! For production use, use certificates from a trusted certificate authority.

use s3s::dto::{GetObjectInput, GetObjectOutput};
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3Request, S3Response, S3Result};

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;

use clap::Parser;

/// A minimal S3 implementation for demonstration purposes
#[derive(Debug, Clone)]
struct DummyS3;

#[async_trait::async_trait]
impl S3 for DummyS3 {
    async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        Err(s3s::s3_error!(NotImplemented, "GetObject is not implemented"))
    }
}

/// Load certificates from a PEM file
fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
    if certs.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "No valid certificates found in file"));
    }
    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(io::Error::other)
}

/// Create TLS server configuration from certificate and key files
fn create_tls_config(cert_path: &Path, key_path: &Path) -> io::Result<ServerConfig> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(io::Error::other)?;

    // Use default protocol versions (TLS 1.2 and 1.3)
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(config)
}

#[derive(clap::Parser)]
#[command(name = "https-example")]
#[command(about = "HTTPS server example for s3s", long_about = None)]
struct Args {
    /// Path to the TLS certificate file (PEM format)
    #[arg(long)]
    cert: PathBuf,

    /// Path to the TLS private key file (PEM format)
    #[arg(long)]
    key: PathBuf,

    /// Host to listen on
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(long, default_value = "8014")]
    port: u16,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Install the default crypto provider (required for rustls)
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Parse command line arguments
    let args = Args::parse();

    // Setup tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    // Create a simple S3 service
    let s3_service = {
        let builder = S3ServiceBuilder::new(DummyS3);
        builder.build()
    };

    // Create TLS configuration from certificate files
    let tls_config = create_tls_config(&args.cert, &args.key)?;
    let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

    // Bind to address
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;

    tracing::info!("HTTPS server listening on https://{}", addr);
    tracing::info!("Using certificate from: {:?}", args.cert);
    tracing::info!("Using private key from: {:?}", args.key);
    tracing::info!("Press Ctrl+C to stop");

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        let (stream, remote_addr) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                tracing::info!("Received Ctrl+C, shutting down...");
                break;
            }
        };

        tracing::debug!("Accepted connection from {}", remote_addr);

        // Perform TLS handshake
        let tls_stream = match tls_acceptor.accept(stream).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("TLS handshake failed from {}: {}", remote_addr, e);
                continue;
            }
        };

        tracing::debug!("TLS handshake completed for {}", remote_addr);

        // Serve the connection
        let conn = http_server.serve_connection(TokioIo::new(tls_stream), s3_service.clone());
        let conn = graceful.watch(conn.into_owned());

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!("Error serving connection: {}", e);
            }
        });
    }

    // Graceful shutdown
    tokio::select! {
        () = graceful.shutdown() => {
            tracing::info!("Gracefully shut down!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            tracing::info!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    Ok(())
}
