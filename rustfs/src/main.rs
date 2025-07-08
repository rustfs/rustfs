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

mod admin;
mod auth;
mod config;
mod console;
mod error;
mod event;
// mod grpc;
pub mod license;
mod logging;
mod server;
mod service;
mod storage;
mod update_checker;

use crate::auth::IAMAuth;
use crate::console::{CONSOLE_CONFIG, init_console_cfg};
// Ensure the correct path for parse_license is imported
use crate::event::shutdown_event_notifier;
use crate::server::{SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownSignal, wait_for_shutdown};
use bytes::Bytes;
use chrono::Datelike;
use clap::Parser;
use http::{HeaderMap, Request as HttpRequest, Response};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
    service::TowerToHyperService,
};
use license::init_license;
use rustfs_common::globals::set_global_addr;
use rustfs_config::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
use rustfs_ecstore::cmd::bucket_replication::init_bucket_replication_pool;
use rustfs_ecstore::config as ecconfig;
use rustfs_ecstore::config::GLOBAL_ConfigSys;
use rustfs_ecstore::heal::background_heal_ops::init_auto_heal;
use rustfs_ecstore::rpc::make_server;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::{
    StorageAPI,
    endpoints::EndpointServerPools,
    global::{create_background_services_cancel_token, set_global_rustfs_port, shutdown_background_services},
    heal::data_scanner::init_data_scanner,
    notification_sys::new_global_notification_sys,
    set_global_endpoints,
    store::ECStore,
    store::init_local_disks,
    update_erasure_type,
};
use rustfs_iam::init_iam_sys;
use rustfs_obs::{SystemObserver, init_obs, set_global_guard};
use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use rustfs_utils::net::parse_and_resolve_address;
use rustls::ServerConfig;
use s3s::service::S3Service;
use s3s::{host::MultiDomain, service::S3ServiceBuilder};
use service::hybrid;
use socket2::SockRef;
use std::io::{Error, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio_rustls::TlsAcceptor;
use tonic::{Request, Status, metadata::MetadataValue};
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, error, info, instrument, warn};

const MI_B: usize = 1024 * 1024;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(clippy::result_large_err)]
fn check_auth(req: Request<()>) -> std::result::Result<Request<()>, Status> {
    let token: MetadataValue<_> = "rustfs rpc".parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}

#[instrument]
fn print_server_info() {
    let cfg = CONSOLE_CONFIG.get().unwrap();
    let current_year = chrono::Utc::now().year();

    // Use custom macros to print server information
    info!("RustFS Object Storage Server");
    info!("Copyright: 2024-{} RustFS, Inc", current_year);
    info!("License: {}", cfg.license());
    info!("Version: {}", cfg.version_info());
    info!("Docs: {}", cfg.doc());
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse the obtained parameters
    let opt = config::Opt::parse();

    // Initialize the configuration
    init_license(opt.license.clone());

    // Initialize Observability
    let (_logger, guard) = init_obs(Some(opt.clone().obs_endpoint)).await;

    // Store in global storage
    set_global_guard(guard).map_err(Error::other)?;

    // Run parameters
    run(opt).await
}

/// Sets up the TLS acceptor if certificates are available.
#[instrument(skip(tls_path))]
async fn setup_tls_acceptor(tls_path: &str) -> Result<Option<TlsAcceptor>> {
    if tls_path.is_empty() || tokio::fs::metadata(tls_path).await.is_err() {
        debug!("TLS path is not provided or does not exist, starting with HTTP");
        return Ok(None);
    }

    debug!("Found TLS directory, checking for certificates");

    // 1. Try to load all certificates from the directory (multi-cert support)
    if let Ok(cert_key_pairs) = rustfs_utils::load_all_certs_from_directory(tls_path) {
        if !cert_key_pairs.is_empty() {
            debug!("Found {} certificates, creating multi-cert resolver", cert_key_pairs.len());
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(rustfs_utils::create_multi_cert_resolver(cert_key_pairs)?));
            server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
            return Ok(Some(TlsAcceptor::from(Arc::new(server_config))));
        }
    }

    // 2. Fallback to legacy single certificate mode
    let key_path = format!("{tls_path}/{RUSTFS_TLS_KEY}");
    let cert_path = format!("{tls_path}/{RUSTFS_TLS_CERT}");
    if tokio::try_join!(tokio::fs::metadata(&key_path), tokio::fs::metadata(&cert_path)).is_ok() {
        debug!("Found legacy single TLS certificate, starting with HTTPS");
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certs = rustfs_utils::load_certs(&cert_path).map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
        let key = rustfs_utils::load_private_key(&key_path).map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
        return Ok(Some(TlsAcceptor::from(Arc::new(server_config))));
    }

    debug!("No valid TLS certificates found in the directory, starting with HTTP");
    Ok(None)
}

#[instrument(skip(opt))]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);

    if let Some(region) = opt.region {
        rustfs_ecstore::global::set_global_region(region);
    }

    let server_addr = parse_and_resolve_address(opt.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    debug!("server_address {}", &server_address);

    // Set up AK and SK
    rustfs_ecstore::global::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone()));

    set_global_rustfs_port(server_port);

    // The listening address and port are obtained from the parameters
    let listener = TcpListener::bind(server_address.clone()).await?;
    // Obtain the listener address
    let local_addr: SocketAddr = listener.local_addr()?;
    debug!("Listening on: {}", local_addr);
    let local_ip = match rustfs_utils::get_local_ip() {
        Some(ip) => {
            debug!("Obtained local IP address: {}", ip);
            ip
        }
        None => {
            warn!("Unable to obtain local IP address, using fallback IP: {}", local_addr.ip());
            local_addr.ip()
        }
    };

    // For RPC
    let (endpoint_pools, setup_type) =
        EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone()).map_err(Error::other)?;

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "Formatting {}st pool, {} set(s), {} drives per set.",
            i + 1,
            eps.set_count,
            eps.drives_per_set
        );

        if eps.drives_per_set > 1 {
            warn!("WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.");
        }
    }

    // Detailed endpoint information (showing all API endpoints)
    let api_endpoints = format!("http://{local_ip}:{server_port}");
    let localhost_endpoint = format!("http://127.0.0.1:{server_port}");
    info!("   API: {}  {}", api_endpoints, localhost_endpoint);
    info!("   RootUser: {}", opt.access_key.clone());
    info!("   RootPass: {}", opt.secret_key.clone());
    if DEFAULT_ACCESS_KEY.eq(&opt.access_key) && DEFAULT_SECRET_KEY.eq(&opt.secret_key) {
        warn!(
            "Detected default credentials '{}:{}', we recommend that you change these values with 'RUSTFS_ACCESS_KEY' and 'RUSTFS_SECRET_KEY' environment variables",
            DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY
        );
    }

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );

        for ep in eps.endpoints.as_ref().iter() {
            info!("  - {}", ep);
        }
    }

    set_global_addr(&opt.address).await;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // Initialize the local disk
    init_local_disks(endpoint_pools.clone()).await.map_err(Error::other)?;

    // Setup S3 service
    // This project uses the S3S library to implement S3 services
    let s3_service = {
        let store = storage::ecfs::FS::new();
        let mut b = S3ServiceBuilder::new(store.clone());

        let access_key = opt.access_key.clone();
        let secret_key = opt.secret_key.clone();
        debug!("authentication is enabled {}, {}", &access_key, &secret_key);

        b.set_auth(IAMAuth::new(access_key, secret_key));
        b.set_access(store.clone());
        b.set_route(admin::make_admin_route()?);

        if !opt.server_domains.is_empty() {
            info!("virtual-hosted-style requests are enabled use domain_name {:?}", &opt.server_domains);
            b.set_host(MultiDomain::new(&opt.server_domains).map_err(Error::other)?);
        }

        b.build()
    };

    tokio::spawn(async move {
        // Record the PID-related metrics of the current process
        let meter = opentelemetry::global::meter("system");
        let obs_result = SystemObserver::init_process_observer(meter).await;
        match obs_result {
            Ok(_) => {
                info!("Process observer initialized successfully");
            }
            Err(e) => {
                error!("Failed to initialize process observer: {}", e);
            }
        }
    });

    let tls_acceptor = setup_tls_acceptor(opt.tls_path.as_deref().unwrap_or_default()).await?;

    let state_manager = ServiceStateManager::new();
    let worker_state_manager = state_manager.clone();
    // Update service status to Starting
    state_manager.update(ServiceState::Starting);

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        let (mut sigterm_inner, mut sigint_inner) = {
            // Unix platform specific code
            let sigterm_inner = signal(SignalKind::terminate()).expect("Failed to create SIGTERM signal handler");
            let sigint_inner = signal(SignalKind::interrupt()).expect("Failed to create SIGINT signal handler");
            (sigterm_inner, sigint_inner)
        };

        let http_server = Arc::new(ConnBuilder::new(TokioExecutor::new()));
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let graceful = Arc::new(GracefulShutdown::new());
        debug!("graceful initiated");

        // service ready
        worker_state_manager.update(ServiceState::Ready);
        let tls_acceptor = tls_acceptor.map(Arc::new);

        loop {
            debug!("Waiting for new connection...");
            let (socket, _) = {
                #[cfg(unix)]
                {
                    tokio::select! {
                        res = listener.accept() => match res {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!("error accepting connection: {err}");
                                continue;
                            }
                        },
                        _ = ctrl_c.as_mut() => {
                            info!("Ctrl-C received in worker thread");
                            let _ = shutdown_tx_clone.send(());
                            break;
                        },
                       Some(_) = sigint_inner.recv() => {
                           info!("SIGINT received in worker thread");
                           let _ = shutdown_tx_clone.send(());
                           break;
                       },
                       Some(_) = sigterm_inner.recv() => {
                           info!("SIGTERM received in worker thread");
                           let _ = shutdown_tx_clone.send(());
                           break;
                       },
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received in worker thread");
                            break;
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    tokio::select! {
                        res = listener.accept() => match res {
                            Ok(conn) => conn,
                            Err(err) => {
                                error!("error accepting connection: {err}");
                                continue;
                            }
                        },
                        _ = ctrl_c.as_mut() => {
                            info!("Ctrl-C received in worker thread");
                            let _ = shutdown_tx_clone.send(());
                            break;
                        },
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received in worker thread");
                            break;
                        }
                    }
                }
            };

            let socket_ref = SockRef::from(&socket);
            if let Err(err) = socket_ref.set_nodelay(true) {
                warn!(?err, "Failed to set TCP_NODELAY");
            }
            if let Err(err) = socket_ref.set_recv_buffer_size(4 * MI_B) {
                warn!(?err, "Failed to set set_recv_buffer_size");
            }
            if let Err(err) = socket_ref.set_send_buffer_size(4 * MI_B) {
                warn!(?err, "Failed to set set_send_buffer_size");
            }

            process_connection(socket, tls_acceptor.clone(), http_server.clone(), s3_service.clone(), graceful.clone());
        }

        worker_state_manager.update(ServiceState::Stopping);
        match Arc::try_unwrap(graceful) {
            Ok(g) => {
                tokio::select! {
                    () = g.shutdown() => {
                        debug!("Gracefully shutdown!");
                    },
                    () = tokio::time::sleep(Duration::from_secs(10)) => {
                        debug!("Waited 10 seconds for graceful shutdown, aborting...");
                    }
                }
            }
            Err(arc_graceful) => {
                error!("Cannot perform graceful shutdown, other references exist err: {:?}", arc_graceful);
                tokio::time::sleep(Duration::from_secs(10)).await;
                debug!("Timeout reached, forcing shutdown");
            }
        }
        worker_state_manager.update(ServiceState::Stopped);
    });

    // init store
    let store = ECStore::new(server_addr, endpoint_pools.clone()).await.inspect_err(|err| {
        error!("ECStore::new {:?}", err);
    })?;

    ecconfig::init();
    // config system configuration
    GLOBAL_ConfigSys.init(store.clone()).await?;

    // Initialize event notifier
    event::init_event_notifier().await;

    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(Error::other)?;

    let buckets = buckets_list.into_iter().map(|v| v.name).collect();

    init_bucket_metadata_sys(store.clone(), buckets).await;

    init_iam_sys(store.clone()).await?;

    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::other(err)
    })?;

    // init scanner and auto heal with unified cancellation token
    let _background_services_cancel_token = create_background_services_cancel_token();
    init_data_scanner().await;
    init_auto_heal().await;
    // init console configuration
    init_console_cfg(local_ip, server_port);

    print_server_info();
    init_bucket_replication_pool().await;

    // Async update check (optional)
    tokio::spawn(async {
        use crate::update_checker::{UpdateCheckError, check_updates};

        match check_updates().await {
            Ok(result) => {
                if result.update_available {
                    if let Some(latest) = &result.latest_version {
                        info!(
                            "🚀 New version available: {} -> {} (current: {})",
                            result.current_version, latest.version, result.current_version
                        );
                        if let Some(notes) = &latest.release_notes {
                            info!("📝 Release notes: {}", notes);
                        }
                        if let Some(url) = &latest.download_url {
                            info!("🔗 Download URL: {}", url);
                        }
                    }
                } else {
                    debug!("✅ Current version is up to date: {}", result.current_version);
                }
            }
            Err(UpdateCheckError::HttpError(e)) => {
                debug!("Version check network error (this is normal): {}", e);
            }
            Err(e) => {
                debug!("Version check failed (this is normal): {}", e);
            }
        }
    });

    if opt.console_enable {
        debug!("console is enabled");
        let access_key = opt.access_key.clone();
        let secret_key = opt.secret_key.clone();
        let console_address = opt.console_address.clone();
        let tls_path = opt.tls_path.clone();

        if console_address.is_empty() {
            error!("console_address is empty");
            return Err(Error::other("console_address is empty".to_string()));
        }

        tokio::spawn(async move {
            console::start_static_file_server(&console_address, local_ip, &access_key, &secret_key, tls_path).await;
        });
    }

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        #[cfg(unix)]
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            handle_shutdown(&state_manager, &shutdown_tx).await;
        }
        #[cfg(not(unix))]
        ShutdownSignal::CtrlC => {
            handle_shutdown(&state_manager, &shutdown_tx).await;
        }
    }

    info!("server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

/// Process a single incoming TCP connection.
///
/// This function is executed in a new Tokio task and it will:
/// 1. If TLS is configured, perform TLS handshake.
/// 2. Build a complete service stack for this connection, including S3, RPC services, and all middleware.
/// 3. Use Hyper to handle HTTP requests on this connection.
/// 4. Incorporate connections into the management of elegant closures.
#[instrument(skip_all, fields(peer_addr = %socket.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string())))]
fn process_connection(
    socket: TcpStream,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    http_server: Arc<ConnBuilder<TokioExecutor>>,
    s3_service: S3Service,
    graceful: Arc<GracefulShutdown>,
) {
    tokio::spawn(async move {
        // Build services inside each connected task to avoid passing complex service types across tasks,
        // It also ensures that each connection has an independent service instance.
        let rpc_service = NodeServiceServer::with_interceptor(make_server(), check_auth);
        let hybrid_service = ServiceBuilder::new()
            .layer(CatchPanicLayer::new())
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &HttpRequest<_>| {
                        let span = tracing::info_span!("http-request",
                            status_code = tracing::field::Empty,
                            method = %request.method(),
                            uri = %request.uri(),
                            version = ?request.version(),
                        );
                        for (header_name, header_value) in request.headers() {
                            if header_name == "user-agent" || header_name == "content-type" || header_name == "content-length" {
                                span.record(header_name.as_str(), header_value.to_str().unwrap_or("invalid"));
                            }
                        }

                        span
                    })
                    .on_request(|request: &HttpRequest<_>, _span: &Span| {
                        info!(
                            counter.rustfs_api_requests_total = 1_u64,
                            key_request_method = %request.method().to_string(),
                            key_request_uri_path = %request.uri().path().to_owned(),
                            "handle request api total",
                        );
                        debug!("http started method: {}, url path: {}", request.method(), request.uri().path())
                    })
                    .on_response(|response: &Response<_>, latency: Duration, _span: &Span| {
                        _span.record("http response status_code", tracing::field::display(response.status()));
                        debug!("http response generated in {:?}", latency)
                    })
                    .on_body_chunk(|chunk: &Bytes, latency: Duration, _span: &Span| {
                        info!(histogram.request.body.len = chunk.len(), "histogram request body length",);
                        debug!("http body sending {} bytes in {:?}", chunk.len(), latency)
                    })
                    .on_eos(|_trailers: Option<&HeaderMap>, stream_duration: Duration, _span: &Span| {
                        debug!("http stream closed after {:?}", stream_duration)
                    })
                    .on_failure(|_error, latency: Duration, _span: &Span| {
                        info!(counter.rustfs_api_requests_failure_total = 1_u64, "handle request api failure total");
                        debug!("http request failure error: {:?} in {:?}", _error, latency)
                    }),
            )
            .layer(CorsLayer::permissive())
            .service(hybrid(s3_service, rpc_service));
        let hybrid_service = TowerToHyperService::new(hybrid_service);

        // Decide whether to handle HTTPS or HTTP connections based on the existence of TLS Acceptor
        if let Some(acceptor) = tls_acceptor {
            debug!("TLS handshake start");
            match acceptor.accept(socket).await {
                Ok(tls_socket) => {
                    debug!("TLS handshake successful");
                    let stream = TokioIo::new(tls_socket);
                    let conn = http_server.serve_connection(stream, hybrid_service);
                    if let Err(err) = graceful.watch(conn).await {
                        handle_connection_error(&*err);
                    }
                }
                Err(err) => {
                    error!(?err, "TLS handshake failed");
                    return; // Failed to end the task directly
                }
            }
            debug!("TLS handshake success");
        } else {
            debug!("Http handshake start");
            let stream = TokioIo::new(socket);
            let conn = http_server.serve_connection(stream, hybrid_service);
            if let Err(err) = graceful.watch(conn).await {
                handle_connection_error(&*err);
            }
            debug!("Http handshake success");
        };
    });
}

/// Handles the shutdown process of the server
async fn handle_shutdown(state_manager: &ServiceStateManager, shutdown_tx: &tokio::sync::broadcast::Sender<()>) {
    info!("Shutdown signal received in main thread");
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Stop background services (data scanner and auto heal) gracefully
    info!("Stopping background services (data scanner and auto heal)...");
    shutdown_background_services();

    // Stop the notification system
    shutdown_event_notifier().await;

    info!("Server is stopping...");
    let _ = shutdown_tx.send(());

    // Wait for the worker thread to complete the cleaning work
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;

    // the last updated status is stopped
    state_manager.update(ServiceState::Stopped);
    info!("Server stopped current ");
}

/// Handles connection errors by logging them with appropriate severity
fn handle_connection_error(err: &(dyn std::error::Error + 'static)) {
    if let Some(hyper_err) = err.downcast_ref::<hyper::Error>() {
        if hyper_err.is_incomplete_message() {
            warn!("The HTTP connection is closed prematurely and the message is not completed:{}", hyper_err);
        } else if hyper_err.is_closed() {
            warn!("The HTTP connection is closed:{}", hyper_err);
        } else if hyper_err.is_parse() {
            error!("HTTP message parsing failed:{}", hyper_err);
        } else if hyper_err.is_user() {
            error!("HTTP user-custom error:{}", hyper_err);
        } else if hyper_err.is_canceled() {
            warn!("The HTTP connection is canceled:{}", hyper_err);
        } else {
            error!("Unknown hyper error:{:?}", hyper_err);
        }
    } else if let Some(io_err) = err.downcast_ref::<Error>() {
        error!("Unknown connection IO error:{}", io_err);
    } else {
        error!("Unknown connection error type:{:?}", err);
    }
}
