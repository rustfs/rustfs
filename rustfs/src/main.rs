mod admin;
mod auth;
mod config;
mod console;
mod event;
mod grpc;
pub mod license;
mod logging;
mod server;
mod service;
mod storage;

use crate::auth::IAMAuth;
use crate::console::{init_console_cfg, CONSOLE_CONFIG};
// Ensure the correct path for parse_license is imported
use crate::server::{wait_for_shutdown, ServiceState, ServiceStateManager, ShutdownSignal, SHUTDOWN_TIMEOUT};
use bytes::Bytes;
use chrono::Datelike;
use clap::Parser;
use common::{
    error::{Error, Result},
    globals::set_global_addr,
};
use ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
use ecstore::config as ecconfig;
use ecstore::config::GLOBAL_ConfigSys;
use ecstore::heal::background_heal_ops::init_auto_heal;
use ecstore::store_api::BucketOptions;
use ecstore::utils::net;
use ecstore::StorageAPI;
use ecstore::{
    endpoints::EndpointServerPools,
    heal::data_scanner::init_data_scanner,
    set_global_endpoints,
    store::{init_local_disks, ECStore},
    update_erasure_type,
};
use ecstore::{global::set_global_rustfs_port, notification_sys::new_global_notification_sys};
use grpc::make_server;
use http::{HeaderMap, Request as HttpRequest, Response};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
    service::TowerToHyperService,
};
use iam::init_iam_sys;
use license::init_license;
use protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use rustfs_config::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustfs_obs::{init_obs, set_global_guard, SystemObserver};
use rustls::ServerConfig;
use s3s::{host::MultiDomain, service::S3ServiceBuilder};
use service::hybrid;
use socket2::SockRef;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio_rustls::TlsAcceptor;
use tonic::{metadata::MetadataValue, Request, Status};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};
use tracing::{instrument, Span};

const MI_B: usize = 1024 * 1024;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(clippy::result_large_err)]
fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
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
    info!("Version: {}", cfg.version());
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
    set_global_guard(guard)?;

    // Run parameters
    run(opt).await
}

#[instrument(skip(opt))]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);

    // Initialize event notifier
    // event::init_event_notifier(opt.event_config).await;

    let server_addr = net::parse_and_resolve_address(opt.address.as_str())?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    debug!("server_address {}", &server_address);

    // Set up AK and SK
    iam::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone()))?;

    set_global_rustfs_port(server_port);

    // The listening address and port are obtained from the parameters
    let listener = TcpListener::bind(server_address.clone()).await?;
    // Obtain the listener address
    let local_addr: SocketAddr = listener.local_addr()?;
    // let local_ip = utils::get_local_ip().ok_or(local_addr.ip()).unwrap();
    let local_ip = rustfs_utils::get_local_ip().ok_or(local_addr.ip()).unwrap();

    // For RPC
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone())
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Print RustFS-style logging for pool formatting
    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "Formatting {}st pool, {} set(s), {} drives per set.",
            i + 1,
            eps.set_count,
            eps.drives_per_set
        );

        // Add warning for host with multiple drives in a set (similar to RustFS)
        if eps.drives_per_set > 1 {
            warn!("WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.");
        }
    }

    // Detailed endpoint information (showing all API endpoints)
    let api_endpoints = format!("http://{}:{}", local_ip, server_port);
    let localhost_endpoint = format!("http://127.0.0.1:{}", server_port);
    info!("   API: {}  {}", api_endpoints, localhost_endpoint);
    info!("   RootUser: {}", opt.access_key.clone());
    info!("   RootPass: {}", opt.secret_key.clone());
    if DEFAULT_ACCESS_KEY.eq(&opt.access_key) && DEFAULT_SECRET_KEY.eq(&opt.secret_key) {
        warn!("Detected default credentials '{}:{}', we recommend that you change these values with 'RUSTFS_ACCESS_KEY' and 'RUSTFS_SECRET_KEY' environment variables", DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY);
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
    init_local_disks(endpoint_pools.clone())
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Setup S3 service
    // This project uses the S3S library to implement S3 services
    let s3_service = {
        let store = storage::ecfs::FS::new();
        // let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(server_address.clone(), endpoint_pools).await?);
        let mut b = S3ServiceBuilder::new(store.clone());

        let access_key = opt.access_key.clone();
        let secret_key = opt.secret_key.clone();
        // Displays info information
        debug!("authentication is enabled {}, {}", &access_key, &secret_key);

        b.set_auth(IAMAuth::new(access_key, secret_key));

        b.set_access(store.clone());

        b.set_route(admin::make_admin_route()?);

        if !opt.server_domains.is_empty() {
            info!("virtual-hosted-style requests are enabled use domain_name {:?}", &opt.server_domains);
            b.set_host(MultiDomain::new(&opt.server_domains)?);
        }

        // // Enable parsing virtual-hosted-style requests
        // if let Some(dm) = opt.domain_name {
        //     info!("virtual-hosted-style requests are enabled use domain_name {}", &dm);
        //     b.set_base_domain(dm);
        // }

        // if domain_name.is_some() {
        //     info!(
        //         "virtual-hosted-style requests are enabled use domain_name {}",
        //         domain_name.as_ref().unwrap()
        //     );
        //     b.set_base_domain(domain_name.unwrap());
        // }

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

    let tls_path = opt.tls_path.clone().unwrap_or_default();
    let has_tls_certs = tokio::fs::metadata(&tls_path).await.is_ok();
    let tls_acceptor = if has_tls_certs {
        debug!("Found TLS directory, checking for certificates");

        // 1. Try to load all certificates directly (including root and subdirectories)
        match rustfs_utils::load_all_certs_from_directory(&tls_path) {
            Ok(cert_key_pairs) if !cert_key_pairs.is_empty() => {
                debug!("Found {} certificates, starting with HTTPS", cert_key_pairs.len());
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

                // create a multi certificate configuration
                let mut server_config = ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(Arc::new(rustfs_utils::create_multi_cert_resolver(cert_key_pairs)?));

                server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
                Some(TlsAcceptor::from(Arc::new(server_config)))
            }
            _ => {
                // 2. If the synthesis fails, fall back to the traditional document certificate mode (backward compatible)
                let key_path = format!("{}/{}", tls_path, RUSTFS_TLS_KEY);
                let cert_path = format!("{}/{}", tls_path, RUSTFS_TLS_CERT);
                let has_single_cert =
                    tokio::try_join!(tokio::fs::metadata(key_path.clone()), tokio::fs::metadata(cert_path.clone())).is_ok();

                if has_single_cert {
                    debug!("Found legacy single TLS certificate, starting with HTTPS");
                    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
                    let certs =
                        rustfs_utils::load_certs(cert_path.as_str()).map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
                    let key = rustfs_utils::load_private_key(key_path.as_str())
                        .map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
                    let mut server_config = ServerConfig::builder()
                        .with_no_client_auth()
                        .with_single_cert(certs, key)
                        .map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
                    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
                    Some(TlsAcceptor::from(Arc::new(server_config)))
                } else {
                    debug!("No valid TLS certificates found, starting with HTTP");
                    None
                }
            }
        }
    } else {
        debug!("TLS certificates not found, starting with HTTP");
        None
    };

    let rpc_service = NodeServiceServer::with_interceptor(make_server(), check_auth);
    let state_manager = ServiceStateManager::new();
    let worker_state_manager = state_manager.clone();
    // Update service status to Starting
    state_manager.update(ServiceState::Starting);

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        // error handling improvements
        let sigterm_inner = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(e) => {
                error!("Failed to create SIGTERM signal handler: {}", e);
                return;
            }
        };
        let sigint_inner = match signal(SignalKind::interrupt()) {
            Ok(signal) => signal,
            Err(e) => {
                error!("Failed to create SIGINT signal handler: {}", e);
                return;
            }
        };

        let mut sigterm_inner = sigterm_inner;
        let mut sigint_inner = sigint_inner;
        let hybrid_service = TowerToHyperService::new(
            tower::ServiceBuilder::new()
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
                                if header_name == "user-agent" || header_name == "content-type" || header_name == "content-length"
                                {
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
                .service(hybrid(s3_service, rpc_service)),
        );

        let http_server = Arc::new(ConnBuilder::new(TokioExecutor::new()));
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let graceful = Arc::new(GracefulShutdown::new());
        debug!("graceful initiated");

        // service ready
        worker_state_manager.update(ServiceState::Ready);
        let value = hybrid_service.clone();
        loop {
            debug!("waiting for SIGINT or SIGTERM has_tls_certs: {}", has_tls_certs);
            // Wait for a connection
            let (socket, _) = tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok(conn) => conn,
                        Err(err) => {
                            error!("error accepting connection: {err}");
                            continue;
                        }
                    }
                }
                _ = ctrl_c.as_mut() => {
                    info!("Ctrl-C received in worker thread");
                    let _ = shutdown_tx_clone.send(());
                    break;
                }

                _ = sigint_inner.recv() => {
                    info!("SIGINT received in worker thread");
                    let _ = shutdown_tx_clone.send(());
                    break;
                }
                _ = sigterm_inner.recv() => {
                    info!("SIGTERM received in worker thread");
                    let _ = shutdown_tx_clone.send(());
                    break;
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received in worker thread");
                    break;
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

            if has_tls_certs {
                debug!("TLS certificates found, starting with SIGINT");
                let tls_socket = match tls_acceptor
                    .as_ref()
                    .ok_or_else(|| rustfs_utils::certs_error("TLS not configured".to_string()))
                    .unwrap()
                    .accept(socket)
                    .await
                {
                    Ok(tls_socket) => tls_socket,
                    Err(err) => {
                        error!("TLS handshake failed {}", err);
                        continue;
                    }
                };

                let http_server_clone = http_server.clone();
                let value_clone = value.clone();
                let graceful_clone = graceful.clone();

                tokio::task::spawn_blocking(move || {
                    tokio::runtime::Runtime::new()
                        .expect("Failed to create runtime")
                        .block_on(async move {
                            let conn = http_server_clone.serve_connection(TokioIo::new(tls_socket), value_clone);
                            let conn = graceful_clone.watch(conn);
                            if let Err(err) = conn.await {
                                // Handle hyper::Error and low-level IO errors at a more granular level
                                handle_connection_error(&*err);
                            }
                        });
                });
                debug!("TLS handshake success");
            } else {
                debug!("Http handshake start");

                let http_server_clone = http_server.clone();
                let value_clone = value.clone();
                let graceful_clone = graceful.clone();
                tokio::spawn(async move {
                    let conn = http_server_clone.serve_connection(TokioIo::new(socket), value_clone);
                    let conn = graceful_clone.watch(conn);
                    if let Err(err) = conn.await {
                        // Handle hyper::Error and low-level IO errors at a more granular level
                        handle_connection_error(&*err);
                    }
                });
                debug!("Http handshake success");
            }
        }
        worker_state_manager.update(ServiceState::Stopping);
        match Arc::try_unwrap(graceful) {
            Ok(g) => {
                // Successfully obtaining unique ownership, you can call shutdown
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
                // There are other references that cannot be obtained for unique ownership
                error!("Cannot perform graceful shutdown, other references exist err: {:?}", arc_graceful);
                // In this case, we can only wait for the timeout
                tokio::time::sleep(Duration::from_secs(10)).await;
                debug!("Timeout reached, forcing shutdown");
            }
        }
        worker_state_manager.update(ServiceState::Stopped);
    });

    // init store
    let store = ECStore::new(server_addr.clone(), endpoint_pools.clone())
        .await
        .map_err(|err| {
            error!("ECStore::new {:?}", &err);
            Error::from_string(err.to_string())
        })?;

    ecconfig::init();
    // config system configuration
    GLOBAL_ConfigSys.init(store.clone()).await?;

    // event system configuration
    // GLOBAL_EventSys.init(store.clone()).await?;

    // Initialize event notifier
    event::init_event_notifier(opt.event_config).await;

    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    let buckets = buckets_list.into_iter().map(|v| v.name).collect();

    init_bucket_metadata_sys(store.clone(), buckets).await;

    init_iam_sys(store.clone()).await?;

    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::from_string(err.to_string())
    })?;

    // init scanner
    init_data_scanner().await;
    // init auto heal
    init_auto_heal().await;

    init_console_cfg(local_ip, server_port);

    print_server_info();

    if opt.console_enable {
        debug!("console is enabled");
        let access_key = opt.access_key.clone();
        let secret_key = opt.secret_key.clone();
        let console_address = opt.console_address.clone();
        let tls_path = opt.tls_path.clone();

        if console_address.is_empty() {
            error!("console_address is empty");
            return Err(Error::from_string("console_address is empty".to_string()));
        }

        tokio::spawn(async move {
            console::start_static_file_server(&console_address, local_ip, &access_key, &secret_key, tls_path).await;
        });
    }

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            info!("Shutdown signal received in main thread");
            // update the status to stopping first
            state_manager.update(ServiceState::Stopping);

            // Stop the notification system
            if rustfs_event::is_ready() {
                // stop event notifier
                rustfs_event::shutdown().await.map_err(|err| {
                    error!("Failed to shut down the notification system: {}", err);
                    Error::from_string(err.to_string())
                })?;
            }

            info!("Server is stopping...");
            let _ = shutdown_tx.send(());
            // Wait for the worker thread to complete the cleaning work
            tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
            // the last updated status is stopped
            state_manager.update(ServiceState::Stopped);
            info!("Server stopped current ");
        }
    }

    info!("server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

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
    } else if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
        error!("Unknown connection IO error:{}", io_err);
    } else {
        error!("Unknown connection error type:{:?}", err);
    }
}
