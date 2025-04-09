mod admin;
mod auth;
mod config;
mod console;
mod grpc;
pub mod license;
mod logging;
mod service;
mod storage;
mod utils;

use crate::auth::IAMAuth;
use crate::console::{init_console_cfg, CONSOLE_CONFIG};
use crate::utils::error;
// Ensure the correct path for parse_license is imported
use chrono::Datelike;
use clap::Parser;
use common::{
    error::{Error, Result},
    globals::set_global_addr,
};
use config::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use ecstore::heal::background_heal_ops::init_auto_heal;
use ecstore::utils::net::{self, get_available_port};
use ecstore::{
    endpoints::EndpointServerPools,
    heal::data_scanner::init_data_scanner,
    set_global_endpoints,
    store::{init_local_disks, ECStore},
    update_erasure_type,
};
use ecstore::{global::set_global_rustfs_port, notification_sys::new_global_notification_sys};
use grpc::make_server;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
    service::TowerToHyperService,
};
use iam::init_iam_sys;
use license::init_license;
use protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use rustfs_obs::{init_obs, load_config, set_global_guard, InitLogStatus};
use rustls::ServerConfig;
use s3s::{host::MultiDomain, service::S3ServiceBuilder};
use service::hybrid;
use std::sync::Arc;
use std::{io::IsTerminal, net::SocketAddr};
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio_rustls::TlsAcceptor;
use tonic::{metadata::MetadataValue, Request, Status};
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, info_span, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(target_os = "linux")]
fn notify_systemd(state: &str) {
    use libsystemd::daemon::{notify, NotifyState};
    let notify_state = match state {
        "ready" => NotifyState::Ready,
        "stopping" => NotifyState::Stopping,
        _ => {
            warn!("Unsupported state passed to notify_systemd: {}", state);
            return;
        }
    };

    if let Err(e) = notify(false, &[notify_state]) {
        error!("Failed to notify systemd: {}", e);
    } else {
        debug!("Successfully notified systemd: {}", state);
    }
    info!("Systemd notifications are enabled on linux (state: {})", state);
}

#[cfg(not(target_os = "linux"))]
fn notify_systemd(state: &str) {
    info!("Systemd notifications are not available on this platform not linux (state: {})", state);
}

#[allow(dead_code)]
fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let enable_color = std::io::stdout().is_terminal();

    let subscriber = tracing_subscriber::fmt::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .with_file(true)
        .with_line_number(true)
        .finish()
        .with(ErrorLayer::default());

    subscriber.try_init().expect("failed to set global default subscriber");
}

fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token: MetadataValue<_> = "rustfs rpc".parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}

fn print_server_info() {
    let cfg = CONSOLE_CONFIG.get().unwrap();
    let current_year = chrono::Utc::now().year();

    // 使用自定义宏打印服务器信息
    info!("RustFS Object Storage Server");
    info!("Copyright: 2024-{} RustFS, Inc", current_year);
    info!("License: {}", cfg.license());
    info!("Version: {}", cfg.version());
    info!("Docs: {}", cfg.doc());
}

#[tokio::main]
async fn main() -> Result<()> {
    // config::init_config();

    // Parse the obtained parameters
    let opt = config::Opt::parse();

    init_license(opt.license.clone());

    // Load the configuration file
    let config = load_config(Some(opt.clone().obs_config));

    // Initialize Observability
    let (_logger, guard) = init_obs(config.clone()).await;

    // Store in global storage
    set_global_guard(guard)?;

    // Log initialization status
    InitLogStatus::init_start_log(&config.observability).await?;

    // Run parameters
    run(opt).await
}

// #[tokio::main]
async fn run(opt: config::Opt) -> Result<()> {
    let span = info_span!("trace-main-run");
    let _enter = span.enter();

    debug!("opt: {:?}", &opt);

    let mut server_addr = net::check_local_server_addr(opt.address.as_str())?;

    if server_addr.port() == 0 {
        server_addr.set_port(get_available_port());
    }

    let server_port = server_addr.port();

    let server_address = server_addr.to_string();

    debug!("server_address {}", &server_address);

    //设置 AK 和 SK
    iam::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone()))?;

    set_global_rustfs_port(server_port);

    //监听地址，端口从参数中获取
    let listener = TcpListener::bind(server_address.clone()).await?;
    //获取监听地址
    let local_addr: SocketAddr = listener.local_addr()?;
    let local_ip = utils::get_local_ip().ok_or(local_addr.ip()).unwrap();

    // 用于 rpc
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone())
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Print MinIO-style logging for pool formatting
    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "Formatting {}st pool, {} set(s), {} drives per set.",
            i + 1,
            eps.set_count,
            eps.drives_per_set
        );

        // Add warning for host with multiple drives in a set (similar to MinIO)
        if eps.drives_per_set > 1 {
            warn!("WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.");
        }
    }

    // Detailed endpoint information (showing all API endpoints)
    let api_endpoints = format!("http://{}:{}", local_ip, server_port);
    let localhost_endpoint = format!("http://127.0.0.1:{}", server_port);
    info!("API: {}  {}", api_endpoints, localhost_endpoint);
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

    // 初始化本地磁盘
    init_local_disks(endpoint_pools.clone())
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Setup S3 service
    // 本项目使用 s3s 库来实现 s3 服务
    let service = {
        let store = storage::ecfs::FS::new();
        // let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(server_address.clone(), endpoint_pools).await?);
        let mut b = S3ServiceBuilder::new(store.clone());

        let access_key = opt.access_key.clone();
        let secret_key = opt.secret_key.clone();
        //显示 info 信息
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

    let rpc_service = NodeServiceServer::with_interceptor(make_server(), check_auth);

    let tls_path = opt.tls_path.clone().unwrap_or_default();
    let key_path = format!("{}/{}", tls_path, RUSTFS_TLS_KEY);
    let cert_path = format!("{}/{}", tls_path, RUSTFS_TLS_CERT);
    let has_tls_certs = tokio::try_join!(tokio::fs::metadata(key_path.clone()), tokio::fs::metadata(cert_path.clone())).is_ok();
    let tls_acceptor = if has_tls_certs {
        debug!("Found TLS certificates, starting with HTTPS");
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certs = utils::load_certs(cert_path.as_str()).map_err(|e| error(e.to_string()))?;
        let key = utils::load_private_key(key_path.as_str()).map_err(|e| error(e.to_string()))?;
        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| error(e.to_string()))?;
        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
        Some(TlsAcceptor::from(Arc::new(server_config)))
    } else {
        debug!("TLS certificates not found, starting with HTTP");
        None
    };

    // Create an oneshot channel to wait for the service to start
    let (tx, rx) = tokio::sync::oneshot::channel();
    // 启动服务
    notify_service_state(ServiceState::Starting);
    tokio::spawn(async move {
        // 错误处理改进
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
        let hyper_service = service.into_shared();
        let hybrid_service = TowerToHyperService::new(
            tower::ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                .service(hybrid(hyper_service, rpc_service)),
        );

        let http_server = ConnBuilder::new(TokioExecutor::new());
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();
        debug!("graceful initiated");

        // Send a message to the main thread to indicate that the server has started
        let _ = tx.send(());

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
                    drop(listener);
                    eprintln!("Ctrl-C received, starting shutdown");
                    break;
                }

                _ = sigint_inner.recv() => {
                    info!("SIGINT received in worker thread");
                    break;
                }
                _ = sigterm_inner.recv() => {
                    info!("SIGTERM received in worker thread");
                    break;
                }
            };

            if has_tls_certs {
                debug!("TLS certificates found, starting with SIGINT");
                let tls_socket = match tls_acceptor
                    .as_ref()
                    .ok_or_else(|| error("TLS not configured".to_string()))
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
                let conn = http_server.serve_connection(TokioIo::new(tls_socket), hybrid_service.clone());
                let conn = graceful.watch(conn.into_owned());
                tokio::task::spawn_blocking(move || {
                    tokio::runtime::Runtime::new()
                        .expect("Failed to create runtime")
                        .block_on(async move {
                            if let Err(err) = conn.await {
                                error!("Https Connection error: {}", err);
                            }
                        });
                });
                debug!("TLS handshake success");
            } else {
                debug!("Http handshake start");
                let conn = http_server.serve_connection(TokioIo::new(socket), hybrid_service.clone());
                let conn = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(err) = conn.await {
                        error!("Http Connection error: {}", err);
                    }
                });
                debug!("Http handshake success");
            }
        }

        tokio::select! {
            () = graceful.shutdown() => {
                 debug!("Gracefully shutdown!");
            },
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                 debug!("Waited 10 seconds for graceful shutdown, aborting...");
            }
        }
    });

    // init store
    let store = ECStore::new(server_address.clone(), endpoint_pools.clone())
        .await
        .map_err(|err| {
            error!("ECStore::new {:?}", &err);
            Error::from_string(err.to_string())
        })?;

    ECStore::init(store.clone()).await.map_err(|err| {
        error!("ECStore init failed {:?}", &err);
        Error::from_string(err.to_string())
    })?;
    debug!("init store success!");

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

    // 执行休眠 1 秒钟
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // Wait for the HTTP service to finish starting
    if rx.await.is_ok() {
        notify_systemd("ready");
    } else {
        info!("Failed to start the server");
    }

    // 主线程中监听信号
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            eprintln!("Ctrl-C received, starting shutdown");
            notify_systemd("stopping");
        }

        _ = sigint.recv() => {
            info!("SIGINT received, starting shutdown");
            notify_systemd("stopping");
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received, starting shutdown");
            notify_systemd("stopping");
        }
    }

    info!("server is stopped");
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug)]
enum ShutdownSignal {
    CtrlC,
    Sigterm,
    Sigint,
}
#[allow(dead_code)]
async fn wait_for_shutdown() -> ShutdownSignal {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigint = signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C signal");
            ShutdownSignal::CtrlC
        }
        _ = sigint.recv() => {
            info!("Received SIGINT signal");
            ShutdownSignal::Sigint
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM signal");
            ShutdownSignal::Sigterm
        }
    }
}
#[allow(dead_code)]
#[derive(Debug)]
enum ServiceState {
    Starting,
    Ready,
    Stopping,
    Stopped,
}
#[allow(dead_code)]
fn notify_service_state(state: ServiceState) {
    match state {
        ServiceState::Starting => {
            info!("Service is starting...");
            #[cfg(target_os = "linux")]
            if let Err(e) = libsystemd::daemon::notify(false, &[libsystemd::daemon::NotifyState::Status("Starting...")]) {
                error!("Failed to notify systemd of starting state: {}", e);
            }
        }
        ServiceState::Ready => {
            info!("Service is ready");
            notify_systemd("ready");
        }
        ServiceState::Stopping => {
            info!("Service is stopping...");
            notify_systemd("stopping");
        }
        ServiceState::Stopped => {
            info!("Service has stopped");
            #[cfg(target_os = "linux")]
            if let Err(e) = libsystemd::daemon::notify(false, &[libsystemd::daemon::NotifyState::Status("Stopped")]) {
                error!("Failed to notify systemd of stopped state: {}", e);
            }
        }
    }
}
