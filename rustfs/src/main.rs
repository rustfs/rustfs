mod admin;
mod auth;
mod config;
mod console;
mod grpc;
mod service;
mod storage;
use crate::auth::IAMAuth;
use clap::Parser;
use common::{
    error::{Error, Result},
    globals::set_global_addr,
};
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
use protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use s3s::{host::MultiDomain, service::S3ServiceBuilder};
use service::hybrid;
use std::{io::IsTerminal, net::SocketAddr};
use tokio::net::TcpListener;
use tonic::{metadata::MetadataValue, Request, Status};
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info, warn};
use tracing_error::ErrorLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    let subscriber = fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
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

fn main() -> Result<()> {
    //解析获得到的参数
    let opt = config::Opt::parse();

    //设置trace
    setup_tracing();

    //运行参数
    run(opt)
}

#[tokio::main]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);

    let mut server_addr = net::check_local_server_addr(opt.address.as_str()).unwrap();

    if server_addr.port() == 0 {
        server_addr.set_port(get_available_port());
    }

    let server_port = server_addr.port();

    let server_address = server_addr.to_string();

    debug!("server_address {}", &server_address);

    //设置AK和SK

    iam::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone())).unwrap();
    set_global_rustfs_port(server_port);

    //监听地址,端口从参数中获取
    let listener = TcpListener::bind(server_address.clone()).await?;
    //获取监听地址
    let local_addr: SocketAddr = listener.local_addr()?;

    // 用于rpc
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone())
        .map_err(|err| Error::from_string(err.to_string()))?;

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        debug!(
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );
    }

    set_global_addr(&opt.address).await;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // 初始化本地磁盘
    init_local_disks(endpoint_pools.clone())
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Setup S3 service
    // 本项目使用s3s库来实现s3服务
    let service = {
        let store = storage::ecfs::FS::new();
        // let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(server_address.clone(), endpoint_pools).await?);
        let mut b = S3ServiceBuilder::new(store.clone());

        //显示info信息
        info!("authentication is enabled {}, {}", &opt.access_key, &opt.secret_key);
        b.set_auth(IAMAuth::new(opt.access_key, opt.secret_key));

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

    tokio::spawn(async move {
        let hyper_service = service.into_shared();

        let hybrid_service = TowerToHyperService::new(
            tower::ServiceBuilder::new()
                .layer(CorsLayer::permissive())
                .service(hybrid(hyper_service, rpc_service)),
        );

        let http_server = ConnBuilder::new(TokioExecutor::new());
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();
        println!("server is running at http://{local_addr}");

        loop {
            let (socket, _) = tokio::select! {
                res =  listener.accept() => {
                    match res {
                        Ok(conn) => conn,
                        Err(err) => {
                            tracing::error!("error accepting connection: {err}");
                            continue;
                        }
                    }
                }
                _ = ctrl_c.as_mut() => {
                    break;
                }
            };

            let conn = http_server.serve_connection(TokioIo::new(socket), hybrid_service.clone());
            let conn = graceful.watch(conn.into_owned());
            tokio::spawn(async move {
                let _ = conn.await;
            });
        }

        tokio::select! {
            () = graceful.shutdown() => {
                 tracing::debug!("Gracefully shutdown!");
            },
            () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                 tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
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
        error!("ECStore init faild {:?}", &err);
        Error::from_string(err.to_string())
    })?;
    warn!(" init store success!");

    init_iam_sys(store.clone()).await.unwrap();

    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys faild {:?}", &err);
        Error::from_string(err.to_string())
    })?;

    // init scanner
    init_data_scanner().await;
    // init auto heal
    init_auto_heal().await;

    info!("server was started");

    if opt.console_enable {
        info!("console is enabled");
        tokio::spawn(async move {
            let ep = if !opt.server_domains.is_empty() {
                format!("http://{}", opt.server_domains[0].clone())
            } else {
                format!("http://127.0.0.1:{}", server_port)
            };

            console::start_static_file_server(&opt.console_address, &ep).await;
        });
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {

        }
    }

    info!("server is stopped");
    Ok(())
}
