mod config;
mod grpc;
mod service;
mod storage;

use clap::Parser;
use common::error::{Error, Result};
use ecstore::{
    config::GLOBAL_ConfigSys,
    endpoints::EndpointServerPools,
    set_global_endpoints,
    store::{init_local_disks, ECStore},
    update_erasure_type,
};
use grpc::make_server;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
    service::TowerToHyperService,
};
use protos::proto_gen::node_service::node_service_server::NodeServiceServer;
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use service::hybrid;
use std::{io::IsTerminal, net::SocketAddr, str::FromStr};
use tokio::net::TcpListener;
use tonic::{metadata::MetadataValue, Request, Status};
use tracing::{debug, info};
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

    //监听地址,端口从参数中获取
    let listener = TcpListener::bind(opt.address.clone()).await?;
    //获取监听地址
    let local_addr: SocketAddr = listener.local_addr()?;

    // 用于rpc
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(opt.address.clone().as_str(), opt.volumes.clone())
        .map_err(|err| Error::from_string(err.to_string()))?;

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        debug!(
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );
    }

    set_global_endpoints(endpoint_pools.as_ref().clone()).await;
    update_erasure_type(setup_type).await;

    // 初始化本地磁盘
    init_local_disks(endpoint_pools.clone())
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    // Setup S3 service
    // 本项目使用s3s库来实现s3服务
    let service = {
        let store = storage::ecfs::FS::new();
        // let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(opt.address.clone(), endpoint_pools).await?);
        let mut b = S3ServiceBuilder::new(store.clone());
        //设置AK和SK
        //其中部份内容从config配置文件中读取
        let mut access_key = String::from_str(config::DEFAULT_ACCESS_KEY).unwrap();
        let mut secret_key = String::from_str(config::DEFAULT_SECRET_KEY).unwrap();

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            access_key = ak;
            secret_key = sk;
        }
        //显示info信息
        info!("authentication is enabled {}, {}", &access_key, &secret_key);
        b.set_auth(SimpleAuth::from_single(access_key, secret_key));

        b.set_access(store.clone());

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
    info!(" init store success!");

    tokio::spawn(async move {
        let hyper_service = service.into_shared();
        let adm_service = admin::register_admin_router();

        let hybrid_service = TowerToHyperService::new(hybrid(hyper_service, rpc_service, adm_service));

        let http_server = ConnBuilder::new(TokioExecutor::new());
        let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();
        info!("server is running at http://{local_addr}");

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
    let store = ECStore::new(opt.address.clone(), endpoint_pools.clone())
        .await
        .map_err(|err| Error::from_string(err.to_string()))?;

    store.init().await.map_err(|err| Error::from_string(err.to_string()))?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {

        }
    }

    info!("server is stopped");
    Ok(())
}
