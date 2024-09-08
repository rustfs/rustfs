mod config;
mod storage;

use clap::Parser;
use ecstore::error::Result;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use std::{io::IsTerminal, net::SocketAddr, str::FromStr};
use tokio::net::TcpListener;
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

    // let mut domain_name = {
    //     netif::up()?
    //         .map(|x| x.address().to_owned())
    //         .filter(|v| v.is_ipv4())
    //         // .filter(|v| v.is_ipv4() && !v.is_loopback() && !v.is_unspecified())
    //         .map(|v| format!("{}", v))
    //         .next()
    //         .and_then(|ip| {
    //             if let SocketAddr::V4(ipv4) = local_addr {
    //                 Some(format!("{}:{}", ip, ipv4.port()))
    //             } else {
    //                 None
    //             }
    //         })
    // };

    // Setup S3 service
    // 本项目使用s3s库来实现s3服务
    let service = {
        let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(opt.address.clone(), opt.volumes.clone()).await?);
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

        // Enable parsing virtual-hosted-style requests
        if let Some(dm) = opt.domain_name {
            info!("virtual-hosted-style requests are enabled use domain_name {}", &dm);
            b.set_base_domain(dm);
        }

        // if domain_name.is_some() {
        //     info!(
        //         "virtual-hosted-style requests are enabled use domain_name {}",
        //         domain_name.as_ref().unwrap()
        //     );
        //     b.set_base_domain(domain_name.unwrap());
        // }

        b.build()
    };

    let hyper_service = service.into_shared();

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

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

        let conn = http_server.serve_connection(TokioIo::new(socket), hyper_service.clone());
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

    info!("server is stopped");
    Ok(())
}
