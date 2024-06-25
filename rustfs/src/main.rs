mod config;
mod storage;

use anyhow::Result;
use clap::Parser;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use std::io::IsTerminal;
use tokio::net::TcpListener;
use tracing::{debug, info};

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

fn main() -> Result<()> {
    let opt = config::Opt::parse();

    setup_tracing();

    run(opt)
}

#[tokio::main]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);
    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(storage::ecfs::FS::new(
            opt.address.clone(),
            opt.volumes.clone(),
        )?);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = opt.domain_name {
            b.set_base_domain(domain_name);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    let listener = TcpListener::bind(opt.address).await?;
    let local_addr = listener.local_addr()?;

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
