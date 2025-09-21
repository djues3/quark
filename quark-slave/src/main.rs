use std::net::SocketAddr;

use tokio::net::TcpListener;

use clap::Parser;
use opendal::Operator;
use quark_core::proto::{greeter_server::GreeterServer, slave_service_server::SlaveServiceServer};
use quark_slave::*;
use tonic::transport::Server;
use tracing::info;

/// Slave for Quark
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    listen_addr: SocketAddr,

    #[arg(long, default_value = "/tmp/quarkfs-slave/slave1/")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind(args.listen_addr).await?;
    let addr = listener.local_addr()?;

    let greeter = hello::MyGreeter {};
    let builder = opendal::services::Fs::default().root(&args.data_dir);
    let operator = Operator::new(builder)?.finish();

    let slave = slave::SlaveService { fs: operator };

    info!("Quark slave listening on {}", addr);

    let server = Server::builder()
        .add_service(GreeterServer::new(greeter))
        .add_service(SlaveServiceServer::new(slave));
    server
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;

    Ok(())
}
