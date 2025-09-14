use std::sync::Arc;

use clap::Parser;
use opendal::Operator;
use rustydfs_core::proto::{
    greeter_server::GreeterServer, slave_service_server::SlaveServiceServer,
};
use rustydfs_slave::*;
use tonic::transport::Server;
use tracing::info;

/// Slave for RustyDFS
#[derive(Parser)]
struct Args {
    /// Adds artificial delay to responses, to simulate high network latency
    #[arg(long, env = "RUSTYDFS_DELAY")]
    delay: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let config = ServerConfig { delay: args.delay };

    println!("{:?}", config);

    tracing_subscriber::fmt::init();
    let addr = "[::1]:8080".parse()?;
    let greeter = hello::MyGreeter::new(Arc::new(config));
    let builder = opendal::services::Fs::default().root("/tmp/rdfs-slave/slave1");
    let operator = Operator::new(builder)?.finish();

    let slave = slave::SlaveService { fs: operator };

    info!("GreeterServer listening on {}", addr);

    let server = Server::builder()
        .add_service(GreeterServer::new(greeter))
        .add_service(SlaveServiceServer::new(slave));
    server.serve(addr).await?;

    Ok(())
}
