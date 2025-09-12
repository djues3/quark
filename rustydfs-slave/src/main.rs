use std::sync::Arc;

use clap::Parser;
use rustydfs_core::proto::greeter_server::GreeterServer;
use tonic::transport::Server;
use tracing::info;

mod hello;
mod slave;
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

    info!("GreeterServer listening on {}", addr);

    let server = Server::builder().add_service(GreeterServer::new(greeter));
    server.serve(addr).await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ServerConfig {
    delay: bool,
}
