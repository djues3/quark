use std::{sync::Arc, time::Duration};

use rand::Rng;
use quark_core::proto;
use tonic::{Request, Response};
use tracing::{Level, info, instrument, span};

use crate::ServerConfig;

#[derive(Debug, Clone)]
pub struct MyGreeter {
    pub(crate) config: Arc<ServerConfig>,
}

#[tonic::async_trait]
impl proto::greeter_server::Greeter for MyGreeter {
    #[instrument(skip_all)]
    async fn say_hello(
        &self,
        request: Request<proto::HelloRequest>,
    ) -> Result<Response<proto::HelloResponse>, tonic::Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let s = span!(Level::INFO, "abcd");
        let _guard = s.enter();
        info!("Got a request from {:?}", request.name);

        if self.config.delay {
            let duration = delay();
            info!("Sleeping for {duration}ms");
            tokio::time::sleep(Duration::from_millis(duration)).await;
        };

        let reply = proto::HelloResponse {
            message: format!("Hello {}!", request.name),
        };
        info!("Processed request in {:?}", start.elapsed());
        Ok(Response::new(reply))
    }
}

impl MyGreeter {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        MyGreeter { config: config }
    }
}

fn delay() -> u64 {
    let mut rng = rand::rng();
    rng.random_range(100..500)
}
