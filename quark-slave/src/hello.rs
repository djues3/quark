use quark_core::proto;
use tonic::{Request, Response};
use tracing::{Level, info, instrument, span};

#[derive(Debug, Clone)]
pub struct MyGreeter {}

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

        let reply = proto::HelloResponse {
            message: format!("Hello {}!", request.name),
        };
        info!("Processed request in {:?}", start.elapsed());
        Ok(Response::new(reply))
    }
}
