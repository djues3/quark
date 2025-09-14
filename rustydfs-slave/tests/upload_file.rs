use rustydfs_core::proto::slave_service_client::SlaveServiceClient;
use rustydfs_core::proto::upload_block_request::Content;
use rustydfs_core::proto::{self, UploadBlockRequest};
use rustydfs_slave::slave::SlaveService;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tempfile::TempDir;
use tokio_stream::{self as stream};
use tonic::Request;
use tonic::transport::{Server, Uri};

/// A helper function which gets a free port from the OS
fn get_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to a free port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}
/// A helper function to set up a test environment.
/// It initializes temp storage, starts the gRPC server on a random free port,
/// and returns a connected client, the server address, and the storage operator.
async fn setup_test_env() -> (
    SlaveServiceClient<tonic::transport::Channel>,
    String,
    opendal::Operator,
) {
    // tracing_subscriber::fmt::init();

    let temp_dir = TempDir::new().expect("Failed to create temp_dir");
    let dir = temp_dir.path().to_str().unwrap();
    tracing::info!("TempDir: {dir}");
    let builder = opendal::services::Fs::default();
    let builder = builder.root(temp_dir.path().to_str().unwrap());
    let operator = opendal::Operator::new(builder).unwrap().finish();

    // 2. Create our service instance with the in-memory operator
    let service = SlaveService {
        fs: operator.clone(),
    };
    let service = proto::slave_service_server::SlaveServiceServer::new(service);

    // 3. Bind the server to a random, free port
    let port = get_free_port();
    let addr = format!("[::1]:{}", port).parse().unwrap();

    // Run the server in a background task
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();
    });

    // Give the server a moment to start up
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Create a client and connect to the server
    let uri: Uri = format!("http://[::1]:{}", port).parse().unwrap();
    let client = SlaveServiceClient::connect(uri).await.unwrap();

    (client, format!("[::1]:{}", port), operator)
}

#[tokio::test]
async fn test_upload_block_success() {
    // --- Setup ---
    let (mut client, _, fs) = setup_test_env().await;

    // --- Prepare Data ---
    let block_id: String = String::from("successful-upload-test");
    let chunk1_data = b"Hello, ".to_vec();
    let chunk2_data = b"World!".to_vec();
    let full_content = [chunk1_data.clone(), chunk2_data.clone()].concat();
    let requests = vec![
        proto::UploadBlockRequest {
            content: Some(Content::Metadata(proto::BlockMetadata {
                block_id: block_id.as_bytes().to_vec(),
            })),
        },
        proto::UploadBlockRequest {
            content: Some(Content::Chunk(proto::DataChunk {
                checksum: Sha256::digest(&chunk1_data).to_vec(),
                data: chunk1_data,
            })),
        },
        proto::UploadBlockRequest {
            content: Some(Content::Chunk(proto::DataChunk {
                checksum: Sha256::digest(&chunk2_data).to_vec(),
                data: chunk2_data,
            })),
        },
    ];

    // --- Act ---
    let request_stream = stream::iter(requests);
    let response = client
        .upload_block(Request::new(request_stream))
        .await
        .unwrap();
    let response = response.into_inner();

    // --- Assert Response ---
    assert_eq!(response.block_id, block_id.as_bytes());
    assert_eq!(response.size, full_content.len() as u64);

    // --- Assert Side-Effect (Data was actually written) ---
    let written_data = fs.read(&block_id).await.unwrap();

    assert_eq!(written_data.to_vec(), full_content);
}

#[tokio::test]
async fn test_upload_block_checksum_mismatch() {
    // --- Setup ---
    let (mut client, _, fs) = setup_test_env().await;

    let block_id = String::from("checksum-fail-test");

    // --- Prepare Data ---
    let requests = vec![
        UploadBlockRequest {
            content: Some(Content::Metadata(proto::BlockMetadata {
                block_id: block_id.as_bytes().to_vec(),
            })),
        },
        UploadBlockRequest {
            content: Some(Content::Chunk(proto::DataChunk {
                data: b"some data".to_vec(),
                // Deliberately wrong checksum
                checksum: b"not-the-right-checksum".to_vec(),
            })),
        },
    ];

    // --- Act ---
    let request_stream = stream::iter(requests);
    let result = client.upload_block(Request::new(request_stream)).await;

    let status = result.as_ref().err().unwrap();
    let data = fs.read(&block_id).await;

    // --- Assert ---
    assert!(result.is_err());
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("Data chunk failed checksum verification")
    );
    assert!(data.is_err())
}

#[tokio::test]
async fn test_upload_block_metadata_not_first() {
    // --- Setup ---
    let (mut client, _, _) = setup_test_env().await;

    // --- Prepare Data (Send chunk first) ---
    let requests = vec![UploadBlockRequest {
        content: Some(Content::Chunk(proto::DataChunk {
            data: b"some data".to_vec(),
            checksum: Sha256::digest(b"some data").to_vec(),
        })),
    }];
    let request_stream = stream::iter(requests);
    // --- Act ---
    let result = client.upload_block(Request::new(request_stream)).await;

    // --- Assert ---
    assert!(result.is_err());
    let status = result.err().unwrap();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status
            .message()
            .contains("Metadata must be the first message")
    );
}
