use quark_core::proto::{self, UploadBlockResponse};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{info, info_span};
use uuid::Uuid;

#[derive(Debug, Error)]
enum UploadError {
    #[error("Error reading from client stream: {0}")]
    StreamError(#[from] tonic::Status),

    #[error("Stream was empty; metadata is required")]
    NoInitialMessage,

    #[error("Request message had no content")]
    MissingContent,

    #[error("Metadata must be the first message")]
    InvalidFirstMessage,

    #[error("block_id is not valid UTF-8")]
    InvalidBlockId(#[from] std::string::FromUtf8Error),

    #[error("Received unexpected metadata after the first message")]
    UnexpectedMetadata,

    #[error("Data chunk failed checksum verification")]
    ChecksumMismatch,

    #[error("Storage write failed")]
    StorageError(#[from] opendal::Error),
}

pub struct SlaveService {
    pub fs: opendal::Operator,
}

#[tonic::async_trait]
impl proto::slave_service_server::SlaveService for SlaveService {
    async fn upload_block(
        &self,
        request: tonic::Request<tonic::Streaming<proto::UploadBlockRequest>>,
    ) -> Result<tonic::Response<proto::UploadBlockResponse>, tonic::Status> {
        let span = info_span!("Upload block request");
        let _guard = span.enter();

        let mut stream = request.into_inner();

        match self.handle_upload_stream(&mut stream).await {
            Ok(response) => {
                info!("Block upload successful");
                Ok(tonic::Response::new(response))
            }
            Err(e) => {
                tracing::error!("Block upload failed: {e}");
                Err(e.into())
            }
        }
    }
}

impl SlaveService {
    async fn handle_upload_stream(
        &self,
        stream: &mut Streaming<proto::UploadBlockRequest>,
    ) -> Result<UploadBlockResponse, UploadError> {
        let block_id = self.handle_metadata(stream).await?;

        // the uuid protects against races in case of client retry
        let part_file = format!("{block_id}.{}.part", Uuid::now_v7());
        info!(block_id = %block_id, "Received metadata, starting chunk processing.");
        let mut total_size: u64 = 0;
        while let Some(message) = stream.next().await {
            let message = message?;
            match message.content {
                Some(proto::upload_block_request::Content::Chunk(chunk)) => {
                    if !verify_checksum(&chunk.data, &chunk.checksum) {
                        self.fs.delete(&part_file).await.ok();
                        return Err(UploadError::ChecksumMismatch);
                    }
                    let chunk_size = chunk.data.len();

                    self.fs
                        .write_with(&part_file, chunk.data)
                        .append(true)
                        .await?;
                    total_size += chunk_size as u64;
                    info!(block_id = %block_id, chunk_size, "Appended chunk to partfile");
                }
                Some(proto::upload_block_request::Content::Metadata(_)) => {
                    return Err(UploadError::UnexpectedMetadata);
                }
                None => {
                    return Err(UploadError::MissingContent);
                }
            }
        }
        info!(block_id = %block_id, part_file = %part_file, total_size, "Block upload finished, commiting file.");
        self.fs.rename(&part_file, &block_id).await?;

        Ok(proto::UploadBlockResponse {
            block_id: block_id.into(),
            size: total_size,
        })
    }

    async fn handle_metadata(
        &self,
        stream: &mut Streaming<proto::UploadBlockRequest>,
    ) -> Result<String, UploadError> {
        let first = stream.next().await.ok_or(UploadError::NoInitialMessage)??;
        match first.content {
            Some(proto::upload_block_request::Content::Metadata(meta)) => {
                Ok(String::from_utf8(meta.block_id)?)
            }
            Some(_) => Err(UploadError::InvalidFirstMessage),
            None => Err(UploadError::NoInitialMessage),
        }
    }
}

fn verify_checksum(data: &[u8], checksum: &[u8]) -> bool {
    let hash = Sha256::digest(data);

    hash.as_slice() == checksum
}

impl From<UploadError> for tonic::Status {
    fn from(value: UploadError) -> Self {
        match value {
            UploadError::NoInitialMessage
            | UploadError::MissingContent
            | UploadError::InvalidFirstMessage
            | UploadError::InvalidBlockId(_)
            | UploadError::UnexpectedMetadata => {
                tonic::Status::failed_precondition(value.to_string())
            }
            UploadError::ChecksumMismatch => tonic::Status::invalid_argument(value.to_string()),
            UploadError::StorageError(internal) => {
                info!("{internal}");
                tonic::Status::internal("Internal storage error occurred")
            }
            UploadError::StreamError(status) => status,
        }
    }
}
