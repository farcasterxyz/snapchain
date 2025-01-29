use crate::proto::FarcasterNetwork;
use aws_config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{BuildError, DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use serde::{Deserialize, Serialize};
use std::fs::{self};
use std::io;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use thiserror::Error;
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub endpoint_url: String,
    pub s3_bucket: String,
    pub backup_dir: String,
    pub backup_on_startup: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoint_url: "".to_string(),
            s3_bucket: "snapchain-snapshots".to_string(),
            backup_dir: ".rocks.backup".to_string(),
            backup_on_startup: false,
        }
    }
}
fn snapshot_directory(network: FarcasterNetwork, shard_id: u32) -> String {
    return format!("{}/{}", network.as_str_name(), shard_id);
}

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error(transparent)]
    SystemTimeError(#[from] SystemTimeError),

    #[error(transparent)]
    IoError(#[from] io::Error),

    #[error(transparent)]
    ByteStreamError(#[from] ByteStreamError),

    #[error(transparent)]
    PutError(#[from] SdkError<PutObjectError, HttpResponse>),

    #[error(transparent)]
    DeleteObjectsError(#[from] SdkError<DeleteObjectsError, HttpResponse>),

    #[error(transparent)]
    ListObjectsError(#[from] SdkError<ListObjectsV2Error, HttpResponse>),

    #[error(transparent)]
    BuildError(#[from] BuildError),

    #[error("unable to convert time to date")]
    DateError,
}

async fn create_s3_client(snapshot_config: &Config) -> aws_sdk_s3::Client {
    // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION are loaded from envvars
    let config = aws_config::load_from_env().await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .endpoint_url(snapshot_config.endpoint_url.clone())
        .region(Region::new("auto".to_string()))
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

pub async fn clear_snapshots(
    network: FarcasterNetwork,
    snapshot_config: &Config,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let s3_client = create_s3_client(&snapshot_config).await;
    let snapshot_dir = snapshot_directory(network, shard_id);
    info!("Clearing snapshots under {}", snapshot_dir.clone());
    let objects = s3_client
        .list_objects_v2()
        .bucket(snapshot_config.s3_bucket.clone())
        .prefix(snapshot_dir.clone())
        .send()
        .await?;
    if let Some(contents) = objects.contents {
        let objects = contents
            .into_iter()
            .filter_map(|object| match object.key {
                None => None,
                Some(key) => Some(ObjectIdentifier::builder().key(key).build()),
            })
            .collect::<Result<Vec<ObjectIdentifier>, BuildError>>()?;
        let delete_request = Delete::builder().set_objects(Some(objects)).build()?;
        let delete_result = s3_client
            .delete_objects()
            .bucket(snapshot_config.s3_bucket.clone())
            .delete(delete_request)
            .send()
            .await;
        if let Err(err) = &delete_result {
            info!(
                "Error uploading to s3: {}, bucket: {}",
                DisplayErrorContext(err),
                snapshot_dir
            );
        };
        delete_result?;
    }
    Ok(())
}

pub async fn upload_to_s3(
    network: FarcasterNetwork,
    chunked_dir_path: String,
    snapshot_config: &Config,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    info!(shard_id, chunked_dir_path, "Starting upload to s3");
    let start_timetamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let start_date = chrono::DateTime::from_timestamp_millis(start_timetamp)
        .ok_or(SnapshotError::DateError)?
        .date_naive();
    let s3_client = create_s3_client(&snapshot_config).await;
    let upload_dir = format!(
        "{}/snapshot-{}-{}.tar.gz",
        snapshot_directory(network, shard_id),
        start_date,
        start_timetamp / 1000
    );
    let files = fs::read_dir(chunked_dir_path)?;
    for entry in files {
        let entry = entry?;
        let key = format!("{}/{}", upload_dir, entry.file_name().to_string_lossy());

        info!(key, "Uploading chunk to s3");
        let byte_stream = ByteStream::from_path(entry.path()).await?;
        let upload_result = s3_client
            .put_object()
            .key(key.clone())
            .bucket(snapshot_config.s3_bucket.clone())
            .body(byte_stream)
            .send()
            .await;
        if let Err(err) = &upload_result {
            info!(
                "Error uploading to s3: {}, key: {}, bucket: {}",
                DisplayErrorContext(err),
                key,
                snapshot_config.s3_bucket
            );
        }
        upload_result?;
    }
    Ok(())
}
