use crate::proto::FarcasterNetwork;
use aws_config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError, SdkBody};
use serde::{Deserialize, Serialize};
use std::fs::{self};
use std::io;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    endpoint_url: String,
    s3_bucket: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoint_url: "".to_string(),
            s3_bucket: "snapchain-snapshots".to_string(),
        }
    }
}
pub fn snapshot_directory(network: FarcasterNetwork, shard_id: u32) -> String {
    return format!("snapchain-snapshots/{}/{}", network.as_str_name(), shard_id);
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
    SdkError(#[from] SdkError<PutObjectError, HttpResponse>),

    #[error("unable to convert time to date")]
    DateError,
}

pub async fn upload_to_s3(
    network: FarcasterNetwork,
    chunked_dir_path: String,
    snapshot_config: &Config,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let start_timetamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
    let start_date = chrono::DateTime::from_timestamp_millis(start_timetamp)
        .ok_or(SnapshotError::DateError)?
        .date_naive();
    let region = "auto";
    let config = aws_config::SdkConfig::builder()
        .region(Region::new(region.to_string()))
        .endpoint_url(snapshot_config.endpoint_url.clone())
        .build();
    let s3 = aws_sdk_s3::Client::new(&config);
    let upload_dir = format!(
        "{}/snapshot-{}-{}.tar.gz",
        snapshot_directory(network, shard_id),
        start_date,
        start_timetamp / 1000
    );
    let files = fs::read_dir(chunked_dir_path)?;
    for entry in files {
        let entry = entry.unwrap();
        let key = format!("{}/{}", upload_dir, entry.file_name().to_string_lossy());

        let byte_stream = ByteStream::from_path(entry.path()).await?;
        s3.put_object()
            .key(key)
            .bucket(snapshot_config.s3_bucket.clone())
            .body(byte_stream)
            .send()
            .await?;
    }
    Ok(())
}
