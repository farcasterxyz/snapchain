use crate::proto::FarcasterNetwork;
use crate::utils::statsd_wrapper::StatsdClientWrapper;
use aws_config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::{BuildError, DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::io::{self, BufReader, Read};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tar::Archive;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncWriteExt, BufWriter};

use tracing::{error, info};

use super::RocksdbError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub endpoint_url: String,
    pub s3_bucket: String,
    pub backup_dir: String,
    pub backup_on_startup: bool,
    pub load_db_from_snapshot: bool,
    pub force_load_db_from_snapshot: bool,
    pub snapshot_download_url: String,
    pub snapshot_download_dir: String,
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoint_url: "".to_string(),
            s3_bucket: "snapchain-snapshots".to_string(),
            backup_dir: ".rocks.backup".to_string(),
            snapshot_download_dir: ".rocks.snapshot".to_string(),
            backup_on_startup: false,
            load_db_from_snapshot: true,
            force_load_db_from_snapshot: false,
            snapshot_download_url: "https://pub-d352dd8819104a778e20d08888c5a661.r2.dev"
                .to_string(),
            aws_access_key_id: "".to_string(),
            aws_secret_access_key: "".to_string(),
        }
    }
}

impl Config {
    pub fn snapshot_upload_enabled(&self) -> bool {
        !self.aws_access_key_id.is_empty() && !self.aws_secret_access_key.is_empty()
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
    CreateMultipartUploadError(#[from] SdkError<CreateMultipartUploadError, HttpResponse>),

    #[error(transparent)]
    CompleteMultipartUploadError(#[from] SdkError<CompleteMultipartUploadError, HttpResponse>),

    #[error(transparent)]
    AbortMultipartUploadError(#[from] SdkError<AbortMultipartUploadError, HttpResponse>),

    #[error(transparent)]
    UploadPartError(#[from] SdkError<UploadPartError, HttpResponse>),

    #[error(transparent)]
    DeleteObjectsError(#[from] SdkError<DeleteObjectsError, HttpResponse>),

    #[error(transparent)]
    ListObjectsError(#[from] SdkError<ListObjectsV2Error, HttpResponse>),

    #[error(transparent)]
    BuildError(#[from] BuildError),

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("unable to convert time to date")]
    DateError,

    #[error("unable to find parent directory")]
    MissingParentDirectory,

    #[error("unable to convert file name to string")]
    UnableToParseFileName,

    #[error("upload already in progress")]
    UploadAlreadyInProgress,

    #[error(transparent)]
    RocksDbError(#[from] RocksdbError),
}

#[derive(Serialize, Deserialize)]
struct SnapshotMetadata {
    key_base: String,
    chunks: Vec<String>,
    timestamp: i64,
}

async fn create_s3_client(snapshot_config: &Config) -> aws_sdk_s3::Client {
    // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_REGION are loaded from envvars
    let credentials = Credentials::new(
        snapshot_config.aws_access_key_id.clone(),
        snapshot_config.aws_secret_access_key.clone(),
        None,
        None,
        "manual",
    );
    let s3_config = aws_sdk_s3::config::Config::builder()
        .force_path_style(true)
        .endpoint_url(snapshot_config.endpoint_url.clone())
        .credentials_provider(credentials)
        .region(Some(Region::new("auto")))
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

async fn get_objects_under_key(
    s3_client: &Client,
    snapshot_config: &Config,
    prefix: String,
) -> Result<Vec<ObjectIdentifier>, SnapshotError> {
    let objects = s3_client
        .list_objects_v2()
        .bucket(snapshot_config.s3_bucket.clone())
        .prefix(prefix)
        .send()
        .await?;
    if let Some(contents) = objects.contents {
        Ok(contents
            .into_iter()
            .filter_map(|object| match object.key {
                None => None,
                Some(key) => Some(ObjectIdentifier::builder().key(key).build()),
            })
            .collect::<Result<Vec<ObjectIdentifier>, BuildError>>()?)
    } else {
        Ok(vec![])
    }
}

pub async fn clear_old_snapshots(
    network: FarcasterNetwork,
    snapshot_config: &Config,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let s3_client = create_s3_client(&snapshot_config).await;
    let snapshot_dir = snapshot_directory(network, shard_id);
    let objects = get_objects_under_key(&s3_client, &snapshot_config, snapshot_dir.clone()).await?;
    let metadata = download_metadata(network, shard_id, snapshot_config).await?;
    let old_objects = objects
        .into_iter()
        .filter(|object| {
            !object.key().contains(&metadata.key_base)
                && !object.key().contains(&metadata_path(network, shard_id))
        })
        .collect_vec();
    if old_objects.is_empty() {
        return Ok(());
    } else {
        info!(
            num_objects = old_objects.len(),
            "Clearing old snapshots under {}",
            snapshot_dir.clone()
        );
        let delete_request = Delete::builder().set_objects(Some(old_objects)).build()?;
        let delete_result = s3_client
            .delete_objects()
            .bucket(snapshot_config.s3_bucket.clone())
            .delete(delete_request)
            .send()
            .await;
        if let Err(err) = &delete_result {
            info!(
                "Error clearing snapshot from s3: {}, bucket: {}",
                DisplayErrorContext(err),
                snapshot_dir
            );
        };
        delete_result?;
        Ok(())
    }
}

fn metadata_path(network: FarcasterNetwork, shard_id: u32) -> String {
    format!(
        "{}/{}",
        snapshot_directory(network, shard_id),
        "latest.json"
    )
}

async fn download_metadata(
    network: FarcasterNetwork,
    shard_id: u32,
    snapshot_config: &Config,
) -> Result<SnapshotMetadata, SnapshotError> {
    let metadata_url = format!(
        "{}/{}",
        snapshot_config.snapshot_download_url,
        metadata_path(network, shard_id)
    );
    info!("Retrieving metadata from {}", metadata_url);
    let metadata = reqwest::get(metadata_url)
        .await?
        .json::<SnapshotMetadata>()
        .await?;
    Ok(metadata)
}

pub async fn download_snapshots(
    network: FarcasterNetwork,
    snapshot_config: &Config,
    db_dir: String,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let snapshot_dir = snapshot_config.snapshot_download_dir.clone();
    std::fs::create_dir_all(snapshot_dir.clone())?;

    let metadata_json = download_metadata(network, shard_id, snapshot_config).await?;
    let base_path = metadata_json.key_base;

    let mut local_chunks = vec![];
    for chunk in metadata_json.chunks {
        info!("Downloading zipped snapshot chunk {}", chunk);
        let download_path = format!(
            "{}/{}/{}",
            snapshot_config.snapshot_download_url, base_path, chunk
        );

        let filename = format!("{}/{}", snapshot_dir, chunk);
        let mut file = BufWriter::new(tokio::fs::File::create(filename.clone()).await?);
        let download_response = reqwest::get(download_path).await?;
        let mut byte_stream = download_response.bytes_stream();

        while let Some(piece) = byte_stream.next().await {
            file.write_all(&piece?).await?;
        }
        file.flush().await?;
        local_chunks.push(filename);
    }

    let tar_filename = format!("{}/snapshot.tar", snapshot_dir);
    let mut tar_file = BufWriter::new(tokio::fs::File::create(tar_filename.clone()).await?);

    for filename in local_chunks {
        info!("Unzipping snapshot chunk {}", filename);
        let file = std::fs::File::open(filename)?;
        let reader = BufReader::new(file);
        let mut gz_decoder = GzDecoder::new(reader);
        let mut buffer = Vec::new();
        // These files are small, 100MB max each
        gz_decoder.read_to_end(&mut buffer)?;
        tar_file.write_all(&buffer).await?;
    }
    tar_file.flush().await?;

    let file = std::fs::File::open(tar_filename.clone())?;
    info!("Unpacking snapshot file {}", tar_filename);
    let mut archive = Archive::new(file);
    archive.unpack(&db_dir)?;

    std::fs::remove_dir_all(snapshot_dir)?;
    Ok(())
}

pub async fn upload_to_s3(
    network: FarcasterNetwork,
    chunked_dir_path: String,
    snapshot_config: &Config,
    shard_id: u32,
    statsd_client: &StatsdClientWrapper,
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
    let files = std::fs::read_dir(chunked_dir_path)?;
    let mut file_names: Vec<String> = vec![];
    for entry in files {
        let entry = entry?;
        let file_name = entry
            .file_name()
            .to_str()
            .ok_or(SnapshotError::UnableToParseFileName)?
            .to_string();
        let key = format!("{}/{}", upload_dir, file_name);

        let mut file = tokio::fs::File::open(entry.path()).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        s3_client
            .put_object()
            .bucket(snapshot_config.s3_bucket.clone())
            .key(key.clone())
            .body(ByteStream::from(buffer))
            .send()
            .await?;

        info!(key, "Finished uploading snapshot to s3");
        statsd_client.count_with_shard(shard_id, "snapshots.successful_upload", 1);

        file_names.push(file_name)
    }

    let metadata = SnapshotMetadata {
        key_base: upload_dir,
        chunks: file_names,
        timestamp: start_timetamp,
    };

    let metadata_json = serde_json::to_string(&metadata)?;
    let metadata_key = metadata_path(network, shard_id);
    let upload_result = s3_client
        .put_object()
        .bucket(snapshot_config.s3_bucket.clone())
        .key(metadata_key.clone())
        .body(ByteStream::from(metadata_json.as_bytes().to_vec()))
        .content_type("application/json")
        .send()
        .await;

    if let Err(err) = &upload_result {
        error!(
            "Error uploading metadata to s3: {}, key: {}, bucket: {}",
            DisplayErrorContext(err),
            metadata_key,
            snapshot_config.s3_bucket
        );
    }
    upload_result?;
    Ok(())
}
