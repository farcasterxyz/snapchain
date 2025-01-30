use crate::proto::FarcasterNetwork;
use aws_config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{BuildError, DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs::{self};
use std::io::{self};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tar::{Archive, EntryType};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub endpoint_url: String,
    pub s3_bucket: String,
    pub backup_dir: String,
    pub backup_on_startup: bool,
    pub load_db_from_snapshot: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            endpoint_url: "".to_string(),
            s3_bucket: "snapchain-snapshots".to_string(),
            backup_dir: ".rocks.backup".to_string(),
            backup_on_startup: false,
            load_db_from_snapshot: true,
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

#[derive(Serialize, Deserialize)]
struct SnapshotMetadata {
    key_base: String,
    chunks: Vec<String>,
    timestamp: i64,
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

pub async fn clear_snapshots(
    network: FarcasterNetwork,
    snapshot_config: &Config,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let s3_client = create_s3_client(&snapshot_config).await;
    let snapshot_dir = snapshot_directory(network, shard_id);
    info!("Clearing snapshots under {}", snapshot_dir.clone());
    let objects = get_objects_under_key(&s3_client, &snapshot_config, snapshot_dir.clone()).await?;
    if objects.is_empty() {
        return Ok(());
    } else {
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

pub async fn download_snapshots(
    network: FarcasterNetwork,
    snapshot_config: &Config,
    db_dir: String,
    shard_id: u32,
) {
    let snapshot_dir = ".rocks.snapshot";
    let metadata_url = format!(
        "{}/{}",
        snapshot_config.endpoint_url,
        metadata_path(network, shard_id)
    );
    let metadata_bytes = reqwest::get(metadata_url)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let metadata_json: SnapshotMetadata = serde_json::from_slice(&metadata_bytes.to_vec()).unwrap();
    let base_path = metadata_json.key_base;
    for chunk in metadata_json.chunks {
        let download_path = format!("{}/{}/{}", snapshot_config.endpoint_url, base_path, chunk);
        let download_response = reqwest::get(download_path).await.unwrap();
        let mut byte_stream = download_response.bytes_stream();
        let mut file = BufWriter::new(
            tokio::fs::File::create(format!("{}/{}", snapshot_dir, chunk))
                .await
                .unwrap(),
        );

        while let Some(Ok(piece)) = byte_stream.next().await {
            file.write_all(&piece).await.unwrap();
        }
    }

    for entry in fs::read_dir(snapshot_dir).unwrap() {
        let file = std::fs::File::open(entry.unwrap().path()).unwrap();
        let gz_decoder = GzDecoder::new(file);
        let mut archive = Archive::new(gz_decoder);
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let mut path = PathBuf::from_str(&db_dir).unwrap();
            path.extend(entry.path().unwrap().into_iter().skip(1));
            match entry.header().entry_type() {
                EntryType::Regular => {
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    let mut new_file = std::fs::File::create(path.to_str().unwrap()).unwrap();
                    // TODO(aditi): We should not using a big blocking operation here. Should use chunks.
                    std::io::copy(&mut entry, &mut new_file).unwrap();
                }
                EntryType::Directory => std::fs::create_dir_all(path).unwrap(),
                _ => {}
            }
        }
    }
    std::fs::remove_dir_all(snapshot_dir).unwrap();
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
    let mut file_names: Vec<String> = vec![];
    for entry in files {
        let entry = entry?;
        let file_name = entry.file_name().to_str().unwrap().to_string();
        let key = format!("{}/{}", upload_dir, file_name);

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
        file_names.push(file_name);
    }

    let metadata = SnapshotMetadata {
        key_base: upload_dir,
        chunks: file_names,
        timestamp: start_timetamp,
    };

    let metadata_json = serde_json::to_string(&metadata).unwrap();
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
        info!(
            "Error uploading to s3: {}, key: {}, bucket: {}",
            DisplayErrorContext(err),
            metadata_key,
            snapshot_config.s3_bucket
        );
    }
    upload_result?;
    Ok(())
}
