use super::RocksdbError;
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
use std::collections::HashMap;
use std::io::{self, BufReader, Read};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tar::Archive;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_retry2::{Retry, RetryError};
use tracing::{error, info, warn};

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
    shard_ids: Vec<u32>,
) -> Result<(), SnapshotError> {
    let snapshot_dir = snapshot_config.snapshot_download_dir.clone();
    std::fs::create_dir_all(snapshot_dir.clone())?;

    // First, fetch metadata for all shards
    let mut all_metadata = HashMap::new();
    for &shard_id in &shard_ids {
        let metadata = download_metadata(network, shard_id, snapshot_config).await?;
        all_metadata.insert(shard_id.to_string(), metadata);
    }

    // Persist metadata.json file
    let metadata_file_path = format!("{}/metadata.json", snapshot_dir);
    let metadata_json = serde_json::to_string_pretty(&all_metadata)?;
    std::fs::write(&metadata_file_path, metadata_json)?;
    info!("Persisted metadata to {}", metadata_file_path);

    // Create a multi-progress bar for overall progress
    let multi_progress = indicatif::MultiProgress::new();
    let main_style = indicatif::ProgressStyle::default_bar()
        .template("{spinner:.green} Overall Progress [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} chunks ({eta})")
        .unwrap()
        .progress_chars("=>-");

    // Count total chunks for the main progress bar
    let mut total_chunks = 0;
    for metadata in all_metadata.values() {
        total_chunks += metadata.chunks.len();
    }

    let main_pb = multi_progress.add(indicatif::ProgressBar::new(total_chunks as u64));
    main_pb.set_style(main_style);

    // Check if we're in a TTY environment - if not, disable progress bars
    let use_progress_bars = atty::is(atty::Stream::Stdout);
    let global_start = SystemTime::now();
    if !use_progress_bars {
        info!("No TTY detected, progress will be logged instead of shown as progress bars");
        info!(
            "Starting download of {} chunks across {} shards",
            total_chunks,
            shard_ids.len()
        );
    }

    // Process each shard sequentially
    for &shard_id in &shard_ids {
        let metadata_json = &all_metadata[&shard_id.to_string()];
        let base_path = &metadata_json.key_base;
        let shard_start = SystemTime::now();

        std::fs::create_dir_all(format!("{}/shard-{}", snapshot_dir, shard_id))?;

        let mut local_chunks = vec![];
        // Process each chunk sequentially within the shard
        for (chunk_index, chunk) in metadata_json.chunks.iter().enumerate() {
            let eta_str = if !use_progress_bars && chunk_index > 0 {
                let elapsed = shard_start.elapsed().unwrap_or_default().as_secs();
                let remaining_chunks = metadata_json.chunks.len() - chunk_index;
                let eta_seconds = (elapsed * remaining_chunks as u64) / chunk_index as u64;
                format!(
                    "Shard ETA: {}h{}m",
                    eta_seconds / 3600,
                    (eta_seconds % 3600) / 60
                )
            } else {
                String::new()
            };

            info!(
                "Downloading zipped snapshot chunk {} for shard {} ({}/{} chunks in shard) {}",
                chunk,
                shard_id,
                chunk_index + 1,
                metadata_json.chunks.len(),
                eta_str
            );
            let download_path = format!(
                "{}/{}/{}",
                snapshot_config.snapshot_download_url, base_path, chunk
            );

            let filename = format!("{}/shard-{}/{}", snapshot_dir, shard_id, chunk);

            // Set up individual progress bar for this chunk (only if TTY available)
            let chunk_pb = if use_progress_bars {
                multi_progress.add(indicatif::ProgressBar::new(0)) // Length set in download_file
            } else {
                indicatif::ProgressBar::hidden()
            };

            let retry_strategy = tokio_retry2::strategy::FixedInterval::from_millis(10_000).take(5);
            // Directly await the download of the current chunk before proceeding to the next
            let result = Retry::spawn(retry_strategy, || {
                let download_path_clone = download_path.clone();
                let filename_clone = filename.clone();
                let chunk_pb_clone = chunk_pb.clone();
                let main_pb_clone = main_pb.clone();
                async move {
                    let result = download_file(
                        &download_path_clone,
                        &filename_clone,
                        chunk_pb_clone,
                        main_pb_clone,
                        use_progress_bars,
                        global_start,
                    )
                    .await;
                    match result {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            warn!("Failed to download {} due to error: {}", filename_clone, e);
                            RetryError::to_transient(e)
                        }
                    }
                }
            })
            .await;

            chunk_pb.finish_and_clear(); // Remove the finished chunk bar

            if let Err(e) = result {
                error!("Failed to download snapshot chunk {}: {}", filename, e);
                main_pb.finish_with_message("Download failed!");
                return Err(SnapshotError::from(e));
            }
            local_chunks.push(filename);
        }

        let tar_filename = format!("{}/shard_{}_snapshot.tar", snapshot_dir, shard_id);
        let mut tar_file = BufWriter::new(tokio::fs::File::create(tar_filename.clone()).await?);

        for filename in local_chunks {
            info!(
                "Unzipping snapshot chunk {} for shard {}",
                filename, shard_id
            );
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
        info!(
            "Unpacking snapshot file {} for shard {}",
            tar_filename, shard_id
        );
        let mut archive = Archive::new(file);
        std::fs::create_dir_all(&db_dir)?;
        archive.unpack(&db_dir)?;
    }

    main_pb.finish_with_message("All snapshots downloaded successfully!");
    std::fs::remove_dir_all(snapshot_dir)?;
    Ok(())
}

async fn download_file(
    url: &str,
    filename: &str,
    chunk_pb: indicatif::ProgressBar,
    main_pb: indicatif::ProgressBar,
    use_progress_bars: bool,
    global_start: SystemTime,
) -> Result<(), SnapshotError> {
    let mut file = BufWriter::new(tokio::fs::File::create(filename).await?);
    let download_response = reqwest::get(url).await?.error_for_status()?;
    let content_length = download_response.content_length();

    // Configure the progress bar for the current chunk
    if let Some(length) = content_length {
        chunk_pb.set_length(length);
        if !use_progress_bars {
            info!(
                "Starting download of {} ({} bytes)",
                filename.split('/').last().unwrap_or(filename),
                length
            );
        }
    }
    chunk_pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} {msg:30.bold} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
            .unwrap()
            .progress_chars("=>-"),
    );
    chunk_pb.set_message(filename.split('/').last().unwrap_or(filename).to_string());

    let mut byte_stream = download_response.bytes_stream();

    while let Some(piece) = byte_stream.next().await {
        let chunk = piece?;
        file.write_all(&chunk).await?;
        let chunk_len = chunk.len() as u64;

        // Update only the individual chunk progress bar during download
        chunk_pb.inc(chunk_len);
    }
    file.flush().await?;

    // File integrity check
    let file_size = tokio::fs::metadata(filename).await?.len();
    if let Some(content_length) = content_length {
        if file_size != content_length {
            return Err(SnapshotError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Downloaded file size {} does not match content length {}",
                    file_size, content_length
                ),
            )));
        }
    } else {
        warn!(
            "Content-Length header was not present for {}. Cannot verify downloaded file size {}.",
            url, file_size
        );
    }

    // Increment the main progress bar by 1 chunk after successful download
    main_pb.inc(1);

    if !use_progress_bars {
        let current_chunk = main_pb.position();
        let total_chunks = main_pb.length().unwrap_or(0);

        // Calculate global ETA based on overall progress
        let global_eta_str = if current_chunk > 0 {
            let global_elapsed = global_start.elapsed().unwrap_or_default().as_secs();
            let remaining_chunks = total_chunks - current_chunk;
            let global_eta_seconds = (global_elapsed * remaining_chunks) / current_chunk;
            let eta_formatted = format!(
                "{}h{}m",
                global_eta_seconds / 3600,
                (global_eta_seconds % 3600) / 60
            );
            format!(" (ETA: {})", eta_formatted)
        } else {
            String::new()
        };

        info!(
            "Completed download of {} ({} bytes). Progress: {}/{} chunks ({}%){}",
            filename.split('/').last().unwrap_or(filename),
            file_size,
            current_chunk,
            total_chunks,
            if total_chunks > 0 {
                (current_chunk * 100) / total_chunks
            } else {
                0
            },
            global_eta_str
        );
    }

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
    let mut s3_client = create_s3_client(&snapshot_config).await;
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

        let result = s3_client
            .put_object()
            .bucket(snapshot_config.s3_bucket.clone())
            .key(key.clone())
            .body(ByteStream::from(buffer.clone()))
            .send()
            .await;

        if let Err(err) = &result {
            error!(
                "Error uploading snapshot to s3: {}, key: {}, bucket: {}",
                DisplayErrorContext(err),
                key,
                snapshot_config.s3_bucket
            );

            // The sdk retries by default, but certain errors are not retriable like credentials
            // expiring, so retry manually once with a fresh client.
            s3_client = create_s3_client(&snapshot_config).await;
            s3_client
                .put_object()
                .bucket(snapshot_config.s3_bucket.clone())
                .key(key.clone())
                .body(ByteStream::from(buffer))
                .send()
                .await?;
        }
        info!(key, "Finished uploading snapshot to s3");
        statsd_client.count_with_shard(shard_id, "snapshots.successful_upload", 1, vec![]);

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
