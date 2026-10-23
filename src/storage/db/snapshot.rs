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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum BootstrapMethod {
    Snapshot,
    Replicate,
}

impl Default for BootstrapMethod {
    fn default() -> Self {
        BootstrapMethod::Snapshot
    }
}

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
    pub bootstrap_method: BootstrapMethod,
    pub replication_peers: Vec<String>,
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
            bootstrap_method: BootstrapMethod::default(),
            replication_peers: vec![],
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

        assemble_and_unpack_shard(&snapshot_dir, shard_id, local_chunks, &db_dir).await?;
    }

    main_pb.finish_with_message("All snapshots downloaded successfully!");
    // Per-shard staging has already been reclaimed; this clears metadata.json
    // and the directory itself.
    std::fs::remove_dir_all(snapshot_dir)?;
    Ok(())
}

/// Remove snapshot staging that is no longer needed, best effort.
///
/// Reclaiming disk must never abort a restore that is otherwise succeeding, so
/// failures are logged and swallowed. The corollary is that the reduced peak
/// usage this enables is contingent on these removals succeeding: if they fail,
/// staging accumulates for the rest of the restore exactly as it did before.
fn reclaim_staging(path: &str, description: &str, shard_id: u32) {
    let result = if std::path::Path::new(path).is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(err) = result {
        warn!(
            "Could not remove {} {} for shard {}: {}. \
             Snapshot staging will not be reclaimed until the restore completes.",
            description, path, shard_id, err
        );
    }
}

/// Assemble one shard's downloaded chunks into a tar, unpack it into the
/// database directory, and reclaim the staging both steps leave behind.
///
/// Staging is released at the two points where it provably stops being needed:
/// each chunk once it has been folded into the tar, since chunks are read
/// exactly once; and the tar plus the shard's chunk directory once the archive
/// has been unpacked. Previously all of it was retained until every shard had
/// finished, making peak disk usage a multiple of the database being restored
/// rather than proportional to it.
///
/// Split out of `download_snapshots` so it can be exercised without an S3
/// client — everything S3-dependent happens before this point.
async fn assemble_and_unpack_shard(
    snapshot_dir: &str,
    shard_id: u32,
    local_chunks: Vec<String>,
    db_dir: &str,
) -> Result<(), SnapshotError> {
    let tar_filename = format!("{}/shard_{}_snapshot.tar", snapshot_dir, shard_id);
    let mut tar_file = BufWriter::new(tokio::fs::File::create(tar_filename.clone()).await?);

    for filename in local_chunks {
        info!(
            "Unzipping snapshot chunk {} for shard {}",
            filename, shard_id
        );
        let file = std::fs::File::open(&filename)?;
        let reader = BufReader::new(file);
        let mut gz_decoder = GzDecoder::new(reader);
        let mut buffer = Vec::new();
        // These files are small, 100MB max each
        gz_decoder.read_to_end(&mut buffer)?;
        tar_file.write_all(&buffer).await?;

        // The decoder owns the reader, which owns the chunk's file handle.
        // Close it before unlinking: Unix tolerates removing an open file,
        // Windows does not.
        drop(gz_decoder);
        reclaim_staging(&filename, "snapshot chunk", shard_id);
    }
    tar_file.flush().await?;
    // Close the writer before reopening the same path for reading, rather than
    // relying on a flushed-but-still-open handle.
    drop(tar_file);

    let file = std::fs::File::open(&tar_filename)?;
    info!(
        "Unpacking snapshot file {} for shard {}",
        tar_filename, shard_id
    );
    let mut archive = Archive::new(file);
    std::fs::create_dir_all(db_dir)?;
    archive.unpack(db_dir)?;

    // The archive still owns the tar's file handle; same reasoning as above.
    drop(archive);
    reclaim_staging(&tar_filename, "snapshot archive", shard_id);
    reclaim_staging(
        &format!("{}/shard-{}", snapshot_dir, shard_id),
        "snapshot chunk directory",
        shard_id,
    );
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

        let retry_strategy = tokio_retry2::strategy::FixedInterval::from_millis(10_000).take(5);
        Retry::spawn(retry_strategy, || {
            let buffer = buffer.clone();
            let key = key.clone();
            let bucket = snapshot_config.s3_bucket.clone();
            let config = snapshot_config.clone();
            async move {
                let client = create_s3_client(&config).await;
                let result = client
                    .put_object()
                    .bucket(bucket.clone())
                    .key(key.clone())
                    .body(ByteStream::from(buffer))
                    .send()
                    .await;
                match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        warn!(
                            "Failed to upload chunk to s3: {}, key: {}, bucket: {}",
                            DisplayErrorContext(&err),
                            key,
                            bucket
                        );
                        RetryError::to_transient(SnapshotError::from(err))
                    }
                }
            }
        })
        .await?;

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
    let retry_strategy = tokio_retry2::strategy::FixedInterval::from_millis(10_000).take(5);
    Retry::spawn(retry_strategy, || {
        let body = metadata_json.as_bytes().to_vec();
        let key = metadata_key.clone();
        let bucket = snapshot_config.s3_bucket.clone();
        let config = snapshot_config.clone();
        async move {
            let client = create_s3_client(&config).await;
            let result = client
                .put_object()
                .bucket(bucket.clone())
                .key(key.clone())
                .body(ByteStream::from(body))
                .content_type("application/json")
                .send()
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(err) => {
                    warn!(
                        "Failed to upload metadata to s3: {}, key: {}, bucket: {}",
                        DisplayErrorContext(&err),
                        key,
                        bucket
                    );
                    RetryError::to_transient(SnapshotError::from(err))
                }
            }
        }
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod snapshot_staging_tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write as _;
    use std::path::Path;
    use tempfile::TempDir;

    /// A tar archive holding a single named entry.
    fn make_tar(entry: &str, contents: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, entry, contents).unwrap();
            builder.finish().unwrap();
        }
        bytes
    }

    /// Write a tar stream split across several gzipped chunks, which is the
    /// shape the restore actually finds on disk: each chunk is a fragment of
    /// one archive, not an archive of its own.
    fn write_chunks(shard_dir: &Path, tar_bytes: &[u8], parts: usize) -> Vec<String> {
        std::fs::create_dir_all(shard_dir).unwrap();
        let part_len = tar_bytes.len().div_ceil(parts);
        tar_bytes
            .chunks(part_len)
            .enumerate()
            .map(|(i, part)| {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
                encoder.write_all(part).unwrap();
                let path = shard_dir.join(format!("chunk_{:04}.bin", i));
                std::fs::write(&path, encoder.finish().unwrap()).unwrap();
                path.to_string_lossy().into_owned()
            })
            .collect()
    }

    #[tokio::test]
    async fn extracts_multiple_shards_and_reclaims_staging() {
        let snapshot_dir = TempDir::new().unwrap();
        let db_dir = TempDir::new().unwrap();
        let snapshot_path = snapshot_dir.path().to_string_lossy().into_owned();
        let db_path = db_dir.path().to_string_lossy().into_owned();

        for shard_id in [1u32, 2u32] {
            let entry = format!("shard-{}.sst", shard_id);
            let contents = format!("contents of shard {}", shard_id);
            let tar_bytes = make_tar(&entry, contents.as_bytes());
            let shard_dir = snapshot_dir.path().join(format!("shard-{}", shard_id));
            let chunks = write_chunks(&shard_dir, &tar_bytes, 3);
            assert_eq!(chunks.len(), 3, "chunk fixture should span several files");

            assemble_and_unpack_shard(&snapshot_path, shard_id, chunks, &db_path)
                .await
                .unwrap();

            // Unpacked where the database expects it.
            let restored = std::fs::read_to_string(db_dir.path().join(&entry)).unwrap();
            assert_eq!(restored, contents);

            // Staging for this shard is gone before the next one begins, which
            // is the whole point: previously it survived until every shard
            // finished.
            assert!(
                !shard_dir.exists(),
                "shard {} chunk directory should be reclaimed",
                shard_id
            );
            assert!(
                !snapshot_dir
                    .path()
                    .join(format!("shard_{}_snapshot.tar", shard_id))
                    .exists(),
                "shard {} tar should be reclaimed",
                shard_id
            );
        }

        // Both shards' data coexists in the database.
        assert!(db_dir.path().join("shard-1.sst").exists());
        assert!(db_dir.path().join("shard-2.sst").exists());
    }

    #[tokio::test]
    async fn assembly_failure_propagates() {
        let snapshot_dir = TempDir::new().unwrap();
        let db_dir = TempDir::new().unwrap();
        let shard_dir = snapshot_dir.path().join("shard-1");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Not gzip. Decompression must fail rather than silently producing an
        // empty or truncated database.
        let bad = shard_dir.join("chunk_0000.bin");
        std::fs::write(&bad, b"this is not gzip data").unwrap();

        let result = assemble_and_unpack_shard(
            &snapshot_dir.path().to_string_lossy(),
            1,
            vec![bad.to_string_lossy().into_owned()],
            &db_dir.path().to_string_lossy(),
        )
        .await;

        assert!(result.is_err(), "a corrupt chunk must abort the restore");
    }

    #[tokio::test]
    async fn unpack_failure_propagates() {
        let snapshot_dir = TempDir::new().unwrap();
        let db_dir = TempDir::new().unwrap();
        let shard_dir = snapshot_dir.path().join("shard-1");

        // Valid gzip whose contents are not a tar archive.
        let chunks = write_chunks(&shard_dir, b"valid gzip, invalid tar", 1);

        let result = assemble_and_unpack_shard(
            &snapshot_dir.path().to_string_lossy(),
            1,
            chunks,
            &db_dir.path().to_string_lossy(),
        )
        .await;

        assert!(
            result.is_err(),
            "an unreadable archive must abort the restore"
        );
    }

    #[test]
    fn reclaim_staging_removes_files_and_directories() {
        let dir = TempDir::new().unwrap();

        let file = dir.path().join("archive.tar");
        std::fs::write(&file, b"x").unwrap();
        reclaim_staging(&file.to_string_lossy(), "snapshot archive", 1);
        assert!(!file.exists());

        let nested = dir.path().join("shard-1");
        std::fs::create_dir_all(nested.join("deeper")).unwrap();
        std::fs::write(nested.join("deeper").join("chunk_0000.bin"), b"x").unwrap();
        reclaim_staging(&nested.to_string_lossy(), "snapshot chunk directory", 1);
        assert!(!nested.exists());
    }

    #[test]
    fn reclaim_staging_tolerates_failure() {
        // Reclaiming disk is best effort. A path that cannot be removed must be
        // logged and ignored, never propagated — a restore that has otherwise
        // succeeded should not fail because cleanup did.
        let dir = TempDir::new().unwrap();
        reclaim_staging(
            &dir.path().join("does-not-exist").to_string_lossy(),
            "snapshot archive",
            1,
        );
    }
}
