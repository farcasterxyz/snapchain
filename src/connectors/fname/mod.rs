use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc},
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};

use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::{
    proto::{FnameTransfer, UserNameProof, UserNameType, ValidatorMessage},
    storage::store::{engine::MempoolMessage, node_local_state::LocalStateStore},
    utils::statsd_wrapper::StatsdClientWrapper,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub start_from: Option<u64>,
    pub stop_at: Option<u64>,
    pub url: String,
    pub disable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            start_from: None,
            stop_at: None,
            url: "https://fnames.farcaster.xyz/transfers".to_string(),
            disable: false,
        }
    }
}

#[derive(Clone)]
pub enum FnameRequest {
    RetryFid(u64),
    RetryFname(String),
}

#[derive(Deserialize, Debug)]
struct TransfersData {
    transfers: Vec<Transfer>,
}

#[derive(Deserialize, Debug)]
struct CurrentTransfer {
    transfer: Transfer,
}

#[derive(Deserialize, Debug, Clone)]
struct Transfer {
    id: u64,

    #[allow(dead_code)] // TODO
    timestamp: u64,

    #[allow(dead_code)] // TODO
    username: String,

    #[allow(dead_code)] // TODO
    owner: String,

    #[allow(dead_code)] // TODO
    from: u64,

    #[allow(dead_code)] // TODO
    to: u64,

    #[allow(dead_code)] // TODO
    user_signature: String,

    #[allow(dead_code)] // TODO
    server_signature: String,
}

#[derive(Error, Debug)]
enum FetchError {
    #[error("non-sequential IDs found")]
    NonSequentialIds { position: u64, id: u64 },

    #[error("stop fetching")]
    Stop,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("invalid format")]
    InvalidFormat,
}

pub struct Fetcher {
    position: u64,
    cfg: Config,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    statsd_client: StatsdClientWrapper,
    local_state_store: LocalStateStore,
    fname_request_rx: broadcast::Receiver<FnameRequest>,
}

impl Fetcher {
    pub fn new(
        cfg: Config,
        mempool_tx: mpsc::Sender<MempoolRequest>,
        statsd_client: StatsdClientWrapper,
        local_state_store: LocalStateStore,
        fname_request_rx: broadcast::Receiver<FnameRequest>,
    ) -> Self {
        Fetcher {
            position: 0,
            cfg,
            mempool_tx,
            statsd_client,
            local_state_store,
            fname_request_rx,
        }
    }

    fn record_username_proof(&self, transfer_id: u64) {
        match self
            .local_state_store
            .set_latest_fname_transfer_id(transfer_id)
        {
            Err(err) => {
                error!(
                    transfer_id,
                    err = err.to_string(),
                    "Unable to store last username proof",
                );
            }
            _ => {}
        }
    }

    fn latest_fname_transfer_in_db(&self) -> u64 {
        match self.local_state_store.get_latest_fname_transfer_id() {
            Ok(id) => id.unwrap_or(0),
            Err(err) => {
                error!(
                    err = err.to_string(),
                    "Unable to retrieve last username proof",
                );
                0
            }
        }
    }

    fn count(&self, key: &str, value: i64) {
        self.statsd_client
            .count(format!("fnames.{}", key).as_str(), value);
    }

    fn gauge(&self, key: &str, value: u64) {
        self.statsd_client
            .gauge(format!("fnames.{}", key).as_str(), value);
    }

    async fn fetch(&mut self) -> Result<(), FetchError> {
        loop {
            let url = format!("{}?from_id={}", self.cfg.url, self.position);
            debug!(%url, "fetching transfers");

            let response = reqwest::get(&url).await?.json::<TransfersData>().await?;

            let count = response.transfers.len();

            if count == 0 {
                return Ok(());
            }

            info!(count, position = self.position, "found new transfers");

            let mut last_transfer_id = 0;
            for t in response.transfers {
                if t.id <= self.position {
                    return Err(FetchError::NonSequentialIds {
                        id: t.id,
                        position: self.position,
                    });
                }
                if self.cfg.stop_at.is_some() && t.id >= self.cfg.stop_at.unwrap() {
                    return Err(FetchError::Stop);
                }
                self.position = t.id;
                let res = self.submit_transfer(&t).await;
                if let Err(err) = res {
                    error!(
                        transfer_id = t.id,
                        err = err.to_string(),
                        "Error processing fname transfer"
                    );
                }
                self.gauge("latest_transfer_id", t.id);
                last_transfer_id = t.id;
            }
            if last_transfer_id > 0 {
                self.record_username_proof(last_transfer_id);
            }
        }
    }

    async fn submit_transfer(&mut self, t: &Transfer) -> Result<(), FetchError> {
        let owner = hex::decode(t.owner[2..].to_string());
        let signature = hex::decode(t.server_signature[2..].to_string());

        if owner.is_err() || signature.is_err() {
            return Err(FetchError::InvalidFormat);
        }

        let username_proof = UserNameProof {
            timestamp: t.timestamp,
            name: t.username.clone().into_bytes(),
            owner: owner.unwrap(),
            signature: signature.unwrap(),
            fid: t.to,
            r#type: UserNameType::UsernameTypeFname as i32,
        };
        self.count("num_transfers", 1);
        if let Err(err) = self
            .mempool_tx
            .send(MempoolRequest::AddMessage(
                MempoolMessage::ValidatorMessage(ValidatorMessage {
                    on_chain_event: None,
                    fname_transfer: Some(FnameTransfer {
                        id: t.id,
                        from_fid: t.from,
                        proof: Some(username_proof),
                    }),
                }),
                MempoolSource::Local,
                None,
            ))
            .await
        {
            error!(
                from = t.from,
                to = t.to,
                err = err.to_string(),
                "Unable to send fname transfer to mempool"
            )
        }
        info!(
            from = t.from,
            fid = t.to,
            name = t.username.clone(),
            "Processed fname transfer"
        );
        Ok(())
    }

    async fn set_initial_position(&mut self) -> Result<(), FetchError> {
        match self.cfg.start_from {
            None => {
                let url = format!("{}/current", self.cfg.url);
                debug!(%url, "fetching transfers");

                let response = reqwest::get(&url).await?.json::<CurrentTransfer>().await?;

                self.position = response.transfer.id;
            }
            Some(start_from) => {
                self.position = start_from.max(self.latest_fname_transfer_in_db());
            }
        };
        Ok(())
    }

    async fn retry_fid(&mut self, fid: u64) -> Result<(), FetchError> {
        info!(fid, "Retrying fname events for fid");

        let url = format!("{}?fid={}", self.cfg.url, fid);
        debug!(%url, "fetching current transfer for retry");

        let response = reqwest::get(&url).await?.json::<TransfersData>().await?;

        for t in response.transfers {
            info!(
                fid,
                transfer_id = t.id,
                username = t.username,
                "Retrying fname transfer"
            );
            if let Err(e) = self.submit_transfer(&t).await {
                error!(
                    fid,
                    transfer_id = t.id,
                    err = e.to_string(),
                    "Error processing fname transfer during retry"
                );
            }
        }

        Ok(())
    }

    async fn retry_fname(&mut self, fname: &str) -> Result<(), FetchError> {
        info!(fname, "Retrying fname events for fname");

        let url = format!("{}?fname={}", self.cfg.url, fname);
        debug!(%url, "fetching current transfer for retry");

        let response = reqwest::get(&url).await?.json::<TransfersData>().await?;

        for t in response.transfers {
            info!(
                fid = t.to,
                transfer_id = t.id,
                username = t.username,
                "Retrying fname transfer"
            );
            if let Err(e) = self.submit_transfer(&t).await {
                error!(
                    fid = t.to,
                    transfer_id = t.id,
                    err = e.to_string(),
                    "Error processing fname transfer during retry"
                );
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> () {
        match self.set_initial_position().await {
            Ok(()) => {}
            Err(err) => {
                // We will just keep the default, 0
                warn!(
                    "Unable to set initial position for fname ingest {}",
                    err.to_string()
                )
            }
        }
        info!(start_id = self.position, "Starting fname ingest");

        loop {
            tokio::select! {
                biased;

                request = self.fname_request_rx.recv() => {
                    match request {
                        Err(_) => {
                            // Ignore, this can happen if we don't run an admin server
                        },
                        Ok(request) => {
                            match request {
                                FnameRequest::RetryFid(retry_fid) => {
                                    if let Err(err) = self.retry_fid(retry_fid).await {
                                        error!(fid = retry_fid, "Unable to retry fid: {}", err.to_string())
                                    }
                                },
                                FnameRequest::RetryFname(fname) => {
                                    if let Err(err) = self.retry_fname(fname.as_str()).await {
                                        error!(fname, "Unable to retry fname: {}", err.to_string())
                                    }
                                }
                            }
                        }
                    }
                }

                _ = sleep(Duration::from_secs(5)) => {
                    let result = self.fetch().await;

                    if let Err(e) = result {
                        match e {
                            FetchError::NonSequentialIds { id, position } => {
                                error!(id, position, %e);
                            }
                            FetchError::Reqwest(request_error) => {
                                warn!(error = %request_error, "reqwest error fetching transfers");
                            }
                            FetchError::Stop => {
                                info!(position = self.position, "stopped fetching transfers");
                                return;
                            }
                            FetchError::InvalidFormat => {
                                error!("fname server returning different format than expected");
                            }
                        }
                    }
                }
            }
        }
    }
}
