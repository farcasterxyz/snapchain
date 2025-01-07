use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn};

use crate::{
    proto::{FnameTransfer, UserNameProof, UserNameType, ValidatorMessage},
    storage::store::engine::MempoolMessage,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub start_from: u64, // for testing
    pub stop_at: u64,    // for testing
    pub url: String,
    pub disable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            start_from: 0,
            stop_at: 200, // set this default to a small value for now, revisit later
            url: "https://fnames.farcaster.xyz/transfers".to_string(),
            disable: false,
        }
    }
}

#[derive(Deserialize, Debug)]
struct TransfersData {
    transfers: Vec<Transfer>,
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
}

pub struct Fetcher {
    position: u64,
    transfers: Vec<Transfer>,
    cfg: Config,
    mempool_tx: mpsc::Sender<MempoolMessage>,
}

impl Fetcher {
    pub fn new(cfg: Config, mempool_tx: mpsc::Sender<MempoolMessage>) -> Self {
        Fetcher {
            position: cfg.start_from,
            transfers: vec![],
            cfg: cfg,
            mempool_tx,
        }
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

            for t in response.transfers {
                if t.id <= self.position {
                    return Err(FetchError::NonSequentialIds {
                        id: t.id,
                        position: self.position,
                    });
                }
                if t.id > self.cfg.stop_at {
                    return Err(FetchError::Stop);
                }
                self.position = t.id;
                self.transfers.push(t.clone()); // Just store these for now, we'll use them later

                let username_proof = UserNameProof {
                    timestamp: t.timestamp,
                    name: t.username.into_bytes(),
                    owner: t.owner.into_bytes(),
                    signature: t.server_signature.into_bytes(),
                    fid: t.to,
                    r#type: UserNameType::UsernameTypeFname as i32,
                };
                if let Err(err) = self
                    .mempool_tx
                    .send(MempoolMessage::ValidatorMessage(ValidatorMessage {
                        on_chain_event: None,
                        fname_transfer: Some(FnameTransfer {
                            id: t.to,
                            from_fid: t.from,
                            proof: Some(username_proof),
                        }),
                    }))
                    .await
                {
                    error!(
                        from = t.from,
                        to = t.to,
                        err = err.to_string(),
                        "Unable to send fname transfer to mempool"
                    )
                }
            }
        }
    }

    pub async fn run(&mut self) -> () {
        loop {
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
                }
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}
