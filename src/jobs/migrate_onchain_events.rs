use crate::mempool::mempool::{MempoolRequest, MempoolSource};
use crate::storage::db::{PageOptions, RocksDbTransactionBatch};
use crate::storage::store::block_engine::BlockStores;
use crate::storage::store::mempool_poller::MempoolMessage;
use crate::storage::store::node_local_state::LocalStateStore;
use crate::storage::store::stores::Stores;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tracing::{error, info};

// TODO(aditi): This is not a job right now. It's an rpc because that gives us more control over when it runs. Shift it to a job if needed once stable.

const MIGRATION_BATCH_SIZE: usize = 100;
const MAX_MEMPOOL_SIZE: u64 = 1_000;

pub async fn migrate_onchain_events(
    shard_stores: Stores,
    block_stores: BlockStores,
    mempool_tx: mpsc::Sender<MempoolRequest>,
    local_state_store: LocalStateStore,
) {
    let shard_id = shard_stores.shard_id;
    let mut batches_finished = 0;

    info!(shard_id, "Started migrating onchain events");
    loop {
        let page_token = local_state_store
            .get_onchain_events_migration_page_token(shard_id)
            .unwrap_or_else(|e| {
                error!(
                    "Error getting migration page token for shard {}: {}",
                    shard_id, e
                );
                None
            });

        match migrate_shard_onchain_events_batch(
            &shard_stores,
            &block_stores,
            &mempool_tx,
            page_token,
        )
        .await
        {
            Ok(next_page_token) => {
                // Update the page token after processing this batch
                if let Err(e) = local_state_store
                    .set_onchain_events_migration_page_token(shard_id, next_page_token.clone())
                {
                    error!(
                        "Error updating migration page token for shard {}: {}",
                        shard_id, e
                    );
                    return;
                }
                if next_page_token.is_none() {
                    info!(shard_id, "Finished migrating all onchain events");
                    return;
                } else {
                    batches_finished += 1;
                    if batches_finished % 100 == 0 {
                        info!(
                            shard_id,
                            "Finished migrating {} batches of onchain events", batches_finished
                        );
                    }
                }
            }
            Err(e) => {
                error!(shard_id, "Error migrating onchain events: {}", e);
                return;
            }
        }

        if let Err(err) = wait_for_mempool_to_clear(&mempool_tx).await {
            error!("Error polling mempool for size {}", err);
            return;
        }
    }
}

async fn wait_for_mempool_to_clear(
    mempool_tx: &mpsc::Sender<MempoolRequest>,
) -> Result<(), String> {
    loop {
        let (tx, rx) = oneshot::channel();
        if let Err(e) = mempool_tx.send(MempoolRequest::GetSize(tx)).await {
            return Err(format!("Error sending message to mempool: {}", e));
        }
        if let Ok(sizes) = rx.await {
            let size = sizes.get(&0).unwrap();
            if *size < MAX_MEMPOOL_SIZE {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn migrate_shard_onchain_events_batch(
    source_shard_store: &Stores,
    block_stores: &BlockStores,
    mempool_tx: &mpsc::Sender<MempoolRequest>,
    page_token: Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, String> {
    let page_options = PageOptions {
        page_size: Some(MIGRATION_BATCH_SIZE),
        page_token,
        reverse: false,
    };

    // Get all onchain events using pagination
    let events_page = source_shard_store
        .onchain_event_store
        .get_all_onchain_events(&page_options)
        .map_err(|e| format!("Error getting onchain events: {}", e))?;

    for event in &events_page.onchain_events {
        // Check if event already exists in shard 0 by merging and seeing that the result isn't an error (duplicate error)
        if let Ok(_) = block_stores
            .onchain_event_store
            .merge_onchain_event(event.clone(), &mut &mut RocksDbTransactionBatch::new())
        {
            // Submit OnchainEventForMigration to mempool
            let mempool_message = MempoolMessage::OnchainEventForMigration(event.clone());

            if let Err(e) = mempool_tx
                .send(MempoolRequest::AddMessage(
                    mempool_message,
                    MempoolSource::Local,
                    None,
                ))
                .await
            {
                return Err(format!("Error sending message to mempool: {}", e));
            }
        }
    }
    Ok(events_page.next_page_token)
}
