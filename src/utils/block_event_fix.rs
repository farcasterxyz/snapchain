use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::block_engine::BlockStores;
use crate::storage::store::stores::Stores;
use snapchain_proto::BlockEventType;
use tracing::info;

pub async fn reconcile_heartbeat_event(
    source_stores: BlockStores,
    target_stores: Stores,
    target_seqnum: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Looking for block event with seqnum {}", target_seqnum);
    if let Some(block_event) = source_stores
        .block_event_store
        .get_block_event_by_seqnum(target_seqnum)?
    {
        if block_event.data.as_ref().unwrap().r#type() == BlockEventType::Heartbeat {
            info!("Successfully found the block event in the block!");

            // Create a transaction batch and put the block event
            let mut txn = RocksDbTransactionBatch::new();

            if target_stores.block_event_store.max_seqnum().unwrap_or(0) + 1 == block_event.seqnum()
            {
                info!("Calling put_block_event to insert into block event store...");
                target_stores
                    .block_event_store
                    .put_block_event(&block_event, &mut txn)?;

                // Commit the transaction
                info!("Committing transaction...");
                target_stores.db.commit(txn)?;

                info!("Successfully reprocessed the block event!");
            } else {
                info!(
                    shard = target_stores.shard_id,
                    "Block event already exists in shard"
                )
            }
        } else {
            info!("Could not find block event with seqnum {}", target_seqnum);
        }
    } else {
        info!("Could not find block event with seqnum {}", target_seqnum);
    }

    Ok(())
}
