use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::block_engine::BlockStores;
use crate::storage::store::stores::Stores;
use snapchain_proto::BlockEventType;
use tracing::info;

pub async fn reconcile_heartbeat_events(
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

            let next_seqnum = target_stores.block_event_store.max_seqnum()? + 1;
            // Check that we're inserting the next block event
            if next_seqnum == block_event.seqnum() {
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
                    next_seqnum, target_seqnum, "Block event not next in line"
                )
            }
        } else {
            info!("Block event is not a heartbeat event {}", target_seqnum);
        }
    } else {
        info!("Could not find block event with seqnum {}", target_seqnum);
    }

    Ok(())
}
