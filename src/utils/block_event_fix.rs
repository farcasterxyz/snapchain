use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::store::block_engine::BlockStores;
use crate::storage::store::stores::Stores;
use snapchain_proto::BlockEventType;
use tracing::info;

pub async fn reprocess_block_event(
    source_stores: BlockStores,
    target_stores: Stores,
    target_block_number: u64,
    target_seqnum: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Looking for block event with seqnum {} in block {}",
        target_seqnum, target_block_number
    );

    // Get the block by height
    info!("Fetching block {}...", target_block_number);
    let block = source_stores
        .block_store
        .get_block_by_height(target_block_number)?
        .expect("Block not found");

    // Find the block event with the target seqnum
    let mut target_event = None;
    for block_event in &block.events {
        // We can only do this for heartbeat events
        if block_event.seqnum() == target_seqnum
            && block_event.data.as_ref().unwrap().r#type() == BlockEventType::Heartbeat
        {
            info!(
                "Found matching block event! seqnum={}",
                block_event.seqnum()
            );
            target_event = Some(block_event.clone());
            break;
        }
    }

    match target_event {
        Some(event) => {
            info!("Successfully found the block event in the block!");

            // Create a transaction batch and put the block event
            let mut txn = RocksDbTransactionBatch::new();

            info!("Calling put_block_event to insert into block event store...");
            target_stores
                .block_event_store
                .put_block_event(&event, &mut txn)?;

            // Commit the transaction
            info!("Committing transaction...");
            target_stores.db.commit(txn)?;

            info!("Successfully reprocessed the block event!");
        }
        None => {
            info!(
                "Could not find block event with seqnum {} in block {}",
                target_seqnum, target_block_number
            );
        }
    }

    Ok(())
}
