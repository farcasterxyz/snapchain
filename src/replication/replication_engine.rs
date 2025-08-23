use crate::{
    consensus::proposer::ProposalSource,
    core::util::FarcasterTime,
    proto,
    storage::{
        db::RocksDbTransactionBatch,
        store::engine::ShardEngine,
        trie::merkle_trie::{self, TrieKey},
    },
    version::version::EngineVersion,
};

/*
 *
 * This extension to the ShardEngine provides the ability to replicate FID state between
 * two nodes for a shard.
 * TODO: Remove this class with the replication client
 */
impl ShardEngine {
    pub fn account_root_for_fid(&self, fid: u64) -> Vec<u8> {
        let stores = self.get_stores();
        stores
            .trie
            .get_hash(
                &stores.db,
                &mut RocksDbTransactionBatch::new(),
                &TrieKey::for_fid(fid),
            )
            .unwrap()
    }

    pub fn replay_transaction(
        &mut self,
        tx: &proto::Transaction,
    ) -> Result<(), crate::core::error::HubError> {
        let db = self.get_stores().db.clone();
        let mut tx_batch = RocksDbTransactionBatch::new();
        let ctx = merkle_trie::Context::new();

        let (_, _, validation_errors) = self
            .replay_snapchain_txn(
                &ctx,
                &tx,
                &mut tx_batch,
                ProposalSource::Commit,
                // TODO: consider making this more precise w/:
                // EngineVersion::version_for(snapshot_block_timestamp, network)
                EngineVersion::current(self.network),
                &FarcasterTime::current(),
            )
            .unwrap();

        if !validation_errors.is_empty() {
            return Err(crate::core::error::HubError {
                code: "internal_error".to_string(),
                message: format!("Validation errors found: {:?}", validation_errors),
            });
        }

        db.commit(tx_batch).unwrap();
        Ok(())
    }
}
