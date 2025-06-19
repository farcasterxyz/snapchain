use crate::core::error::HubError;
use crate::proto::HubEvent;
use crate::storage::constants::{RootPrefix, PAGE_SIZE_MAX};
use crate::storage::db::RocksDbTransactionBatch;
use crate::storage::db::{PageOptions, RocksDB};
use crate::storage::util::increment_vec_u8;
use prost::Message as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

const HEIGHT_BITS: u32 = 50;
pub const SEQUENCE_BITS: u32 = 14; // Allows for 16384 events per block

pub struct EventsPage {
    pub events: Vec<HubEvent>,
    pub next_page_token: Option<Vec<u8>>,
}

pub struct HubEventIdGenerator {
    current_height: u64, // current block number
    current_seq: u64,    // current event index within the block number
}

impl HubEventIdGenerator {
    fn new(current_block: Option<u64>, last_seq: Option<u64>) -> Self {
        HubEventIdGenerator {
            current_height: current_block.unwrap_or(0),
            current_seq: last_seq.unwrap_or(0),
        }
    }

    fn set_current_height(&mut self, height: u64) {
        self.current_height = height;
        self.current_seq = 0;
    }

    fn generate_id(&mut self, event: &HubEvent) -> Result<u64, HubError> {
        if self.current_height >= 2u64.pow(HEIGHT_BITS) {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: format!(
                    "height cannot fit in event id. Height > {} bits",
                    HEIGHT_BITS
                ),
            });
        }

        // BLOCK_CONFIRMED Event Sequence Assignment:
        // BLOCK_CONFIRMED events are special metadata events that mark the completion
        // of a block. They always get sequence number 0 within each block to ensure
        // they appear first in event ordering. This provides a consistent way for
        // clients to detect when a block has been finalized and know the total
        // number of events in that block.
        let sequence = match event.r#type() {
            crate::proto::HubEventType::BlockConfirmed => {
                // BLOCK_CONFIRMED always gets sequence 0
                0
            }
            _ => {
                // All other events get sequences starting from 1
                // This ensures BLOCK_CONFIRMED (sequence 0) always comes first
                if self.current_seq == 0 {
                    self.current_seq = 1; // Skip sequence 0 for non-BLOCK_CONFIRMED
                }
                if self.current_seq >= 2u64.pow(SEQUENCE_BITS) {
                    return Err(HubError {
                        code: "bad_request.invalid_param".to_string(),
                        message: format!(
                            "sequence cannot fit in event id. Seq> {} bits",
                            SEQUENCE_BITS
                        ),
                    });
                }
                let seq = self.current_seq;
                self.current_seq += 1;
                seq
            }
        };

        let event_id = Self::make_event_id(self.current_height, sequence);
        Ok(event_id)
    }

    pub fn make_event_id(height: u64, seq: u64) -> u64 {
        let shifted_height = height << SEQUENCE_BITS;
        let padded_seq = seq & ((1 << SEQUENCE_BITS) - 1); // Ensures seq fits in SEQUENCE_BITS
        shifted_height | padded_seq
    }

    pub fn extract_height_and_seq(event_id: u64) -> (u64, u64) {
        let height = event_id >> SEQUENCE_BITS;
        let seq = event_id & ((1 << SEQUENCE_BITS) - 1);
        (height, seq)
    }
}

pub struct StoreEventHandler {
    generator: Arc<Mutex<HubEventIdGenerator>>,
}

impl StoreEventHandler {
    pub fn new() -> Arc<Self> {
        Arc::new(StoreEventHandler {
            generator: Arc::new(Mutex::new(HubEventIdGenerator::new(None, None))),
        })
    }

    pub fn set_current_height(&self, height: u64) {
        let mut generator = self.generator.lock().unwrap();
        generator.set_current_height(height);
    }

    // This is named "commit_transaction" but the commit doesn't actually happen here. This function is provided a [txn] that's committed elsewhere.
    pub fn commit_transaction(
        &self,
        txn: &mut RocksDbTransactionBatch,
        raw_event: &mut HubEvent,
    ) -> Result<u64, HubError> {
        // Acquire the lock so we don't generate multiple IDs. This also serves as the commit lock
        let mut generator = self.generator.lock().unwrap();

        // Generate the event ID
        let event_id = generator.generate_id(&raw_event)?;
        raw_event.id = event_id;

        HubEvent::put_event_transaction(txn, &raw_event)?;

        Ok(event_id)
    }
}

impl HubEvent {
    fn make_event_key(event_id: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + 8);

        key.push(RootPrefix::HubEvents as u8); // HubEvents prefix, 1 byte
        key.extend_from_slice(&event_id.to_be_bytes());

        key
    }

    pub fn put_event_transaction(
        txn: &mut RocksDbTransactionBatch,
        event: &HubEvent,
    ) -> Result<(), HubError> {
        let key = Self::make_event_key(event.id);
        let value = event.encode_to_vec();

        txn.put(key, value);

        Ok(())
    }

    pub fn get_events(
        db: Arc<RocksDB>,
        start_id: u64,
        stop_id: Option<u64>,
        page_options: Option<PageOptions>,
    ) -> Result<EventsPage, HubError> {
        let start_prefix = Self::make_event_key(start_id);
        let stop_prefix = match stop_id {
            Some(id) => Self::make_event_key(id),
            None => increment_vec_u8(&Self::make_event_key(std::u64::MAX)),
        };

        let mut events = Vec::new();
        let mut last_key = vec![];
        let page_options = page_options.unwrap_or_else(|| PageOptions::default());

        db.for_each_iterator_by_prefix_paged(
            Some(start_prefix),
            Some(stop_prefix),
            &page_options,
            |key, value| {
                let event = HubEvent::decode(value).map_err(|e| HubError::from(e))?;
                events.push(event);
                if events.len() >= page_options.page_size.unwrap_or(PAGE_SIZE_MAX) {
                    last_key = key.to_vec();
                    return Ok(true); // Stop iterating
                }
                Ok(false) // Continue iterating
            },
        )?;

        Ok(EventsPage {
            events,
            next_page_token: if last_key.len() > 0 {
                Some(last_key)
            } else {
                None
            },
        })
    }

    pub fn get_event(db: Arc<RocksDB>, event_id: u64) -> Result<HubEvent, HubError> {
        let key = Self::make_event_key(event_id);
        let buf = db.get(&key)?;
        if buf.is_none() {
            return Err(HubError::not_found("Event not found"));
        }

        match HubEvent::decode(buf.unwrap().as_slice()) {
            Ok(event) => Ok(event),
            Err(_) => Err(HubError {
                code: "internal_error".to_string(),
                message: "could not decode event".to_string(),
            }),
        }
    }

    pub async fn prune_events_util(
        db: Arc<RocksDB>,
        stop_height: u64,
        page_options: &PageOptions,
        throttle: Duration,
    ) -> Result<u32, HubError> {
        let stop_event_id = HubEventIdGenerator::make_event_id(stop_height, 0);
        let start_event_key = Self::make_event_key(0);
        let stop_event_key = Self::make_event_key(stop_event_id);
        let total_pruned = db
            .delete_paginated(
                Some(start_event_key),
                Some(stop_event_key),
                &page_options,
                throttle,
                Some(|total_pruned: u32| {
                    info!("Pruning events... pruned: {}", total_pruned);
                }),
            )
            .await?;
        Ok(total_pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{HubEvent, HubEventType};
    use crate::storage::db::RocksDbTransactionBatch;

    fn create_test_event(event_type: HubEventType) -> HubEvent {
        HubEvent {
            r#type: event_type as i32,
            id: 0, // Will be set by generator
            body: None,
            block_number: 0,
            shard_index: 0,
            timestamp: 0,
        }
    }

    #[test]
    fn test_block_confirmed_sequence_assignment() {
        let mut generator = HubEventIdGenerator::new(Some(123), Some(0));

        // Test BLOCK_CONFIRMED gets sequence 0
        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 123);
        assert_eq!(sequence, 0);

        // Test regular event gets sequence 1 (sequence 0 is skipped)
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 123);
        assert_eq!(sequence, 1);

        // Test another regular event gets sequence 2
        let regular_event2 = create_test_event(HubEventType::PruneMessage);
        let event_id = generator.generate_id(&regular_event2).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 123);
        assert_eq!(sequence, 2);

        // Test another BLOCK_CONFIRMED still gets sequence 0
        let block_confirmed2 = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed2).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 123);
        assert_eq!(sequence, 0);
    }

    #[test]
    fn test_event_id_generation_with_block_confirmed() {
        let mut generator = HubEventIdGenerator::new(Some(456), Some(0));

        // Test BLOCK_CONFIRMED event ID generation
        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();

        // Verify the event ID is correctly constructed
        let expected_id = HubEventIdGenerator::make_event_id(456, 0);
        assert_eq!(event_id, expected_id);

        // Test extraction works correctly
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, 456);
        assert_eq!(sequence, 0);

        // Test regular event ID generation
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();

        let expected_id = HubEventIdGenerator::make_event_id(456, 1);
        assert_eq!(event_id, expected_id);

        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, 456);
        assert_eq!(sequence, 1);
    }

    #[test]
    fn test_event_id_generator_state_management() {
        let mut generator = HubEventIdGenerator::new(Some(100), Some(5));

        // Test initial state
        assert_eq!(generator.current_height, 100);
        assert_eq!(generator.current_seq, 5);

        // Test height transition
        generator.set_current_height(200);
        assert_eq!(generator.current_height, 200);
        assert_eq!(generator.current_seq, 0); // Should reset to 0

        // Test sequence progression
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 200);
        assert_eq!(sequence, 1);
        assert_eq!(generator.current_seq, 2); // Should be incremented

        // Test BLOCK_CONFIRMED doesn't affect sequence counter
        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 200);
        assert_eq!(sequence, 0);
        assert_eq!(generator.current_seq, 2); // Should remain unchanged
    }

    #[test]
    fn test_make_event_id_and_extraction() {
        // Test basic event ID creation
        let height = 12345;
        let sequence = 678;
        let event_id = HubEventIdGenerator::make_event_id(height, sequence);

        let (extracted_height, extracted_sequence) =
            HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(extracted_height, height);
        assert_eq!(extracted_sequence, sequence);

        // Test with sequence that needs masking
        let height = 999;
        let sequence = 2u64.pow(SEQUENCE_BITS) + 100; // Should be masked
        let event_id = HubEventIdGenerator::make_event_id(height, sequence);

        let (extracted_height, extracted_sequence) =
            HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(extracted_height, height);
        assert_eq!(extracted_sequence, 100); // Should be masked to fit in SEQUENCE_BITS

        // Test edge cases
        let event_id = HubEventIdGenerator::make_event_id(0, 0);
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, 0);
        assert_eq!(sequence, 0);

        let event_id =
            HubEventIdGenerator::make_event_id(u64::MAX >> SEQUENCE_BITS, (1 << SEQUENCE_BITS) - 1);
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, u64::MAX >> SEQUENCE_BITS);
        assert_eq!(sequence, (1 << SEQUENCE_BITS) - 1);
    }

    #[test]
    fn test_store_event_handler() {
        let handler = StoreEventHandler::new();

        // Test initial state
        handler.set_current_height(100);

        // Test commit transaction with BLOCK_CONFIRMED
        let mut event = create_test_event(HubEventType::BlockConfirmed);
        let mut txn = RocksDbTransactionBatch::new();

        let event_id = handler.commit_transaction(&mut txn, &mut event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 100);
        assert_eq!(sequence, 0);
        assert_eq!(event.id, event_id);

        // Test commit transaction with regular event
        let mut regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = handler
            .commit_transaction(&mut txn, &mut regular_event)
            .unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 100);
        assert_eq!(sequence, 1);
        assert_eq!(regular_event.id, event_id);
    }

    #[test]
    fn test_sequence_skip_behavior() {
        let mut generator = HubEventIdGenerator::new(Some(500), Some(0));

        // Test that sequence 0 is skipped for non-BLOCK_CONFIRMED events
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 500);
        assert_eq!(sequence, 1); // Should skip 0 and start at 1
        assert_eq!(generator.current_seq, 2); // Should be incremented to 2

        // Test BLOCK_CONFIRMED still gets sequence 0
        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 500);
        assert_eq!(sequence, 0);
        assert_eq!(generator.current_seq, 2); // Should remain unchanged

        // Test next regular event gets sequence 2
        let regular_event2 = create_test_event(HubEventType::PruneMessage);
        let event_id = generator.generate_id(&regular_event2).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 500);
        assert_eq!(sequence, 2);
        assert_eq!(generator.current_seq, 3);
    }

    #[test]
    fn test_height_transition_with_block_confirmed() {
        let mut generator = HubEventIdGenerator::new(Some(100), Some(10));

        // Test height transition resets sequence
        generator.set_current_height(200);
        assert_eq!(generator.current_height, 200);
        assert_eq!(generator.current_seq, 0);

        // Test BLOCK_CONFIRMED gets sequence 0 in new height
        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 200);
        assert_eq!(sequence, 0);
        assert_eq!(generator.current_seq, 0); // Should remain 0 for BLOCK_CONFIRMED

        // Test regular event gets sequence 1 in new height
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 200);
        assert_eq!(sequence, 1);
        assert_eq!(generator.current_seq, 2);
    }

    #[test]
    fn test_block_confirmed_sequence_consistency() {
        let mut generator = HubEventIdGenerator::new(Some(300), Some(0));

        // Test multiple BLOCK_CONFIRMED events in same block all get sequence 0
        for i in 0..5 {
            let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
            let event_id = generator.generate_id(&block_confirmed).unwrap();
            let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

            assert_eq!(height, 300);
            assert_eq!(sequence, 0, "BLOCK_CONFIRMED {} should get sequence 0", i);
        }

        // Verify sequence counter is still 0
        assert_eq!(generator.current_seq, 0);

        // Test regular events still get proper sequences
        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 300);
        assert_eq!(sequence, 1);
        assert_eq!(generator.current_seq, 2);
    }

    #[test]
    fn test_event_id_generator_edge_cases() {
        // Test with maximum valid height
        let max_height = (1u64 << HEIGHT_BITS) - 1;
        let mut generator = HubEventIdGenerator::new(Some(max_height), Some(0));

        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let event_id = generator.generate_id(&block_confirmed).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, max_height);
        assert_eq!(sequence, 0);

        // Test with maximum valid sequence
        let max_seq = (1u64 << SEQUENCE_BITS) - 1;
        let mut generator = HubEventIdGenerator::new(Some(100), Some(max_seq - 1));

        let regular_event = create_test_event(HubEventType::MergeMessage);
        let event_id = generator.generate_id(&regular_event).unwrap();
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);

        assert_eq!(height, 100);
        assert_eq!(sequence, max_seq - 1); // Should use current_seq value, not max_seq
    }

    #[test]
    fn test_event_id_generator_overflow_handling() {
        // Test height overflow
        let overflow_height = 1u64 << HEIGHT_BITS;
        let mut generator = HubEventIdGenerator::new(Some(overflow_height), Some(0));

        let block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let result = generator.generate_id(&block_confirmed);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("height cannot fit in event id"));
        assert!(error.message.contains(&HEIGHT_BITS.to_string()));

        // Test sequence overflow - need to set current_seq to exactly the limit
        // The overflow check is: if self.current_seq >= 2u64.pow(SEQUENCE_BITS)
        // So we need current_seq to be >= 16384 to trigger the error
        let overflow_seq = 1u64 << SEQUENCE_BITS; // This is 16384
        let mut generator = HubEventIdGenerator::new(Some(100), Some(overflow_seq));

        let regular_event = create_test_event(HubEventType::MergeMessage);
        let result = generator.generate_id(&regular_event);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("sequence cannot fit in event id"));
        assert!(error.message.contains(&SEQUENCE_BITS.to_string()));
    }

    #[test]
    fn test_store_event_handler_concurrent_access() {
        let handler = StoreEventHandler::new();

        // Test multiple concurrent transactions
        let mut txn1 = RocksDbTransactionBatch::new();
        let mut txn2 = RocksDbTransactionBatch::new();

        // Set initial height
        handler.set_current_height(100);

        // Create events for first transaction
        let mut block_confirmed = create_test_event(HubEventType::BlockConfirmed);
        let mut regular_event = create_test_event(HubEventType::MergeMessage);

        // Commit first transaction
        let block_confirmed_id = handler
            .commit_transaction(&mut txn1, &mut block_confirmed)
            .unwrap();
        let regular_event_id = handler
            .commit_transaction(&mut txn1, &mut regular_event)
            .unwrap();

        let (height1, seq1) = HubEventIdGenerator::extract_height_and_seq(block_confirmed_id);
        let (height2, seq2) = HubEventIdGenerator::extract_height_and_seq(regular_event_id);

        assert_eq!(height1, 100);
        assert_eq!(seq1, 0);
        assert_eq!(height2, 100);
        assert_eq!(seq2, 1);

        // Create events for second transaction
        let mut block_confirmed2 = create_test_event(HubEventType::BlockConfirmed);
        let mut regular_event2 = create_test_event(HubEventType::PruneMessage);

        // Commit second transaction
        let block_confirmed_id2 = handler
            .commit_transaction(&mut txn2, &mut block_confirmed2)
            .unwrap();
        let regular_event_id2 = handler
            .commit_transaction(&mut txn2, &mut regular_event2)
            .unwrap();

        let (height3, seq3) = HubEventIdGenerator::extract_height_and_seq(block_confirmed_id2);
        let (height4, seq4) = HubEventIdGenerator::extract_height_and_seq(regular_event_id2);

        assert_eq!(height3, 100);
        assert_eq!(seq3, 0); // BLOCK_CONFIRMED always gets 0
        assert_eq!(height4, 100);
        assert_eq!(seq4, 2); // Should continue from previous sequence
    }

    #[test]
    fn test_event_id_generator_initialization() {
        // Test with None values
        let generator = HubEventIdGenerator::new(None, None);
        assert_eq!(generator.current_height, 0);
        assert_eq!(generator.current_seq, 0);

        // Test with Some values
        let generator = HubEventIdGenerator::new(Some(123), Some(456));
        assert_eq!(generator.current_height, 123);
        assert_eq!(generator.current_seq, 456);

        // Test mixed None/Some values
        let generator = HubEventIdGenerator::new(Some(789), None);
        assert_eq!(generator.current_height, 789);
        assert_eq!(generator.current_seq, 0);

        let generator = HubEventIdGenerator::new(None, Some(999));
        assert_eq!(generator.current_height, 0);
        assert_eq!(generator.current_seq, 999);
    }

    #[test]
    fn test_event_id_make_and_extract_edge_cases() {
        // Test with zero values
        let event_id = HubEventIdGenerator::make_event_id(0, 0);
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, 0);
        assert_eq!(sequence, 0);

        // Test with maximum values
        let max_height = (1u64 << HEIGHT_BITS) - 1;
        let max_sequence = (1u64 << SEQUENCE_BITS) - 1;
        let event_id = HubEventIdGenerator::make_event_id(max_height, max_sequence);
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, max_height);
        assert_eq!(sequence, max_sequence);

        // Test sequence masking
        let oversized_sequence = (1u64 << SEQUENCE_BITS) + 100;
        let event_id = HubEventIdGenerator::make_event_id(100, oversized_sequence);
        let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(event_id);
        assert_eq!(height, 100);
        assert_eq!(sequence, 100); // Should be masked
    }

    #[test]
    fn test_event_id_generator_state_persistence() {
        let mut generator = HubEventIdGenerator::new(Some(100), Some(0));

        // Generate several events
        let events = vec![
            HubEventType::BlockConfirmed,
            HubEventType::MergeMessage,
            HubEventType::PruneMessage,
            HubEventType::RevokeMessage,
        ];

        let mut event_ids = Vec::new();
        for event_type in events {
            let event = create_test_event(event_type);
            let event_id = generator.generate_id(&event).unwrap();
            event_ids.push(event_id);
        }

        // Verify state progression
        assert_eq!(generator.current_height, 100);
        assert_eq!(generator.current_seq, 4); // Should be 4 after 4 events (0, 1, 2, 3) - increments after each use

        // Verify event ID sequences
        let expected_sequences = vec![0, 1, 2, 3];
        for (i, event_id) in event_ids.iter().enumerate() {
            let (height, sequence) = HubEventIdGenerator::extract_height_and_seq(*event_id);
            assert_eq!(height, 100);
            assert_eq!(sequence, expected_sequences[i]);
        }
    }
}
