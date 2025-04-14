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

    fn generate_id(&mut self) -> Result<u64, HubError> {
        if self.current_height >= 2u64.pow(HEIGHT_BITS) {
            return Err(HubError {
                code: "bad_request.invalid_param".to_string(),
                message: format!(
                    "height cannot fit in event id. Height > {} bits",
                    HEIGHT_BITS
                ),
            });
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

        let event_id = Self::make_event_id(self.current_height, self.current_seq);
        self.current_seq += 1;
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
        let event_id = generator.generate_id()?;
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
