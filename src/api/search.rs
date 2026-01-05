//! Full-text search indexer using Tantivy.
//!
//! This module provides full-text search capabilities for:
//! - Casts (by text content)
//! - Users (by username, display name, bio)
//!
//! # Architecture
//!
//! The search index is stored on disk using Tantivy. The indexer processes
//! messages asynchronously and updates the index in batches for efficiency.
//!
//! # Usage
//!
//! ```ignore
//! let search = SearchIndexer::new(config, index_path)?;
//! search.process_event(&event).await?;
//! let results = search.search_casts("hello world", 10)?;
//! ```

use crate::api::config::SearchConfig;
use crate::api::events::IndexEvent;
use crate::api::indexer::{Indexer, IndexerError, IndexerStats};
use crate::proto::message_data::Body;
use crate::proto::{Message, MessageType};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, STORED, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

/// A search result for a cast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastSearchResult {
    pub fid: u64,
    pub hash: String,
    pub text: String,
    pub timestamp: u32,
    pub score: f32,
}

/// A search result for a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSearchResult {
    pub fid: u64,
    pub username: Option<String>,
    pub display_name: Option<String>,
    pub score: f32,
}

/// Schema fields for the cast index.
struct CastFields {
    fid: Field,
    hash: Field,
    text: Field,
    timestamp: Field,
}

/// Full-text search indexer using Tantivy.
pub struct SearchIndexer {
    config: SearchConfig,
    index: Index,
    writer: RwLock<IndexWriter>,
    reader: IndexReader,
    fields: CastFields,
    checkpoint: AtomicU64,
    docs_indexed: AtomicU64,
}

impl SearchIndexer {
    /// Create a new search indexer.
    ///
    /// # Arguments
    /// * `config` - Search configuration
    /// * `index_path` - Path to store the Tantivy index
    pub fn new<P: AsRef<Path>>(config: SearchConfig, index_path: P) -> Result<Self, IndexerError> {
        // Create schema
        let mut schema_builder = Schema::builder();
        let fid = schema_builder.add_u64_field("fid", STORED);
        let hash = schema_builder.add_text_field("hash", STORED);
        let text = schema_builder.add_text_field("text", TEXT | STORED);
        let timestamp = schema_builder.add_u64_field("timestamp", STORED);
        let schema = schema_builder.build();

        // Create or open index
        let index_path = index_path.as_ref();
        std::fs::create_dir_all(index_path).map_err(|e| {
            IndexerError::Storage(format!("Failed to create index directory: {}", e))
        })?;

        let index = Index::create_in_dir(index_path, schema.clone())
            .or_else(|_| Index::open_in_dir(index_path))
            .map_err(|e| IndexerError::Storage(format!("Failed to open index: {}", e)))?;

        // Create writer with memory budget
        let memory_bytes = config.memory_budget_mb * 1024 * 1024;
        let writer = index
            .writer(memory_bytes)
            .map_err(|e| IndexerError::Storage(format!("Failed to create writer: {}", e)))?;

        // Create reader
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| IndexerError::Storage(format!("Failed to create reader: {}", e)))?;

        let fields = CastFields {
            fid,
            hash,
            text,
            timestamp,
        };

        Ok(Self {
            config,
            index,
            writer: RwLock::new(writer),
            reader,
            fields,
            checkpoint: AtomicU64::new(0),
            docs_indexed: AtomicU64::new(0),
        })
    }

    /// Search for casts by text.
    pub fn search_casts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<CastSearchResult>, IndexerError> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.fields.text]);

        let query = query_parser
            .parse_query(query)
            .map_err(|e| IndexerError::Storage(format!("Failed to parse query: {}", e)))?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .map_err(|e| IndexerError::Storage(format!("Search failed: {}", e)))?;

        let mut results = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| IndexerError::Storage(format!("Failed to retrieve doc: {}", e)))?;

            let fid = doc
                .get_first(self.fields.fid)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let hash = doc
                .get_first(self.fields.hash)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = doc
                .get_first(self.fields.text)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let timestamp = doc
                .get_first(self.fields.timestamp)
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            results.push(CastSearchResult {
                fid,
                hash,
                text,
                timestamp,
                score,
            });
        }

        Ok(results)
    }

    /// Index a cast message.
    fn index_cast(&self, message: &Message) -> Result<bool, IndexerError> {
        let data = match &message.data {
            Some(d) => d,
            None => return Ok(false),
        };

        let msg_type = MessageType::try_from(data.r#type).unwrap_or(MessageType::None);
        if msg_type != MessageType::CastAdd {
            return Ok(false);
        }

        let cast_body = match &data.body {
            Some(Body::CastAddBody(body)) => body,
            _ => return Ok(false),
        };

        // Skip empty text
        if cast_body.text.trim().is_empty() {
            return Ok(false);
        }

        let hash_hex = hex::encode(&message.hash);

        let writer = self
            .writer
            .write()
            .map_err(|_| IndexerError::Storage("Failed to acquire write lock".to_string()))?;

        writer
            .add_document(doc!(
                self.fields.fid => data.fid,
                self.fields.hash => hash_hex,
                self.fields.text => cast_body.text.clone(),
                self.fields.timestamp => data.timestamp as u64,
            ))
            .map_err(|e| IndexerError::Storage(format!("Failed to add document: {}", e)))?;

        self.docs_indexed.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Commit pending changes to the index.
    fn commit(&self) -> Result<(), IndexerError> {
        let mut writer = self
            .writer
            .write()
            .map_err(|_| IndexerError::Storage("Failed to acquire write lock".to_string()))?;

        writer
            .commit()
            .map_err(|e| IndexerError::Storage(format!("Failed to commit: {}", e)))?;

        Ok(())
    }

    /// Get the number of documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

#[async_trait]
impl Indexer for SearchIndexer {
    fn name(&self) -> &'static str {
        "search"
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn process_event(&self, event: &IndexEvent) -> Result<(), IndexerError> {
        match event {
            IndexEvent::MessageCommitted { message, .. } => {
                self.index_cast(message)?;
                self.commit()?;
            }
            IndexEvent::MessagesCommitted { messages, .. } => {
                let mut indexed = 0;
                for message in messages {
                    if self.index_cast(message)? {
                        indexed += 1;
                    }
                }
                if indexed > 0 {
                    self.commit()?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_batch(&self, events: &[IndexEvent]) -> Result<(), IndexerError> {
        let mut indexed = 0;

        for event in events {
            match event {
                IndexEvent::MessageCommitted { message, .. } => {
                    if self.index_cast(message)? {
                        indexed += 1;
                    }
                }
                IndexEvent::MessagesCommitted { messages, .. } => {
                    for message in messages {
                        if self.index_cast(message)? {
                            indexed += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        if indexed > 0 {
            self.commit()?;
        }

        Ok(())
    }

    fn last_checkpoint(&self) -> u64 {
        self.checkpoint.load(Ordering::SeqCst)
    }

    async fn save_checkpoint(&self, event_id: u64) -> Result<(), IndexerError> {
        self.checkpoint.store(event_id, Ordering::SeqCst);
        Ok(())
    }

    fn stats(&self) -> IndexerStats {
        IndexerStats {
            items_indexed: self.docs_indexed.load(Ordering::Relaxed),
            last_event_id: self.checkpoint.load(Ordering::SeqCst),
            last_block_height: 0,
            backfill_complete: true,
            size_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{CastAddBody, MessageData};

    fn create_cast_message(fid: u64, text: &str, hash: Vec<u8>) -> Message {
        Message {
            hash,
            data: Some(MessageData {
                fid,
                r#type: MessageType::CastAdd as i32,
                timestamp: 1000,
                body: Some(Body::CastAddBody(CastAddBody {
                    text: text.to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn create_test_indexer() -> (SearchIndexer, tempfile::TempDir) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let config = SearchConfig {
            enabled: true,
            backfill_on_startup: false,
            backfill_batch_size: 100,
            memory_budget_mb: 50,
            ..Default::default()
        };
        let indexer = SearchIndexer::new(config, tmp_dir.path()).unwrap();
        (indexer, tmp_dir)
    }

    #[test]
    fn test_indexer_creation() {
        let (indexer, _tmp_dir) = create_test_indexer();
        assert!(indexer.is_enabled());
        assert_eq!(indexer.name(), "search");
    }

    #[tokio::test]
    async fn test_index_cast() {
        let (indexer, _tmp_dir) = create_test_indexer();

        let msg = create_cast_message(123, "Hello world from Farcaster!", vec![1, 2, 3, 4]);
        let event = IndexEvent::message(msg, 1, 1);

        indexer.process_event(&event).await.unwrap();

        // Need to reload the reader to see the new doc
        indexer.reader.reload().unwrap();

        let results = indexer.search_casts("hello", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fid, 123);
        assert!(results[0].text.contains("Hello"));
    }

    #[tokio::test]
    async fn test_search_multiple_casts() {
        let (indexer, _tmp_dir) = create_test_indexer();

        // Index multiple casts
        let messages = vec![
            create_cast_message(100, "The quick brown fox", vec![1, 0, 0, 0]),
            create_cast_message(200, "Hello world", vec![2, 0, 0, 0]),
            create_cast_message(300, "Farcaster is amazing", vec![3, 0, 0, 0]),
            create_cast_message(400, "Quick update on the project", vec![4, 0, 0, 0]),
        ];

        let event = IndexEvent::messages(messages, 1, 1);
        indexer.process_event(&event).await.unwrap();
        indexer.reader.reload().unwrap();

        // Search for "quick" - should find 2 results
        let results = indexer.search_casts("quick", 10).unwrap();
        assert_eq!(results.len(), 2);

        // Search for "farcaster" - should find 1 result
        let results = indexer.search_casts("farcaster", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fid, 300);
    }

    #[tokio::test]
    async fn test_empty_text_not_indexed() {
        let (indexer, _tmp_dir) = create_test_indexer();

        let msg = create_cast_message(123, "   ", vec![1, 2, 3, 4]);
        let event = IndexEvent::message(msg, 1, 1);

        indexer.process_event(&event).await.unwrap();
        indexer.reader.reload().unwrap();

        assert_eq!(indexer.num_docs(), 0);
    }

    #[test]
    fn test_search_no_results() {
        let (indexer, _tmp_dir) = create_test_indexer();

        let results = indexer.search_casts("nonexistent", 10).unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let (indexer, _tmp_dir) = create_test_indexer();

        let events: Vec<IndexEvent> = (0..5)
            .map(|i| {
                let msg = create_cast_message(
                    100 + i,
                    &format!("Test message number {}", i),
                    vec![i as u8, 0, 0, 0],
                );
                IndexEvent::message(msg, 1, i)
            })
            .collect();

        indexer.process_batch(&events).await.unwrap();
        indexer.reader.reload().unwrap();

        let results = indexer.search_casts("message", 10).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let (indexer, _tmp_dir) = create_test_indexer();

        assert_eq!(indexer.last_checkpoint(), 0);
        indexer.save_checkpoint(100).await.unwrap();
        assert_eq!(indexer.last_checkpoint(), 100);
    }
}
