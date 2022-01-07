// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::mem;
use std::path::PathBuf;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::IngestSourceParams;
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position, SourceCheckpoint};
use quickwit_wal::Reader;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::models::RawDocBatch;
use crate::source::{IndexerMessage, Source, SourceContext, TypedSourceFactory};

/// Factory for instantiating a [`WalSource`].
pub struct WalSourceFactory;

#[async_trait]
impl TypedSourceFactory for WalSourceFactory {
    type Source = WalSource;
    type Params = IngestSourceParams;

    async fn typed_create_source(
        params: IngestSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        WalSource::try_new(params, checkpoint).await
    }
}

/// A [`Source`] implementation that reads records from a Quickwit WAL and relies on the sequence
/// number exposed by the WAL records for checkpointing.
pub struct WalSource {
    /// Partition ID used for checkpointing.
    partition_id: PartitionId,
    /// Position of the last successfully checkpointed record.
    previous_position: Position,
    /// Current position of the source in the WAL.
    current_offset: u64,
    /// WAL reader.
    reader: Reader,
    /// Number of bytes processed by the source.
    num_bytes_processed: usize,
    /// Number of records processed by the source, including invalid records.
    num_records_processed: usize,
    /// Number of invalid records, i.e., that were empty or contained invalid UTF-8 characters.
    num_invalid_records: usize,
}

impl WalSource {
    pub async fn try_new(
        params: IngestSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let partition_id =
            PartitionId::from(params.log_dir_path.canonicalize()?.to_str().context("")?);
        let previous_position = checkpoint
            .position_for_partition(&partition_id)
            .unwrap_or(&Position::Beginning)
            .clone();
        let current_offset = match &previous_position {
            Position::Beginning => 0,
            Position::Offset(offset) => offset.parse::<u64>()?,
        };
        let mut reader = Reader::open(&params.log_dir_path).await?;
        reader.seek(current_offset).await?;

        Ok(Self {
            partition_id,
            previous_position,
            current_offset,
            reader,
            num_bytes_processed: 0,
            num_records_processed: 0,
            num_invalid_records: 0,
        })
    }
}

#[async_trait]
impl Source for WalSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<IndexerMessage>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let mut batch_num_bytes = 0;
        let mut docs = Vec::new();

        let deadline = tokio::time::sleep(quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            assert!(self.current_offset == self.reader.current_offset());

            tokio::select! {
                entry_res = self.reader.next_entry() => {
                    match entry_res? {
                        None => break,
                        Some(entry) => {
                            match parse_record(entry.payload) {
                                Some(doc) => {
                                    docs.push(doc);
                                    batch_num_bytes += entry.payload.len();
                                },
                                _ =>
                                    self.num_invalid_records += 1,
                            };
                            self.current_offset = entry.next_offset();
                            self.num_bytes_processed += entry.payload.len();
                            self.num_records_processed += 1;

                            if batch_num_bytes >= 5_000_000 {
                                break;
                            }
                        },
                    };
                },
                _ = &mut deadline => break,
            }
        }

        if docs.len() > 0 {
            let mut checkpoint_delta = CheckpointDelta::default();
            let current_position = Position::from(self.current_offset);
            let previous_position =
                mem::replace(&mut self.previous_position, current_position.clone());
            checkpoint_delta
                .record_partition_delta(
                    self.partition_id.clone(),
                    previous_position,
                    current_position,
                )
                .context("Failed to record partition delta.")?;
            let batch = RawDocBatch {
                docs,
                checkpoint_delta,
            };
            ctx.send_message(batch_sink, IndexerMessage::from(batch))
                .await?;
        }
        Err(ActorExitStatus::Success)
        // Ok(())
    }

    fn name(&self) -> String {
        "WalSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "current_offset":  self.current_offset,
            "num_bytes_processed": self.num_bytes_processed,
            "num_records_processed": self.num_records_processed,
            "num_invalid_records": self.num_invalid_records,
        })
    }
}

fn parse_record(record: &[u8]) -> Option<String> {
    match String::from_utf8(record.to_vec()) {
        Ok(doc) if doc.len() > 0 => {
            debug!(
                // topic = ?message.topic(),
                // partition_id = ?message.partition(),
                // offset = ?message.offset(),
                // timestamp = ?message.timestamp(),
                // num_bytes = ?message.payload_len(),
                "Record received.",
            );
            return Some(doc);
        }
        Ok(_) => debug!(
            // topic = ?message.topic(),
            // partition = ?message.partition(),
            // offset = ?message.offset(),
            // timestamp = ?message.timestamp(),
            "Record is empty."
        ),
        Err(error) => warn!(
            // topic = ?message.topic(),
            // partition = ?message.partition(),
            // offset = ?message.offset(),
            // timestamp = ?message.timestamp(),
            error = ?error,
            "Record contains invalid UTF-8 characters."
        ),
    };
    None
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_wal::Writer;
    use serde_json::json;

    use super::*;
    use crate::source::{quickwit_supported_sources, SourceActor};

    #[tokio::test]
    async fn test_ingest_source() {
        let universe = Universe::new();

        let temp_dir = tempfile::tempdir().unwrap();
        let partition_id =
            PartitionId::from(temp_dir.path().canonicalize().unwrap().to_str().unwrap());
        let mut writer = Writer::open(temp_dir.path()).await.unwrap();

        let source_config = SourceConfig {
            source_id: "test-wal-source".to_string(),
            source_params: SourceParams::ingest(temp_dir.path()),
        };
        let source_loader = quickwit_supported_sources();
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = SourceCheckpoint::default();
            let source = source_loader
                .load_source(source_config.clone(), checkpoint)
                .await
                .unwrap();
            let actor = SourceActor {
                source,
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn_async();
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = inbox.drain_available_message_for_test();
            assert!(messages.is_empty());

            let expected_state = json!({
                "current_offset":  0,
                "num_bytes_processed": 0,
                "num_records_processed": 0,
                "num_invalid_records": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        writer.append(b"").await.unwrap();
        writer.append(b"Record #0").await.unwrap();
        writer.append(b"").await.unwrap();
        writer.append(b"Record #1").await.unwrap();
        writer.flush().await.unwrap();
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = SourceCheckpoint::default();
            let source = source_loader
                .load_source(source_config.clone(), checkpoint)
                .await
                .unwrap();
            let actor = SourceActor {
                source,
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn_async();
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = inbox.drain_available_message_for_test();
            assert_eq!(messages.len(), 1);

            if let IndexerMessage::Batch(batch) = &messages[0] {
                let expected_docs = vec!["Record #0", "Record #1"];
                assert_eq!(batch.docs, expected_docs);

                let mut expected_checkpoint_delta = CheckpointDelta::default();
                expected_checkpoint_delta
                    .record_partition_delta(
                        PartitionId::from(partition_id.clone()),
                        Position::Beginning,
                        Position::from(58u64),
                    )
                    .unwrap();
                assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);
            } else {
                panic!("");
            }
            let expected_state = json!({
                "current_offset":  58,
                "num_bytes_processed": 18,
                "num_records_processed": 4,
                "num_invalid_records": 2,
            });
            assert_eq!(exit_state, expected_state);
        }
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint: SourceCheckpoint = vec![(partition_id.clone(), 29u64)]
                .into_iter()
                .map(|(partition_id, offset)| {
                    (PartitionId::from(partition_id), Position::from(offset))
                })
                .collect();
            let source = source_loader
                .load_source(source_config.clone(), checkpoint)
                .await
                .unwrap();
            let actor = SourceActor {
                source,
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_actor(actor).spawn_async();
            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = inbox.drain_available_message_for_test();
            assert_eq!(messages.len(), 1);

            if let IndexerMessage::Batch(batch) = &messages[0] {
                let expected_docs = vec!["Record #1"];
                assert_eq!(batch.docs, expected_docs);

                let mut expected_checkpoint_delta = CheckpointDelta::default();
                expected_checkpoint_delta
                    .record_partition_delta(
                        PartitionId::from(partition_id),
                        Position::from(29u64),
                        Position::from(58u64),
                    )
                    .unwrap();
                assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);
            } else {
                panic!("");
            }
            let expected_state = json!({
                "current_offset":  58,
                "num_bytes_processed": 9,
                "num_records_processed": 2,
                "num_invalid_records": 1,
            });
            assert_eq!(exit_state, expected_state);
        }
    }
}
