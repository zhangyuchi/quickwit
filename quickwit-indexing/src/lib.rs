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

use std::sync::Arc;

use anyhow::bail;
use quickwit_actors::Universe;
use quickwit_config::{IndexerConfig, IndexingSettings, SourceConfig};
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;

use crate::actors::{IndexingPipelineParams, IndexingPipelineSupervisor};
use crate::models::{IndexingDirectory, IndexingStatistics};
pub use crate::split_store::{
    get_tantivy_directory_from_split_bundle, IndexingSplitStore, IndexingSplitStoreParams,
    SplitFolder,
};

pub mod actors;
mod controlled_directory;
mod garbage_collection;
pub mod merge_policy;
pub mod models;
pub mod source;
mod split_store;
#[cfg(test)]
mod test_utils;

#[cfg(test)]
pub use test_utils::{mock_split_meta, TestSandbox};

pub use self::garbage_collection::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionStats,
};
pub use self::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};

pub async fn index_data(
    index_id: &str,
    indexer_config: IndexerConfig,
    indexing_settings: IndexingSettings,
    source_config: SourceConfig,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<IndexingStatistics> {
    let indexing_directory_path = indexer_config.data_dir_path.join(index_id);
    let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;
    let indexing_pipeline_params = IndexingPipelineParams {
        index_id: index_id.to_string(),
        indexing_directory,
        indexing_settings,
        source_config,
        metastore,
        storage_uri_resolver,
    };
    let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);
    let universe = Universe::new();
    let (_pipeline_mailbox, pipeline_handler) =
        universe.spawn_actor(indexing_supervisor).spawn_async();
    let (pipeline_termination, statistics) = pipeline_handler.join().await;
    if !pipeline_termination.is_success() {
        bail!(pipeline_termination);
    }
    Ok(statistics)
}

pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}
