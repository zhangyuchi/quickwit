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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, AsyncActor, Mailbox, Universe,
};
use quickwit_config::IndexConfig;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;

use crate::models::IndexingDirectory;
use crate::{IndexingPipelineParams, IndexingPipelineSupervisor};

pub struct IndexingServer {
    data_dir_path: PathBuf,
    indexing_pipeline_handles: HashMap<String, ActorHandle<IndexingPipelineSupervisor>>,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
}

impl IndexingServer {
    pub fn new(
        data_dir_path: PathBuf,
        metastore: Arc<dyn Metastore>,
        storage_uri_resolver: StorageUriResolver,
    ) -> Self {
        Self {
            data_dir_path,
            indexing_pipeline_handles: Default::default(),
            metastore,
            storage_uri_resolver,
        }
    }

    pub fn spawn(self) -> (Mailbox<IndexingServerMessage>, ActorHandle<IndexingServer>) {
        let universe = Universe::new();
        universe.spawn_actor(self).spawn_async()
    }

    fn spawn_indexing_pipeline(
        &mut self,
        ctx: &ActorContext<IndexingServerMessage>,
        index_id: &str,
    ) -> anyhow::Result<()> {
        // let index_config = self.metastore.index_config(index_id).await?;
        let index_config: IndexConfig;
        index_config.validate();

        let indexing_directory_path = self.data_dir_path.join(index_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path).await?;

        let indexing_pipeline_params = IndexingPipelineParams {
            index_id: index_id.to_string(),
            indexing_directory,
            indexing_settings: index_config.indexing_settings,
            source_config: index_config.source_configs.pop().unwrap(),
            metastore: self.metastore.clone(),
            storage_uri_resolver: self.storage_uri_resolver.clone(),
        };
        let indexing_pipeline_supervisor =
            IndexingPipelineSupervisor::new(indexing_pipeline_params);
        let (indexing_pipeline_supervisor_mailbox, indexing_pipeline_supervisor_handle) =
            ctx.spawn_actor(indexing_pipeline_supervisor).spawn_async();
        self.indexing_pipeline_handles
            .insert(index_id.to_string(), indexing_pipeline_supervisor_handle);
        Ok(())
    }
}

#[derive(Debug)]
pub enum IndexingServerMessage {
    StartIndexingPipeline(String),
}

impl Actor for IndexingServer {
    type Message = IndexingServerMessage;
    type ObservableState = serde_json::Value;

    fn observable_state(&self) -> Self::ObservableState {
        serde_json::json!({})
    }
}

#[async_trait]
impl AsyncActor for IndexingServer {
    async fn process_message(
        &mut self,
        message: IndexingServerMessage,
        ctx: &ActorContext<IndexingServerMessage>,
    ) -> Result<(), ActorExitStatus> {
        match message {
            IndexingServerMessage::StartIndexingPipeline(index_id) => {
                self.spawn_indexing_pipeline(ctx, &index_id)?
            }
        }
        Ok(())
    }
}
