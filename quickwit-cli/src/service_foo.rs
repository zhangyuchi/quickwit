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

use std::ffi::OsString;
use std::fs::canonicalize;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::ArgMatches;
use itertools::any;
use opentelemetry::trace::Span;
use quickwit_config::{IndexConfig, IndexingSettings, ServerConfig};
use quickwit_indexing::actors::{IndexingPipelineParams, IndexingServer};
use quickwit_indexing::index_data;
use quickwit_metastore::{MetastoreFactory, MetastoreUriResolver};
use quickwit_storage::{quickwit_storage_uri_resolver, OwnedBytes};
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub struct StartIndexerArgs {
    pub server_config_path: String,
    pub index_id: String,
}

fn parse_start_indexer_args(matches: &ArgMatches) -> anyhow::Result<StartIndexerArgs> {
    let server_config_path = matches
        .value_of("server-config-path")
        .expect("`server-config-path` should be a required arg.")
        .to_string();
    let index_id = matches
        .value_of("index-id")
        .expect("`index-id` should be a required arg.")
        .to_string();
    Ok(StartIndexerArgs {
        server_config_path,
        index_id,
    })
}

async fn start_indexer(args: &StartIndexerArgs) -> anyhow::Result<()> {
    let server_config = ServerConfig::from_file(&args.server_config_path).await?;
    let metastore = MetastoreUriResolver::default()
        .resolve(&server_config.metastore_uri)
        .await?;
    let indexer_config = server_config.indexer_config.ok_or(anyhow::anyhow!(""))?;
    indexer_config.validate()?;
    let indexing_server = IndexingServer::new(
        indexer_config.data_dir_path,
        metastore,
        quickwit_storage_uri_resolver(),
    );
    let (indexing_server_mailbox, indexing_server_handle) = indexing_server.spawn();
    // server_config.indexer.split_store_max_num_bytes;
    // server_config.indexer.split_store_max_num_files;
    // let indexing_server_grpc = IndexingService::new(indexing_server_mailbox);
    // let index_config = metastore.index_config(index_id).await?;
    // let mut index_config: IndexConfig = unimplemented!();
    // index_config.validate()?;
    // server_config.indexer.data_dir_path;
    // // storage
    // index_data(metastore, args.index_id, indexing_settings, source_configs).await?;
    Ok(())
}
