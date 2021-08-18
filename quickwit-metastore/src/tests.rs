/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::collections::HashSet;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use async_trait::async_trait;
use chrono::Utc;
use tokio::time::{sleep, Duration};

#[macro_use]
use super::tests;

use quickwit_index_config::AllFlattenIndexConfig;

use crate::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};

#[async_trait]
pub trait DefaultForTest {
    async fn default_for_test() -> Self;
}

pub async fn test_metastore_create_index<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;
    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
    };

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Create an index that already exists
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexAlreadyExists { .. }));

    // Delete an index
    let result = metastore.delete_index(index_id).await.unwrap();
    assert!(matches!(result, ()));
}

pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;
    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
    };

    // Delete a non-existent index
    let result = metastore
        .delete_index("non-existent-index")
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Delete an index
    let result = metastore.delete_index(index_id).await.unwrap();
    assert!(matches!(result, ()));
}

macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod tests {
            #[tokio::test]
            async fn test_metastore_create_index() {
                crate::tests::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_metastore_delete_index::<$metastore_type>().await;
            }
        }

    }
}
