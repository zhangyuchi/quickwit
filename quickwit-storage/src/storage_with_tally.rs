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

use std::ops::{Deref, Range};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use stable_deref_trait::StableDeref;
use tantivy::directory::OwnedBytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{PutPayload, Storage, StorageErrorKind, StorageResult};

#[derive(Clone)]
pub struct MemoryTally {
    permits: Arc<Semaphore>,
    capacity_in_kb: u64,
}

impl MemoryTally {
    pub fn with_capacity(capacity_in_bytes: u64) -> MemoryTally {
        let capacity_in_kb = capacity_in_bytes / 1_000;
        MemoryTally {
            permits: Arc::new(Semaphore::new(capacity_in_kb as usize)),
            capacity_in_kb,
        }
    }

    pub fn capacity_in_bytes(&self) -> u64 {
        self.capacity_in_kb * 1_000u64
    }

    pub fn memory_usage(&self) -> u64 {
        (self.capacity_in_kb - self.permits.available_permits() as u64) * 1000
    }
}

pub struct StorageWithTally {
    storage: Arc<dyn Storage>,
    memory_tally: MemoryTally,
}

impl StorageWithTally {
    pub fn wrap_with_tally(storage: Arc<dyn Storage>, memory_tally: MemoryTally) -> Self {
        StorageWithTally {
            storage,
            memory_tally,
        }
    }

    pub fn capacity_in_bytes(&self) -> u64 {
        self.memory_tally.capacity_in_bytes()
    }

    pub fn memory_usage(&self) -> u64 {
        self.memory_tally.memory_usage()
    }
}

struct OwnedBytesWithMemoryGuard {
    payload: OwnedBytes,
    _mem_guard: OwnedSemaphorePermit,
}

unsafe impl StableDeref for OwnedBytesWithMemoryGuard {}

impl Deref for OwnedBytesWithMemoryGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.payload.deref()
    }
}

#[async_trait]
impl Storage for StorageWithTally {
    async fn put(&self, path: &Path, payload: PutPayload) -> StorageResult<()> {
        self.storage.put(path, payload).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        self.storage.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let mem_guard =
            Semaphore::acquire_many_owned(self.memory_tally.permits.clone(), range.len() as u32)
                .await
                .map_err(|err| StorageErrorKind::InternalError.with_error(err))?;
        let payload = self.storage.get_slice(path, range).await?;
        Ok(OwnedBytes::new(OwnedBytesWithMemoryGuard {
            payload,
            _mem_guard: mem_guard,
        }))
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let num_bytes = self.file_num_bytes(path).await? as usize;
        self.get_slice(path, 0..num_bytes).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }
}
