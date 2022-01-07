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

use std::path::{Path, PathBuf};

use anyhow::bail;

use crate::segment_reader::{Entry, SegmentReader};
use crate::{list_segments, SEGMENT_MAX_NUM_BYTES};

pub struct Reader {
    log_dir: PathBuf,
    reader: SegmentReader,
}

impl Reader {
    pub async fn open<P: AsRef<Path>>(log_dir: P) -> anyhow::Result<Self> {
        let segments = list_segments(&log_dir).await?;
        let reader = if let Some(segment) = segments.first() {
            SegmentReader::open(&log_dir, segment.base_offset).await?
        } else {
            bail!(
                "Failed to open write-ahead log `{}`: no segment files were found.",
                log_dir.as_ref().display()
            )
        };
        Ok(Reader {
            log_dir: log_dir.as_ref().to_path_buf(),
            reader,
        })
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<&[u8]>> {
        if self.reader.num_bytes() >= SEGMENT_MAX_NUM_BYTES {
            self.reader = SegmentReader::open(&self.log_dir, self.reader.current_offset()).await?;
        }
        let payload = self.reader.next().await?;
        Ok(payload)
    }

    pub async fn next_entry<'a>(&'a mut self) -> anyhow::Result<Option<Entry<'a>>> {
        if self.reader.num_bytes() >= SEGMENT_MAX_NUM_BYTES {
            self.reader = SegmentReader::open(&self.log_dir, self.reader.current_offset()).await?;
        }
        let entry = self.reader.next_entry().await?;
        Ok(entry)
    }

    pub async fn seek(&mut self, offset: u64) -> anyhow::Result<()> {
        if self.current_offset() == offset {
            return Ok(());
        }
        let segments = list_segments(&self.log_dir).await?;

        if segments.is_empty() {
            bail!(
                "Failed to seek to offset `{}` in write-ahead log `{}`: no segment files were \
                 found.",
                offset,
                self.log_dir.display()
            )
        }
        let min_offset = segments[0].base_offset;
        let max_offset = segments[segments.len() - 1].top_offset;

        if offset < min_offset || offset > max_offset {
            bail!(
                "SeekError: offset `{}` is out of write-ahead log range `[{}, {})`.",
                offset,
                min_offset,
                max_offset
            );
        }
        let partition_point = segments
            .partition_point(|segment| segment.top_offset <= offset)
            .min(segments.len() - 1);
        let base_offset = segments[partition_point].base_offset;
        let mut reader = SegmentReader::open(&self.log_dir, base_offset).await?;
        reader.seek(offset).await?;
        self.reader = reader;
        Ok(())
    }

    pub fn current_offset(&self) -> u64 {
        self.reader.current_offset()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.log_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{truncate_wal, Writer};

    #[tokio::test]
    async fn test_reader() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut writer = Writer::open(temp_dir.path()).await.unwrap();
        writer.append(b"A log entry at offset 00.").await.unwrap();
        writer.append(b"A log entry at offset 35.").await.unwrap();
        writer.append(b"A log entry at offset 70.").await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = Reader::open(temp_dir.path()).await.unwrap();
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 00.")
        ));
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 35.")
        ));
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 70.")
        ));
        assert!(reader.next().await.unwrap().is_none());

        reader.seek(70).await.unwrap();
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 70.")
        ));

        reader.seek(35).await.unwrap();
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 35.")
        ));

        reader.seek(0).await.unwrap();
        assert!(matches!(
            reader.next().await.unwrap(),
            Some(b"A log entry at offset 00.")
        ));

        reader.seek(105).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());

        truncate_wal(temp_dir.path(), 70).await.unwrap();
        reader.seek(0).await.unwrap_err();
    }
}
