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

use crate::segment_writer::SegmentWriter;
use crate::{list_segments, SEGMENT_MAX_NUM_BYTES};

pub struct Writer {
    log_dir: PathBuf,
    writer: SegmentWriter,
}

impl Writer {
    /// Opens the WAL in append mode.
    pub async fn open<P: AsRef<Path>>(log_dir: P) -> anyhow::Result<Self> {
        let segments = list_segments(&log_dir).await?;
        let writer = if let Some(segment) = segments.last() {
            SegmentWriter::open(&log_dir, segment.base_offset).await?
        } else {
            SegmentWriter::create(&log_dir, 0).await?
        };
        Ok(Writer {
            log_dir: log_dir.as_ref().to_path_buf(),
            writer,
        })
    }

    pub async fn append<B: AsRef<[u8]>>(&mut self, payload: B) -> anyhow::Result<u64> {
        if self.writer.num_bytes() >= SEGMENT_MAX_NUM_BYTES {
            self.writer.flush().await?;
            self.writer =
                SegmentWriter::create(&self.log_dir, self.writer.current_offset()).await?;
        }
        let num_bytes_written = self.writer.append(payload).await?;
        Ok(num_bytes_written)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn sync(&mut self) -> anyhow::Result<()> {
        self.writer.sync().await?;
        Ok(())
    }

    pub fn current_offset(&self) -> u64 {
        self.writer.current_offset()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.log_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{segment_file_name, Segment, HEADER_NUM_BYTES};

    #[tokio::test]
    async fn test_writer() {
        let temp_dir = tempfile::tempdir().unwrap();

        let segment0_base_offset = 0;
        let segment0_top_offset = (HEADER_NUM_BYTES + 37) * 2;
        let segment0_filepath = temp_dir
            .path()
            .join(segment_file_name(segment0_base_offset));

        let segment1_base_offset = segment0_top_offset;
        let segment1_top_offset = segment1_base_offset + HEADER_NUM_BYTES + 21;
        let segment1_filepath = temp_dir
            .path()
            .join(segment_file_name(segment1_base_offset));

        let mut writer = Writer::open(temp_dir.path()).await.unwrap();
        writer
            .append(b"A 37-byte long write-ahead log entry.")
            .await
            .unwrap();
        writer
            .append(b"A 37-byte long write-ahead log entry.")
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let expected_segments = vec![Segment {
            filepath: segment0_filepath.clone(),
            base_offset: 0,
            top_offset: segment0_top_offset,
        }];
        assert_eq!(
            list_segments(temp_dir.path()).await.unwrap(),
            expected_segments
        );

        writer.append(b"A 21-byte long entry.").await.unwrap();
        writer.flush().await.unwrap();

        let expected_segments = vec![
            Segment {
                filepath: segment0_filepath,
                base_offset: 0,
                top_offset: segment0_top_offset,
            },
            Segment {
                filepath: segment1_filepath,
                base_offset: segment1_base_offset,
                top_offset: segment1_top_offset,
            },
        ];
        assert_eq!(
            list_segments(temp_dir.path()).await.unwrap(),
            expected_segments
        );
    }
}
