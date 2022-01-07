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

use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

use crate::{segment_file_name, HEADER_MAGIC, HEADER_NUM_BYTES};

pub(crate) struct SegmentWriter {
    filepath: PathBuf,
    writer: tokio::io::BufWriter<File>,
    base_offset: u64,
    current_pos: u64,
}

impl SegmentWriter {
    pub(crate) async fn create<P: AsRef<Path>>(
        log_dir: P,
        base_offset: u64,
    ) -> anyhow::Result<Self> {
        let filename = segment_file_name(base_offset);
        let filepath = log_dir.as_ref().join(filename);
        let file = tokio::fs::File::create(&filepath).await?;
        let writer = tokio::io::BufWriter::new(file);
        Ok(Self {
            filepath,
            writer,
            base_offset,
            current_pos: 0,
        })
    }

    pub(crate) async fn open<P: AsRef<Path>>(log_dir: P, base_offset: u64) -> anyhow::Result<Self> {
        let filename = segment_file_name(base_offset);
        let filepath = log_dir.as_ref().join(filename);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&filepath)
            .await?;
        let current_pos = file.seek(SeekFrom::End(0)).await?;
        let writer = tokio::io::BufWriter::new(file);
        Ok(SegmentWriter {
            filepath,
            writer,
            base_offset,
            current_pos,
        })
    }

    // | 1. MAGIC (2 bytes) | 2. CRC32 (4 bytes) | 3. Entry length (4 bytes) | 4. Entry bytes |
    pub(crate) async fn append<B: AsRef<[u8]>>(&mut self, entry: B) -> anyhow::Result<u64> {
        let bytes = entry.as_ref();
        // 1. Write magic.
        self.writer.write_u16_le(HEADER_MAGIC).await?;
        // 2. Compute and write CRC.
        let checksum = crc32fast::hash(bytes);
        self.writer.write_u32_le(checksum).await?;
        // 3. Write entry length.
        self.writer.write_u32_le(bytes.len() as u32).await?;
        // 4. Write entry bytes.
        self.writer.write_all(bytes).await?;
        let num_bytes_written = HEADER_NUM_BYTES + bytes.len() as u64;
        self.current_pos += num_bytes_written;
        Ok(num_bytes_written)
    }

    pub(crate) async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn sync(&mut self) -> anyhow::Result<()> {
        self.writer.flush().await?;
        self.writer.get_mut().sync_data().await?;
        Ok(())
    }

    pub(crate) fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub(crate) fn current_offset(&self) -> u64 {
        self.base_offset + self.current_pos
    }

    // Number of bytes written by the segment writer.
    pub(crate) fn num_bytes(&self) -> u64 {
        self.current_pos
    }

    pub(crate) fn path(&self) -> &Path {
        &self.filepath
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;
    use crate::HEADER_MAGIC;

    fn assert_log_eq(log: &[u8], payload: &[u8]) {
        assert_eq!(
            u16::from_le_bytes(log[..2].try_into().unwrap()),
            HEADER_MAGIC
        );
        assert_eq!(
            u32::from_le_bytes(log[2..6].try_into().unwrap()),
            crc32fast::hash(payload)
        );
        assert_eq!(
            u32::from_le_bytes(log[6..10].try_into().unwrap()),
            payload.len() as u32
        );
        assert_eq!(&log[10..], payload);
    }

    #[tokio::test]
    async fn test_segment_writer() {
        let temp_dir = tempfile::tempdir().unwrap();
        {
            let mut segment_writer = SegmentWriter::create(temp_dir.path(), 123).await.unwrap();
            assert_eq!(
                segment_writer.path(),
                temp_dir.path().join(segment_file_name(123))
            );
            assert_eq!(segment_writer.base_offset(), 123);
            assert_eq!(segment_writer.current_offset(), 123);
            assert_eq!(segment_writer.num_bytes(), 0);

            assert_eq!(segment_writer.append(b"Hello, World!").await.unwrap(), 23);
            assert_eq!(segment_writer.current_offset(), 123 + 23);
            assert_eq!(segment_writer.num_bytes(), 23);

            assert_eq!(segment_writer.append(b"Hola, Mundo!").await.unwrap(), 22);
            assert_eq!(segment_writer.current_offset(), 123 + 45);
            assert_eq!(segment_writer.num_bytes(), 45);

            segment_writer.flush().await.unwrap();

            let segment_bytes = tokio::fs::read(segment_writer.path()).await.unwrap();
            assert_eq!(segment_bytes.len(), 45);

            assert_log_eq(&segment_bytes[..23], b"Hello, World!");
            assert_log_eq(&segment_bytes[23..], b"Hola, Mundo!");
        }
        {
            let mut segment_writer = SegmentWriter::open(temp_dir.path(), 123).await.unwrap();
            assert_eq!(segment_writer.base_offset(), 123);
            assert_eq!(segment_writer.current_offset(), 123 + 45);
            assert_eq!(segment_writer.num_bytes(), 45);

            assert_eq!(segment_writer.append(b"Hallo, Welt!").await.unwrap(), 22);
            assert_eq!(segment_writer.current_offset(), 123 + 67);
            assert_eq!(segment_writer.num_bytes(), 67);

            segment_writer.flush().await.unwrap();

            let segment_bytes = tokio::fs::read(segment_writer.path()).await.unwrap();
            assert_eq!(segment_bytes.len(), 67);

            assert_log_eq(&segment_bytes[..23], b"Hello, World!");
            assert_log_eq(&segment_bytes[23..45], b"Hola, Mundo!");
            assert_log_eq(&segment_bytes[45..], b"Hallo, Welt!");
        }
    }
}
