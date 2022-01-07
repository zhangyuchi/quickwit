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

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::bail;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::{segment_file_name, HEADER_MAGIC, HEADER_NUM_BYTES, SEGMENT_MAX_NUM_BYTES};

pub(crate) struct SegmentReader {
    filepath: PathBuf,
    reader: tokio::io::BufReader<File>,
    buffer: Vec<u8>,
    base_offset: u64,
    current_pos: u64,
}

impl SegmentReader {
    pub(crate) async fn open<P: AsRef<Path>>(log_dir: P, base_offset: u64) -> anyhow::Result<Self> {
        let filename = segment_file_name(base_offset);
        let filepath = log_dir.as_ref().join(filename);
        let file = tokio::fs::File::open(&filepath).await?;
        let reader = tokio::io::BufReader::new(file);
        Ok(Self {
            filepath,
            reader,
            buffer: vec![0; 1],
            base_offset,
            current_pos: 0,
        })
    }

    pub(crate) async fn seek(&mut self, offset: u64) -> anyhow::Result<()> {
        if offset < self.base_offset || offset >= self.base_offset + SEGMENT_MAX_NUM_BYTES {
            anyhow::bail!("SeekError: offset `{}` is out of segment range.", offset);
        }
        self.current_pos = self
            .reader
            .seek(SeekFrom::Start(offset - self.base_offset))
            .await?;

        let buffer = self.reader.fill_buf().await?;

        if buffer.len() >= 2 {
            let magic = u16::from_le_bytes(buffer[..2].try_into().unwrap());

            if magic != HEADER_MAGIC {
                anyhow::bail!("SeekError: offset `{}`", offset)
            }
        }
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> anyhow::Result<Option<&[u8]>> {
        match self.reader.read_u16_le().await {
            Ok(HEADER_MAGIC) => (),
            Ok(_) => bail!("ReadError"),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => bail!(error),
        };
        let checksum = self.reader.read_u32_le().await?;
        let payload_num_bytes = self.reader.read_u32_le().await? as usize;
        if payload_num_bytes > self.buffer.len() {
            self.buffer = vec![0; payload_num_bytes]
        }
        let payload = &mut self.buffer[..payload_num_bytes];
        self.reader.read_exact(payload).await?;
        if checksum != crc32fast::hash(payload) {
            anyhow::bail!("CorruptionError")
        }
        self.current_pos += HEADER_NUM_BYTES + payload.len() as u64;
        Ok(Some(payload))
    }

    pub(crate) async fn next_entry<'a>(&'a mut self) -> anyhow::Result<Option<Entry<'a>>> {
        match self.reader.read_u16_le().await {
            Ok(HEADER_MAGIC) => (),
            Ok(_) => bail!("ReadError"),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => bail!(error),
        };
        let checksum = self.reader.read_u32_le().await?;
        let payload_num_bytes = self.reader.read_u32_le().await? as usize;
        if payload_num_bytes > self.buffer.len() {
            self.buffer = vec![0; payload_num_bytes]
        }
        let offset = self.current_offset();
        let payload = &mut self.buffer[..payload_num_bytes];
        self.reader.read_exact(payload).await?;
        if checksum != crc32fast::hash(payload) {
            anyhow::bail!("CorruptionError")
        }
        let entry = Entry {
            offset,
            checksum,
            payload,
        };
        self.current_pos += HEADER_NUM_BYTES + payload.len() as u64;
        Ok(Some(entry))
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

#[derive(Debug)]
pub struct Entry<'a> {
    offset: u64,
    checksum: u32,
    pub payload: &'a [u8],
}

impl Entry<'_> {
    pub fn next_offset(&self) -> u64 {
        self.offset + self.payload.len() as u64 + HEADER_NUM_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_writer::SegmentWriter;

    #[tokio::test]
    async fn test_segment_reader() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut segment_writer = SegmentWriter::create(temp_dir.path(), 123).await.unwrap();
        segment_writer.append(b"Hello, World!").await.unwrap();
        segment_writer.append(b"Hola, Mundo!").await.unwrap();
        segment_writer.append(b"Hallo, Welt!").await.unwrap();
        segment_writer.flush().await.unwrap();

        let mut segment_reader = SegmentReader::open(temp_dir.path(), 123).await.unwrap();
        assert!(matches!(
            segment_reader.next().await.unwrap(),
            Some(b"Hello, World!")
        ));
        assert!(matches!(
            segment_reader.next().await.unwrap(),
            Some(b"Hola, Mundo!")
        ));
        assert!(matches!(
            segment_reader.next().await.unwrap(),
            Some(b"Hallo, Welt!")
        ));
        assert!(segment_reader.next().await.unwrap().is_none());

        segment_reader.seek(122).await.unwrap_err();
        segment_reader
            .seek(123 + SEGMENT_MAX_NUM_BYTES)
            .await
            .unwrap_err();
        segment_reader.seek(124).await.unwrap_err();

        segment_reader.seek(123).await.unwrap();
        assert!(matches!(
            segment_reader.next().await.unwrap(),
            Some(b"Hello, World!")
        ));
        segment_reader.seek(123 + 45).await.unwrap();
        assert!(matches!(
            segment_reader.next().await.unwrap(),
            Some(b"Hallo, Welt!")
        ));
    }
}
