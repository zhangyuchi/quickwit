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
use std::fmt::Debug;
use std::io::{self, ErrorKind, SeekFrom, Write};
use std::iter;
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use quickwit_common::HOTCACHE_FILENAME;
use rusoto_core::ByteStream;
use tantivy::directory::OwnedBytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::{BundleStorageFileOffsets, PutPayload};

/// Payload of a split which builds the split bundle and hotcache on the fly and streams it to the
/// storage.
#[derive(Clone)]
pub struct SplitPayload {
    payloads: Vec<Box<dyn PutPayload>>,
    /// bytes range of the footer (hotcache + bundle metadata)
    pub footer_range: Range<u64>,
}

#[async_trait]
impl PutPayload for SplitPayload {
    fn len(&self) -> u64 {
        self.payloads.iter().map(|payload| payload.len()).sum()
    }

    async fn range_byte_stream(&self, mut range: Range<u64>) -> io::Result<ByteStream> {
        let mut bytestreams: Vec<_> = Vec::new();
        for payload in &self.payloads {
            let payload_len = payload.len();
            if range.start >= payload.len() {
                // The current payload does not intersect with the range
                range = (range.start - payload_len)..(range.end - payload_len);
                continue;
            } else if range.end > payload_len {
                // This payload intersects with the range, and it is NOT the last one.
                bytestreams.push(payload.range_byte_stream(range.start..payload_len).await?);
                range = range.start - payload_len..range.end - payload_len;
            } else {
                // This is the last chunk
                bytestreams.push(payload.range_byte_stream(range.start..range.end).await?);
                break;
            }
        }
        let concat_stream = ByteStream::new(stream::iter(bytestreams).flatten());
        Ok(concat_stream)
    }
}

/// Compute split offsets used by the SplitPayload.
pub fn get_split_payload_streamer(
    split_files: &[PathBuf],
    hotcache: &[u8],
) -> io::Result<SplitPayload> {
    let mut split_offset_computer = SplitPayloadBuilder::new();
    for file in split_files {
        split_offset_computer.add_file(file)?;
    }
    let offsets = split_offset_computer.finalize(hotcache)?;
    Ok(offsets)
}

#[derive(Clone)]
struct FilePayload {
    len: u64,
    path: PathBuf,
}

#[async_trait]
impl PutPayload for FilePayload {
    fn len(&self) -> u64 {
        self.len
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        assert!(!range.is_empty());
        assert!(range.end <= self.len);
        let mut file = tokio::fs::File::open(&self.path).await?;
        if range.start > 0 {
            file.seek(SeekFrom::Start(range.start)).await?;
        }
        if range.end == self.len {
            return Ok(ByteStream::new(ReaderStream::new(file)));
        }
        Ok(ByteStream::new(ReaderStream::new(
            file.take(range.end - range.start),
        )))
    }
}

/// SplitOffsetComputer is used to bundle together multiple files into a single split file.
#[derive(Default, Debug)]
pub struct SplitPayloadBuilder {
    metadata: BundleStorageFileOffsets,
    current_offset: usize,
}

impl SplitPayloadBuilder {
    /// Creates a new SplitOffsetComputer, to which files and the hotcache can be added.
    pub fn new() -> Self {
        SplitPayloadBuilder {
            current_offset: 0,
            metadata: Default::default(),
        }
    }

    /// Adds the file to the bundle file.
    ///
    /// The hotcache needs to be the last file that is added, in order to be able to read
    /// the hotcache and the metadata in one continous read.
    pub fn add_file(&mut self, path: &Path) -> io::Result<()> {
        let file = std::fs::metadata(path)?;
        let file_range = self.current_offset as u64..self.current_offset as u64 + file.len() as u64;
        self.current_offset += file.len() as usize;
        self.metadata.files.insert(path.to_owned(), file_range);
        Ok(())
    }

    /// Writes the bundle file offsets metadata at the end of the bundle file,
    /// and returns the byte-range of this metadata information.
    pub fn finalize(self, hotcache: &[u8]) -> io::Result<SplitPayload> {
        // Build the footer.
        let mut footer_bytes = vec![];
        // Fix paths to be relative
        let metadata_with_fixed_paths = self
            .metadata
            .files
            .iter()
            .map(|(path, range)| {
                let file_name = PathBuf::from(path.file_name().ok_or_else(|| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        format!("could not extract file_name from path {:?}", path),
                    )
                })?);
                Ok((file_name, range.start..range.end))
            })
            .collect::<Result<HashMap<_, _>, io::Error>>()?;

        let metadata_json = serde_json::to_string(&BundleStorageFileOffsets {
            files: metadata_with_fixed_paths,
            files_and_data: iter::once((
                PathBuf::from(HOTCACHE_FILENAME.to_string()),
                OwnedBytes::new(hotcache.to_owned()),
            ))
            .collect(),
        })?;

        footer_bytes.write_all(metadata_json.as_bytes())?;
        let metadata_json_len = metadata_json.len() as u64;
        footer_bytes.write_all(&metadata_json_len.to_le_bytes())?;

        let mut payloads: Vec<Box<dyn PutPayload>> = Vec::new();
        for (path, byte_range) in self.metadata.files {
            let file_payload = FilePayload {
                path,
                len: byte_range.end - byte_range.start,
            };
            payloads.push(Box::new(file_payload));
        }

        payloads.push(Box::new(footer_bytes.to_vec()));

        Ok(SplitPayload {
            payloads,
            footer_range: self.current_offset as u64
                ..self.current_offset as u64 + footer_bytes.len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[tokio::test]
    async fn test_split_offset_computer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer =
            get_split_payload_streamer(&[test_filepath1, test_filepath2], &[1, 2, 3])?;

        assert_eq!(
            split_streamer.get_start_and_end_block_index(0..1).unwrap(),
            (0, 0)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(1..2).unwrap(),
            (0, 0)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(2..3).unwrap(),
            (1, 1)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(3..4).unwrap(),
            (1, 1)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(4..5).unwrap(),
            (1, 1)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(4..6).unwrap(),
            (1, 2)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(0..5).unwrap(),
            (0, 1)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(0..6).unwrap(),
            (0, 2)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(0..7).unwrap(),
            (0, 2)
        );
        assert_eq!(
            split_streamer.get_start_and_end_block_index(0..8).unwrap(),
            (0, 2)
        );
        assert!(split_streamer
            .get_start_and_end_block_index(140..150)
            .is_err());
        assert!(split_streamer
            .get_start_and_end_block_index(0..150)
            .is_err());

        Ok(())
    }

    #[cfg(test)]
    async fn fetch_data(
        split_streamer: &SplitPayload,
        range: Range<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut data = vec![];
        split_streamer
            .range_byte_stream(range)
            .await?
            .into_async_read()
            .read_to_end(&mut data)
            .await?;
        Ok(data)
    }

    #[tokio::test]
    async fn test_split_streamer() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("a");
        let test_filepath2 = temp_dir.path().join("b");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = get_split_payload_streamer(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[1, 2, 3],
        )?;

        // border case 1 exact start of first block
        assert_eq!(fetch_data(&split_streamer, 0..1).await?, vec![123]);
        assert_eq!(fetch_data(&split_streamer, 0..2).await?, vec![123, 76]);
        assert_eq!(fetch_data(&split_streamer, 0..3).await?, vec![123, 76, 99]);

        // border 2 case skip and take cross adjacent blocks
        assert_eq!(fetch_data(&split_streamer, 1..3).await?, vec![76, 99]);

        // border 3 case skip and take in seperate blocks with full block between
        assert_eq!(
            fetch_data(&split_streamer, 1..6).await?,
            vec![76, 99, 55, 44, 123]
        );

        // border case 4 exact middle block
        assert_eq!(fetch_data(&split_streamer, 2..5).await?, vec![99, 55, 44]);

        // border case 5, no skip but take in middle block
        assert_eq!(fetch_data(&split_streamer, 2..4).await?, vec![99, 55]);

        // border case 6 skip and take in middle block
        assert_eq!(fetch_data(&split_streamer, 3..4).await?, vec![55]);

        // border case 7 start exact last block - footer
        assert_eq!(
            fetch_data(&split_streamer, 5..10).await?,
            vec![123, 34, 102, 105, 108]
        );
        // border case 8 skip and take in last block  - footer
        assert_eq!(
            fetch_data(&split_streamer, 6..10).await?,
            vec![34, 102, 105, 108]
        );

        let total_len = split_streamer.len().await?;
        let all_data = fetch_data(&split_streamer, 0..total_len).await?;

        // last 8 bytes are the length of the bundle metadata
        assert_eq!(all_data[all_data.len() - 8..], 97_u64.to_le_bytes());
        Ok(())
    }
}
