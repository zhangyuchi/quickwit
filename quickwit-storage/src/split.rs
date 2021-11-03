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
use std::io::{self, Cursor, ErrorKind, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::StreamExt;
use rusoto_core::ByteStream;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::{BundleStorageFileOffsets, PutPayload};

trait DataSource: tokio::io::AsyncRead + Send + Sync + Unpin {}
impl<T: tokio::io::AsyncRead + Send + Sync + Unpin> DataSource for T {}

/// Payload of a split which builds the split bundle and hotcache on the fly and streams it to the
/// storage.
#[derive(Clone)]
pub struct SplitPayload {
    /// Total split size
    pub split_size: usize,
    /// Files and their target range in the split bundle.
    pub files_and_offsets: Vec<(PathBuf, Range<usize>)>,
    /// bytes range of the footer (hotcache + bundle metadata)
    pub footer_range: Range<usize>,
    /// data of the footer, containng
    /// [bundle metadata, bundle metadata len 8byte, hotcache, hotcache len 8 bytes]
    pub footer_bytes: Vec<u8>,
    /// The end offset of the data provider [File, .. File, SplitFooter]
    pub block_offsets: Vec<Range<usize>>,
}

impl SplitPayload {
    /// Get start and end block index for a range, where start of the Range is in the start block
    /// and end of the range is in the end block.
    fn get_start_and_end_block_index(&self, range: Range<usize>) -> io::Result<(usize, usize)> {
        get_start_and_end_block_index(&self.block_offsets, range)
    }

    /// Get and seek in DataSource
    ///
    /// We apply seek now because the DataSource trait does not allow it.
    async fn get_datasource(
        &self,
        index: usize,
        start_file_offset: u64,
    ) -> io::Result<Box<dyn DataSource>> {
        let files_len = self.files_and_offsets.len();

        if index < files_len {
            let (path, _) = &self.files_and_offsets[index];
            let mut ds = Box::new(tokio::fs::File::open(path).await?);
            if start_file_offset != 0 {
                ds.seek(SeekFrom::Start(start_file_offset)).await?;
            }
            Ok(ds)
        } else {
            let mut ds = Box::new(Cursor::new(self.footer_bytes.to_vec()));
            if start_file_offset != 0 {
                ds.seek(SeekFrom::Start(start_file_offset)).await?;
            }
            Ok(ds)
        }
    }
}

#[async_trait]
impl PutPayload for SplitPayload {
    async fn len(&self) -> io::Result<u64> {
        Ok(self.split_size as u64)
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        let (start_index, end_index) =
            self.get_start_and_end_block_index(range.start as usize..range.end as usize)?;

        let start_file_offset = range.start - self.block_offsets[start_index].start as u64;
        // If we have skip and take in the same block, we need to take the skip into account for
        // the take calculation
        let same_block_skip = if start_index == end_index {
            start_file_offset
        } else {
            0
        };
        let last_block_take =
            range.end - self.block_offsets[end_index].start as u64 - same_block_skip;

        // build data_sources, apply seek in first
        let mut datasources: Vec<Box<dyn DataSource>> = vec![];
        for index in start_index..=end_index {
            // seek in first datasource
            let seek_offset = if index == start_index {
                start_file_offset
            } else {
                0
            };
            let mut ds = self.get_datasource(index, seek_offset).await?;

            // apply take on last datasource
            if index == end_index {
                ds = Box::new(ds.take(last_block_take));
            }
            datasources.push(ds);
        }

        // Combine and convert to bytestream
        let stream = futures::stream::iter(datasources.into_iter()).flat_map(ReaderStream::new);
        Ok(ByteStream::new(stream))
    }
}

/// compute split offsets used by the SplitStreamer
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

/// Get start and end block index for a range, where start of the Range is in the start block
/// and end of the range is in the end block.
fn get_start_and_end_block_index(
    blocks_ranges: &[Range<usize>],
    range: Range<usize>,
) -> io::Result<(usize, usize)> {
    let get_start = blocks_ranges
        .iter()
        .position(|block_range| block_range.end > range.start)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "range is out of Range"))?;
    let get_end = blocks_ranges
        .iter()
        .position(|block_range| block_range.end >= range.end)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "range is out of Range"))?;
    Ok((get_start, get_end))
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
        })?;

        footer_bytes.write_all(metadata_json.as_bytes())?;
        let metadata_json_len = metadata_json.len() as u64;
        footer_bytes.write_all(&metadata_json_len.to_le_bytes())?;
        footer_bytes.write_all(hotcache)?;
        footer_bytes.write_all(&hotcache.len().to_le_bytes())?;

        let mut files_and_offsets = self
            .metadata
            .files
            .iter()
            .map(|(file, range)| (file.to_owned(), range.start as usize..range.end as usize))
            .collect::<Vec<_>>();
        files_and_offsets.sort_by_key(|(_file, range)| range.start);

        let mut block_offsets = self
            .metadata
            .files
            .iter()
            .map(|(_, range)| (range.start as usize..range.end as usize))
            .collect::<Vec<_>>();
        block_offsets.sort_by_key(|range| range.start);

        let split_size = self.current_offset + footer_bytes.len();
        block_offsets.push(self.current_offset..split_size);

        let split_offsets = SplitPayload {
            split_size,
            files_and_offsets,
            footer_range: self.current_offset..self.current_offset + footer_bytes.len(),
            footer_bytes,
            block_offsets,
        };

        Ok(split_offsets)
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
            .get_start_and_end_block_index(100..110)
            .is_err());
        assert!(split_streamer
            .get_start_and_end_block_index(0..100)
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

        // border case skip and take cross adjacent blocks
        assert_eq!(fetch_data(&split_streamer, 1..3).await?, vec![76, 99]);

        // border case skip and take in seperate blocks with full block between
        assert_eq!(
            fetch_data(&split_streamer, 1..6).await?,
            vec![76, 99, 55, 44, 123]
        );

        // border case 2 exact middle block
        assert_eq!(fetch_data(&split_streamer, 2..5).await?, vec![99, 55, 44]);

        // border case, no skip but take in middle block
        assert_eq!(fetch_data(&split_streamer, 2..4).await?, vec![99, 55]);

        // border case skip and take in middle block
        assert_eq!(fetch_data(&split_streamer, 3..4).await?, vec![55]);

        // border case 3 start exact last block - footer
        assert_eq!(
            fetch_data(&split_streamer, 5..10).await?,
            vec![123, 34, 102, 105, 108]
        );
        // border case 4 skip and take in last block  - footer
        assert_eq!(
            fetch_data(&split_streamer, 6..10).await?,
            vec![34, 102, 105, 108]
        );

        let total_len = split_streamer.len().await?;
        let all_data = fetch_data(&split_streamer, 0..total_len).await?;

        // last 8 bytes are the length of the hotcache bytes
        assert_eq!(all_data[all_data.len() - 8..], 3_u64.to_le_bytes());
        Ok(())
    }
}
