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

use anyhow::{bail, Context};

mod reader;
mod segment_reader;
mod segment_writer;
mod writer;

pub use reader::Reader;
pub use writer::Writer;

pub(crate) const HEADER_MAGIC: u16 = 1337;
pub(crate) const HEADER_NUM_BYTES: u64 = 2 + 4 + 4; // magic + crc + payload length

pub(crate) const SEGMENT_FILE_NAME_EXTENSION: &str = "log";
pub(crate) const SEGMENT_FILE_NAME_LEN: usize = 20;
pub(crate) const SEGMENT_MAX_NUM_BYTES: u64 = if cfg!(test) { 64 } else { 128_000_000 };

/// Parses segment base offset from file name.
pub(crate) fn segment_base_offset<P: AsRef<Path>>(filepath: P) -> anyhow::Result<u64> {
    let filename = filepath.as_ref().file_name().unwrap().to_str().unwrap();
    let base_offset = filename[0..SEGMENT_FILE_NAME_LEN]
        .parse::<u64>()
        .with_context(|| {
            format!(
                "Failed to parse base offset from segment file name `{}`.",
                filepath.as_ref().display()
            )
        })?;
    Ok(base_offset)
}

pub(crate) fn segment_file_name(base_offset: u64) -> String {
    format!("{:0>20}.{}", base_offset, SEGMENT_FILE_NAME_EXTENSION)
}

/// A WAL segment stores a portion of the WAL between `base_offset` (inclusive) and `top_offset`
/// (exclusive) where `top_offset` equals `base_offset` + sizeof(segment). The WAL aims to create
/// segments of size greater or equal to `SEGMENT_MAX_NUM_BYTES` depending on the size of the last
/// entry inserted into the segment. A segment's `base_offset` is encoded in the segment's file
/// name. Within a WAL directory, the segment with the greatest `base_offset` is the segment being
/// written to. The other segments are immutable.
#[derive(Debug, PartialEq)]
pub(crate) struct Segment {
    filepath: PathBuf,
    base_offset: u64,
    top_offset: u64,
}

/// Returns the list of segment files in the WAL directory sorted by base offset.
pub(crate) async fn list_segments<P: AsRef<Path>>(log_dir: P) -> anyhow::Result<Vec<Segment>> {
    let mut segments: Vec<Segment> = Vec::new();
    let mut entries = tokio::fs::read_dir(log_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let filepath = entry.path();
        let base_offset = segment_base_offset(&filepath)?;
        let top_offset = base_offset + entry.metadata().await?.len();
        let segment = Segment {
            filepath,
            base_offset,
            top_offset,
        };
        segments.push(segment);
    }
    segments.sort_by(|left, right| left.base_offset.cmp(&right.base_offset));

    for window in segments.windows(2) {
        if window[0].top_offset != window[1].base_offset {
            bail!("CorruptionError")
        }
    }
    Ok(segments)
}

/// Removes the WAL segments with a `top_offset` lower or equal to `offset`.
pub async fn truncate_wal<P: AsRef<Path>>(log_dir: P, offset: u64) -> anyhow::Result<()> {
    let mut segments = list_segments(log_dir).await?;
    segments.pop(); // Exclude the "write" segment.

    for segment in segments {
        if segment.top_offset <= offset {
            tokio::fs::remove_file(segment.filepath).await?;
        } else {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_load_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        assert!(list_segments(temp_dir.path()).await.unwrap().is_empty());
        {
            let segment0_filepath = temp_dir.path().join(segment_file_name(0));
            let mut segment0 = tokio::fs::File::create(&segment0_filepath).await.unwrap();
            segment0.write_all(b"twelve bytes").await.unwrap();

            let segment1_filepath = temp_dir.path().join(segment_file_name(12));
            let mut segment1 = tokio::fs::File::create(&segment1_filepath).await.unwrap();
            segment1.write_all(b"fourteen bytes").await.unwrap();

            let expected_segments = vec![
                Segment {
                    filepath: segment0_filepath,
                    base_offset: 0,
                    top_offset: 12,
                },
                Segment {
                    filepath: segment1_filepath,
                    base_offset: 12,
                    top_offset: 26,
                },
            ];
            assert_eq!(
                list_segments(temp_dir.path()).await.unwrap(),
                expected_segments
            );

            segment0.set_len(10).await.unwrap();
            list_segments(temp_dir.path()).await.unwrap_err();
        }
    }

    #[tokio::test]
    async fn test_truncate_wal() {
        let temp_dir = tempfile::tempdir().unwrap();

        let segment0_filepath = temp_dir.path().join(segment_file_name(0));
        let mut segment0 = tokio::fs::File::create(&segment0_filepath).await.unwrap();
        segment0.write_all(b"twelve bytes").await.unwrap();

        let segment1_filepath = temp_dir.path().join(segment_file_name(12));
        let mut segment1 = tokio::fs::File::create(&segment1_filepath).await.unwrap();
        segment1.write_all(b"fourteen bytes").await.unwrap();

        let segment2_filepath = temp_dir.path().join(segment_file_name(26));
        let mut segment2 = tokio::fs::File::create(&segment2_filepath).await.unwrap();
        segment2.write_all(b"twelve bytes").await.unwrap();

        truncate_wal(temp_dir.path(), 0).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 3);

        truncate_wal(temp_dir.path(), 11).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 3);

        truncate_wal(temp_dir.path(), 12).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 2);

        truncate_wal(temp_dir.path(), 25).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 2);

        truncate_wal(temp_dir.path(), 26).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 1);

        truncate_wal(temp_dir.path(), 38).await.unwrap();
        assert_eq!(list_segments(temp_dir.path()).await.unwrap().len(), 1);
    }
}
