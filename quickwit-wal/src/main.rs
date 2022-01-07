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

use std::env;
use std::time::Instant;

use quickwit_wal::Writer;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() {
    let mut args = env::args();
    args.next();
    let log_dir_path = args.next().expect("Path to WAL directory is missing.");
    let mut writer = Writer::open(&log_dir_path).await.unwrap();

    let reader = BufReader::new(stdin());
    let mut lines = reader.lines();

    let mut num_bytes = 0;
    let mut num_lines = 0;
    let start = Instant::now();

    while let Some(line) = lines.next_line().await.unwrap() {
        writer.append(&line).await.unwrap();
        num_bytes += line.len();
        num_lines += 1;
    }
    println!(
        "Appended {} docs to WAL located at `{}` in {:.2}s ({:.2}MB/s).",
        num_lines,
        log_dir_path,
        start.elapsed().as_secs_f64(),
        num_bytes as f64 / 1_000_000f64 / start.elapsed().as_secs_f64()
    )
}
