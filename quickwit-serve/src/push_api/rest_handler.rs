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

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use quickwit_proto::push_api::{DocBatch, IngestRequest, TailRequest};
use quickwit_pushapi::{add_doc, PushApiServiceImpl, iter_doc_payloads};
use serde::Serialize;
use thiserror::Error;
use tracing::info;
use warp::{reject, Filter, Rejection};

use crate::Format;

#[derive(Debug, Error)]
#[error("Body is not utf-8.")]
struct InvalidUtf8;

impl warp::reject::Reject for InvalidUtf8 {}

#[derive(Debug, Error)]
#[error("The PushAPI is not available.")]
struct PushApiServiceUnavailable;

impl warp::reject::Reject for PushApiServiceUnavailable {}

pub fn ingest_handler() -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    ingest_filter()
        .and(warp::any().and_then(|| async {
            if let Some(push_api_service) = quickwit_pushapi::get_push_api_service() {
                Ok(push_api_service)
            } else {
                Err(reject::custom(PushApiServiceUnavailable))
            }
        }))
        .and_then(ingest)
}

fn ingest_filter() -> impl Filter<Extract = (String, String), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "ingest")
        .and(warp::post())
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&*body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
}

fn lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines().filter_map(|line| {
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            return None;
        }
        Some(line_trimmed)
    })
}

async fn ingest(
    index_id: String,
    payload: String,
    push_api_service: Arc<PushApiServiceImpl>,
) -> Result<impl warp::Reply, Infallible> {
    info!(payload=%payload, "ingest");
    let mut doc_batch = DocBatch::default();
    doc_batch.index_id = index_id;
    for doc_payload in lines(&payload) {
        add_doc(doc_payload.as_bytes(), &mut doc_batch);
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch],
    };
    let ingest_resp = push_api_service.ingest(ingest_req).await;
    Ok(Format::PrettyJson.make_reply(ingest_resp))
}

pub fn tail_handler() -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    tail_filter()
        .and(warp::any().and_then(|| async {
            if let Some(push_api_service) = quickwit_pushapi::get_push_api_service() {
                Ok(push_api_service)
            } else {
                Err(reject::custom(PushApiServiceUnavailable))
            }
        }))
        .and_then(tail)
}

fn tail_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!("api" / "v1" / String / "tail").and(warp::get())
}


#[derive(Serialize)]
struct TailRestResponse {
    docs: Vec<String>
}

async fn tail(
    index_id: String,
    push_api_service: Arc<PushApiServiceImpl>,
) -> Result<impl warp::Reply, Infallible> {
    let tail_res = push_api_service.tail(TailRequest { index_id }).await
        .map(|index| {
            let mut docs = Vec::new();
            if let Some(doc_batch) = index.doc_batch.as_ref() {
                for doc_payload in iter_doc_payloads(doc_batch) {
                    if let Ok(doc_utf8) = std::str::from_utf8(doc_payload) {
                        docs.push(doc_utf8.to_string());
                    }
                }
            }
            TailRestResponse {
                docs
            }
        });
    Ok(Format::PrettyJson.make_reply(tail_res))
}
