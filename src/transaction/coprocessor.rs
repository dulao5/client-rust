// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The coprocessor RPC of [`CopSender`](crate::CopSender).
//!
//! The request and response payloads are opaque bytes: callers encode a `tipb.DAGRequest` (or
//! any other coprocessor request type) themselves. Splitting a request into region tasks,
//! paging, scheduling and response handling are the caller's: tidb-rs ports TiDB's
//! `store/copr` for that (block D07.2) and sends each task through
//! [`CopSender::send`](crate::CopSender::send). The earlier `Transaction::coprocessor`
//! (this client's own task splitting and retries) is retired.

use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::region::RegionWithLeader;
use crate::store::Request;
use crate::Error;
use crate::Result;

/// `coprocessor.Request.tp` for a `tipb.DAGRequest`.
pub const REQ_TYPE_DAG: i64 = 103;
/// `coprocessor.Request.tp` for a `tipb.AnalyzeReq`.
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// `coprocessor.Request.tp` for a `tipb.ChecksumRequest`.
pub const REQ_TYPE_CHECKSUM: i64 = 105;

/// Default per-RPC timeout of a coprocessor request (TiDB's `copr-req-timeout`).
pub const DEFAULT_COPROCESSOR_TIMEOUT: Duration = Duration::from_secs(60);

/// The RPC wrapper: carries the coprocessor-specific timeout.
#[derive(Clone)]
pub(crate) struct CoprocessorRpc {
    pub(crate) inner: coprocessor::Request,
    pub(crate) timeout: Duration,
}

#[async_trait]
impl Request for CoprocessorRpc {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        _timeout: Duration,
    ) -> Result<Box<dyn std::any::Any>> {
        // TiKV gives up at the RPC timeout too (client-go `SendReqCtx`,
        // internal/locate/region_request.go:1393-1397), unless the request has a deadline.
        let mut inner = self.inner.clone();
        let ctx = inner.context.get_or_insert_with(Default::default);
        let timeout = if ctx.max_execution_duration_ms > 0 {
            Duration::from_millis(ctx.max_execution_duration_ms)
        } else {
            ctx.max_execution_duration_ms = self.timeout.as_millis() as u64;
            self.timeout
        };
        let mut req = inner.into_request();
        req.set_timeout(timeout);
        let start = std::time::Instant::now();
        let result = client.clone().coprocessor(req).await;
        let wall_ns = result.as_ref().map_or(0, |r| {
            crate::store::exec_details_wall_time_ns(r.get_ref().exec_details_v2.as_ref())
        });
        crate::metrics::rpc(
            crate::metrics::CmdType::Cop,
            crate::store::context_store_id(self.inner.context.as_ref()),
            start.elapsed(),
            wall_ns,
        );
        result
            .map(|r| Box::new(r.into_inner()) as Box<dyn std::any::Any>)
            .map_err(Error::from)
    }

    fn label(&self) -> &'static str {
        "coprocessor"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        &self.inner
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
            region: leader.ver_id(),
        })?;
        ctx.region_id = leader.region.id;
        ctx.region_epoch = leader.region.region_epoch.clone();
        ctx.peer = Some(leader_peer.clone());
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        ctx.api_version = api_version.into();
    }

    fn set_lock_hints(&mut self, resolved: Vec<u64>, committed: Vec<u64>) {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        ctx.resolved_locks = resolved;
        ctx.committed_locks = committed;
    }
}
