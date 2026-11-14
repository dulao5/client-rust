// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! One coprocessor RPC to one region, with client-go `ClientHelper.SendReqCtx` semantics, for
//! callers that build and schedule coprocessor tasks themselves (tidb-rs ports TiDB's
//! `store/copr` function by function, block D07.2).
//!
//! [`CopSender::send`] mirrors client-go `RegionRequestSender.SendReqCtx`
//! (`internal/locate/region_request.go`) as TiDB's `copIteratorWorker.handleTaskOnce` uses it
//! (`store/copr/coprocessor.go:1415`):
//! - the region is addressed by its [`RegionVerId`]; when the cache no longer has that epoch,
//!   the caller gets a response carrying an `EpochNotMatch` region error, as client-go's
//!   `genRegionErrorResp` when `GetTiKVRPCContext` returns nil;
//! - gRPC errors and the region errors client-go retries in place (NotLeader, ServerIsBusy,
//!   StaleCommand, a `tikv_client_read_timeout` deadline) are retried here, charged to the
//!   caller's [`Backoff`];
//! - the region errors client-go hands back (EpochNotMatch, RegionNotFound, KeyNotInRegion,
//!   StoreNotMatch, unknown ones) update the cache and are returned in the response, so the
//!   caller re-splits its ranges (`handleCopResponse` backs off `BoRegionMiss` and rebuilds
//!   the tasks);
//! - the request's lock hints ([`ReadLockHints`]) are attached to every RPC, as
//!   `NewClientHelper(store, &resolvedLocks, &committedLocks, false)` does.
//!
//! Differences kept (spec D07.2 part 2): no replica read (leader only, as the rest of this
//! client, parity table §3.1), no region buckets (locations carry none), the store's
//! estimated wait time is always zero (no load-based selection), and the store address is not
//! known here (empty).

use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::time::sleep;

use super::coprocessor::CoprocessorRpc;
use crate::backoff::Backoff;
use crate::internal_err;
use crate::metrics;
use crate::metrics::BackoffType;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::coprocessor;
use crate::proto::errorpb;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::region::RegionVerId;
use crate::request::plan::check_store_liveness;
use crate::request::plan::grpc_error_action;
use crate::request::plan::handle_region_error;
use crate::request::plan::is_grpc_error;
use crate::request::plan::liveness_target;
use crate::request::plan::GrpcErrorAction;
use crate::request::Keyspace;
use crate::stats::tikv_stats;
use crate::store::Request;
use crate::transaction::expand_shared_lock;
use crate::transaction::resolve_locks_for_read;
use crate::transaction::LockBackoff;
use crate::transaction::ReadLockHints;
use crate::Error;
use crate::Key;
use crate::Result;
use crate::Timestamp;
use crate::TimestampExt;

/// client-go `tikv.KeyLocation`, without buckets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegionLocation {
    pub ver_id: RegionVerId,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

/// The part of client-go `tikv.RPCContext` a coprocessor caller reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CopRpcContext {
    pub ver_id: RegionVerId,
    pub peer: metapb::Peer,
    pub store_id: u64,
    /// `Store.EstimatedWaitTime()`: always zero (no load-based replica selection here).
    pub store_estimated_wait_time: Duration,
    /// `Addr`: not known by this client (empty).
    pub addr: String,
}

/// What [`CopSender::send`] got: client-go returns `(resp, rpcCtx, storeAddr, err)`.
#[derive(Clone, Debug, Default)]
pub struct CopSendResult {
    /// The response; its `region_error` is set when the caller must re-split.
    pub resp: coprocessor::Response,
    /// The context of the last attempt (`None` when no store was reached).
    pub rpc_ctx: Option<CopRpcContext>,
}

/// Sends coprocessor RPCs region by region at a snapshot's timestamp (see the module docs).
pub struct CopSender<PdC: PdClient = PdRpcClient> {
    pd_client: Arc<PdC>,
    keyspace: Keyspace,
    timestamp: Timestamp,
    lock_hints: ReadLockHints,
}

impl<PdC: PdClient> Clone for CopSender<PdC> {
    fn clone(&self) -> Self {
        CopSender {
            pd_client: self.pd_client.clone(),
            keyspace: self.keyspace,
            timestamp: self.timestamp.clone(),
            lock_hints: self.lock_hints.clone(),
        }
    }
}

/// `genRegionErrorResp`: the response the caller sees when the region is gone from the cache.
fn epoch_not_match_resp() -> coprocessor::Response {
    coprocessor::Response {
        region_error: Some(errorpb::Error {
            message: "region epoch is ahead of tikv or the region is not in the cache".into(),
            epoch_not_match: Some(errorpb::EpochNotMatch::default()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Region errors client-go retries inside `SendReqCtx` (`onRegionError` returning true).
fn retried_in_place(e: &errorpb::Error) -> bool {
    e.not_leader.is_some() || e.server_is_busy.is_some() || e.stale_command.is_some()
}

impl<PdC: PdClient> CopSender<PdC> {
    pub(crate) fn new(
        pd_client: Arc<PdC>,
        keyspace: Keyspace,
        timestamp: Timestamp,
    ) -> Result<Self> {
        if !matches!(keyspace, Keyspace::Disable) {
            return Err(internal_err!("CopSender does not support keyspaces"));
        }
        let lock_hints = ReadLockHints::new(timestamp.version());
        Ok(CopSender {
            pd_client,
            keyspace,
            timestamp,
            lock_hints,
        })
    }

    /// The read timestamp.
    pub fn start_ts(&self) -> u64 {
        self.timestamp.version()
    }

    /// The lock hints shared by all RPCs of this sender (TiDB `copIterator.resolvedLocks` /
    /// `committedLocks`).
    pub fn lock_hints(&self) -> &ReadLockHints {
        &self.lock_hints
    }

    /// client-go `RegionCache.BatchLocateKeyRanges`: the regions `ranges` cover, in key order,
    /// each once. For each range: the region holding its start key, then the following ones
    /// until a region contains the end key by end (an empty end key: to the last region).
    pub async fn locate_key_ranges(
        &self,
        _bo: &mut Backoff,
        ranges: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<Vec<RegionLocation>> {
        let mut out: Vec<RegionLocation> = Vec::new();
        for (start, end) in ranges {
            let mut key = start.clone();
            loop {
                let region = self
                    .pd_client
                    .region_for_key(&Key::from(key.clone()))
                    .await?;
                let ver_id = region.ver_id();
                let region_end = region.region.end_key.clone();
                if out.last().is_none_or(|l| l.ver_id != ver_id) {
                    out.push(RegionLocation {
                        ver_id,
                        start_key: region.region.start_key.clone(),
                        end_key: region_end.clone(),
                    });
                }
                // ContainsByEnd(end): start < end <= region end.
                let contains_by_end = region.region.start_key.as_slice() < end.as_slice()
                    && (region_end.is_empty() || end.as_slice() <= region_end.as_slice());
                if contains_by_end || region_end.is_empty() {
                    break;
                }
                key = region_end;
            }
        }
        Ok(out)
    }

    /// client-go `RegionCache.GetTiKVRPCContext` for a leader read: `None` when the cache no
    /// longer has this epoch of the region or it has no leader.
    pub async fn rpc_context(&self, ver_id: &RegionVerId) -> Result<Option<CopRpcContext>> {
        let region = self.pd_client.region_for_id(ver_id.id).await?;
        if region.ver_id() != *ver_id {
            return Ok(None);
        }
        let Some(leader) = region.leader.clone() else {
            return Ok(None);
        };
        Ok(Some(CopRpcContext {
            ver_id: ver_id.clone(),
            store_id: leader.store_id,
            peer: leader,
            store_estimated_wait_time: Duration::ZERO,
            addr: String::new(),
        }))
    }

    /// Backs off `t` on `bo`, or returns `error` wrapped as the budget's exhaustion.
    async fn backoff(bo: &mut Backoff, t: BackoffType, error: Error) -> Result<()> {
        match bo.next_delay_for(t) {
            Some(delay) => {
                metrics::backoff(t, delay);
                sleep(delay).await;
                Ok(())
            }
            None => Err(bo.exhausted(error)),
        }
    }

    /// One coprocessor request to region `ver_id` with `SendReqCtx` semantics (module docs).
    /// `req.context` may carry the caller's fields (isolation level, priority, ...); the
    /// region, epoch, peer and lock hints are set here. `timeout` is the RPC timeout; a
    /// `max_execution_duration_ms` already in the context is the first attempt's deadline
    /// (`tikv_client_read_timeout`), dropped after TiKV reports it exceeded.
    pub async fn send(
        &self,
        bo: &mut Backoff,
        ver_id: &RegionVerId,
        mut req: coprocessor::Request,
        timeout: Duration,
    ) -> Result<CopSendResult> {
        req.context.get_or_insert_with(Default::default).api_version =
            self.keyspace.api_version().into();
        loop {
            let region = self.pd_client.region_for_id(ver_id.id).await?;
            if region.ver_id() != *ver_id {
                return Ok(CopSendResult {
                    resp: epoch_not_match_resp(),
                    rpc_ctx: None,
                });
            }
            let Some(target) = liveness_target(&*self.pd_client, &region) else {
                // Every peer is unreachable: client-go's selector gives up, invalidates the
                // region and hands back a region error.
                self.pd_client.invalidate_region_cache(ver_id.clone()).await;
                return Ok(CopSendResult {
                    resp: epoch_not_match_resp(),
                    rpc_ctx: None,
                });
            };
            let target_store_id = target.get_store_id().ok();
            let fallback_peer = target
                .leader
                .clone()
                .filter(|_| target.leader != region.leader);
            let rpc_ctx = target.leader.clone().map(|peer| CopRpcContext {
                ver_id: ver_id.clone(),
                store_id: peer.store_id,
                peer,
                store_estimated_wait_time: Duration::ZERO,
                addr: String::new(),
            });
            let region_store = match self.pd_client.clone().map_region_to_store(target).await {
                Ok(rs) => rs,
                Err(e) => {
                    if let Some(store_id) = target_store_id {
                        if is_grpc_error(&e) {
                            check_store_liveness(&self.pd_client, store_id).await;
                        }
                        self.pd_client.invalidate_store_cache(store_id).await;
                    }
                    self.pd_client.invalidate_region_cache(ver_id.clone()).await;
                    let t = if is_grpc_error(&e) {
                        BackoffType::TikvRpc
                    } else {
                        BackoffType::RegionMiss
                    };
                    Self::backoff(bo, t, e).await?;
                    continue;
                }
            };

            let mut rpc = CoprocessorRpc {
                inner: req.clone(),
                timeout,
            };
            rpc.set_leader(&region_store.region_with_leader)?;
            if let Some((resolved, committed)) = self.lock_hints.get_all() {
                rpc.set_lock_hints(resolved, committed);
            }
            let stats = tikv_stats(rpc.label());
            let result: Result<coprocessor::Response> = stats
                .done(region_store.client.dispatch(&rpc).await)
                .map(|resp| {
                    *resp
                        .downcast()
                        .expect("Downcast failed: request and response type mismatch")
                });
            let mut resp = match result {
                Ok(resp) => resp,
                Err(e) if is_grpc_error(&e) => {
                    debug!("cop send: grpc error, region {:?}: {:?}", ver_id, e);
                    match grpc_error_action(&e) {
                        GrpcErrorAction::TryNextPeerAndInvalidate => {
                            if let Some(store_id) = target_store_id {
                                check_store_liveness(&self.pd_client, store_id).await;
                                self.pd_client.invalidate_store_cache(store_id).await;
                            }
                            self.pd_client.invalidate_region_cache(ver_id.clone()).await;
                        }
                        GrpcErrorAction::TryNextPeer => {}
                        GrpcErrorAction::Return => return Err(e),
                    }
                    Self::backoff(bo, BackoffType::TikvRpc, e).await?;
                    continue;
                }
                Err(e) => return Err(e),
            };

            if let Some(region_error) = resp.region_error.take() {
                debug!(
                    "cop send: region error, region {:?}: {:?}",
                    ver_id, region_error
                );
                // TiKV gave up at the `tikv_client_read_timeout` deadline: retry at once with
                // the default timeout (`onReadReqConfigurableTimeout`).
                let ctx = req.context.get_or_insert_with(Default::default);
                if ctx.max_execution_duration_ms > 0
                    && region_error
                        .server_is_busy
                        .as_ref()
                        .is_some_and(|b| b.reason.contains("deadline is exceeded"))
                {
                    ctx.max_execution_duration_ms = 0;
                    continue;
                }
                if !retried_in_place(&region_error) {
                    // Update the cache and hand the error back: the caller re-splits.
                    let _ = handle_region_error(
                        self.pd_client.clone(),
                        region_error.clone(),
                        region_store,
                    )
                    .await;
                    resp.region_error = Some(region_error);
                    return Ok(CopSendResult { resp, rpc_ctx });
                }
                let t = metrics::backoff_type_for_region_error(&region_error);
                let resolved =
                    handle_region_error(self.pd_client.clone(), region_error.clone(), region_store)
                        .await?;
                if !resolved {
                    Self::backoff(bo, t, Error::RegionError(Box::new(region_error))).await?;
                }
                continue;
            }

            if let Some(peer) = fallback_peer {
                // The peer took a leader request: it is the leader now (`onSendSuccess`).
                let _ = self.pd_client.update_leader(ver_id.clone(), peer).await;
            }
            return Ok(CopSendResult { resp, rpc_ctx });
        }
    }

    /// TiDB `copIteratorWorker.handleLockErr` minus the event callback: resolves `lock` for
    /// the read (a shared-lock wrapper through its members), records the lock hints, and
    /// waits for a live lock (`BackoffWithMaxSleepTxnLockFast(msBeforeExpired)`), all charged
    /// to `bo`.
    pub async fn resolve_lock(&self, bo: &mut Backoff, lock: kvrpcpb::LockInfo) -> Result<()> {
        let read = resolve_locks_for_read(
            expand_shared_lock(lock),
            self.timestamp.clone(),
            self.pd_client.clone(),
            self.keyspace,
            false,
            bo.clone(),
        )
        .await?;
        self.lock_hints.record(self.timestamp.version(), &read);
        if !read.live.is_empty() {
            let mut lock_backoff = LockBackoff::new(bo.clone());
            match lock_backoff.next_wait(read.ms_before_expired) {
                Some(wait) => wait.wait(self.pd_client.clone()).await?,
                None => return Err(lock_backoff.exhausted(Error::ResolveLockError(read.live))),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;

    fn sender(pd: MockPdClient) -> CopSender<MockPdClient> {
        CopSender::new(Arc::new(pd), Keyspace::Disable, Timestamp::default()).unwrap()
    }

    fn pd(
        f: impl Fn(&coprocessor::Request) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    ) -> MockPdClient {
        MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            f(req
                .downcast_ref::<coprocessor::Request>()
                .expect("cop request"))
        }))
    }

    fn ok(data: &str) -> Result<Box<dyn Any>> {
        Ok(Box::new(coprocessor::Response {
            data: data.as_bytes().to_vec(),
            ..Default::default()
        }))
    }

    fn region_err(e: errorpb::Error) -> Result<Box<dyn Any>> {
        Ok(Box::new(coprocessor::Response {
            region_error: Some(e),
            ..Default::default()
        }))
    }

    fn bo() -> Backoff {
        Backoff::time_budget(2000).start()
    }

    #[tokio::test]
    async fn test_send_success_sets_region_and_lock_hints() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let s = sender(pd(move |r| {
            let c = r.context.clone().unwrap();
            seen2
                .lock()
                .unwrap()
                .push((c.region_id, c.resolved_locks.clone()));
            ok("x")
        }));
        let region = MockPdClient::region1().ver_id();
        let res = s
            .send(
                &mut bo(),
                &region,
                coprocessor::Request::default(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(res.resp.data, b"x");
        assert!(res.resp.region_error.is_none());
        assert_eq!(res.rpc_ctx.unwrap().ver_id, region);
        assert_eq!(seen.lock().unwrap()[0], (region.id, vec![]));
    }

    #[tokio::test]
    async fn test_stale_epoch_returns_epoch_not_match() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = sender(pd(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            ok("x")
        }));
        let mut region = MockPdClient::region1().ver_id();
        region.ver += 1;
        let res = s
            .send(
                &mut bo(),
                &region,
                coprocessor::Request::default(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(res.resp.region_error.unwrap().epoch_not_match.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no RPC for a stale epoch");
    }

    #[tokio::test]
    async fn test_epoch_not_match_is_returned_to_the_caller() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = sender(pd(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            region_err(errorpb::Error {
                epoch_not_match: Some(Default::default()),
                ..Default::default()
            })
        }));
        let region = MockPdClient::region1().ver_id();
        let res = s
            .send(
                &mut bo(),
                &region,
                coprocessor::Request::default(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(res.resp.region_error.unwrap().epoch_not_match.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_server_busy_is_retried_in_place() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let s = sender(pd(move |_| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                region_err(errorpb::Error {
                    server_is_busy: Some(Default::default()),
                    ..Default::default()
                })
            } else {
                ok("y")
            }
        }));
        let region = MockPdClient::region1().ver_id();
        let res = s
            .send(
                &mut Backoff::time_budget(20_000).start(),
                &region,
                coprocessor::Request::default(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(res.resp.data, b"y");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_read_timeout_deadline_retries_without_deadline() {
        let deadlines = Arc::new(Mutex::new(Vec::new()));
        let d = deadlines.clone();
        let s = sender(pd(move |r| {
            let ms = r.context.as_ref().unwrap().max_execution_duration_ms;
            d.lock().unwrap().push(ms);
            if ms == 100 {
                region_err(errorpb::Error {
                    server_is_busy: Some(errorpb::ServerIsBusy {
                        reason: "deadline is exceeded".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            } else {
                ok("z")
            }
        }));
        let mut req = coprocessor::Request::default();
        req.context
            .get_or_insert_with(Default::default)
            .max_execution_duration_ms = 100;
        let region = MockPdClient::region1().ver_id();
        let res = s
            .send(&mut bo(), &region, req, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(res.resp.data, b"z");
        let d = deadlines.lock().unwrap().clone();
        assert_eq!(d[0], 100);
        // The retry has no deadline of its own: `CoprocessorRpc::dispatch` fills in the RPC
        // timeout on the wire (the mock sees the request before that).
        assert_eq!(d[1], 0);
    }

    #[tokio::test]
    async fn test_locate_key_ranges_each_region_once_in_order() {
        let s = sender(pd(|_| ok("")));
        let locs = s
            .locate_key_ranges(
                &mut bo(),
                &[(vec![1], vec![2]), (vec![3], vec![12]), (vec![20], vec![])],
            )
            .await
            .unwrap();
        let ids: Vec<u64> = locs.iter().map(|l| l.ver_id.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_rpc_context_of_cached_and_stale_epochs() {
        let s = sender(pd(|_| ok("")));
        let region = MockPdClient::region1();
        let ctx = s.rpc_context(&region.ver_id()).await.unwrap().unwrap();
        assert_eq!(ctx.store_id, region.leader.unwrap().store_id);
        let mut stale = region.region.clone();
        stale.region_epoch.as_mut().unwrap().version += 1;
        let stale_id = RegionVerId {
            id: stale.id,
            conf_ver: stale.region_epoch.as_ref().unwrap().conf_ver,
            ver: stale.region_epoch.as_ref().unwrap().version,
        };
        assert!(s.rpc_context(&stale_id).await.unwrap().is_none());
    }
}
