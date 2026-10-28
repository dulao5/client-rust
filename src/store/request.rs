// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::proto::kvrpcpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::RegionWithLeader;
use crate::Error;
use crate::Result;

#[async_trait]
pub trait Request: Any + Sync + Send + 'static {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>>;
    fn label(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any;
    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()>;
    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion);
}

/// TiKV's own handling time of a request (ExecDetailsV2 `total_rpc_wall_time_ns`), which
/// client-go subtracts from the RPC time for `tidb_tikvclient_rpc_net_latency_seconds`; 0 if
/// the response does not report it.
pub(crate) trait TikvWallTime {
    fn tikv_wall_time_ns(&self) -> u64 {
        0
    }
}

pub(crate) fn exec_details_wall_time_ns(details: Option<&kvrpcpb::ExecDetailsV2>) -> u64 {
    let Some(d) = details else { return 0 };
    match (&d.time_detail_v2, &d.time_detail) {
        (Some(v2), _) => v2.total_rpc_wall_time_ns,
        (None, Some(v1)) => v1.total_rpc_wall_time_ns,
        (None, None) => 0,
    }
}

macro_rules! wall_time_from_exec_details {
    ($($resp: ident),*) => {
        $(impl TikvWallTime for kvrpcpb::$resp {
            fn tikv_wall_time_ns(&self) -> u64 {
                exec_details_wall_time_ns(self.exec_details_v2.as_ref())
            }
        })*
    };
}

macro_rules! no_wall_time {
    ($($resp: ident),*) => {
        $(impl TikvWallTime for kvrpcpb::$resp {})*
    };
}

wall_time_from_exec_details!(
    GetResponse,
    PrewriteResponse,
    CommitResponse,
    BatchGetResponse,
    BatchRollbackResponse,
    PessimisticRollbackResponse,
    ResolveLockResponse,
    ScanLockResponse,
    PessimisticLockResponse,
    TxnHeartBeatResponse,
    CheckTxnStatusResponse,
    CheckSecondaryLocksResponse
);
no_wall_time!(
    RawGetResponse,
    RawBatchGetResponse,
    RawGetKeyTtlResponse,
    RawPutResponse,
    RawBatchPutResponse,
    RawDeleteResponse,
    RawBatchDeleteResponse,
    RawScanResponse,
    RawBatchScanResponse,
    RawDeleteRangeResponse,
    RawCasResponse,
    RawCoprocessorResponse,
    ScanResponse,
    GcResponse,
    DeleteRangeResponse,
    UnsafeDestroyRangeResponse
);

/// The store a request is addressed to (`Context.peer.store_id`; client-go labels its RPC
/// metrics with it), 0 if not routed yet.
pub(crate) fn context_store_id(ctx: Option<&kvrpcpb::Context>) -> u64 {
    ctx.and_then(|c| c.peer.as_ref()).map_or(0, |p| p.store_id)
}

macro_rules! impl_request {
    ($name: ident, $resp: ident, $fun: ident, $label: literal, $cmd: ident) => {
        #[async_trait]
        impl Request for kvrpcpb::$name {
            async fn dispatch(
                &self,
                client: &TikvClient<Channel>,
                timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                let mut req = self.clone().into_request();
                req.set_timeout(timeout);
                let start = std::time::Instant::now();
                let result = client.clone().$fun(req).await;
                let wall_ns = result.as_ref().map_or(0, |r| {
                    <kvrpcpb::$resp as TikvWallTime>::tikv_wall_time_ns(r.get_ref())
                });
                crate::metrics::rpc(
                    crate::metrics::CmdType::$cmd,
                    context_store_id(self.context.as_ref()),
                    start.elapsed(),
                    wall_ns,
                );
                result
                    .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
                    .map_err(Error::from)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
                    region: leader.ver_id(),
                })?;
                ctx.region_id = leader.region.id;
                ctx.region_epoch = leader.region.region_epoch.clone();
                ctx.peer = Some(leader_peer.clone());
                Ok(())
            }

            fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
                let ctx = self.context.get_or_insert(kvrpcpb::Context::default());
                ctx.api_version = api_version.into();
            }
        }
    };
}

impl_request!(RawGetRequest, RawGetResponse, raw_get, "raw_get", RawGet);
impl_request!(
    RawBatchGetRequest,
    RawBatchGetResponse,
    raw_batch_get,
    "raw_batch_get",
    RawBatchGet
);
impl_request!(
    RawGetKeyTtlRequest,
    RawGetKeyTtlResponse,
    raw_get_key_ttl,
    "raw_get_key_ttl",
    RawGetKeyTtl
);
impl_request!(RawPutRequest, RawPutResponse, raw_put, "raw_put", RawPut);
impl_request!(
    RawBatchPutRequest,
    RawBatchPutResponse,
    raw_batch_put,
    "raw_batch_put",
    RawBatchPut
);
impl_request!(
    RawDeleteRequest,
    RawDeleteResponse,
    raw_delete,
    "raw_delete",
    RawDelete
);
impl_request!(
    RawBatchDeleteRequest,
    RawBatchDeleteResponse,
    raw_batch_delete,
    "raw_batch_delete",
    RawBatchDelete
);
impl_request!(
    RawScanRequest,
    RawScanResponse,
    raw_scan,
    "raw_scan",
    RawScan
);
impl_request!(
    RawBatchScanRequest,
    RawBatchScanResponse,
    raw_batch_scan,
    "raw_batch_scan",
    RawBatchScan
);
impl_request!(
    RawDeleteRangeRequest,
    RawDeleteRangeResponse,
    raw_delete_range,
    "raw_delete_range",
    RawDeleteRange
);
impl_request!(
    RawCasRequest,
    RawCasResponse,
    raw_compare_and_swap,
    "raw_compare_and_swap",
    RawCompareAndSwap
);
impl_request!(
    RawCoprocessorRequest,
    RawCoprocessorResponse,
    raw_coprocessor,
    "raw_coprocessor",
    RawCoprocessor
);

impl_request!(GetRequest, GetResponse, kv_get, "kv_get", Get);
impl_request!(ScanRequest, ScanResponse, kv_scan, "kv_scan", Scan);
impl_request!(
    PrewriteRequest,
    PrewriteResponse,
    kv_prewrite,
    "kv_prewrite",
    Prewrite
);
impl_request!(
    CommitRequest,
    CommitResponse,
    kv_commit,
    "kv_commit",
    Commit
);
impl_request!(
    BatchGetRequest,
    BatchGetResponse,
    kv_batch_get,
    "kv_batch_get",
    BatchGet
);
impl_request!(
    BatchRollbackRequest,
    BatchRollbackResponse,
    kv_batch_rollback,
    "kv_batch_rollback",
    BatchRollback
);
impl_request!(
    PessimisticRollbackRequest,
    PessimisticRollbackResponse,
    kv_pessimistic_rollback,
    "kv_pessimistic_rollback",
    PessimisticRollback
);
impl_request!(
    ResolveLockRequest,
    ResolveLockResponse,
    kv_resolve_lock,
    "kv_resolve_lock",
    ResolveLock
);
impl_request!(
    ScanLockRequest,
    ScanLockResponse,
    kv_scan_lock,
    "kv_scan_lock",
    ScanLock
);
impl_request!(
    PessimisticLockRequest,
    PessimisticLockResponse,
    kv_pessimistic_lock,
    "kv_pessimistic_lock",
    PessimisticLock
);
impl_request!(
    TxnHeartBeatRequest,
    TxnHeartBeatResponse,
    kv_txn_heart_beat,
    "kv_txn_heart_beat",
    TxnHeartBeat
);
impl_request!(
    CheckTxnStatusRequest,
    CheckTxnStatusResponse,
    kv_check_txn_status,
    "kv_check_txn_status",
    CheckTxnStatus
);
impl_request!(
    CheckSecondaryLocksRequest,
    CheckSecondaryLocksResponse,
    kv_check_secondary_locks,
    "kv_check_secondary_locks_request",
    CheckSecondaryLocks
);
impl_request!(GcRequest, GcResponse, kv_gc, "kv_gc", Gc);
impl_request!(
    DeleteRangeRequest,
    DeleteRangeResponse,
    kv_delete_range,
    "kv_delete_range",
    DeleteRange
);
impl_request!(
    UnsafeDestroyRangeRequest,
    UnsafeDestroyRangeResponse,
    unsafe_destroy_range,
    "unsafe_destroy_range",
    UnsafeDestroyRange
);
