// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Metrics under client-go's and PD client's names (feature `client-go-metrics`).
//!
//! TiDB's Grafana dashboard reads the client side of TiKV and PD from the series Go TiDB's
//! client-go (`tidb_tikvclient_*`, namespace `tidb`, subsystem `tikvclient`) and PD client
//! (`pd_client_*`) export. With the feature enabled this crate records the same families, with
//! the same label names, label values and buckets, at the equivalent points, into the default
//! `prometheus` registry. Without it every function here returns at once and nothing is
//! registered (the crate's own `stats` metrics are unaffected either way).
//!
//! Sources: client-go `v2.0.8-0.20261020042752-6ea7c1662c95` (`metrics/metrics.go`,
//! `metrics/shortcuts.go`, `internal/client/client.go`, `internal/locate/region_request.go`,
//! `config/retry/config.go`, `txnkv/txnlock/lock_resolver.go`) and pd/client
//! `v0.0.0-20260804033407-85a975a5ca78` (`metrics/metrics.go`, `clients/tso/request.go`,
//! `clients/tso/stream.go`), the versions TiDB v8.5.8 pins.
//!
//! Recording on the request path touches only pre-resolved children (no label hashing or
//! locks): per store a fixed table of per-command histograms, created on first use.

use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;

use prometheus::exponential_buckets;
use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounter;
use prometheus::IntCounterVec;
use prometheus::Opts;

use crate::proto::errorpb;

const ENABLED: bool = cfg!(feature = "client-go-metrics");

fn histogram_vec(
    ns: &str,
    sub: &str,
    name: &str,
    help: &str,
    buckets: (f64, f64, usize),
    labels: &[&str],
) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace(ns)
            .subsystem(sub)
            .buckets(exponential_buckets(buckets.0, buckets.1, buckets.2).expect("buckets")),
        labels,
    )
    .expect("valid metric");
    let _ = prometheus::register(Box::new(h.clone()));
    h
}

fn counter_vec(ns: &str, sub: &str, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help).namespace(ns).subsystem(sub), labels)
        .expect("valid metric");
    let _ = prometheus::register(Box::new(c.clone()));
    c
}

// ---------------------------------------------------------------------------------------------
// TiKV RPCs.

/// client-go `tikvrpc.CmdType`, as named in the `type` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum CmdType {
    Get,
    Scan,
    Prewrite,
    PessimisticLock,
    PessimisticRollback,
    Commit,
    Cleanup,
    BatchGet,
    BatchRollback,
    ScanLock,
    ResolveLock,
    Gc,
    DeleteRange,
    RawGet,
    RawBatchGet,
    RawPut,
    RawBatchPut,
    RawDelete,
    RawBatchDelete,
    RawDeleteRange,
    RawScan,
    RawBatchScan,
    RawGetKeyTtl,
    RawCompareAndSwap,
    RawCoprocessor,
    UnsafeDestroyRange,
    Cop,
    CheckTxnStatus,
    CheckSecondaryLocks,
    TxnHeartBeat,
}

const CMD_COUNT: usize = CmdType::TxnHeartBeat as usize + 1;

impl CmdType {
    /// The label value (client-go `CmdType.String`; `RawBatchScan` and `RawCoprocessor` have
    /// no client-go counterpart and are named after the request).
    pub fn as_str(self) -> &'static str {
        match self {
            CmdType::Get => "Get",
            CmdType::Scan => "Scan",
            CmdType::Prewrite => "Prewrite",
            CmdType::PessimisticLock => "PessimisticLock",
            CmdType::PessimisticRollback => "PessimisticRollback",
            CmdType::Commit => "Commit",
            CmdType::Cleanup => "Cleanup",
            CmdType::BatchGet => "BatchGet",
            CmdType::BatchRollback => "BatchRollback",
            CmdType::ScanLock => "ScanLock",
            CmdType::ResolveLock => "ResolveLock",
            CmdType::Gc => "GC",
            CmdType::DeleteRange => "DeleteRange",
            CmdType::RawGet => "RawGet",
            CmdType::RawBatchGet => "RawBatchGet",
            CmdType::RawPut => "RawPut",
            CmdType::RawBatchPut => "RawBatchPut",
            CmdType::RawDelete => "RawDelete",
            CmdType::RawBatchDelete => "RawBatchDelete",
            CmdType::RawDeleteRange => "RawDeleteRange",
            CmdType::RawScan => "RawScan",
            CmdType::RawBatchScan => "RawBatchScan",
            CmdType::RawGetKeyTtl => "RawGetKeyTTL",
            CmdType::RawCompareAndSwap => "RawCompareAndSwap",
            CmdType::RawCoprocessor => "RawCoprocessor",
            CmdType::UnsafeDestroyRange => "UnsafeDestroyRange",
            CmdType::Cop => "Cop",
            CmdType::CheckTxnStatus => "CheckTxnStatus",
            CmdType::CheckSecondaryLocks => "CheckSecondaryLocks",
            CmdType::TxnHeartBeat => "TxnHeartBeat",
        }
    }
}

static REQUEST_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "tidb",
        "tikvclient",
        "request_seconds",
        "Bucketed histogram of sending request duration.",
        (0.0005, 2.0, 24),
        &["type", "store", "stale_read", "scope"],
    )
});

static RPC_NET_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "tidb",
        "tikvclient",
        "rpc_net_latency_seconds",
        "Bucketed histogram of time difference between TiDB and TiKV.",
        (0.0001, 2.0, 20),
        &["store", "scope"],
    )
});

/// client-go `storeMetrics`: the children of one store. No request here is stale or internal
/// (`stale_read="false"`, `scope="false"`).
struct StoreMetrics {
    store: String,
    by_cmd: [OnceLock<Histogram>; CMD_COUNT],
    net_latency: Histogram,
}

impl StoreMetrics {
    fn new(store_id: u64) -> StoreMetrics {
        let store = store_id.to_string();
        let m = StoreMetrics {
            net_latency: RPC_NET_LATENCY.with_label_values(&[&store, "false"]),
            by_cmd: [const { OnceLock::new() }; CMD_COUNT],
            store,
        };
        // client-go creates these three at once (`deriveRPCMetrics`), and the internal
        // net-latency child.
        for cmd in [CmdType::Get, CmdType::Cop, CmdType::BatchGet] {
            m.request(cmd);
        }
        RPC_NET_LATENCY.with_label_values(&[&m.store, "true"]);
        m
    }

    fn request(&self, cmd: CmdType) -> &Histogram {
        self.by_cmd[cmd as usize].get_or_init(|| {
            REQUEST_SECONDS.with_label_values(&[cmd.as_str(), &self.store, "false", "false"])
        })
    }
}

/// Stores with small ids (every id in a normal cluster) are cached here; larger ids take the
/// labelled lookup.
const CACHED_STORES: usize = 1024;
static STORES: [OnceLock<StoreMetrics>; CACHED_STORES] = [const { OnceLock::new() }; CACHED_STORES];

/// One TiKV RPC finished after `elapsed` (client-go `RPCClient.sendRequest` →
/// `storeMetrics.updateRPCMetrics`): `request_seconds`, and `rpc_net_latency_seconds` =
/// `elapsed` minus TiKV's `total_rpc_wall_time_ns` when the response reports it.
pub(crate) fn rpc(cmd: CmdType, store_id: u64, elapsed: Duration, tikv_wall_ns: u64) {
    if !ENABLED {
        return;
    }
    let net =
        (tikv_wall_ns > 0).then(|| elapsed.saturating_sub(Duration::from_nanos(tikv_wall_ns)));
    match STORES.get(store_id as usize) {
        Some(slot) => {
            let m = slot.get_or_init(|| StoreMetrics::new(store_id));
            m.request(cmd).observe(elapsed.as_secs_f64());
            if let Some(net) = net {
                m.net_latency.observe(net.as_secs_f64());
            }
        }
        None => {
            let store = store_id.to_string();
            REQUEST_SECONDS
                .with_label_values(&[cmd.as_str(), &store, "false", "false"])
                .observe(elapsed.as_secs_f64());
            if let Some(net) = net {
                RPC_NET_LATENCY
                    .with_label_values(&[&store, "false"])
                    .observe(net.as_secs_f64());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Region errors.

static REGION_ERR: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "tidb",
        "tikvclient",
        "region_err_total",
        "Counter of region errors.",
        &["type", "store"],
    )
});

/// client-go `regionErrorToLabel`.
pub(crate) fn region_error_label(e: &errorpb::Error) -> &'static str {
    if e.not_leader.is_some() {
        "not_leader"
    } else if e.region_not_found.is_some() {
        "region_not_found"
    } else if e.key_not_in_region.is_some() {
        "key_not_in_region"
    } else if e.epoch_not_match.is_some() {
        "epoch_not_match"
    } else if let Some(busy) = &e.server_is_busy {
        if busy.reason.contains("deadline is exceeded") {
            "deadline_exceeded"
        } else {
            "server_is_busy"
        }
    } else if e.stale_command.is_some() {
        "stale_command"
    } else if e.store_not_match.is_some() {
        "store_not_match"
    } else if e.raft_entry_too_large.is_some() {
        "raft_entry_too_large"
    } else if e.max_timestamp_not_synced.is_some() {
        "max_timestamp_not_synced"
    } else if e.read_index_not_ready.is_some() {
        "read_index_not_ready"
    } else if e.proposal_in_merging_mode.is_some() {
        "proposal_in_merging_mode"
    } else if e.data_is_not_ready.is_some() {
        "data_is_not_ready"
    } else if e.region_not_initialized.is_some() {
        "region_not_initialized"
    } else if e.disk_full.is_some() {
        "disk_full"
    } else if e.recovery_in_progress.is_some() {
        "recovery_in_progress"
    } else if e.flashback_in_progress.is_some() {
        "flashback_in_progress"
    } else if e.flashback_not_prepared.is_some() {
        "flashback_not_prepared"
    } else if e.is_witness.is_some() {
        "peer_is_witness"
    } else if e.message.contains("Deadline is exceeded") {
        "deadline_exceeded"
    } else if e.mismatch_peer_id.is_some() {
        "mismatch_peer_id"
    } else if e.bucket_version_not_match.is_some() {
        "bucket_version_not_match"
    } else {
        "unknown"
    }
}

/// A region error came back from `store_id` (client-go `RegionRequestSender.onRegionError`).
pub(crate) fn region_error(e: &errorpb::Error, store_id: u64) {
    if !ENABLED {
        return;
    }
    REGION_ERR
        .with_label_values(&[region_error_label(e), &store_id.to_string()])
        .inc();
}

// ---------------------------------------------------------------------------------------------
// Backoff.

/// client-go backoff configs, by their metric label (`metrics/shortcuts.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BackoffType {
    /// `BoTiKVRPC`: gRPC errors.
    TikvRpc,
    /// `BoTxnLockFast` (label `tikvLockFast`): waiting for a live lock on the read path.
    TikvLockFast,
    /// `BoRegionMiss`: region errors that need a region reload.
    RegionMiss,
    /// `BoRegionScheduling`: a region without a reachable leader.
    RegionScheduling,
    /// `BoTiKVServerBusy` (label `serverBusy`).
    ServerBusy,
    /// `BoStaleCmd`.
    StaleCommand,
    /// `BoTxnNotFound`, whose metric is client-go's `BackoffHistogramEmpty` (label `""`).
    TxnNotFound,
}

const BACKOFF_COUNT: usize = BackoffType::TxnNotFound as usize + 1;
const BACKOFF_LABELS: [&str; BACKOFF_COUNT] = [
    "tikvRPC",
    "tikvLockFast",
    "regionMiss",
    "regionScheduling",
    "serverBusy",
    "staleCommand",
    "",
];

static BACKOFF: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "tidb",
        "tikvclient",
        "backoff_seconds",
        "total backoff seconds of a single backoffer.",
        (0.0005, 2.0, 29),
        &["type"],
    )
});
static BACKOFF_CHILDREN: LazyLock<Vec<Histogram>> = LazyLock::new(|| {
    BACKOFF_LABELS
        .iter()
        .map(|l| BACKOFF.with_label_values(&[l]))
        .collect()
});

/// The backoff to record for a region error (client-go `onRegionError`'s choice of config).
pub(crate) fn backoff_type_for_region_error(e: &errorpb::Error) -> BackoffType {
    if e.server_is_busy.is_some() {
        BackoffType::ServerBusy
    } else if e.stale_command.is_some() {
        BackoffType::StaleCommand
    } else if e.not_leader.as_ref().is_some_and(|nl| nl.leader.is_none()) {
        BackoffType::RegionScheduling
    } else {
        BackoffType::RegionMiss
    }
}

/// About to sleep `delay` before a retry (client-go `Backoffer.BackoffWithCfgAndMaxSleep`
/// observes each sleep).
pub(crate) fn backoff(t: BackoffType, delay: Duration) {
    if !ENABLED {
        return;
    }
    BACKOFF_CHILDREN[t as usize].observe(delay.as_secs_f64());
}

// ---------------------------------------------------------------------------------------------
// Region cache.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RegionCacheOp {
    /// A key's region was not cached and was loaded from PD.
    GetRegionWhenMiss,
    GetRegionById,
    GetStore,
    /// An entry was dropped (only counted with `ok`).
    InvalidateRegionFromCache,
}

const REGION_CACHE_LABELS: [&str; 4] = [
    "get_region_when_miss",
    "get_region_by_id",
    "get_store",
    "invalidate_region_from_cache",
];

static REGION_CACHE_OPS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "tidb",
        "tikvclient",
        "region_cache_operations_total",
        "Counter of region cache.",
        &["type", "result"],
    )
});
/// `[ok, err][op]`.
static REGION_CACHE_CHILDREN: [[OnceLock<IntCounter>; 4]; 2] =
    [const { [const { OnceLock::new() }; 4] }; 2];

static LOAD_REGION_CACHE: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "tidb",
        "tikvclient",
        "load_region_cache_seconds",
        "Load region information duration",
        (0.0001, 2.0, 20),
        &["type"],
    )
});
static LOAD_REGION_CHILDREN: [OnceLock<Histogram>; 3] = [const { OnceLock::new() }; 3];

/// A region cache operation finished (client-go `RegionCacheCounterWith*`).
pub(crate) fn region_cache(op: RegionCacheOp, ok: bool) {
    if !ENABLED {
        return;
    }
    REGION_CACHE_CHILDREN[usize::from(!ok)][op as usize]
        .get_or_init(|| {
            REGION_CACHE_OPS.with_label_values(&[
                REGION_CACHE_LABELS[op as usize],
                if ok { "ok" } else { "err" },
            ])
        })
        .inc();
}

/// Loading region or store information from PD took `d` (client-go
/// `LoadRegionCacheHistogram*`, observed on success).
pub(crate) fn load_region(op: RegionCacheOp, d: Duration) {
    if !ENABLED || op == RegionCacheOp::InvalidateRegionFromCache {
        return;
    }
    LOAD_REGION_CHILDREN[op as usize]
        .get_or_init(|| LOAD_REGION_CACHE.with_label_values(&[REGION_CACHE_LABELS[op as usize]]))
        .observe(d.as_secs_f64());
}

// ---------------------------------------------------------------------------------------------
// Lock resolver.

/// client-go `LockResolverCountWith*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LockAction {
    /// `ResolveLocks` with at least one lock.
    Resolve,
    /// A lock whose transaction is finished (committed / rolled back) or expired.
    Expired,
    /// A lock of a live transaction.
    NotExpired,
    /// The caller has to wait for live locks.
    WaitExpired,
    /// `CheckTxnStatus` sent.
    QueryTxnStatus,
    QueryTxnStatusCommitted,
    QueryTxnStatusRolledBack,
    /// `CheckSecondaryLocks` sent.
    QueryCheckSecondaryLocks,
    /// `ResolveLock` sent.
    QueryResolveLocks,
    /// An expired async-commit lock's transaction is being resolved.
    ResolveAsyncCommit,
    /// Locks of several transactions resolved in one request (GC path).
    BatchResolve,
}

const LOCK_LABELS: [&str; 11] = [
    "resolve",
    "expired",
    "not_expired",
    "wait_expired",
    "query_txn_status",
    "query_txn_status_committed",
    "query_txn_status_rolled_back",
    "query_check_secondary_locks",
    "query_resolve_locks",
    "resolve_async_commit",
    "batch_resolve",
];

static LOCK_RESOLVER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "tidb",
        "tikvclient",
        "lock_resolver_actions_total",
        "Counter of lock resolver actions.",
        &["type"],
    )
});
static LOCK_CHILDREN: LazyLock<Vec<IntCounter>> = LazyLock::new(|| {
    LOCK_LABELS
        .iter()
        .map(|l| LOCK_RESOLVER.with_label_values(&[l]))
        .collect()
});

pub(crate) fn lock_resolver(a: LockAction) {
    if !ENABLED {
        return;
    }
    LOCK_CHILDREN[a as usize].inc();
}

// ---------------------------------------------------------------------------------------------
// Replica selector.

static REPLICA_SELECTOR_FAILURE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "tidb",
        "tikvclient",
        "replica_selector_failure_counter",
        "Counter of the reason why the replica selector cannot yield a potential leader.",
        &["type"],
    )
});

// ---------------------------------------------------------------------------------------------
// PD client.

/// pd/client `cmdDuration` / `cmdFailedDuration` types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PdCmd {
    /// Time a caller waited in `Wait()` for its timestamp.
    Wait,
    /// A timestamp, from request to result.
    Tso,
    /// Time between requesting a timestamp and starting to wait for it.
    TsoAsyncWait,
    GetRegion,
    GetRegionById,
    GetStore,
    GetAllStores,
    UpdateGcSafePoint,
    LoadKeyspace,
}

const PD_CMD_COUNT: usize = PdCmd::LoadKeyspace as usize + 1;
const PD_CMD_LABELS: [&str; PD_CMD_COUNT] = [
    "wait",
    "tso",
    "tso_async_wait",
    "get_region",
    "get_region_byid",
    "get_store",
    "get_all_stores",
    "update_gc_safe_point",
    "load_keyspace",
];

static PD_CMD: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "pd_client",
        "cmd",
        "handle_cmds_duration_seconds",
        "Bucketed histogram of processing time (s) of handled success cmds.",
        (0.0005, 2.0, 13),
        &["type"],
    )
});
static PD_CMD_FAILED: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "pd_client",
        "cmd",
        "handle_failed_cmds_duration_seconds",
        "Bucketed histogram of processing time (s) of failed handled cmds.",
        (0.0005, 2.0, 13),
        &["type"],
    )
});
static PD_CMD_CHILDREN: LazyLock<Vec<Histogram>> = LazyLock::new(|| {
    PD_CMD_LABELS
        .iter()
        .map(|l| PD_CMD.with_label_values(&[l]))
        .collect()
});
static PD_CMD_FAILED_CHILDREN: [OnceLock<Histogram>; PD_CMD_COUNT] =
    [const { OnceLock::new() }; PD_CMD_COUNT];

static PD_REQUEST: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "pd_client",
        "request",
        "handle_requests_duration_seconds",
        "Bucketed histogram of processing time (s) of handled requests.",
        (0.0005, 2.0, 13),
        &["type"],
    )
});
static PD_REQUEST_TSO: LazyLock<Histogram> =
    LazyLock::new(|| PD_REQUEST.with_label_values(&["tso"]));
static PD_REQUEST_TSO_FAILED: LazyLock<Histogram> =
    LazyLock::new(|| PD_REQUEST.with_label_values(&["tso-failed"]));

/// A PD command finished after `d`: successes in `handle_cmds_duration_seconds`, failures in
/// `handle_failed_cmds_duration_seconds`.
pub fn pd_cmd(cmd: PdCmd, d: Duration, ok: bool) {
    if !ENABLED {
        return;
    }
    if ok {
        PD_CMD_CHILDREN[cmd as usize].observe(d.as_secs_f64());
    } else {
        PD_CMD_FAILED_CHILDREN[cmd as usize]
            .get_or_init(|| PD_CMD_FAILED.with_label_values(&[PD_CMD_LABELS[cmd as usize]]))
            .observe(d.as_secs_f64());
    }
}

/// A timestamp future was waited for (pd/client `tso.Request.Wait`): `async_wait` from the
/// request to the start of the wait (`tso_async_wait`), `wait` the wait itself. The request's
/// own `tso` duration is recorded by the client when the timestamp arrives.
pub fn tso_wait(async_wait: Duration, wait: Duration) {
    pd_cmd(PdCmd::TsoAsyncWait, async_wait, true);
    pd_cmd(PdCmd::Wait, wait, true);
}

/// One TSO batch round trip on the stream (pd/client `tsoStream`: `requestDurationTSO`).
pub(crate) fn pd_request_tso(d: Duration, ok: bool) {
    if !ENABLED {
        return;
    }
    if ok {
        PD_REQUEST_TSO.observe(d.as_secs_f64());
    } else {
        PD_REQUEST_TSO_FAILED.observe(d.as_secs_f64());
    }
}

/// Register every family and the children Go creates at start-up, so a scrape shows them
/// before any request (and the dashboard lists them). No-op without `client-go-metrics`.
pub fn register() {
    if !ENABLED {
        return;
    }
    LazyLock::force(&REQUEST_SECONDS);
    LazyLock::force(&RPC_NET_LATENCY);
    LazyLock::force(&REGION_ERR);
    LazyLock::force(&BACKOFF_CHILDREN);
    for op in [
        RegionCacheOp::GetRegionWhenMiss,
        RegionCacheOp::GetRegionById,
        RegionCacheOp::GetStore,
    ] {
        REGION_CACHE_OPS.with_label_values(&[REGION_CACHE_LABELS[op as usize], "ok"]);
        REGION_CACHE_OPS.with_label_values(&[REGION_CACHE_LABELS[op as usize], "err"]);
        LOAD_REGION_CACHE.with_label_values(&[REGION_CACHE_LABELS[op as usize]]);
    }
    REGION_CACHE_OPS.with_label_values(&["invalidate_region_from_cache", "ok"]);
    LazyLock::force(&LOCK_CHILDREN);
    // There is no replica selector: client-go's failure reasons never occur (see the ADR).
    for t in ["exhausted", "invalid", "stale_store"] {
        REPLICA_SELECTOR_FAILURE.with_label_values(&[t]);
    }
    LazyLock::force(&PD_CMD_CHILDREN);
    // pd/client creates the failure children of these commands at start-up too.
    for cmd in [
        PdCmd::Tso,
        PdCmd::GetRegion,
        PdCmd::GetRegionById,
        PdCmd::GetStore,
        PdCmd::GetAllStores,
        PdCmd::UpdateGcSafePoint,
        PdCmd::LoadKeyspace,
    ] {
        PD_CMD_FAILED_CHILDREN[cmd as usize]
            .get_or_init(|| PD_CMD_FAILED.with_label_values(&[PD_CMD_LABELS[cmd as usize]]));
    }
    LazyLock::force(&PD_REQUEST_TSO);
    LazyLock::force(&PD_REQUEST_TSO_FAILED);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_error_labels_follow_client_go() {
        let e = errorpb::Error {
            not_leader: Some(Default::default()),
            ..Default::default()
        };
        assert_eq!(region_error_label(&e), "not_leader");
        assert_eq!(
            backoff_type_for_region_error(&e),
            BackoffType::RegionScheduling
        );
        let e = errorpb::Error {
            server_is_busy: Some(errorpb::ServerIsBusy {
                reason: "deadline is exceeded".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(region_error_label(&e), "deadline_exceeded");
        assert_eq!(backoff_type_for_region_error(&e), BackoffType::ServerBusy);
        assert_eq!(region_error_label(&errorpb::Error::default()), "unknown");
    }

    #[cfg(feature = "client-go-metrics")]
    #[test]
    fn records_under_client_go_names() {
        register();
        rpc(CmdType::Get, 1, Duration::from_micros(300), 100_000);
        rpc(CmdType::Cop, 5000, Duration::from_millis(2), 0);
        pd_cmd(PdCmd::Tso, Duration::from_micros(200), true);
        tso_wait(Duration::ZERO, Duration::from_micros(100));
        let families = prometheus::gather();
        let find = |name: &str| families.iter().find(|f| f.get_name() == name);
        for name in [
            "tidb_tikvclient_request_seconds",
            "tidb_tikvclient_rpc_net_latency_seconds",
            "tidb_tikvclient_backoff_seconds",
            "tidb_tikvclient_region_cache_operations_total",
            "tidb_tikvclient_load_region_cache_seconds",
            "tidb_tikvclient_lock_resolver_actions_total",
            "tidb_tikvclient_replica_selector_failure_counter",
            "pd_client_cmd_handle_cmds_duration_seconds",
            "pd_client_request_handle_requests_duration_seconds",
        ] {
            assert!(find(name).is_some(), "{name} not registered");
        }
        let req = find("tidb_tikvclient_request_seconds").unwrap();
        let labels: Vec<Vec<(String, String)>> = req
            .get_metric()
            .iter()
            .map(|m| {
                m.get_label()
                    .iter()
                    .map(|l| (l.get_name().to_owned(), l.get_value().to_owned()))
                    .collect()
            })
            .collect();
        let want = |t: &str, s: &str| {
            vec![
                ("scope".to_owned(), "false".to_owned()),
                ("stale_read".to_owned(), "false".to_owned()),
                ("store".to_owned(), s.to_owned()),
                ("type".to_owned(), t.to_owned()),
            ]
        };
        assert!(labels.contains(&want("Get", "1")), "{labels:?}");
        assert!(labels.contains(&want("Cop", "5000")), "{labels:?}");
    }
}
