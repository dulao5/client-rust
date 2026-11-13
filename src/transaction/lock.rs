// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;

use fail::fail_point;
use log::debug;
use log::error;
use log::info;
use log::warn;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::kv::HexRepr;
use crate::pd::PdClient;
use crate::Key;

use crate::metrics;
use crate::metrics::BackoffType;
use crate::metrics::LockAction as LockMetric;
use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::pdpb::Timestamp;
use crate::region::RegionVerId;
use crate::request::plan::handle_region_error;
use crate::request::plan::is_grpc_error;
use crate::request::CollectSingle;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::store::RegionStore;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::transaction::requests::new_check_secondary_locks_request;
use crate::transaction::requests::new_check_txn_status_request;
use crate::transaction::requests::CollectSecondaryLocks;
use crate::transaction::requests::SecondaryLocksStatus;
use crate::transaction::requests::TransactionStatus;
use crate::transaction::requests::TransactionStatusKind;
use crate::Error;
use crate::Result;

pub(crate) fn format_key_for_log(key: &[u8]) -> String {
    let prefix_len = key.len().min(16);
    format!("len={}, prefix={}", key.len(), HexRepr(&key[..prefix_len]))
}

/// Refuse to resolve a SHARED lock wrapper, as client-go does (`resolveLocks` /
/// `BatchResolveLocks`: "misuse ... trying to resolve a shared lock directly",
/// txnlock/lock_resolver.go:291, :605; `Lock.IsShared` is `Op_SharedLock`, :207).
///
/// The contract (`kvrpcpb.LockInfo.shared_lock_infos`) is explicit: a shared lock's real
/// holders live ONLY in `shared_lock_infos` — "DO NOT read from the wrapper LockInfo",
/// whose own `lock_version` is unset; resolving the wrapper would check transaction 0.
/// The one reader that expands a wrapper into its members is the coprocessor
/// ([`expand_shared_lock`], as TiDB's `copIteratorWorker.handleLockErr`); snapshot reads and
/// GC refuse it, as client-go. The members themselves are ordinary prewrite or pessimistic
/// locks (`SharedPessimisticLock` counts as pessimistic, [`is_pessimistic`]) and are not
/// refused.
///
/// TiKV v8.5.8 never returns a wrapper to a read: `check_ts_conflict` ignores shared locks
/// (components/txn_types/src/lock.rs:370-373, :431-434). Wrappers appear on write paths,
/// in `scan_lock`, and in a `PrimaryMismatch` from `CheckTxnStatus`.
pub(crate) fn reject_shared_locks(locks: &[kvrpcpb::LockInfo]) -> Result<()> {
    let wrapper = |l: &kvrpcpb::LockInfo| {
        !l.shared_lock_infos.is_empty() || l.lock_type == kvrpcpb::Op::SharedLock as i32
    };
    if locks.iter().any(wrapper) {
        return Err(Error::StringError(
            "misuse of resolve locks: trying to resolve a shared lock directly; its member \
             locks must be extracted and resolved one by one"
                .to_owned(),
        ));
    }
    Ok(())
}

/// The locks to resolve for a lock met by a coprocessor request: the members of a shared
/// lock wrapper, or the lock itself (TiDB `copIteratorWorker.handleLockErr`,
/// store/copr/coprocessor.go:1824-1831).
pub(crate) fn expand_shared_lock(lock: kvrpcpb::LockInfo) -> Vec<kvrpcpb::LockInfo> {
    if lock.shared_lock_infos.is_empty() {
        vec![lock]
    } else {
        lock.shared_lock_infos
    }
}

/// client-go `Lock.IsPessimistic` (txnlock/lock_resolver.go:201): a pessimistic lock, or the
/// pessimistic member of a shared lock.
pub(crate) fn is_pessimistic(lock: &kvrpcpb::LockInfo) -> bool {
    lock.lock_type == kvrpcpb::Op::PessimisticLock as i32
        || lock.lock_type == kvrpcpb::Op::SharedPessimisticLock as i32
}

/// _Resolves_ the given locks. Returns locks still live. When there is no live locks, all the given locks are resolved.
///
/// If a key has a lock, the latest status of the key is unknown. We need to "resolve" the lock,
/// which means the key is finally either committed or rolled back, before we read the value of
/// the key. We first use `CheckTxnStatus` to get the transaction's final status (committed or
/// rolled back), then use `ResolveLock` to resolve the remaining locks in the transaction.
///
/// An expired async-commit lock needs an extra step in between: TiKV refuses to roll it back
/// via `CheckTxnStatus`, so the transaction's final status is first recovered from all of its
/// secondary locks (`CheckSecondaryLocks`), and then every lock of the transaction is resolved.
/// [`resolve_locks`] that also reports how long until the first live lock expires
/// ([`ReadLocks::ms_before_expired`]), for callers that back off (client-go `ResolveLocks`
/// returns that TTL).
pub(crate) async fn resolve_locks_with_expiry(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<ReadLocks> {
    resolve_locks_impl(locks, timestamp, pd_client, keyspace, false, false).await
}

pub async fn resolve_locks(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    Ok(
        resolve_locks_impl(locks, timestamp, pd_client, keyspace, false, false)
            .await?
            .live,
    )
}

/// What [`resolve_locks_for_read`] found out about the encountered locks: client-go's
/// `ResolveLockResult` (txnlock/lock_resolver.go:415) without the TTL.
#[derive(Debug, Default)]
pub(crate) struct ReadLocks {
    /// Locks of transactions that are still alive and must be waited for.
    pub live: Vec<kvrpcpb::LockInfo>,
    /// Start timestamps of transactions whose locks the read may ignore (`canIgnore`, sent as
    /// `Context.resolved_locks`): rolled back, committed after the read's timestamp, or alive
    /// with their `min_commit_ts` pushed past it.
    pub ignorable: Vec<u64>,
    /// Start timestamps of transactions committed at or before the read's timestamp
    /// (`canAccess`, sent as `Context.committed_locks`): TiKV reads through their locks.
    pub accessible: Vec<u64>,
    /// Milliseconds until the first of the `live` locks expires, 0 if none or already
    /// expired: client-go `msBeforeTxnExpired` (`txnExpireTime`, lock_resolver.go:671), the
    /// `TTL` of `ResolveLockResult`. Callers cap each lock-backoff sleep with it
    /// ([`lock_backoff_sleep`]).
    pub ms_before_expired: i64,
}

impl ReadLocks {
    /// client-go `txnExpireTime.update` (lock_resolver.go:676): keep the minimum, negatives
    /// as 0.
    fn update_expire(&mut self, ms: i64) {
        let ms = ms.max(0);
        if self.live.is_empty() || ms < self.ms_before_expired {
            self.ms_before_expired = ms;
        }
    }
}

/// How long to sleep before retrying a read that met live locks, given what
/// [`resolve_locks_for_read`] / [`resolve_locks_with_expiry`] found: client-go
/// `bo.BackoffWithMaxSleepTxnLockFast(msBeforeExpired)` (txnsnapshot/snapshot.go:582, :927;
/// TiDB store/copr/coprocessor.go:1843), called only when `msBeforeExpired > 0`. The next
/// delay of `backoff`, capped at the time left until the first live lock expires; zero, and
/// no attempt spent, when that time is 0 (retry at once, as client-go). `None` when the
/// backoff is exhausted.
///
/// One difference: client-go's budget is total sleep time, so capped sleeps use up less of
/// it; `Backoff` counts attempts, so each capped sleep still spends one.
pub(crate) fn lock_backoff_sleep(
    backoff: &mut Backoff,
    ms_before_expired: i64,
) -> Option<Duration> {
    if ms_before_expired <= 0 {
        return Some(Duration::ZERO);
    }
    let cap = Duration::from_millis(ms_before_expired as u64);
    backoff.next_delay_duration().map(|d| d.min(cap))
}

/// The lock hints of a snapshot or of one coprocessor request: client-go's pair of
/// `util.TSSet`s, `KVSnapshot.resolvedLocks` / `committedLocks`
/// (txnsnapshot/snapshot.go:124) and TiDB's `copIterator.resolvedLocks` / `committedLocks`
/// (store/copr/coprocessor.go:708). A read that resolves locks for read adds the
/// transactions it may ignore or read through; every later request of the same snapshot (or
/// coprocessor request) sends the whole sets in `Context.resolved_locks` /
/// `Context.committed_locks`, as `ClientHelper.SendReqCtx` does with `GetAll()`
/// (txnsnapshot/client_helper.go). Cheap to clone: the sets are shared.
///
/// **The sets are only valid for one fixed read timestamp.** Whether a transaction may be
/// ignored (committed after the read, min_commit_ts pushed past it) or read through
/// (committed at or before it) depends on the read's timestamp, so the hints must never be
/// shared by reads at different timestamps: a future read-committed mode, a pessimistic
/// `for_update_ts` read or a stale read needs its own hints. The timestamp is recorded at
/// creation and checked (`debug_assert`) whenever results are recorded.
#[derive(Clone, Debug)]
pub struct ReadLockHints {
    read_ts: u64,
    sets: Arc<std::sync::RwLock<HintSets>>,
}

#[derive(Debug, Default)]
struct HintSets {
    resolved: HashSet<u64>,
    committed: HashSet<u64>,
}

impl ReadLockHints {
    /// Empty hints for reads at `read_ts`.
    pub fn new(read_ts: u64) -> ReadLockHints {
        ReadLockHints {
            read_ts,
            sets: Arc::default(),
        }
    }

    /// The read timestamp these hints are valid for.
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    /// Add transactions whose locks the read may ignore (`resolvedLocks.Put`).
    pub fn put_resolved(&self, txns: impl IntoIterator<Item = u64>) {
        let mut sets = self.sets.write().unwrap_or_else(|e| e.into_inner());
        sets.resolved.extend(txns);
    }

    /// Add transactions the read may read through (`committedLocks.Put`).
    pub fn put_committed(&self, txns: impl IntoIterator<Item = u64>) {
        let mut sets = self.sets.write().unwrap_or_else(|e| e.into_inner());
        sets.committed.extend(txns);
    }

    /// Record what [`resolve_locks_for_read`] found at `read_ts` (client-go
    /// `ClientHelper.ResolveLocksWithOpts`). Done before the read retries, so the retry
    /// already carries them; the cleanup itself may still be running in the background.
    pub(crate) fn record(&self, read_ts: u64, locks: &ReadLocks) {
        debug_assert_eq!(
            read_ts, self.read_ts,
            "lock hints recorded by a read at another timestamp"
        );
        if locks.ignorable.is_empty() && locks.accessible.is_empty() {
            return;
        }
        let mut sets = self.sets.write().unwrap_or_else(|e| e.into_inner());
        sets.resolved.extend(locks.ignorable.iter().copied());
        sets.committed.extend(locks.accessible.iter().copied());
    }

    /// Both sets (sorted), or `None` while both are empty (`GetAll()`).
    pub fn get_all(&self) -> Option<(Vec<u64>, Vec<u64>)> {
        let sets = self.sets.read().unwrap_or_else(|e| e.into_inner());
        if sets.resolved.is_empty() && sets.committed.is_empty() {
            return None;
        }
        let mut resolved: Vec<u64> = sets.resolved.iter().copied().collect();
        let mut committed: Vec<u64> = sets.committed.iter().copied().collect();
        resolved.sort_unstable();
        committed.sort_unstable();
        Some((resolved, committed))
    }
}

/// [`resolve_locks`] for a read: client-go's `resolveLocks` with `ForRead` (lock_resolver.go:
/// 497, reached through `ClientHelper.ResolveLocksWithOpts`). The transactions of the
/// encountered locks are classified as in lock_resolver.go:624-639, and the read may then
/// ignore or read through their locks instead of waiting (see [`ReadLocks`]). The locks of
/// transactions whose fate is settled are resolved in the background
/// ([`resolve_in_background`], a `tokio::spawn`ed task: call this from within a Tokio
/// runtime), as client-go does for reads; stale pessimistic locks are
/// still rolled back before returning (client-go: "pessimistic locks don't block read so it
/// needn't be async", lock_resolver.go:572).
///
/// TiDB's autocommit point get at `u64::MAX` uses this with `timestamp = u64::MAX`, as
/// client-go does (`KVSnapshot.get` goes through the same `ClientHelper`): `CheckTxnStatus`
/// then answers `MinCommitTsPushed` for a live transaction that is not committed, and every
/// committed one can be read through.
///
/// `lite`: client-go's `ResolveLocksOptions.Lite`, set by a snapshot's point get
/// (`NewClientHelper(.., resolveLite = true)`, txnsnapshot/snapshot.go:786): resolve only the
/// encountered key instead of the whole region (see [`RESOLVE_LOCK_LITE_THRESHOLD`]).
pub(crate) async fn resolve_locks_for_read(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    lite: bool,
) -> Result<ReadLocks> {
    resolve_locks_impl(locks, timestamp, pd_client, keyspace, true, lite).await
}

async fn resolve_locks_impl(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    for_read: bool,
    lite: bool,
) -> Result<ReadLocks> {
    debug!("resolving locks");
    reject_shared_locks(&locks)?;
    if !locks.is_empty() {
        metrics::lock_resolver(LockMetric::Resolve);
    }
    let ts = pd_client.clone().get_timestamp().await?;
    let caller_start_ts = timestamp.version();
    let current_ts = ts.version();

    let mut out = ReadLocks::default();
    // Final transaction statuses are cached in the resolver's context, so a transaction met
    // through several locks is queried (or recovered) only once.
    let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
    // Regions already swept by ResolveLock, per transaction.
    let mut clean_regions: HashMap<u64, HashSet<RegionVerId>> = HashMap::new();
    // We must check txn status for *all* locks, not only TTL-expired ones.
    //
    // TTL only indicates whether a lock is *possibly* orphaned; it does not mean the transaction
    // is still running. A transaction may already be committed/rolled back while its locks are
    // still visible (e.g. cleanup/resolve hasn't finished, retries after region errors, etc.).
    // If we only resolve TTL-expired locks, we can unnecessarily sleep/backoff until TTL even
    // though `CheckTxnStatus` would already report `Committed`/`RolledBack`.
    //
    // This matches the client-go `LockResolver.ResolveLocksWithOpts` flow: query txn status for
    // each encountered lock, then resolve immediately when the status is final.
    for lock in locks {
        let (commit_version, keys) = match lock_resolver
            .action_for_lock(
                &lock,
                caller_start_ts,
                current_ts,
                pd_client.clone(),
                keyspace,
            )
            .await?
        {
            LockAction::Resolve {
                commit_version,
                keys,
            } => {
                metrics::lock_resolver(LockMetric::Expired);
                if for_read {
                    // lock_resolver.go:624-634: rolled back, or committed after the read →
                    // ignore; committed at or before it → read through.
                    let verdict = if commit_version == 0 || commit_version > caller_start_ts {
                        ReadVerdict::Ignore
                    } else {
                        ReadVerdict::Access
                    };
                    out.classify(lock.lock_version, verdict);
                }
                (commit_version, keys)
            }
            LockAction::Live {
                primary_lock,
                ttl,
                min_commit_ts_pushed,
            } => {
                // lock_resolver.go:616-624: a live transaction is waited for, unless the read
                // may ignore it because its min_commit_ts was pushed past the read.
                if for_read && min_commit_ts_pushed {
                    out.ignorable.push(lock.lock_version);
                } else {
                    metrics::lock_resolver(LockMetric::NotExpired);
                    // lock_resolver.go:619 / :637: `UntilExpired(l.TxnID, status.ttl)`.
                    out.update_expire(lock_until_expired_ms(lock.lock_version, ttl, ts.clone()));
                    out.live.push(primary_lock);
                }
                continue;
            }
            LockAction::Done { read } => {
                metrics::lock_resolver(LockMetric::Expired);
                if for_read {
                    out.classify(lock.lock_version, read);
                }
                continue;
            }
        };
        if for_read {
            // A read does not wait for the cleanup: it already knows how to treat the
            // transaction's locks (client-go lock_resolver.go:584-596, and :1092 for an
            // async-commit transaction, `asyncResolveAll = forRead`).
            resolve_in_background(
                &lock,
                keys,
                commit_version,
                lite,
                pd_client.clone(),
                keyspace,
            );
            continue;
        }
        let regions = clean_regions.entry(lock.lock_version).or_default();
        let plan = ResolvePlan::new(&lock, keys, lite);
        for (key, lite) in plan.requests() {
            resolve_lock_with_retry(
                key.into(),
                lock.lock_version,
                commit_version,
                lock.is_txn_file,
                lite,
                pd_client.clone(),
                keyspace,
                OPTIMISTIC_BACKOFF,
                regions,
            )
            .await?;
        }
    }
    if !out.live.is_empty() {
        metrics::lock_resolver(LockMetric::WaitExpired);
    }
    Ok(out)
}

impl ReadLocks {
    // glue: lock_resolver.go:624-639 (the canIgnore / canAccess lists)
    fn classify(&mut self, txn: u64, verdict: ReadVerdict) {
        match verdict {
            ReadVerdict::Ignore => self.ignorable.push(txn),
            ReadVerdict::Access => self.accessible.push(txn),
            ReadVerdict::Neither => {}
        }
    }
}

/// How a read may treat the locks of a transaction whose lock was dealt with on its own
/// (lock_resolver.go:624-639).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadVerdict {
    /// `canIgnore`.
    Ignore,
    /// `canAccess`.
    Access,
    /// Neither: the read retries and meets the lock again (lock_resolver.go:635, with no
    /// time to wait).
    Neither,
}

impl ReadVerdict {
    /// The classification of a settled status for a read at `caller_start_ts`.
    // glue: lock_resolver.go:624-639
    fn of(status: &TransactionStatus, caller_start_ts: u64) -> ReadVerdict {
        match &status.kind {
            TransactionStatusKind::Committed(ts) if ts.version() > caller_start_ts => {
                ReadVerdict::Ignore
            }
            TransactionStatusKind::Committed(_) => ReadVerdict::Access,
            // `IsRolledBack` (lock_resolver.go:127) is `is_determined` for this kind.
            TransactionStatusKind::RolledBack if status.is_determined() => ReadVerdict::Ignore,
            _ if status.action == kvrpcpb::Action::MinCommitTsPushed => ReadVerdict::Ignore,
            _ => ReadVerdict::Neither,
        }
    }
}

/// What `resolve_locks` has to do about one encountered lock.
enum LockAction {
    /// The transaction's fate is known: resolve its locks with `commit_version` (zero rolls
    /// back) in the regions of `keys` and of the encountered lock itself.
    Resolve {
        commit_version: u64,
        keys: Vec<Vec<u8>>,
    },
    /// The transaction is still alive; its primary lock is reported back to the caller.
    /// `min_commit_ts_pushed`: `CheckTxnStatus` answered `MinCommitTsPushed`.
    Live {
        primary_lock: kvrpcpb::LockInfo,
        /// The TTL `CheckTxnStatus` answered (client-go `status.ttl`).
        ttl: u64,
        min_commit_ts_pushed: bool,
    },
    /// The lock has been dealt with on its own (a stale pessimistic lock rolled back by
    /// itself) and nothing else needs resolving. `read`: how a read may treat the
    /// transaction's locks.
    Done { read: ReadVerdict },
}

impl LockAction {
    /// The action for a transaction whose `CheckTxnStatus` answer is taken as is.
    fn from_status(status: &TransactionStatus) -> Self {
        match &status.kind {
            TransactionStatusKind::Committed(ts) => LockAction::Resolve {
                commit_version: ts.version(),
                keys: Vec::new(),
            },
            TransactionStatusKind::RolledBack => LockAction::Resolve {
                commit_version: 0,
                keys: Vec::new(),
            },
            TransactionStatusKind::Locked(ttl, primary_lock) => LockAction::Live {
                primary_lock: primary_lock.clone(),
                ttl: *ttl,
                min_commit_ts_pushed: status.action == kvrpcpb::Action::MinCommitTsPushed,
            },
        }
    }
}

/// client-go `TiKVClient.ResolveLockLiteThreshold` (config/client.go:193, default 16): a lock
/// of a transaction with fewer keys is resolved "lite", for its own key only
/// (lock_resolver.go:1208). TiDB's `tikv-client.resolve-lock-lite-threshold`; not
/// configurable here yet.
pub(crate) const RESOLVE_LOCK_LITE_THRESHOLD: u64 = 16;

/// Backoff of a background lock cleanup: client-go gives it its own backoffer of
/// `asyncResolveLockMaxBackoff` = 40 s (lock_resolver.go:48, :586, :1093). Here an attempt
/// budget with about the same total sleep: 2, 4, ..., 256 ms, then 80 × 500 ms ≈ 40.5 s.
pub(crate) const ASYNC_RESOLVE_LOCK_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 88);

/// Which `ResolveLock` requests settle a transaction met through `lock`: client-go
/// `resolveLock` (lock_resolver.go:1204-1271) for a 2PC transaction, or every key of an
/// async-commit one (`keys`, from its primary; region-wide, see R8 in tidb-rs's
/// specs/D07.1-store/lock-resolver.md).
struct ResolvePlan {
    keys: Vec<Vec<u8>>,
    /// The encountered key, unless it needs no request.
    key: Option<Vec<u8>>,
    /// Resolve `key` alone (`Keys = [l.Key]`) instead of its whole region.
    lite: bool,
}

impl ResolvePlan {
    // glue: client-go resolveLock's lite decision
    fn new(lock: &kvrpcpb::LockInfo, keys: Vec<Vec<u8>>, lite: bool) -> ResolvePlan {
        // lock_resolver.go:1208: `resolveLite := lite || l.TxnSize < resolveLockLiteThreshold`
        // (only for a 2PC transaction: async commit resolves every key it recovered).
        let lite = keys.is_empty() && (lite || lock.txn_size < RESOLVE_LOCK_LITE_THRESHOLD);
        // :1210: "The lock has been resolved by getTxnStatusFromLock": CheckTxnStatus on the
        // primary already committed or rolled it back.
        let key = (!(lite && lock.key == lock.primary_lock)).then(|| lock.key.clone());
        ResolvePlan { keys, key, lite }
    }

    /// (key, lite) of each request, in order.
    // glue: client-go resolveLock / resolveAsyncResolveData
    fn requests(&self) -> impl Iterator<Item = (&Vec<u8>, bool)> {
        self.keys
            .iter()
            .map(|k| (k, false))
            .chain(self.key.iter().map(move |k| (k, self.lite)))
    }
}

/// Resolve the locks of a settled transaction in a background task, for a read
/// (client-go: the goroutines of lock_resolver.go:587 and :1094). `keys` are the other keys of
/// an async-commit transaction (empty otherwise); the encountered lock's key is always
/// resolved. The task does not share the read's cleaned regions (client-go passes an empty
/// map, lock_resolver.go:588) and only logs its failure: the read has already moved on, and
/// a later reader resolves again.
///
/// The task is started with `tokio::spawn`, so this must be called from within a Tokio
/// runtime (every client-rust request path is). Unlike client-go there is no
/// `LockResolver.Close` to cancel these tasks; each ends when its backoff runs out (about
/// 40 s at most) or with the runtime.
// glue: client-go `go func() { lr.resolveLock(asyncBo, ...) }()`
fn resolve_in_background(
    lock: &kvrpcpb::LockInfo,
    keys: Vec<Vec<u8>>,
    commit_version: u64,
    lite: bool,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) {
    let start_version = lock.lock_version;
    let is_txn_file = lock.is_txn_file;
    let plan = ResolvePlan::new(lock, keys, lite);
    tokio::spawn(async move {
        let mut regions = HashSet::new();
        for (k, lite) in plan.requests() {
            if let Err(e) = resolve_lock_with_retry(
                k.into(),
                start_version,
                commit_version,
                is_txn_file,
                lite,
                pd_client.clone(),
                keyspace,
                ASYNC_RESOLVE_LOCK_BACKOFF,
                &mut regions,
            )
            .await
            {
                info!(
                    "failed to resolve lock asynchronously, start_ts: {}, commit_ts: {}, error: {:?}",
                    start_version, commit_version, e
                );
                return;
            }
        }
    });
}

/// Roll back one pessimistic lock, leaving the rest of its transaction alone. This is
/// client-go's `resolvePessimisticLock`.
async fn pessimistic_rollback_lock(
    lock: &kvrpcpb::LockInfo,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<()> {
    if lock.key == lock.primary_lock {
        // The lock is its transaction's primary: the CheckTxnStatus just sent to this very
        // key, with `resolving_pessimistic_lock` set, has already unlocked it if it had
        // expired. If TiKV reported a different primary instead, the lock at this key has
        // been re-acquired by the transaction and is legitimate.
        return Ok(());
    }
    let for_update_ts = if lock.lock_for_update_ts == 0 {
        u64::MAX
    } else {
        lock.lock_for_update_ts
    };
    let req = requests::new_pessimistic_rollback_request(
        vec![lock.key.clone()],
        lock.lock_version,
        for_update_ts,
    );
    let plan = crate::request::PlanBuilder::new(pd_client, keyspace, req)
        .retry_multi_region(DEFAULT_REGION_BACKOFF)
        .extract_error()
        .plan();
    plan.execute().await?;
    Ok(())
}

/// Resolve every lock of transaction `start_version` in the region holding `key`, unless that
/// region is already in `clean_regions`. ResolveLock sweeps the whole region, so one successful
/// request per region is enough no matter how many of the transaction's keys point to it.
///
/// `lite`: resolve `key` alone (`ResolveLockRequest.keys = [key]`, client-go
/// lock_resolver.go:1230-1235); the region is then not recorded as clean (:1267).
#[allow(clippy::too_many_arguments)]
async fn resolve_lock_with_retry(
    key: &Key,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    lite: bool,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    mut backoff: Backoff,
    clean_regions: &mut HashSet<RegionVerId>,
) -> Result<()> {
    debug!("resolving locks with retry");
    let mut attempt = 0;
    loop {
        attempt += 1;
        debug!("resolving locks: attempt {}", attempt);
        let store = pd_client.clone().store_for_key(key).await?;
        let ver_id = store.region_with_leader.ver_id();
        if clean_regions.contains(&ver_id) {
            return Ok(());
        }
        let mut request =
            requests::new_resolve_lock_request(start_version, commit_version, is_txn_file);
        if lite {
            metrics::lock_resolver(LockMetric::ResolveLockLite);
            request.keys = vec![key.to_vec()];
        }
        metrics::lock_resolver(LockMetric::QueryResolveLocks);
        let plan_builder =
            match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
                .single_region_with_store(store.clone())
                .await
            {
                Ok(plan_builder) => plan_builder,
                Err(Error::LeaderNotFound { region }) => {
                    pd_client.invalidate_region_cache(region.clone()).await;
                    match backoff.next_delay_duration() {
                        Some(duration) => {
                            metrics::backoff(BackoffType::RegionScheduling, duration);
                            sleep(duration).await;
                            continue;
                        }
                        None => return Err(Error::LeaderNotFound { region }),
                    }
                }
                Err(err) => return Err(err),
            };
        let plan = plan_builder.extract_error().plan();
        let sent = async { plan.execute().await.map(|_| ()) };
        // R12: concurrent region-wide resolves of one transaction in one region are sent once
        // (client-go `reqCollapse`; lite requests carry keys and are never collapsed).
        let result = match pd_client.lock_resolver_state() {
            Some(state) if !lite => state.collapse_resolve(ver_id.id, start_version, sent).await,
            _ => sent.await,
        };
        match result {
            Ok(()) => {
                if !lite {
                    clean_regions.insert(ver_id);
                }
                return Ok(());
            }
            // Retry on region error. `ResolveLockResponse` can have at most 1 error, and
            // the variant now says which kind it is rather than requiring a type test on
            // a popped element.
            Err(Error::MultipleRegionErrors(mut errors)) => match errors.pop() {
                Some(Error::RegionError(e)) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        let backoff_type = metrics::backoff_type_for_region_error(&e);
                        let region_error_resolved =
                            handle_region_error(pd_client.clone(), *e, store.clone()).await?;
                        if !region_error_resolved {
                            metrics::backoff(backoff_type, duration);
                            sleep(duration).await;
                        }
                        continue;
                    }
                    None => return Err(Error::RegionError(e)),
                },
                Some(e) => return Err(e),
                None => unreachable!(),
            },
            Err(Error::MultipleKeyErrors(mut errors)) => match errors.pop() {
                Some(Error::KeyError(key_err)) => {
                    // Keyspace is not truncated here because we need full key info for logging.
                    error!(
                        "resolve_lock error, unexpected resolve err: {:?}, lock: {{key: {}, start_version: {}, commit_version: {}, is_txn_file: {}}}",
                        key_err,
                        format_key_for_log(key),
                        start_version,
                        commit_version,
                        is_txn_file,
                    );
                    return Err(Error::KeyError(key_err));
                }
                Some(e) => return Err(e),
                None => unreachable!(),
            },
            Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                Some(duration) => {
                    pd_client.invalidate_region_cache(ver_id.clone()).await;
                    if let Ok(store_id) = store.region_with_leader.get_store_id() {
                        pd_client.invalidate_store_cache(store_id).await;
                    }
                    metrics::backoff(BackoffType::TikvRpc, duration);
                    sleep(duration).await;
                    continue;
                }
                None => return Err(e),
            },
            Err(e) => return Err(e),
        }
    }
}

/// client-go `ResolvedCacheSize` (txnlock/lock_resolver.go:44).
pub const RESOLVED_CACHE_SIZE: usize = 2048;

/// The lock-resolution state that client-go keeps per `KVStore`, shared by every request of
/// one cluster client (reached through [`PdClient::lock_resolver_state`]):
///
/// - the settled transaction statuses, `LockResolver.mu.resolved` with its FIFO of
///   [`RESOLVED_CACHE_SIZE`] (txnlock/lock_resolver.go:61-76, `saveResolved` :236,
///   `getResolved` :265) — R6;
/// - the region-wide `ResolveLock` requests in flight, client-go's `resolveRegionSf`
///   singleflight (internal/client/client_collapse.go:82, keyed by region and start_ts),
///   so that concurrent readers of one locked region send one request — R12.
#[derive(Default)]
pub struct LockResolverState {
    resolved: std::sync::Mutex<ResolvedCache>,
    in_flight: std::sync::Mutex<HashMap<(u64, u64), InFlight>>,
}

type InFlight = futures::future::Shared<futures::future::BoxFuture<'static, bool>>;

#[derive(Default)]
struct ResolvedCache {
    map: HashMap<u64, Arc<TransactionStatus>>,
    order: std::collections::VecDeque<u64>,
}

impl LockResolverState {
    /// client-go `getResolved`.
    pub fn get_resolved(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        let cache = self.resolved.lock().unwrap_or_else(|e| e.into_inner());
        cache.map.get(&txn_id).cloned()
    }

    /// client-go `saveResolved` (lock_resolver.go:236-263): only settled statuses; a second
    /// save of a transaction must agree with the first (`HasSameDeterminedStatus`). As in
    /// client-go v8.5.8 (c3b5079), a violation panics in every build: the cache would
    /// otherwise make reads resolve the transaction's locks both ways.
    pub fn save_resolved(&self, txn_id: u64, status: Arc<TransactionStatus>) {
        if !status.is_determined() {
            error!(
                "unexpected undetermined status saved to cache, txn_id: {}, status: {:?}",
                txn_id, status
            );
            panic!("unexpected undetermined status saved to cache");
        }
        let mut cache = self.resolved.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(saved) = cache.map.get(&txn_id) {
            if !saved.has_same_determined_status(&status) {
                error!(
                    "unexpected txn status saving to the cache, the existing status is not equal to the new one, txn_id: {}, existing: {:?}, new: {:?}",
                    txn_id, saved, status
                );
                panic!("unexpected txn status saved to cache with existing different entry");
            }
            return;
        }
        cache.map.insert(txn_id, status);
        cache.order.push_back(txn_id);
        if cache.map.len() > RESOLVED_CACHE_SIZE {
            if let Some(front) = cache.order.pop_front() {
                cache.map.remove(&front);
            }
        }
    }

    /// Run `send` (a region-wide `ResolveLock` of transaction `start_version` in region
    /// `region_id`) unless the same request is already in flight; then wait for that one
    /// instead (client-go `resolveRegionSf.DoChan`). Only success is shared: if the request
    /// in flight fails, a waiting caller sends its own, so it sees its own error (client-rust
    /// errors are not `Clone`; client-go shares the error).
    pub(crate) async fn collapse_resolve<F>(
        &self,
        region_id: u64,
        start_version: u64,
        send: F,
    ) -> Result<()>
    where
        F: Future<Output = Result<()>>,
    {
        let key = (region_id, start_version);
        let waiting = {
            let in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            in_flight.get(&key).cloned()
        };
        if let Some(f) = waiting {
            if f.await {
                return Ok(());
            }
            return send.await;
        }
        let (tx, rx) = futures::channel::oneshot::channel::<bool>();
        let shared: InFlight = rx.map(|r| r.unwrap_or(false)).boxed().shared();
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, shared);
        // Removes the entry even if this future is dropped mid-request; waiters then see a
        // dropped sender (false) and send their own.
        let _guard = InFlightGuard { state: self, key };
        let result = send.await;
        let _ = tx.send(result.is_ok());
        result
    }
}

struct InFlightGuard<'a> {
    state: &'a LockResolverState,
    key: (u64, u64),
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.state
            .in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

#[derive(Default, Clone)]
pub struct ResolveLocksContext {
    // Record the status of each transaction.
    pub(crate) resolved: Arc<RwLock<HashMap<u64, Arc<TransactionStatus>>>>,
    pub(crate) clean_regions: Arc<RwLock<HashMap<u64, HashSet<RegionVerId>>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolveLocksOptions {
    pub async_commit_only: bool,
    pub batch_size: u32,
}

impl Default for ResolveLocksOptions {
    fn default() -> Self {
        Self {
            async_commit_only: false,
            batch_size: 1024,
        }
    }
}

impl ResolveLocksContext {
    pub async fn get_resolved(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        self.resolved.read().await.get(&txn_id).cloned()
    }

    pub async fn save_resolved(&mut self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
        self.resolved.write().await.insert(txn_id, txn_status);
    }

    pub async fn is_region_cleaned(&self, txn_id: u64, region: &RegionVerId) -> bool {
        self.clean_regions
            .read()
            .await
            .get(&txn_id)
            .map(|regions| regions.contains(region))
            .unwrap_or(false)
    }

    pub async fn save_cleaned_region(&mut self, txn_id: u64, region: RegionVerId) {
        self.clean_regions
            .write()
            .await
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(region);
    }
}

pub struct LockResolver {
    ctx: ResolveLocksContext,
}

impl LockResolver {
    pub fn new(ctx: ResolveLocksContext) -> Self {
        Self { ctx }
    }

    /// Decide what `resolve_locks` has to do about `lock`: query its transaction's status and,
    /// for an expired async-commit transaction, recover that status from the secondary locks.
    /// This is the per-lock decision of client-go's `LockResolver.resolveLocks`.
    async fn action_for_lock(
        &mut self,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        current_ts: u64,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
    ) -> Result<LockAction> {
        let status = match self
            .get_txn_status_from_lock(
                OPTIMISTIC_BACKOFF,
                lock,
                caller_start_ts,
                current_ts,
                false,
                pd_client.clone(),
                keyspace,
            )
            .await
        {
            Ok(status) => status,
            Err(Error::KeyError(key_err))
                if key_err.primary_mismatch.is_some() && is_pessimistic(lock) =>
            {
                // The encountered pessimistic lock points at a stale primary: the transaction
                // changed its primary after writing this lock (pingcap/tidb#42937). Roll back
                // only this stale lock — the transaction itself may still be alive, and a
                // region-wide ResolveLock could roll back its other, legitimate locks.
                pessimistic_rollback_lock(lock, pd_client, keyspace).await?;
                // client-go goes on with the zero `TxnStatus{}` (lock_resolver.go:533), which
                // `IsRolledBack`: a read ignores the transaction (lock_resolver.go:624).
                return Ok(LockAction::Done {
                    read: ReadVerdict::Ignore,
                });
            }
            Err(err) => return Err(err),
        };

        let primary_lock = match &status.kind {
            TransactionStatusKind::Locked(_, primary_lock)
                if status.is_expired && primary_lock.use_async_commit =>
            {
                primary_lock
            }
            TransactionStatusKind::Locked(..) => return Ok(LockAction::from_status(&status)),
            _ if is_pessimistic(lock) => {
                // The primary named by this pessimistic lock is gone, but that does not settle
                // the transaction's fate: when resolving a pessimistic lock TiKV deliberately
                // writes no rollback record (`LockNotExistDoNothing`,
                // `TtlExpirePessimisticRollback`), because a pessimistic transaction may have
                // moved its primary and still be alive. Whatever the answer, only the
                // encountered lock itself may be removed — a region-wide ResolveLock could roll
                // back the transaction's legitimate locks, prewrite locks included. Pessimistic
                // locks never block reads, so nothing else is waiting on this region either.
                pessimistic_rollback_lock(lock, pd_client, keyspace).await?;
                // lock_resolver.go:571 then :624 with the status as answered.
                return Ok(LockAction::Done {
                    read: ReadVerdict::of(&status, caller_start_ts),
                });
            }
            _ => return Ok(LockAction::from_status(&status)),
        };

        // TiKV will not roll back an expired async-commit primary: the transaction's decision
        // has to be recovered from its secondaries instead.
        metrics::lock_resolver(LockMetric::ResolveAsyncCommit);
        let secondary_status = self
            .check_all_secondaries(
                pd_client.clone(),
                keyspace,
                primary_lock.secondaries.clone(),
                lock.lock_version,
            )
            .await?;
        if secondary_status.fallback_2pc {
            // A secondary fell back to plain 2PC, so the primary decides after all. Ask again,
            // this time letting TiKV roll back the expired primary.
            let status = self
                .get_txn_status_from_lock(
                    OPTIMISTIC_BACKOFF,
                    lock,
                    caller_start_ts,
                    current_ts,
                    true,
                    pd_client,
                    keyspace,
                )
                .await?;
            return Ok(LockAction::from_status(&status));
        }

        let commit_version =
            secondary_status.resolved_commit_version(primary_lock.min_commit_ts)?;
        // The recovered status is final: cache it so further locks of this transaction skip
        // the recovery, as client-go's `resolveAsyncCommitLock` does with `saveResolved`.
        let recovered = Arc::new(TransactionStatus::from_commit_version(commit_version));
        self.ctx
            .save_resolved(lock.lock_version, recovered.clone())
            .await;
        if let Some(state) = pd_client.lock_resolver_state() {
            state.save_resolved(lock.lock_version, recovered);
        }
        // Every lock of the transaction is resolved, not only the encountered one.
        let mut keys = primary_lock.secondaries.clone();
        keys.push(primary_lock.key.clone());
        Ok(LockAction::Resolve {
            commit_version,
            keys,
        })
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    ///
    /// Note: Will rollback RUNNING transactions. ONLY use in GC.
    pub async fn cleanup_locks(
        &mut self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
        pd_client: Arc<impl PdClient>, // TODO: make pd_client a member of LockResolver
        keyspace: Keyspace,
    ) -> Result<()> {
        // Defense in depth: CleanupLocks::execute refuses these before its filters,
        // but this entry point is public within the crate.
        reject_shared_locks(&locks)?;
        if locks.is_empty() {
            return Ok(());
        }

        fail_point!("before-cleanup-locks", |_| { Ok(()) });

        let region = store.region_with_leader.ver_id();

        let mut txn_infos = HashMap::new();
        for l in locks {
            let txn_id = l.lock_version;
            if txn_infos.contains_key(&txn_id) || self.ctx.is_region_cleaned(txn_id, &region).await
            {
                continue;
            }

            let l_is_pessimistic = is_pessimistic(&l);
            // Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
            let mut status = self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    txn_id,
                    l.primary_lock.clone(),
                    0,
                    u64::MAX,
                    true,
                    false,
                    l_is_pessimistic,
                    l.is_txn_file,
                )
                .await?;

            // If the transaction uses async commit, check_txn_status will reject rolling back the primary lock.
            // Then we need to check the secondary locks to determine the final status of the transaction.
            if let TransactionStatusKind::Locked(_, lock_info) = &status.kind {
                let secondary_status = self
                    .check_all_secondaries(
                        pd_client.clone(),
                        keyspace,
                        lock_info.secondaries.clone(),
                        txn_id,
                    )
                    .await?;
                debug!(
                    "secondary status, txn_id:{}, commit_ts:{:?}, min_commit_version:{}, fallback_2pc:{}",
                    txn_id,
                    secondary_status.commit_ts,
                    secondary_status.min_commit_ts,
                    secondary_status.fallback_2pc,
                );

                if secondary_status.fallback_2pc {
                    debug!("fallback to 2pc, txn_id:{}, check_txn_status again", txn_id);
                    status = self
                        .check_txn_status(
                            pd_client.clone(),
                            keyspace,
                            txn_id,
                            l.primary_lock,
                            0,
                            u64::MAX,
                            true,
                            true,
                            l_is_pessimistic,
                            l.is_txn_file,
                        )
                        .await?;
                } else {
                    let commit_ts =
                        secondary_status.resolved_commit_version(lock_info.min_commit_ts)?;
                    txn_infos.insert(txn_id, (commit_ts, l.is_txn_file));
                    continue;
                }
            }

            match &status.kind {
                TransactionStatusKind::Locked(_, lock_info) => {
                    error!(
                        "cleanup_locks fail to clean locks, this result is not expected. txn_id:{}",
                        txn_id
                    );
                    return Err(Error::ResolveLockError(vec![lock_info.clone()]));
                }
                TransactionStatusKind::Committed(ts) => {
                    txn_infos.insert(txn_id, (ts.version(), l.is_txn_file))
                }
                TransactionStatusKind::RolledBack => txn_infos.insert(txn_id, (0, l.is_txn_file)),
            };
        }

        debug!(
            "batch resolve locks, region:{:?}, txn:{:?}",
            store.region_with_leader.ver_id(),
            txn_infos
        );
        let mut txn_ids = Vec::with_capacity(txn_infos.len());
        let mut txn_info_vec = Vec::with_capacity(txn_infos.len());
        for (txn_id, (commit_ts, is_txn_file)) in txn_infos.into_iter() {
            txn_ids.push(txn_id);
            let mut txn_info = TxnInfo::default();
            txn_info.txn = txn_id;
            txn_info.status = commit_ts;
            txn_info.is_txn_file = is_txn_file;
            txn_info_vec.push(txn_info);
        }
        let cleaned_region = self
            .batch_resolve_locks(pd_client.clone(), keyspace, store.clone(), txn_info_vec)
            .await?;
        for txn_id in txn_ids {
            self.ctx
                .save_cleaned_region(txn_id, cleaned_region.clone())
                .await;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_txn_status(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        is_txn_file: bool,
    ) -> Result<Arc<TransactionStatus>> {
        if let Some(txn_status) = self.ctx.get_resolved(txn_id).await {
            return Ok(txn_status);
        }
        // R6: the cluster client's cache, shared across requests (client-go `getResolved`).
        if let Some(txn_status) = pd_client
            .lock_resolver_state()
            .and_then(|state| state.get_resolved(txn_id))
        {
            return Ok(txn_status);
        }

        // CheckTxnStatus may meet the following cases:
        // 1. LOCK
        // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
        // 1.2 Lock TTL -- active transaction holding the lock.
        // 2. NO LOCK
        // 2.1 Txn Committed
        // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
        // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
        metrics::lock_resolver(LockMetric::QueryTxnStatus);
        let req = new_check_txn_status_request(
            primary,
            txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
            is_txn_file,
        );
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(CollectSingle)
            .extract_error()
            .post_process_default()
            .plan();
        let mut status: TransactionStatus = match plan.execute().await {
            Ok(status) => status,
            // Response key errors now reach us under exactly one variant, whichever
            // shape the plan has: `single_shard_handler` and `ExtractError` both produce
            // `MultipleKeyErrors`. Before the variants were split this arm had to accept
            // two, because a collapsing merge decided which one surfaced — and matching
            // only one left the lock-resolution heal path (`rollback_if_not_exist` after
            // TTL expiry in `get_txn_status_from_lock`) unreachable, so an orphaned
            // secondary lock poisoned its key forever.
            Err(Error::MultipleKeyErrors(mut errors)) => {
                match errors.pop() {
                    Some(Error::KeyError(key_err)) => {
                        if let Some(txn_not_found) = key_err.txn_not_found {
                            return Err(Error::TxnNotFound(txn_not_found));
                        }
                        // A PrimaryMismatch error propagates to `resolve_locks`, which rolls
                        // back the stale pessimistic lock it was reported for.
                        return Err(Error::KeyError(key_err));
                    }
                    Some(err) => return Err(err),
                    None => unreachable!(),
                }
            }
            Err(err) => return Err(err),
        };

        let current = pd_client.clone().get_timestamp().await?;
        status.check_ttl(current);
        match status.kind {
            TransactionStatusKind::Committed(_) => {
                metrics::lock_resolver(LockMetric::QueryTxnStatusCommitted)
            }
            TransactionStatusKind::RolledBack => {
                metrics::lock_resolver(LockMetric::QueryTxnStatusRolledBack)
            }
            TransactionStatusKind::Locked(..) => {}
        }
        let res = Arc::new(status);
        if res.is_cacheable() {
            self.ctx.save_resolved(txn_id, res.clone()).await;
            if let Some(state) = pd_client.lock_resolver_state() {
                state.save_resolved(txn_id, res.clone());
            }
        }
        Ok(res)
    }

    async fn check_all_secondaries(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keys: Vec<Vec<u8>>,
        txn_id: u64,
    ) -> Result<SecondaryLocksStatus> {
        metrics::lock_resolver(LockMetric::QueryCheckSecondaryLocks);
        let req = new_check_secondary_locks_request(keys, txn_id);
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
            .preserve_shard()
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(CollectSecondaryLocks {
                start_version: txn_id,
            })
            .plan();
        plan.execute().await
    }

    async fn batch_resolve_locks(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        store: RegionStore,
        txn_infos: Vec<TxnInfo>,
    ) -> Result<RegionVerId> {
        let ver_id = store.region_with_leader.ver_id();
        metrics::lock_resolver(LockMetric::BatchResolve);
        let request = requests::new_batch_resolve_lock_request(txn_infos.clone());
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
            .single_region_with_store(store.clone())
            .await?
            .extract_error()
            .plan();
        let _ = plan.execute().await?;
        Ok(ver_id)
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_txn_status_from_lock(
        &mut self,
        mut backoff: Backoff,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        current_ts: u64,
        force_sync_commit: bool,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
    ) -> Result<Arc<TransactionStatus>> {
        let current_ts = if lock.lock_ttl == 0 {
            // NOTE: lock_ttl = 0 is a special protocol!!!
            // When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
            // In this case, TiKV use lock TTL = 0 to notify client, and client should resolve the lock!
            // Set current_ts to max uint64 to make the lock expired.
            u64::MAX
        } else {
            current_ts
        };

        let mut rollback_if_not_exist = false;
        loop {
            match self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    lock.lock_version,
                    lock.primary_lock.clone(),
                    caller_start_ts,
                    current_ts,
                    rollback_if_not_exist,
                    force_sync_commit,
                    is_pessimistic(lock),
                    lock.is_txn_file,
                )
                .await
            {
                Ok(status) => return Ok(status),
                Err(Error::TxnNotFound(txn_not_found)) => {
                    let current = pd_client.clone().get_timestamp().await?;
                    if lock_until_expired_ms(lock.lock_version, lock.lock_ttl, current) <= 0 {
                        warn!(
                            "lock txn not found, lock has expired, lock {:?}, caller_start_ts {}, current_ts {}",
                            lock, caller_start_ts, current_ts
                        );
                        rollback_if_not_exist = true;
                        continue;
                    } else if is_pessimistic(lock) {
                        let status = TransactionStatus {
                            kind: TransactionStatusKind::Locked(lock.lock_ttl, lock.clone()),
                            action: kvrpcpb::Action::NoAction,
                            is_expired: false,
                        };
                        return Ok(Arc::new(status));
                    }

                    if let Some(duration) = backoff.next_delay_duration() {
                        metrics::backoff(BackoffType::TxnNotFound, duration);
                        sleep(duration).await;
                        continue;
                    }
                    return Err(Error::TxnNotFound(txn_not_found));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

pub trait HasLocks {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        Vec::new()
    }
}

// Return duration in milliseconds until lock expired.
// If the lock has expired, return a negative value.
pub fn lock_until_expired_ms(lock_version: u64, ttl: u64, current: Timestamp) -> i64 {
    Timestamp::from_version(lock_version).physical + ttl as i64 - current.physical
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use fail::FailScenario;
    use serial_test::serial;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::proto::metapb;
    use crate::region::RegionWithLeader;
    use crate::request::EncodeKeyspace;
    use crate::request::KeyMode;
    use crate::Key;

    /// A transaction ID whose lock is always expired under the mock PD clock:
    /// `Timestamp::from_version` casts the version to `i64` before shifting, so
    /// `u64::MAX` yields a physical time of -1 (production timestamp arithmetic, not
    /// mock behavior), while `MockPdClient::get_timestamp` stands at physical 0 —
    /// any positive TTL has therefore already lapsed.
    const EXPIRED_TXN_VERSION: u64 = u64::MAX;

    /// The async-commit recovery tests place these on both sides of the mock region
    /// boundary (`MockPdClient::region1` ends at `[10]`), so a full recovery must
    /// resolve two regions.
    const PRIMARY_KEY: &[u8] = &[1]; // mock region 1
    const SECONDARY_KEY: &[u8] = &[11]; // mock region 2

    fn encoded(key: &[u8], keyspace: Keyspace) -> Vec<u8> {
        Key::from(key.to_vec())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into()
    }

    /// The primary lock of an expired async-commit transaction — the shape returned
    /// by `CheckTxnStatus` and encountered by readers.
    fn async_commit_primary_lock(
        primary_key: &[u8],
        min_commit_ts: u64,
        secondaries: Vec<Vec<u8>>,
    ) -> kvrpcpb::LockInfo {
        kvrpcpb::LockInfo {
            key: primary_key.to_vec(),
            primary_lock: primary_key.to_vec(),
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            min_commit_ts,
            use_async_commit: true,
            secondaries,
            // Not lite: these tests count region-wide ResolveLock requests (R5 tests the
            // lite mode separately).
            txn_size: 100,
            ..Default::default()
        }
    }

    /// A `CheckTxnStatusResponse` reporting the transaction still locked by `primary`.
    fn still_locked_response(primary: kvrpcpb::LockInfo) -> kvrpcpb::CheckTxnStatusResponse {
        kvrpcpb::CheckTxnStatusResponse {
            lock_ttl: 1,
            lock_info: Some(primary),
            action: kvrpcpb::Action::NoAction as i32,
            ..Default::default()
        }
    }

    /// A still-live async-commit secondary lock.
    fn async_commit_secondary_lock(key: &[u8], min_commit_ts: u64) -> kvrpcpb::LockInfo {
        kvrpcpb::LockInfo {
            key: key.to_vec(),
            lock_version: EXPIRED_TXN_VERSION,
            min_commit_ts,
            use_async_commit: true,
            ..Default::default()
        }
    }

    /// A secondary lock that fell back from async commit to plain 2PC.
    fn fallback_2pc_secondary_lock(key: &[u8]) -> kvrpcpb::LockInfo {
        kvrpcpb::LockInfo {
            key: key.to_vec(),
            lock_version: EXPIRED_TXN_VERSION,
            use_async_commit: false,
            ..Default::default()
        }
    }

    #[test]
    fn shared_lock_wrappers_are_refused_members_are_not() {
        let plain = kvrpcpb::LockInfo {
            key: b"k1".to_vec(),
            lock_version: 7,
            ..Default::default()
        };
        assert!(reject_shared_locks(std::slice::from_ref(&plain)).is_ok());

        // A wrapper: key/lock_version deliberately unset per the contract — resolving
        // it would check transaction 0. Must be refused, not resolved or filtered.
        let wrapper = kvrpcpb::LockInfo {
            shared_lock_infos: vec![kvrpcpb::LockInfo {
                key: b"k2".to_vec(),
                lock_version: 8,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(reject_shared_locks(&[plain.clone(), wrapper]).is_err());

        // Also refused when only the op marks it a wrapper (empty member list).
        let by_op = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedLock as i32,
            ..Default::default()
        };
        assert!(reject_shared_locks(&[by_op]).is_err());

        // A member: the pessimistic lock of one holder, resolved like any pessimistic lock
        // (client-go `IsShared` is only `Op_SharedLock`; `IsPessimistic` covers this one).
        let member = kvrpcpb::LockInfo {
            key: b"k2".to_vec(),
            lock_version: 8,
            lock_type: kvrpcpb::Op::SharedPessimisticLock as i32,
            ..Default::default()
        };
        assert!(reject_shared_locks(std::slice::from_ref(&member)).is_ok());
        assert!(is_pessimistic(&member));

        // The coprocessor expands a wrapper into its members.
        let wrapper = kvrpcpb::LockInfo {
            lock_type: kvrpcpb::Op::SharedLock as i32,
            shared_lock_infos: vec![member.clone(), plain.clone()],
            ..Default::default()
        };
        assert_eq!(expand_shared_lock(wrapper), vec![member, plain.clone()]);
        assert_eq!(expand_shared_lock(plain.clone()), vec![plain]);
    }

    /// The read's view of `CheckTxnStatus` (lock_resolver.go:624-639), for a read at 10 meeting
    /// a lock of transaction 1 (TTL not expired under the mock PD clock):
    /// (commit_version, lock_ttl, action) → (live, ignorable, accessible, ResolveLock sent).
    #[rstest::rstest]
    // Committed at or before the read: read through it.
    #[case(5, 0, kvrpcpb::Action::NoAction, (0, vec![], vec![1], true))]
    #[case(10, 0, kvrpcpb::Action::NoAction, (0, vec![], vec![1], true))]
    // Committed after the read: ignore it.
    #[case(20, 0, kvrpcpb::Action::NoAction, (0, vec![1], vec![], true))]
    // Rolled back: ignore it.
    #[case(0, 0, kvrpcpb::Action::TtlExpireRollback, (0, vec![1], vec![], true))]
    // Alive, min_commit_ts pushed past the read: ignore it, nothing to resolve.
    #[case(0, 100, kvrpcpb::Action::MinCommitTsPushed, (0, vec![1], vec![], false))]
    // Alive otherwise: wait.
    #[case(0, 100, kvrpcpb::Action::NoAction, (1, vec![], vec![], false))]
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_for_read_classifies(
        #[case] commit_version: u64,
        #[case] lock_ttl: u64,
        #[case] action: kvrpcpb::Action,
        #[case] expected: (usize, Vec<u64>, Vec<u64>, bool),
    ) {
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: 1,
            lock_ttl: 100,
            txn_size: 100, // not lite
            ..Default::default()
        };
        let lock_captured = lock.clone();
        let resolve_count = Arc::new(AtomicUsize::new(0));
        let resolve_count_captured = resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert_eq!(req.caller_start_ts, 10);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version,
                        lock_ttl,
                        lock_info: (lock_ttl != 0).then(|| lock_captured.clone()),
                        action: action as i32,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let read = resolve_locks_for_read(
            vec![lock],
            Timestamp::from_version(10),
            client,
            Keyspace::Disable,
            false,
        )
        .await
        .unwrap();
        let (live, ignorable, accessible, resolves) = expected;
        assert_eq!(read.live.len(), live);
        assert_eq!(read.ignorable, ignorable);
        assert_eq!(read.accessible, accessible);
        // A settled transaction's locks are resolved in the background (R2).
        for _ in 0..200 {
            if resolve_count.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(resolve_count.load(Ordering::SeqCst) > 0, resolves);
    }

    /// R2: for a read, the settled transaction's locks are resolved in a background task;
    /// `resolve_locks_for_read` returns before that cleanup finishes (client-go
    /// lock_resolver.go:584-596).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_resolve_locks_for_read_does_not_wait_for_cleanup() {
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let gate_captured = gate.clone();
        let resolve_count = Arc::new(AtomicUsize::new(0));
        let resolve_count_captured = resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 5,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    // Held until the test opens the gate.
                    let (open, cv) = &*gate_captured;
                    let mut open = open.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                    resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![1],
            lock_version: 1,
            lock_ttl: 100,
            txn_size: 100, // not lite
            ..Default::default()
        };
        let read = resolve_locks_for_read(
            vec![lock],
            Timestamp::from_version(10),
            client,
            Keyspace::Disable,
            false,
        )
        .await
        .unwrap();
        assert_eq!(read.accessible, vec![1]);
        assert_eq!(resolve_count.load(Ordering::SeqCst), 0);
        {
            let (open, cv) = &*gate;
            *open.lock().unwrap() = true;
            cv.notify_all();
        }
        for _ in 0..200 {
            if resolve_count.load(Ordering::SeqCst) > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(resolve_count.load(Ordering::SeqCst), 1);
    }

    /// R2: an expired async-commit transaction met by a read: its status is recovered from the
    /// secondaries before returning (the read needs it), and all of its locks are then
    /// resolved in the background (client-go `resolveAsyncCommitLock(.., asyncResolveAll =
    /// forRead)`, lock_resolver.go:1092).
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_for_read_async_commit_in_background() {
        let resolved = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let resolved_captured = resolved.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(still_locked_response(async_commit_primary_lock(
                        PRIMARY_KEY,
                        44,
                        vec![SECONDARY_KEY.to_vec()],
                    ))) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![async_commit_secondary_lock(SECONDARY_KEY, 43)],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolved_captured.lock().unwrap().push(req.commit_version);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = async_commit_primary_lock(PRIMARY_KEY, 44, vec![SECONDARY_KEY.to_vec()]);
        let read = resolve_locks_for_read(
            vec![lock],
            Timestamp::from_version(100),
            client,
            Keyspace::Disable,
            false,
        )
        .await
        .unwrap();
        // Committed at 44, before the read at 100: read through.
        assert!(read.live.is_empty());
        assert_eq!(read.accessible, vec![EXPIRED_TXN_VERSION]);
        for _ in 0..200 {
            if resolved.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Both regions (secondary and primary), with the recovered commit version.
        assert_eq!(*resolved.lock().unwrap(), vec![44, 44]);
    }

    /// R3: the time until the first live lock expires is the minimum over the live locks,
    /// from the TTL `CheckTxnStatus` answered (client-go `txnExpireTime`,
    /// lock_resolver.go:617, :671). Under the mock PD clock (physical 0) and lock versions
    /// with physical 0, it is the TTL itself.
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_reports_time_to_first_expiry() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    // Transaction 1 has 3000 ms left, transaction 2 has 1000 ms.
                    let ttl = if req.lock_ts == 1 { 3000 } else { 1000 };
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: ttl,
                        lock_info: Some(kvrpcpb::LockInfo {
                            key: vec![req.lock_ts as u8],
                            primary_lock: vec![req.lock_ts as u8],
                            lock_version: req.lock_ts,
                            lock_ttl: ttl,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = |txn: u64| kvrpcpb::LockInfo {
            key: vec![txn as u8],
            primary_lock: vec![txn as u8],
            lock_version: txn,
            lock_ttl: 5000,
            ..Default::default()
        };
        let found = resolve_locks_with_expiry(
            vec![lock(1), lock(2)],
            Timestamp::from_version(10),
            client.clone(),
            Keyspace::Disable,
        )
        .await
        .unwrap();
        assert_eq!(found.live.len(), 2);
        assert_eq!(found.ms_before_expired, 1000);
        // Reads report the same.
        let read = resolve_locks_for_read(
            vec![lock(1)],
            Timestamp::from_version(10),
            client,
            Keyspace::Disable,
            false,
        )
        .await
        .unwrap();
        assert_eq!(read.ms_before_expired, 3000);
    }

    /// R3: `BackoffWithMaxSleepTxnLockFast(msBeforeExpired)`: the delay is capped at the time
    /// left; with no time left the retry is immediate and spends no attempt.
    #[test]
    fn lock_backoff_sleep_is_capped_by_expiry() {
        let mut backoff = Backoff::no_jitter_backoff(100, 1000, 2);
        assert_eq!(
            lock_backoff_sleep(&mut backoff, 0),
            Some(std::time::Duration::ZERO)
        );
        assert_eq!(backoff.current_attempts(), 0);
        assert_eq!(
            lock_backoff_sleep(&mut backoff, 30),
            Some(std::time::Duration::from_millis(30))
        );
        assert_eq!(
            lock_backoff_sleep(&mut backoff, 10_000),
            Some(std::time::Duration::from_millis(200))
        );
        assert_eq!(lock_backoff_sleep(&mut backoff, 10_000), None);
    }

    /// R5: lite resolution (client-go `resolveLock`, lock_resolver.go:1204-1271). A
    /// committed 2PC transaction is met through three locks in one region: its primary (key
    /// [1]) and two secondaries ([2], [3]). Each case gives the `ResolveLock` requests sent,
    /// as their `keys` (empty = the whole region):
    /// - small transaction (`txn_size < 16`), or `lite` asked for: the primary needs no request
    ///   (CheckTxnStatus resolved it), each secondary is resolved alone, and nothing is
    ///   recorded as clean, so the second secondary gets its own request;
    /// - big transaction, not lite: one region-wide request, the region is then clean.
    #[rstest::rstest]
    #[case::small_txn(3, false, vec![vec![vec![2u8]], vec![vec![3u8]]])]
    #[case::lite_asked(100, true, vec![vec![vec![2u8]], vec![vec![3u8]]])]
    #[case::big_txn(100, false, vec![vec![]])]
    #[tokio::test]
    #[serial]
    async fn test_resolve_lock_lite(
        #[case] txn_size: u64,
        #[case] lite: bool,
        #[case] expected: Vec<Vec<Vec<u8>>>,
    ) {
        let requests = Arc::new(std::sync::Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let requests_captured = requests.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 5,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    requests_captured.lock().unwrap().push(req.keys.clone());
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = |key: u8| kvrpcpb::LockInfo {
            key: vec![key],
            primary_lock: vec![1],
            lock_version: 1,
            lock_ttl: 100,
            txn_size,
            ..Default::default()
        };
        let locks = vec![lock(1), lock(2), lock(3)];
        if lite {
            // Only reads ask for lite (a snapshot's point get); the cleanup then runs in the
            // background.
            resolve_locks_for_read(
                locks,
                Timestamp::from_version(10),
                client,
                Keyspace::Disable,
                true,
            )
            .await
            .unwrap();
            for _ in 0..200 {
                if requests.lock().unwrap().len() >= expected.len() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let mut got = requests.lock().unwrap().clone();
            got.sort();
            assert_eq!(got, expected);
        } else {
            resolve_locks(
                locks,
                Timestamp::from_version(10),
                client,
                Keyspace::Disable,
            )
            .await
            .unwrap();
            assert_eq!(*requests.lock().unwrap(), expected);
        }
    }

    /// R6: settled statuses are shared across requests of one cluster client: a second,
    /// separate `resolve_locks` call for the same transaction sends no `CheckTxnStatus`
    /// (client-go `getResolved` on the store's `LockResolver`).
    #[tokio::test]
    #[serial]
    async fn test_resolved_status_shared_across_calls() {
        let checks = Arc::new(AtomicUsize::new(0));
        let checks_captured = checks.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    checks_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 5,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let lock = |key: u8| kvrpcpb::LockInfo {
            key: vec![key],
            primary_lock: vec![1],
            lock_version: 1,
            lock_ttl: 100,
            txn_size: 100,
            ..Default::default()
        };
        for key in [2, 3] {
            resolve_locks(
                vec![lock(key)],
                Timestamp::from_version(10),
                client.clone(),
                Keyspace::Disable,
            )
            .await
            .unwrap();
        }
        assert_eq!(checks.load(Ordering::SeqCst), 1);
        assert!(client
            .lock_resolver_state()
            .unwrap()
            .get_resolved(1)
            .is_some());
    }

    fn committed(ts: u64) -> Arc<TransactionStatus> {
        Arc::new(TransactionStatus::from_commit_version(ts))
    }

    /// R6: the cache keeps the last `RESOLVED_CACHE_SIZE` transactions (FIFO), and a
    /// consistent second save is a no-op.
    #[test]
    fn resolved_cache_is_fifo_bounded() {
        let state = LockResolverState::default();
        for txn in 1..=(RESOLVED_CACHE_SIZE as u64 + 1) {
            state.save_resolved(txn, committed(txn + 1_000_000));
        }
        assert!(state.get_resolved(1).is_none());
        assert!(state.get_resolved(2).is_some());
        assert!(state.get_resolved(RESOLVED_CACHE_SIZE as u64 + 1).is_some());
        // Same status again: kept, no panic.
        state.save_resolved(2, committed(2 + 1_000_000));
    }

    /// R6: as client-go v8.5.8 (`saveResolved`, lock_resolver.go:247-253), a different
    /// settled status for a cached transaction panics.
    #[test]
    #[should_panic(expected = "existing different entry")]
    fn resolved_cache_panics_on_conflicting_status() {
        let state = LockResolverState::default();
        state.save_resolved(7, committed(10));
        state.save_resolved(7, Arc::new(TransactionStatus::from_commit_version(0)));
    }

    /// R6: only settled statuses may be cached (lock_resolver.go:237-241).
    #[test]
    #[should_panic(expected = "undetermined status")]
    fn resolved_cache_panics_on_undetermined_status() {
        let state = LockResolverState::default();
        state.save_resolved(
            7,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::RolledBack,
                action: kvrpcpb::Action::LockNotExistDoNothing,
                is_expired: false,
            }),
        );
    }

    /// R12: a region-wide resolve already in flight for the same (region, start_ts) is not
    /// sent again: the second caller waits for it (client-go `resolveRegionSf`). If the one
    /// in flight fails, the waiter sends its own; another key is never collapsed.
    #[rstest::rstest]
    #[case::collapsed(true, 1, 1)]
    #[case::leader_failed(false, 1, 2)]
    #[case::other_txn(true, 2, 2)]
    #[tokio::test]
    async fn test_collapse_resolve(
        #[case] leader_ok: bool,
        #[case] follower_txn: u64,
        #[case] expected_sends: usize,
    ) {
        let state = Arc::new(LockResolverState::default());
        let sends = Arc::new(AtomicUsize::new(0));
        let (release, released) = futures::channel::oneshot::channel::<()>();
        let leader = {
            let (state, sends) = (state.clone(), sends.clone());
            tokio::spawn(async move {
                state
                    .collapse_resolve(9, 1, async move {
                        sends.fetch_add(1, Ordering::SeqCst);
                        let _ = released.await;
                        if leader_ok {
                            Ok(())
                        } else {
                            Err(Error::StringError("injected".to_owned()))
                        }
                    })
                    .await
            })
        };
        // Let the leader register its request.
        while sends.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let follower = {
            let (state, sends) = (state.clone(), sends.clone());
            tokio::spawn(async move {
                state
                    .collapse_resolve(9, follower_txn, async move {
                        sends.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        release.send(()).unwrap();
        assert_eq!(leader.await.unwrap().is_ok(), leader_ok);
        assert!(follower.await.unwrap().is_ok());
        assert_eq!(sends.load(Ordering::SeqCst), expected_sends);
        // Nothing left in flight.
        assert!(state.in_flight.lock().unwrap().is_empty());
    }

    /// `ReadLockHints` (client-go `util.TSSet` pair): `get_all` is `None` while empty, the
    /// sets deduplicate, and clones share them.
    #[test]
    fn read_lock_hints_are_shared_sets() {
        let hints = ReadLockHints::new(10);
        assert_eq!(hints.get_all(), None);
        let clone = hints.clone();
        clone.record(
            10,
            &ReadLocks {
                live: vec![],
                ignorable: vec![7, 3],
                accessible: vec![5],
                ms_before_expired: 0,
            },
        );
        hints.put_resolved([3]);
        hints.put_committed([9]);
        assert_eq!(hints.get_all(), Some((vec![3, 7], vec![5, 9])));
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
    async fn test_resolve_lock_with_retry(#[case] keyspace: Keyspace) {
        let _scenario = FailScenario::setup();

        const MAX_REGION_ERROR_RETRIES: u32 = 10;
        let backoff = Backoff::no_jitter_backoff(0, 0, MAX_REGION_ERROR_RETRIES);

        // Test resolve lock within retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES),
        )
        .unwrap();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                fail::fail_point!("region-error", |_| {
                    let resp = kvrpcpb::ResolveLockResponse {
                        region_error: Some(errorpb::Error::default()),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                });
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));

        let key = vec![1];
        let region1 = MockPdClient::region1();
        let mut clean_regions = HashSet::new();
        resolve_lock_with_retry(
            (&key).into(),
            1,
            2,
            false,
            false,
            client.clone(),
            keyspace,
            backoff.clone(),
            &mut clean_regions,
        )
        .await
        .unwrap();
        assert_eq!(clean_regions, HashSet::from([region1.ver_id()]));

        // Test resolve lock over retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES + 1),
        )
        .unwrap();
        let key = vec![100];
        resolve_lock_with_retry(
            (&key).into(),
            3,
            4,
            false,
            false,
            client,
            keyspace,
            backoff,
            &mut clean_regions,
        )
        .await
        .expect_err("should return error");
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_resolves_committed_even_if_ttl_not_expired() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 1;
        lock.lock_ttl = 100; // not expired under MockPdClient's Timestamp::default()
        lock.txn_size = 100; // not lite: the resolve is counted

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[rstest::rstest]
    // With `Keyspace::Enable` every key gains the keyspace prefix, which places them
    // all in mock region 2 — the recovery then resolves one region instead of two.
    #[case(Keyspace::Disable, 2, &[PRIMARY_KEY])]
    #[case(Keyspace::Enable { keyspace_id: 0 }, 1, &[PRIMARY_KEY])]
    #[case(Keyspace::Disable, 2, &[SECONDARY_KEY, PRIMARY_KEY])]
    #[case(Keyspace::Enable { keyspace_id: 0 }, 1, &[SECONDARY_KEY, PRIMARY_KEY])]
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_recovers_expired_async_commit(
        #[case] keyspace: Keyspace,
        #[case] expected_resolved_regions: usize,
        #[case] encountered_keys: &[&[u8]],
    ) {
        let primary_key = encoded(PRIMARY_KEY, keyspace);
        let secondary_key = encoded(SECONDARY_KEY, keyspace);

        let check_secondary_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let resolved_commit_version = Arc::new(AtomicU64::new(0));

        let check_secondary_count_captured = check_secondary_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let resolved_commit_version_captured = resolved_commit_version.clone();
        let primary_key_captured = primary_key.clone();
        let secondary_key_captured = secondary_key.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert_eq!(req.primary_key, primary_key_captured);
                    return Ok(Box::new(still_locked_response(async_commit_primary_lock(
                        &primary_key_captured,
                        44,
                        vec![secondary_key_captured.clone()],
                    ))) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_count_captured.fetch_add(1, Ordering::SeqCst);
                    // The secondaries listed in the primary lock are already encoded and
                    // must be sent verbatim — re-encoding them would corrupt the keys.
                    assert_eq!(req.keys, vec![secondary_key_captured.clone()]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![async_commit_secondary_lock(&secondary_key_captured, 43)],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    resolved_commit_version_captured.store(req.commit_version, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let locks = encountered_keys
            .iter()
            .map(|key| kvrpcpb::LockInfo {
                key: encoded(key, keyspace),
                primary_lock: primary_key.clone(),
                lock_version: EXPIRED_TXN_VERSION,
                lock_ttl: 1,
                use_async_commit: true,
                ..Default::default()
            })
            .collect();

        let live_locks = resolve_locks(locks, Timestamp::default(), client, keyspace)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_secondary_count.load(Ordering::SeqCst), 1);
        // Every region the transaction wrote to must be resolved, exactly once each.
        assert_eq!(
            resolve_lock_count.load(Ordering::SeqCst),
            expected_resolved_regions
        );
        // The secondary's min_commit_ts (43) is below the primary's (44): the recovered
        // commit version must be the maximum across ALL locks, i.e. the primary's.
        assert_eq!(resolved_commit_version.load(Ordering::SeqCst), 44);
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_recovers_missing_async_secondary_as_rollback() {
        let check_secondary_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let resolved_commit_version = Arc::new(AtomicU64::new(u64::MAX));

        let check_secondary_count_captured = check_secondary_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let resolved_commit_version_captured = resolved_commit_version.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(still_locked_response(async_commit_primary_lock(
                        PRIMARY_KEY,
                        42,
                        vec![SECONDARY_KEY.to_vec()],
                    ))) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_count_captured.fetch_add(1, Ordering::SeqCst);
                    // The requested secondary is absent and commit_ts is zero: TiKV has
                    // established a rollback tombstone, so the transaction must roll back.
                    return Ok(
                        Box::<kvrpcpb::CheckSecondaryLocksResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    resolved_commit_version_captured.store(req.commit_version, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let lock = async_commit_primary_lock(PRIMARY_KEY, 42, vec![SECONDARY_KEY.to_vec()]);

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_secondary_count.load(Ordering::SeqCst), 1);
        // The rollback must reach every region the transaction wrote to: the
        // secondary's region and the primary's.
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 2);
        assert_eq!(resolved_commit_version.load(Ordering::SeqCst), 0);
    }

    #[rstest::rstest]
    #[case(55, 0)]
    #[case(0, 0)]
    #[case(0, 1000)]
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_falls_back_to_2pc_with_real_current_ts(
        #[case] commit_version: u64,
        #[case] lock_ttl: u64,
    ) {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let force_sync_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let resolved_commit_version = Arc::new(AtomicU64::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let force_sync_count_captured = force_sync_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let resolved_commit_version_captured = resolved_commit_version.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    if req.force_sync_commit {
                        force_sync_count_captured.fetch_add(1, Ordering::SeqCst);
                        assert_ne!(req.current_ts, u64::MAX);
                        return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                            commit_version,
                            lock_ttl,
                            lock_info: (lock_ttl != 0).then(|| kvrpcpb::LockInfo {
                                key: PRIMARY_KEY.to_vec(),
                                primary_lock: PRIMARY_KEY.to_vec(),
                                lock_version: EXPIRED_TXN_VERSION,
                                lock_ttl,
                                ..Default::default()
                            }),
                            action: kvrpcpb::Action::NoAction as i32,
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::new(still_locked_response(async_commit_primary_lock(
                        PRIMARY_KEY,
                        42,
                        vec![SECONDARY_KEY.to_vec()],
                    ))) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![fallback_2pc_secondary_lock(SECONDARY_KEY)],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    resolved_commit_version_captured.store(req.commit_version, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let lock = async_commit_primary_lock(PRIMARY_KEY, 42, vec![SECONDARY_KEY.to_vec()]);

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 2);
        assert_eq!(force_sync_count.load(Ordering::SeqCst), 1);
        if lock_ttl == 0 {
            assert!(live_locks.is_empty());
            // After the fallback the transaction is plain 2PC: only the encountered lock's
            // region is resolved, as client-go does.
            assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
            assert_eq!(
                resolved_commit_version.load(Ordering::SeqCst),
                commit_version
            );
        } else {
            assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
            assert_eq!(live_locks.len(), 1);
            assert_eq!(live_locks[0].lock_ttl, lock_ttl);
            assert!(!live_locks[0].use_async_commit);
        }
    }

    /// A region error on `CheckSecondaryLocks` makes the plan re-shard against fresh
    /// region boundaries. Every retried sub-request must be paired with its OWN keys
    /// (`preserve_shard` + `Collect::merge` contract): pairing a stale shard would make
    /// the shorter per-region lock lists below look like missing locks and roll back a
    /// committable transaction.
    #[tokio::test]
    #[serial]
    async fn test_check_secondary_locks_reshards_on_region_error() {
        // The transaction spans primary [1] and secondaries [2] and [3]. Every key
        // starts out in one region; after the simulated split, [2] and [3] live in two.
        fn mock_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            let mut region = RegionWithLeader::default();
            region.region.id = id;
            region.region.start_key = start_key;
            region.region.end_key = end_key;
            region.region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 1,
            });
            region.leader = Some(metapb::Peer {
                store_id: 41,
                ..Default::default()
            });
            region
        }

        let split = Arc::new(AtomicBool::new(false));
        let check_secondary_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let resolved_commit_version = Arc::new(AtomicU64::new(u64::MAX));

        let split_in_dispatch = split.clone();
        let check_secondary_count_captured = check_secondary_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let resolved_commit_version_captured = resolved_commit_version.clone();
        let split_in_region_hook = split.clone();
        let client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(still_locked_response(async_commit_primary_lock(
                        &[1],
                        40,
                        vec![vec![2], vec![3]],
                    ))) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_count_captured.fetch_add(1, Ordering::SeqCst);
                    if req.keys == vec![vec![2], vec![3]] {
                        // First attempt, before the split: fail the whole shard.
                        split_in_dispatch.store(true, Ordering::SeqCst);
                        let resp = kvrpcpb::CheckSecondaryLocksResponse {
                            region_error: Some(errorpb::Error::default()),
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    let min_commit_ts = match req.keys.as_slice() {
                        [key] if key.as_slice() == [2] => 41,
                        [key] if key.as_slice() == [3] => 42,
                        keys => panic!("unexpected CheckSecondaryLocks shard: {:?}", keys),
                    };
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![async_commit_secondary_lock(&req.keys[0], min_commit_ts)],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    resolved_commit_version_captured.store(req.commit_version, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            }))
            .with_region_for_key_hook(move |key: &Key| {
                let key: &[u8] = key.into();
                if !split_in_region_hook.load(Ordering::SeqCst) {
                    Ok(mock_region(1, vec![], vec![10]))
                } else if key < &[3][..] {
                    Ok(mock_region(1, vec![], vec![3]))
                } else {
                    Ok(mock_region(4, vec![3], vec![10]))
                }
            }),
        );

        let lock = async_commit_primary_lock(&[1], 40, vec![vec![2], vec![3]]);

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        // One failed pre-split request plus one per post-split region.
        assert_eq!(check_secondary_count.load(Ordering::SeqCst), 3);
        // Both locks survived, so the transaction must COMMIT at the maximum
        // min_commit_ts (42) — a stale shard pairing would have inferred a missing
        // lock instead and rolled the transaction back (commit version 0).
        assert_eq!(resolved_commit_version.load(Ordering::SeqCst), 42);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_retries_expired_missing_primary_with_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    if !req.rollback_if_not_exist {
                        return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                            error: Some(kvrpcpb::KeyError {
                                txn_not_found: Some(kvrpcpb::TxnNotFound {
                                    start_ts: req.lock_ts,
                                    primary_key: req.primary_key.clone(),
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::<kvrpcpb::CheckTxnStatusResponse>::default() as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![2],
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            ..Default::default()
        };

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 2);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    /// A pessimistic lock whose recorded primary is stale (the transaction changed its
    /// primary, pingcap/tidb#42937) makes TiKV answer CheckTxnStatus with PrimaryMismatch.
    /// Only that stale lock may be rolled back — the transaction itself may still be alive,
    /// so a region-wide ResolveLock is out of the question: each stale lock gets its own
    /// single-key PessimisticRollback, and a second stale lock of the same transaction in
    /// the same region must NOT be skipped or swept along.
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_rolls_back_stale_pessimistic_lock_on_primary_mismatch() {
        let rolled_back_keys = Arc::new(std::sync::Mutex::new(Vec::new()));

        let rolled_back_keys_captured = rolled_back_keys.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                                lock_info: Some(kvrpcpb::LockInfo::default()),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.start_version, EXPIRED_TXN_VERSION);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    assert_eq!(req.keys.len(), 1, "rollback must target a single key");
                    rolled_back_keys_captured
                        .lock()
                        .unwrap()
                        .push(req.keys[0].clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("must not sweep a region for a stale pessimistic lock");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let make_lock = |key: u8| kvrpcpb::LockInfo {
            key: vec![key],
            primary_lock: vec![9],
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };
        // Two stale pessimistic locks of the same transaction in the same mock region.
        let locks = vec![make_lock(1), make_lock(2)];

        let live_locks = resolve_locks(locks, Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(
            *rolled_back_keys.lock().unwrap(),
            vec![vec![1], vec![2]],
            "each stale lock must be rolled back individually"
        );
    }

    /// Whatever CheckTxnStatus says about a pessimistic lock's transaction once it no longer
    /// holds the primary the lock names, only that lock may be removed, and only with a
    /// single-key PessimisticRollback (client-go's `resolvePessimisticLock`). In particular,
    /// `LockNotExistDoNothing` and `TtlExpirePessimisticRollback` are not rollbacks at all:
    /// TiKV wrote no rollback record because the pessimistic transaction may have moved its
    /// primary (pingcap/tidb#42937) and still be alive, so a region-wide ResolveLock could
    /// roll back its legitimate locks — prewrite locks included — behind its back.
    #[rstest::rstest]
    #[case::lock_not_exist_do_nothing(kvrpcpb::CheckTxnStatusResponse {
        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
        ..Default::default()
    })]
    #[case::ttl_expire_pessimistic_rollback(kvrpcpb::CheckTxnStatusResponse {
        action: kvrpcpb::Action::TtlExpirePessimisticRollback as i32,
        ..Default::default()
    })]
    #[case::rolled_back(kvrpcpb::CheckTxnStatusResponse::default())]
    #[case::committed(kvrpcpb::CheckTxnStatusResponse {
        commit_version: 7,
        ..Default::default()
    })]
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_rolls_back_pessimistic_lock_by_itself(
        #[case] final_response: kvrpcpb::CheckTxnStatusResponse,
    ) {
        let rolled_back_keys = Arc::new(std::sync::Mutex::new(Vec::new()));

        let rolled_back_keys_captured = rolled_back_keys.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert!(req.resolving_pessimistic_lock);
                    // TiKV first reports the missing primary as TxnNotFound; the client
                    // retries with `rollback_if_not_exist` once the lock has expired.
                    if !req.rollback_if_not_exist {
                        return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                            error: Some(kvrpcpb::KeyError {
                                txn_not_found: Some(kvrpcpb::TxnNotFound {
                                    start_ts: req.lock_ts,
                                    primary_key: req.primary_key.clone(),
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::new(final_response.clone()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.start_version, EXPIRED_TXN_VERSION);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    assert_eq!(req.keys.len(), 1, "rollback must target a single key");
                    rolled_back_keys_captured
                        .lock()
                        .unwrap()
                        .push(req.keys[0].clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("must not sweep a region for a pessimistic lock");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: vec![9],
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(*rolled_back_keys.lock().unwrap(), vec![vec![1]]);
    }

    /// A pessimistic lock that is its own primary has already been unlocked by the
    /// CheckTxnStatus sent to it (`TtlExpirePessimisticRollback`); no further request is
    /// needed, and certainly not a region-wide ResolveLock.
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_skips_rollback_rpc_for_pessimistic_primary() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert!(req.resolving_pessimistic_lock);
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::TtlExpirePessimisticRollback as i32,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!(
                    "the primary was unlocked by CheckTxnStatus, unexpected request: {:?}",
                    req.type_id()
                );
            },
        )));

        let lock = kvrpcpb::LockInfo {
            key: PRIMARY_KEY.to_vec(),
            primary_lock: PRIMARY_KEY.to_vec(),
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
    }

    /// `LockNotExistDoNothing` for a stale pessimistic lock says nothing about the
    /// transaction's fate, so it must not be cached as "rolled back": a prewrite lock of the
    /// same transaction met right after has to be checked afresh — here the transaction turns
    /// out to be alive — instead of being swept by a region-wide rollback.
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_does_not_cache_undetermined_pessimistic_status() {
        let primary = vec![9];
        let live_primary_lock = kvrpcpb::LockInfo {
            key: primary.clone(),
            primary_lock: primary.clone(),
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            ..Default::default()
        };
        let rolled_back_keys = Arc::new(std::sync::Mutex::new(Vec::new()));
        let prewrite_status_checks = Arc::new(AtomicUsize::new(0));

        let rolled_back_keys_captured = rolled_back_keys.clone();
        let prewrite_status_checks_captured = prewrite_status_checks.clone();
        let live_primary_lock_captured = live_primary_lock.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    if !req.resolving_pessimistic_lock {
                        // Asked on behalf of the prewrite lock: the transaction is alive.
                        prewrite_status_checks_captured.fetch_add(1, Ordering::SeqCst);
                        return Ok(Box::new(still_locked_response(
                            live_primary_lock_captured.clone(),
                        )) as Box<dyn Any>);
                    }
                    if !req.rollback_if_not_exist {
                        return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                            error: Some(kvrpcpb::KeyError {
                                txn_not_found: Some(kvrpcpb::TxnNotFound {
                                    start_ts: req.lock_ts,
                                    primary_key: req.primary_key.clone(),
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rolled_back_keys_captured
                        .lock()
                        .unwrap()
                        .extend(req.keys.iter().cloned());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("must not roll back a live transaction on a stale pessimistic lock");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let stale_pessimistic_lock = kvrpcpb::LockInfo {
            key: vec![1],
            primary_lock: primary.clone(),
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            lock_type: kvrpcpb::Op::PessimisticLock as i32,
            ..Default::default()
        };
        let prewrite_lock = kvrpcpb::LockInfo {
            key: vec![2],
            primary_lock: primary,
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            lock_type: kvrpcpb::Op::Put as i32,
            ..Default::default()
        };

        let live_locks = resolve_locks(
            vec![stale_pessimistic_lock, prewrite_lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
        )
        .await
        .unwrap();

        assert_eq!(*rolled_back_keys.lock().unwrap(), vec![vec![1]]);
        assert_eq!(
            prewrite_status_checks.load(Ordering::SeqCst),
            1,
            "the prewrite lock's status must be queried, not served from the cache"
        );
        assert_eq!(live_locks.len(), 1);
        assert_eq!(live_locks[0].key, live_primary_lock.key);
    }

    /// A PrimaryMismatch for a non-pessimistic lock is unexpected (client-go treats it as an
    /// error too) and must propagate instead of rolling anything back.
    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_propagates_primary_mismatch_for_non_pessimistic_lock() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                                lock_info: Some(kvrpcpb::LockInfo::default()),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("must not resolve a lock on unexpected primary mismatch");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let lock = kvrpcpb::LockInfo {
            key: PRIMARY_KEY.to_vec(),
            primary_lock: vec![2],
            lock_version: EXPIRED_TXN_VERSION,
            lock_ttl: 1,
            ..Default::default()
        };

        let result =
            resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable).await;
        assert!(matches!(result, Err(Error::KeyError(_))));
    }

    #[test]
    fn format_key_for_log_hex_encodes_the_prefix() {
        assert_eq!(format_key_for_log(b"hello"), "len=5, prefix=68656C6C6F");
    }

    #[test]
    fn format_key_for_log_truncates_the_prefix_to_16_bytes() {
        let key: Vec<u8> = (0u8..20).collect();
        assert_eq!(
            format_key_for_log(&key),
            "len=20, prefix=000102030405060708090A0B0C0D0E0F"
        );
    }
}
