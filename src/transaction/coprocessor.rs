// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Coprocessor (e.g. DAG) requests over a transactional snapshot.
//!
//! The request and response payloads are opaque bytes: callers encode a `tipb.DAGRequest` (or
//! any other coprocessor request type) themselves and decode the returned `tipb.SelectResponse`.
//! This module only deals with distributing the request over regions, retries, lock resolution
//! and paging, mirroring TiDB's `store/copr` (`copIterator`).
//!
//! Execution model, per TiDB:
//! - The key ranges are split by region into *tasks* (one task per region).
//! - Up to `concurrency` tasks are executed concurrently. With `keep_order`, responses are
//!   yielded in task (key) order; otherwise in completion order.
//! - A task that meets a region error backs off, re-splits its *remaining* ranges using the
//!   (refreshed) region cache and processes the resulting sub-tasks in order.
//! - A task that meets a lock resolves it (honouring async commit / 1PC through the regular lock
//!   resolver) and retries at the same `start_ts`.
//! - With paging, every response carries the scanned range; the task continues with the
//!   remaining ranges and a growing page size.

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use log::debug;
use log::warn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::backoff::Backoff;
use crate::internal_err;
use crate::metrics;
use crate::metrics::BackoffType;
use crate::pd::PdClient;
use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::region::RegionWithLeader;
use crate::request::plan::grpc_error_action;
use crate::request::plan::handle_region_error;
use crate::request::plan::is_grpc_error;
use crate::request::plan::GrpcErrorAction;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::RetryOptions;
use crate::stats::tikv_stats;
use crate::store::Request;
use crate::timestamp::TimestampExt;
use crate::transaction::resolve_locks;
use crate::BoundRange;
use crate::Error;
use crate::Key;
use crate::Result;

/// `coprocessor.Request.tp` for a `tipb.DAGRequest`.
pub const REQ_TYPE_DAG: i64 = 103;
/// `coprocessor.Request.tp` for a `tipb.AnalyzeReq`.
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// `coprocessor.Request.tp` for a `tipb.ChecksumRequest`.
pub const REQ_TYPE_CHECKSUM: i64 = 105;

/// Default number of region tasks executed concurrently (TiDB's `tidb_distsql_scan_concurrency`).
pub const DEFAULT_COPROCESSOR_CONCURRENCY: usize = 15;
/// Default per-RPC timeout of a coprocessor request (TiDB's `copr-req-timeout`).
pub const DEFAULT_COPROCESSOR_TIMEOUT: Duration = Duration::from_secs(60);

/// Responses buffered per task (keep-order) or per worker (unordered) before a worker blocks.
const RESPONSE_BUFFER_PER_TASK: usize = 2;

/// A coprocessor request: an opaque payload executed by TiKV over a set of key ranges.
///
/// The ranges must be sorted in ascending order and must not overlap. Keys are raw TiKV keys
/// (e.g. TiDB's `t{table_id}_r{handle}` record keys); an empty end key means "to the end".
///
/// ```rust,no_run
/// # use tikv_client::{CopRequest, transaction::REQ_TYPE_DAG};
/// # let dag_bytes: Vec<u8> = vec![];
/// let req = CopRequest::new(REQ_TYPE_DAG, dag_bytes, vec![(b"t\x80".to_vec()..b"t\x81".to_vec()).into()])
///     .keep_order(true)
///     .concurrency(4);
/// ```
#[derive(Clone)]
pub struct CopRequest {
    tp: i64,
    data: Vec<u8>,
    ranges: Vec<BoundRange>,
    keep_order: bool,
    desc: bool,
    concurrency: usize,
    paging: Option<(u64, u64)>,
    schema_ver: i64,
    timeout: Duration,
    retry_options: Option<RetryOptions>,
}

impl fmt::Debug for CopRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopRequest")
            .field("tp", &self.tp)
            .field("data_len", &self.data.len())
            .field("ranges", &self.ranges)
            .field("keep_order", &self.keep_order)
            .field("desc", &self.desc)
            .field("concurrency", &self.concurrency)
            .field("paging", &self.paging)
            .field("schema_ver", &self.schema_ver)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl CopRequest {
    /// Create a request of type `tp` (e.g. [`REQ_TYPE_DAG`]) with the encoded payload `data`.
    pub fn new(tp: i64, data: Vec<u8>, ranges: Vec<BoundRange>) -> CopRequest {
        CopRequest {
            tp,
            data,
            ranges,
            keep_order: false,
            desc: false,
            concurrency: DEFAULT_COPROCESSOR_CONCURRENCY,
            paging: None,
            schema_ver: 0,
            timeout: DEFAULT_COPROCESSOR_TIMEOUT,
            retry_options: None,
        }
    }

    /// Yield responses in key order (descending key order if [`desc`](Self::desc) is set).
    ///
    /// Without it, responses are yielded as soon as any region task produces them.
    pub fn keep_order(mut self, keep_order: bool) -> CopRequest {
        self.keep_order = keep_order;
        self
    }

    /// The DAG scans in descending key order. Must match the payload's scan direction: it
    /// determines task order and how paging computes the remaining ranges.
    pub fn desc(mut self, desc: bool) -> CopRequest {
        self.desc = desc;
        self
    }

    /// Maximum number of region tasks in flight (at least 1).
    pub fn concurrency(mut self, concurrency: usize) -> CopRequest {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Enable TiKV paging: the first page of every task holds at most `min_size` rows, and the
    /// page size doubles on each following page up to `max_size`.
    pub fn paging(mut self, min_size: u64, max_size: u64) -> CopRequest {
        let min_size = min_size.max(1);
        self.paging = Some((min_size, max_size.max(min_size)));
        self
    }

    /// The schema version the payload was built against (`coprocessor.Request.schema_ver`).
    pub fn schema_ver(mut self, schema_ver: i64) -> CopRequest {
        self.schema_ver = schema_ver;
        self
    }

    /// Per-RPC timeout. Defaults to [`DEFAULT_COPROCESSOR_TIMEOUT`].
    pub fn timeout(mut self, timeout: Duration) -> CopRequest {
        self.timeout = timeout;
        self
    }

    /// Backoff for region errors and live locks of *each region task*. Defaults to the
    /// retry options of the transaction/snapshot issuing the request.
    pub fn retry_options(mut self, retry_options: RetryOptions) -> CopRequest {
        self.retry_options = Some(retry_options);
        self
    }
}

/// One coprocessor response: the opaque payload returned by TiKV for (a page of) a region task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopResponse {
    /// The encoded response, e.g. a `tipb.SelectResponse` for DAG requests.
    pub data: Vec<u8>,
    /// The region that produced this response.
    pub region_id: u64,
    /// With paging, the key range this response covers, as reported by TiKV.
    pub range: Option<(Key, Key)>,
}

/// A [`Stream`] of [`CopResponse`]s returned by [`Transaction::coprocessor`].
///
/// The stream ends after yielding the first error. Dropping it cancels all in-flight tasks.
///
/// [`Transaction::coprocessor`]: crate::Transaction::coprocessor
pub struct CopResponseStream {
    receivers: VecDeque<mpsc::Receiver<TaskMessage>>,
    /// Number of `Done` messages still expected from the front receiver.
    pending_done: VecDeque<usize>,
    workers: Vec<JoinHandle<()>>,
    finished: bool,
}

enum TaskMessage {
    Response(Result<CopResponse>),
    /// The task completed; anything after this on a per-task channel would be a bug.
    Done,
}

impl CopResponseStream {
    fn empty() -> CopResponseStream {
        CopResponseStream {
            receivers: VecDeque::new(),
            pending_done: VecDeque::new(),
            workers: Vec::new(),
            finished: true,
        }
    }

    fn finish(&mut self) {
        self.finished = true;
        self.receivers.clear();
        for worker in &self.workers {
            worker.abort();
        }
    }
}

impl Drop for CopResponseStream {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.abort();
        }
    }
}

impl Stream for CopResponseStream {
    type Item = Result<CopResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.finished {
            return Poll::Ready(None);
        }
        loop {
            let Some(receiver) = this.receivers.front_mut() else {
                this.finished = true;
                return Poll::Ready(None);
            };
            match receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(TaskMessage::Response(Ok(response)))) => {
                    return Poll::Ready(Some(Ok(response)))
                }
                Poll::Ready(Some(TaskMessage::Response(Err(e)))) => {
                    this.finish();
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(TaskMessage::Done)) => {
                    let pending = this
                        .pending_done
                        .front_mut()
                        .expect("pending_done tracks receivers");
                    *pending -= 1;
                }
                Poll::Ready(None) => {
                    // All senders are gone. Unless every task reported `Done`, a worker
                    // died (panic/abort) and silently skipping its data would be wrong.
                    this.receivers.pop_front();
                    let pending = this.pending_done.pop_front().unwrap_or_default();
                    if pending != 0 {
                        this.finish();
                        return Poll::Ready(Some(Err(internal_err!(
                            "coprocessor worker exited before finishing its task"
                        ))));
                    }
                }
            }
        }
    }
}

/// A region task: the ranges of one region, processed by one worker.
#[derive(Clone, Debug)]
struct CopTask {
    region: RegionWithLeader,
    ranges: Vec<coprocessor::KeyRange>,
    /// Page size of the next request; 0 disables paging.
    paging_size: u64,
}

/// What processing one RPC of a task produced.
struct TaskOutcome {
    response: Option<CopResponse>,
    /// Tasks to process next, in order, before the rest of the queue.
    remains: Vec<CopTask>,
}

/// State shared by all workers of one coprocessor request.
struct CopContext<PdC: PdClient> {
    pd_client: Arc<PdC>,
    keyspace: Keyspace,
    timestamp: Timestamp,
    /// The request sent to TiKV, minus per-task fields (context, ranges, paging size).
    template: coprocessor::Request,
    timeout: Duration,
    desc: bool,
    max_paging_size: u64,
    retry_options: RetryOptions,
}

/// The RPC wrapper: carries the coprocessor-specific timeout.
#[derive(Clone)]
struct CoprocessorRpc {
    inner: coprocessor::Request,
    timeout: Duration,
}

#[async_trait]
impl Request for CoprocessorRpc {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        _timeout: Duration,
    ) -> Result<Box<dyn std::any::Any>> {
        let mut req = self.inner.clone().into_request();
        req.set_timeout(self.timeout);
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
}

/// Send a coprocessor request at `timestamp` and stream back the responses.
pub(crate) async fn send_coprocessor_request<PdC: PdClient>(
    pd_client: Arc<PdC>,
    keyspace: Keyspace,
    timestamp: Timestamp,
    default_retry_options: RetryOptions,
    req: CopRequest,
) -> Result<CopResponseStream> {
    let ranges = req
        .ranges
        .into_iter()
        .map(|range| {
            let (start, end) = range.encode_keyspace(keyspace, KeyMode::Txn).into_keys();
            coprocessor::KeyRange {
                start: start.into(),
                end: end.map(Into::into).unwrap_or_default(),
            }
        })
        .collect::<Vec<_>>();
    check_ranges_sorted(&ranges)?;

    let (paging_size, max_paging_size) = req.paging.unwrap_or((0, 0));
    let tasks = split_ranges_by_region(&pd_client, &ranges, paging_size, req.desc).await?;
    debug!(
        "coprocessor: tp {}, start_ts {}, {} ranges, {} region tasks, keep_order {}",
        req.tp,
        timestamp.version(),
        ranges.len(),
        tasks.len(),
        req.keep_order
    );
    if tasks.is_empty() {
        return Ok(CopResponseStream::empty());
    }

    let mut template = coprocessor::Request {
        tp: req.tp,
        data: req.data,
        start_ts: timestamp.version(),
        schema_ver: req.schema_ver,
        ..Default::default()
    };
    template
        .context
        .get_or_insert(kvrpcpb::Context::default())
        .api_version = keyspace.api_version().into();

    let ctx = Arc::new(CopContext {
        pd_client,
        keyspace,
        timestamp,
        template,
        timeout: req.timeout,
        desc: req.desc,
        max_paging_size,
        retry_options: req.retry_options.unwrap_or(default_retry_options),
    });

    let task_count = tasks.len();
    let worker_count = req.concurrency.min(task_count);
    let mut receivers = VecDeque::new();
    let mut pending_done = VecDeque::new();
    let mut queue = VecDeque::with_capacity(task_count);
    if req.keep_order {
        for task in tasks {
            let (tx, rx) = mpsc::channel(RESPONSE_BUFFER_PER_TASK);
            queue.push_back((task, tx));
            receivers.push_back(rx);
            pending_done.push_back(1);
        }
    } else {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER_PER_TASK * worker_count);
        for task in tasks {
            queue.push_back((task, tx.clone()));
        }
        receivers.push_back(rx);
        pending_done.push_back(task_count);
    }

    let queue = Arc::new(Mutex::new(queue));
    let workers = (0..worker_count)
        .map(|_| {
            let ctx = ctx.clone();
            let queue = queue.clone();
            tokio::spawn(async move {
                loop {
                    let next = queue.lock().unwrap().pop_front();
                    let Some((task, tx)) = next else {
                        return;
                    };
                    if !run_task(&ctx, task, &tx).await {
                        // The consumer is gone or the request failed; stop pulling tasks.
                        queue.lock().unwrap().clear();
                        return;
                    }
                    let _ = tx.send(TaskMessage::Done).await;
                }
            })
        })
        .collect();

    Ok(CopResponseStream {
        receivers,
        pending_done,
        workers,
        finished: false,
    })
}

fn check_ranges_sorted(ranges: &[coprocessor::KeyRange]) -> Result<()> {
    for (i, range) in ranges.iter().enumerate() {
        let last = i + 1 == ranges.len();
        if range.end.is_empty() && !last {
            return Err(internal_err!(
                "coprocessor: only the last range may be unbounded"
            ));
        }
        if let Some(next) = ranges.get(i + 1) {
            if next.start < range.end {
                return Err(internal_err!(
                    "coprocessor: ranges must be sorted and non-overlapping"
                ));
            }
        }
    }
    Ok(())
}

/// Process one region task (and whatever it splits into) to completion.
///
/// Returns false if the request should stop: an error was sent, or the consumer is gone.
async fn run_task<PdC: PdClient>(
    ctx: &CopContext<PdC>,
    task: CopTask,
    tx: &mpsc::Sender<TaskMessage>,
) -> bool {
    // As in TiDB, one backoff budget covers a task and everything it is split into.
    let mut region_backoff = ctx.retry_options.region_backoff.clone();
    let mut lock_backoff = ctx.retry_options.lock_backoff.clone();
    let mut remains = VecDeque::from([task]);
    while let Some(task) = remains.pop_front() {
        match handle_task_once(ctx, task, &mut region_backoff, &mut lock_backoff).await {
            Ok(outcome) => {
                if let Some(response) = outcome.response {
                    if tx.send(TaskMessage::Response(Ok(response))).await.is_err() {
                        return false;
                    }
                }
                for task in outcome.remains.into_iter().rev() {
                    remains.push_front(task);
                }
            }
            Err(e) => {
                let _ = tx.send(TaskMessage::Response(Err(e))).await;
                return false;
            }
        }
    }
    true
}

/// Send one RPC for `task` and decide what to do next (TiDB's `handleTaskOnce`).
async fn handle_task_once<PdC: PdClient>(
    ctx: &CopContext<PdC>,
    mut task: CopTask,
    region_backoff: &mut Backoff,
    lock_backoff: &mut Backoff,
) -> Result<TaskOutcome> {
    let region_store = match ctx
        .pd_client
        .clone()
        .map_region_to_store(task.region.clone())
        .await
    {
        Ok(region_store) => region_store,
        Err(e) => {
            debug!("coprocessor: failed to map region to store: {:?}", e);
            if let Ok(store_id) = task.region.get_store_id() {
                ctx.pd_client.invalidate_store_cache(store_id).await;
            }
            ctx.pd_client
                .invalidate_region_cache(task.region.ver_id())
                .await;
            return retry_after_backoff(ctx, task, region_backoff, e).await;
        }
    };

    let mut rpc = CoprocessorRpc {
        inner: ctx.template.clone(),
        timeout: ctx.timeout,
    };
    rpc.inner.ranges = task.ranges.clone();
    rpc.inner.paging_size = task.paging_size;
    rpc.set_leader(&region_store.region_with_leader)?;

    let stats = tikv_stats(rpc.label());
    // Downcast right away: `Box<dyn Any>` is not `Send` and must not live across an await.
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
            debug!(
                "coprocessor: grpc error, region {:?}: {:?}",
                task.region.ver_id(),
                e
            );
            match grpc_error_action(&e) {
                GrpcErrorAction::TryNextPeerAndInvalidate => {
                    if let Ok(store_id) = task.region.get_store_id() {
                        ctx.pd_client.invalidate_store_cache(store_id).await;
                    }
                    ctx.pd_client
                        .invalidate_region_cache(task.region.ver_id())
                        .await;
                    return retry_after_backoff(ctx, task, region_backoff, e).await;
                }
                GrpcErrorAction::TryNextPeer => {
                    return retry_after_backoff(ctx, task, region_backoff, e).await;
                }
                GrpcErrorAction::Return => return Err(e),
            }
        }
        Err(e) => return Err(e),
    };

    if let Some(region_error) = resp.region_error.take() {
        debug!(
            "coprocessor: region error, region {:?}: {:?}",
            task.region.ver_id(),
            region_error
        );
        let Some(delay) = region_backoff.next_delay_duration() else {
            warn!(
                "coprocessor: giving up after exhausting retries on region error, region: {:?}",
                task.region.ver_id()
            );
            return Err(Error::RegionError(Box::new(region_error)));
        };
        let backoff_type = metrics::backoff_type_for_region_error(&region_error);
        let resolved =
            handle_region_error(ctx.pd_client.clone(), region_error, region_store).await?;
        if !resolved {
            metrics::backoff(backoff_type, delay);
            sleep(delay).await;
        }
        return resplit(ctx, task).await;
    }

    if let Some(lock) = resp.locked.take() {
        debug!(
            "coprocessor: meet lock, region {:?}, lock_version {}",
            task.region.ver_id(),
            lock.lock_version
        );
        let live_locks = resolve_locks(
            vec![lock],
            ctx.timestamp.clone(),
            ctx.pd_client.clone(),
            ctx.keyspace,
        )
        .await?;
        if !live_locks.is_empty() {
            match lock_backoff.next_delay_duration() {
                Some(delay) => {
                    metrics::backoff(BackoffType::TikvLockFast, delay);
                    sleep(delay).await
                }
                None => return Err(Error::ResolveLockError(live_locks)),
            }
        }
        // Retry the same ranges at the same start_ts and page size.
        return Ok(TaskOutcome {
            response: None,
            remains: vec![task],
        });
    }

    if !resp.other_error.is_empty() {
        warn!(
            "coprocessor: other error, region {:?}: {}",
            task.region.ver_id(),
            resp.other_error
        );
        return Err(Error::KvError {
            message: format!("coprocessor error: {}", resp.other_error),
        });
    }

    let paging_range = resp.range.take();
    let response = CopResponse {
        data: std::mem::take(&mut resp.data),
        region_id: task.region.id(),
        range: paging_range
            .as_ref()
            .map(|r| (r.start.clone().into(), r.end.clone().into())),
    };
    let mut remains = Vec::new();
    // TiKV reports the scanned range when it stopped early (paging); continue after it.
    if let Some(range) = paging_range {
        task.ranges = calculate_remain(task.ranges, &range, ctx.desc);
        if !task.ranges.is_empty() {
            if task.paging_size > 0 {
                task.paging_size = (task.paging_size * 2).min(ctx.max_paging_size);
            }
            remains.push(task);
        }
    }
    Ok(TaskOutcome {
        response: Some(response),
        remains,
    })
}

async fn retry_after_backoff<PdC: PdClient>(
    ctx: &CopContext<PdC>,
    task: CopTask,
    region_backoff: &mut Backoff,
    error: Error,
) -> Result<TaskOutcome> {
    match region_backoff.next_delay_duration() {
        Some(delay) => {
            // gRPC errors back off as client-go's `tikvRPC`; failing to locate the region
            // or store as `regionMiss`.
            let t = if is_grpc_error(&error) {
                BackoffType::TikvRpc
            } else {
                BackoffType::RegionMiss
            };
            metrics::backoff(t, delay);
            sleep(delay).await;
            resplit(ctx, task).await
        }
        None => Err(error),
    }
}

/// Re-split the remaining ranges of `task` with the current region cache.
async fn resplit<PdC: PdClient>(ctx: &CopContext<PdC>, task: CopTask) -> Result<TaskOutcome> {
    let remains =
        split_ranges_by_region(&ctx.pd_client, &task.ranges, task.paging_size, ctx.desc).await?;
    Ok(TaskOutcome {
        response: None,
        remains,
    })
}

/// Split sorted, non-overlapping `ranges` into one task per region, in key order (reversed
/// if `desc`). An empty end key means unbounded.
async fn split_ranges_by_region<PdC: PdClient>(
    pd_client: &Arc<PdC>,
    ranges: &[coprocessor::KeyRange],
    paging_size: u64,
    desc: bool,
) -> Result<Vec<CopTask>> {
    let mut tasks: Vec<CopTask> = Vec::new();
    for range in ranges {
        if !range.end.is_empty() && range.start >= range.end {
            continue;
        }
        let mut start = range.start.clone();
        loop {
            let start_key: Key = start.clone().into();
            let region = pd_client.region_for_key(&start_key).await?;
            if !region.contains(&start_key) {
                return Err(internal_err!(
                    "region {} does not contain key {:?}",
                    region.id(),
                    start
                ));
            }
            let region_end = &region.region.end_key;
            let last_piece =
                region_end.is_empty() || (!range.end.is_empty() && range.end <= *region_end);
            let end = if last_piece {
                range.end.clone()
            } else {
                region_end.clone()
            };
            let piece = coprocessor::KeyRange {
                start,
                end: end.clone(),
            };
            match tasks.last_mut() {
                Some(task) if task.region.id() == region.id() => task.ranges.push(piece),
                _ => tasks.push(CopTask {
                    region,
                    ranges: vec![piece],
                    paging_size,
                }),
            }
            if last_piece {
                break;
            }
            start = end;
        }
    }
    if desc {
        tasks.reverse();
    }
    Ok(tasks)
}

/// The ranges left to scan after TiKV reported scanning `scanned` (TiDB's `calculateRemain`).
///
/// Ascending: everything before `scanned.end` is consumed. Descending: everything at or after
/// `scanned.start` is consumed.
fn calculate_remain(
    ranges: Vec<coprocessor::KeyRange>,
    scanned: &coprocessor::KeyRange,
    desc: bool,
) -> Vec<coprocessor::KeyRange> {
    if desc {
        let split = &scanned.start;
        ranges
            .into_iter()
            .filter(|r| r.start < *split)
            .map(|mut r| {
                if r.end.is_empty() || r.end > *split {
                    r.end = split.clone();
                }
                r
            })
            .collect()
    } else {
        let split = &scanned.end;
        if split.is_empty() {
            return Vec::new();
        }
        ranges
            .into_iter()
            .filter(|r| r.end.is_empty() || r.end > *split)
            .map(|mut r| {
                if r.start < *split {
                    r.start = split.clone();
                }
                r
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use futures::StreamExt;
    use futures::TryStreamExt;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::proto::metapb;

    type Hook = dyn Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static;

    fn range(start: &[u8], end: &[u8]) -> BoundRange {
        if end.is_empty() {
            (start.to_vec()..).into()
        } else {
            (start.to_vec()..end.to_vec()).into()
        }
    }

    fn kr(start: &[u8], end: &[u8]) -> coprocessor::KeyRange {
        coprocessor::KeyRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    fn describe(req: &coprocessor::Request) -> String {
        let ctx = req.context.as_ref().unwrap();
        let ranges = req
            .ranges
            .iter()
            .map(|r| format!("{:?}-{:?}", r.start, r.end))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}|{}", ctx.region_id, ranges)
    }

    /// A response echoing the region and ranges of the request.
    fn echo(req: &coprocessor::Request) -> Result<Box<dyn Any>> {
        Ok(Box::new(coprocessor::Response {
            data: describe(req).into_bytes(),
            ..Default::default()
        }))
    }

    fn region_error(error: errorpb::Error) -> Result<Box<dyn Any>> {
        Ok(Box::new(coprocessor::Response {
            region_error: Some(error),
            ..Default::default()
        }))
    }

    fn mock_pd(
        hook: impl Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    ) -> MockPdClient {
        MockPdClient::new(MockKvClient::with_dispatch_hook(hook))
    }

    fn cop_hook(
        f: impl Fn(&coprocessor::Request) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    ) -> Box<Hook> {
        Box::new(move |req: &dyn Any| {
            let req = req
                .downcast_ref::<coprocessor::Request>()
                .expect("unexpected request type");
            f(req)
        })
    }

    async fn run(pd: MockPdClient, req: CopRequest) -> Result<Vec<CopResponse>> {
        run_with_retry(pd, req, RetryOptions::default_optimistic()).await
    }

    async fn run_with_retry(
        pd: MockPdClient,
        req: CopRequest,
        retry: RetryOptions,
    ) -> Result<Vec<CopResponse>> {
        send_coprocessor_request(
            Arc::new(pd),
            Keyspace::Disable,
            Timestamp::default(),
            retry,
            req,
        )
        .await?
        .try_collect()
        .await
    }

    fn datas(responses: &[CopResponse]) -> Vec<String> {
        responses
            .iter()
            .map(|r| String::from_utf8(r.data.clone()).unwrap())
            .collect()
    }

    fn request(ranges: Vec<BoundRange>) -> CopRequest {
        CopRequest::new(REQ_TYPE_DAG, b"dag".to_vec(), ranges)
    }

    // Mock regions: 1 = [, [10]), 2 = [[10], [250, 250]), 3 = [[250, 250], ).
    fn multi_region_ranges() -> Vec<BoundRange> {
        vec![
            range(&[1], &[3]),
            range(&[5], &[12]),
            range(&[20], &[251]),
            range(&[252], &[]),
        ]
    }

    fn multi_region_expected() -> Vec<String> {
        vec![
            "1|[1]-[3],[5]-[10]".to_owned(),
            "2|[10]-[12],[20]-[250, 250]".to_owned(),
            "3|[250, 250]-[251],[252]-[]".to_owned(),
        ]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_split_ranges_by_region_keep_order() {
        let pd = mock_pd(cop_hook(|req| {
            // Make the first region the slowest: order must still be preserved.
            if req.context.as_ref().unwrap().region_id == 1 {
                std::thread::sleep(Duration::from_millis(100));
            }
            assert_eq!(req.tp, REQ_TYPE_DAG);
            assert_eq!(req.data, b"dag");
            echo(req)
        }));
        let responses = run(pd, request(multi_region_ranges()).keep_order(true))
            .await
            .unwrap();
        assert_eq!(datas(&responses), multi_region_expected());
        assert_eq!(
            responses.iter().map(|r| r.region_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_unordered_returns_all_responses() {
        let pd = mock_pd(cop_hook(|req| {
            if req.context.as_ref().unwrap().region_id == 1 {
                std::thread::sleep(Duration::from_millis(100));
            }
            echo(req)
        }));
        let responses = run(pd, request(multi_region_ranges())).await.unwrap();
        let mut got = datas(&responses);
        // Region 1 is slow, so with concurrency it is not first.
        assert_ne!(got[0], multi_region_expected()[0]);
        got.sort();
        assert_eq!(got, multi_region_expected());
    }

    #[tokio::test]
    async fn test_desc_keep_order() {
        let pd = mock_pd(cop_hook(echo));
        let responses = run(
            pd,
            request(multi_region_ranges()).keep_order(true).desc(true),
        )
        .await
        .unwrap();
        let mut expected = multi_region_expected();
        expected.reverse();
        assert_eq!(datas(&responses), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrency_is_bounded() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let (in_flight_c, max_c) = (in_flight.clone(), max_in_flight.clone());
        let pd = mock_pd(cop_hook(move |req| {
            let now = in_flight_c.fetch_add(1, Ordering::SeqCst) + 1;
            max_c.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(50));
            in_flight_c.fetch_sub(1, Ordering::SeqCst);
            echo(req)
        }));
        let responses = run(
            pd,
            request(multi_region_ranges())
                .keep_order(true)
                .concurrency(2),
        )
        .await
        .unwrap();
        assert_eq!(datas(&responses), multi_region_expected());
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_empty_ranges() {
        let pd = mock_pd(cop_hook(|_| panic!("no request expected")));
        assert!(run(pd, request(vec![])).await.unwrap().is_empty());
        let pd = mock_pd(cop_hook(|_| panic!("no request expected")));
        assert!(run(pd, request(vec![range(&[5], &[5])]))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_unsorted_ranges_rejected() {
        let pd = mock_pd(cop_hook(|_| panic!("no request expected")));
        let err = run(pd, request(vec![range(&[5], &[8]), range(&[1], &[3])]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InternalError { .. }), "{err:?}");
    }

    fn split_region(
        id: u64,
        start: &[u8],
        end: &[u8],
        version: u64,
        store_id: u64,
    ) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = id;
        region.region.start_key = start.to_vec();
        region.region.end_key = end.to_vec();
        region.region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 0,
            version,
        });
        region.leader = Some(metapb::Peer {
            store_id,
            ..Default::default()
        });
        region
    }

    /// Region 2 is split at [100] into 2 (version 1) and 4 once its cache entry is invalidated.
    fn pd_with_split_on_invalidate(
        hook: impl Fn(&coprocessor::Request) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    ) -> (MockPdClient, Arc<AtomicBool>) {
        let split = Arc::new(AtomicBool::new(false));
        let split_for_lookup = split.clone();
        let split_for_invalidate = split.clone();
        let pd = mock_pd(cop_hook(hook))
            .with_region_for_key_hook(move |key| {
                let bytes: &[u8] = key.into();
                Ok(if bytes < &[10][..] {
                    MockPdClient::region1()
                } else if bytes >= &[250, 250][..] {
                    MockPdClient::region3()
                } else if !split_for_lookup.load(Ordering::SeqCst) {
                    MockPdClient::region2()
                } else if bytes < &[100][..] {
                    split_region(2, &[10], &[100], 1, 42)
                } else {
                    split_region(4, &[100], &[250, 250], 1, 44)
                })
            })
            .with_invalidate_region_hook(move |ver_id| {
                if ver_id.id == 2 {
                    split_for_invalidate.store(true, Ordering::SeqCst);
                }
            });
        (pd, split)
    }

    #[tokio::test]
    async fn test_epoch_not_match_resplits_remaining_ranges() {
        let stale_requests = Arc::new(AtomicUsize::new(0));
        let stale = stale_requests.clone();
        let (pd, split) = pd_with_split_on_invalidate(move |req| {
            let ctx = req.context.as_ref().unwrap();
            if ctx.region_id == 2 && ctx.region_epoch.as_ref().unwrap().version == 0 {
                stale.fetch_add(1, Ordering::SeqCst);
                return region_error(errorpb::Error {
                    epoch_not_match: Some(errorpb::EpochNotMatch::default()),
                    ..Default::default()
                });
            }
            echo(req)
        });
        let responses = run(
            pd,
            request(vec![
                range(&[1], &[3]),
                range(&[20], &[200]),
                range(&[252], &[]),
            ])
            .keep_order(true),
        )
        .await
        .unwrap();
        assert!(split.load(Ordering::SeqCst));
        assert_eq!(stale_requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            datas(&responses),
            vec!["1|[1]-[3]", "2|[20]-[100]", "4|[100]-[200]", "3|[252]-[]"]
        );
    }

    #[tokio::test]
    async fn test_region_error_retry_exhausted() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = attempts.clone();
        let pd = mock_pd(cop_hook(move |_| {
            attempts_c.fetch_add(1, Ordering::SeqCst);
            region_error(errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                ..Default::default()
            })
        }));
        let retry = RetryOptions::new(Backoff::no_jitter_backoff(1, 1, 3), Backoff::no_backoff());
        let err = run_with_retry(pd, request(vec![range(&[1], &[3])]), retry)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::RegionError(e) if e.server_is_busy.is_some()),
            "{err:?}"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_not_leader_updates_leader_and_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = attempts.clone();
        let updated = Arc::new(Mutex::new(None));
        let updated_c = updated.clone();
        let pd = mock_pd(cop_hook(move |req| {
            if attempts_c.fetch_add(1, Ordering::SeqCst) == 0 {
                return region_error(errorpb::Error {
                    not_leader: Some(errorpb::NotLeader {
                        region_id: 1,
                        leader: Some(metapb::Peer {
                            id: 7,
                            store_id: 51,
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                });
            }
            echo(req)
        }))
        .with_update_leader_hook(move |ver_id, peer| {
            *updated_c.lock().unwrap() = Some((ver_id.id, peer.store_id));
            Ok(())
        });
        let responses = run(pd, request(vec![range(&[1], &[3])])).await.unwrap();
        assert_eq!(datas(&responses), vec!["1|[1]-[3]"]);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(*updated.lock().unwrap(), Some((1, 51)));
    }

    #[tokio::test]
    async fn test_grpc_error_invalidates_and_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = attempts.clone();
        let invalidated = Arc::new(AtomicUsize::new(0));
        let invalidated_c = invalidated.clone();
        let pd = mock_pd(cop_hook(move |req| {
            if attempts_c.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::GrpcAPI(tonic::Status::unavailable("store down")));
            }
            echo(req)
        }))
        .with_invalidate_region_hook(move |_| {
            invalidated_c.fetch_add(1, Ordering::SeqCst);
        });
        let responses = run(pd, request(vec![range(&[1], &[3])])).await.unwrap();
        assert_eq!(datas(&responses), vec!["1|[1]-[3]"]);
        assert_eq!(invalidated.load(Ordering::SeqCst), 1);

        // A non-retryable status is returned as is.
        let pd = mock_pd(cop_hook(|_| {
            Err(Error::GrpcAPI(tonic::Status::invalid_argument("bad")))
        }));
        let err = run(pd, request(vec![range(&[1], &[3])])).await.unwrap_err();
        assert!(matches!(err, Error::GrpcAPI(_)), "{err:?}");
    }

    #[tokio::test]
    async fn test_other_error_ends_stream() {
        let pd = mock_pd(cop_hook(|req| {
            if req.context.as_ref().unwrap().region_id == 2 {
                return Ok(Box::new(coprocessor::Response {
                    other_error: "boom".to_owned(),
                    ..Default::default()
                }));
            }
            echo(req)
        }));
        let mut stream = send_coprocessor_request(
            Arc::new(pd),
            Keyspace::Disable,
            Timestamp::default(),
            RetryOptions::default_optimistic(),
            request(multi_region_ranges())
                .keep_order(true)
                .concurrency(1),
        )
        .await
        .unwrap();
        assert_eq!(
            datas(&[stream.next().await.unwrap().unwrap()]),
            vec![multi_region_expected()[0].clone()]
        );
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::KvError { message } if message.contains("boom")),
            "{err:?}"
        );
        assert!(stream.next().await.is_none());
    }

    const LOCK_TS: u64 = 5;
    const COMMIT_TS: u64 = 8;

    fn lock_info() -> kvrpcpb::LockInfo {
        kvrpcpb::LockInfo {
            key: vec![2],
            primary_lock: vec![1],
            lock_version: LOCK_TS,
            lock_ttl: 3000,
            ..Default::default()
        }
    }

    /// Dispatch for lock tests: the coprocessor request meets a lock until it is resolved;
    /// `CheckTxnStatus` reports the transaction as still locked for the first
    /// `live_check_count` checks and committed after that.
    fn lock_hook(
        live_check_count: usize,
        cop_attempts: Arc<AtomicUsize>,
        resolved: Arc<Mutex<Vec<(u64, u64)>>>,
    ) -> MockPdClient {
        let checks = AtomicUsize::new(0);
        mock_pd(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<coprocessor::Request>() {
                cop_attempts.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_ts, 0);
                if resolved.lock().unwrap().is_empty() {
                    return Ok(Box::new(coprocessor::Response {
                        locked: Some(lock_info()),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                echo(req)
            } else if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                assert_eq!(req.primary_key, vec![1]);
                assert_eq!(req.lock_ts, LOCK_TS);
                if checks.fetch_add(1, Ordering::SeqCst) < live_check_count {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 3000,
                        lock_info: Some(lock_info()),
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    }));
                }
                Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                    commit_version: COMMIT_TS,
                    ..Default::default()
                }))
            } else if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                resolved
                    .lock()
                    .unwrap()
                    .push((req.start_version, req.commit_version));
                Ok(Box::new(kvrpcpb::ResolveLockResponse::default()))
            } else {
                panic!("unexpected request");
            }
        })
    }

    #[tokio::test]
    async fn test_lock_resolved_then_retried() {
        let cop_attempts = Arc::new(AtomicUsize::new(0));
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let pd = lock_hook(0, cop_attempts.clone(), resolved.clone());
        let responses = run(pd, request(vec![range(&[1], &[3])])).await.unwrap();
        assert_eq!(datas(&responses), vec!["1|[1]-[3]"]);
        assert_eq!(cop_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(*resolved.lock().unwrap(), vec![(LOCK_TS, COMMIT_TS)]);
    }

    #[tokio::test]
    async fn test_live_lock_backs_off_until_committed() {
        let cop_attempts = Arc::new(AtomicUsize::new(0));
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let pd = lock_hook(2, cop_attempts.clone(), resolved.clone());
        let responses = run(pd, request(vec![range(&[1], &[3])])).await.unwrap();
        assert_eq!(datas(&responses), vec!["1|[1]-[3]"]);
        // Two rounds meet a live lock, the third resolves it, the fourth reads.
        assert_eq!(cop_attempts.load(Ordering::SeqCst), 4);
        assert_eq!(*resolved.lock().unwrap(), vec![(LOCK_TS, COMMIT_TS)]);
    }

    #[tokio::test]
    async fn test_live_lock_backoff_exhausted() {
        let cop_attempts = Arc::new(AtomicUsize::new(0));
        let resolved = Arc::new(Mutex::new(Vec::new()));
        let pd = lock_hook(usize::MAX, cop_attempts.clone(), resolved.clone());
        let retry = RetryOptions::new(
            Backoff::no_jitter_backoff(1, 1, 3),
            Backoff::no_jitter_backoff(1, 1, 2),
        );
        let err = run_with_retry(pd, request(vec![range(&[1], &[3])]), retry)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::ResolveLockError(locks) if locks.len() == 1),
            "{err:?}"
        );
        assert_eq!(cop_attempts.load(Ordering::SeqCst), 3);
        assert!(resolved.lock().unwrap().is_empty());
    }

    /// Simulates TiKV paging over one row per key byte: a page returns at most `paging_size`
    /// rows starting at the first range's start, plus the scanned range unless drained.
    fn paging_response(req: &coprocessor::Request) -> Result<Box<dyn Any>> {
        assert_eq!(req.ranges.len(), 1);
        let r = &req.ranges[0];
        let start = r.start[0];
        let end = r.end[0];
        let page_end = start.saturating_add(req.paging_size as u8).min(end);
        Ok(Box::new(coprocessor::Response {
            data: format!("{}:{}..{}", req.paging_size, start, page_end).into_bytes(),
            range: (page_end < end).then(|| kr(&[start], &[page_end])),
            ..Default::default()
        }))
    }

    #[tokio::test]
    async fn test_paging() {
        let pd = mock_pd(cop_hook(paging_response));
        let responses = run(pd, request(vec![range(&[1], &[9])]).paging(2, 3))
            .await
            .unwrap();
        assert_eq!(datas(&responses), vec!["2:1..3", "3:3..6", "3:6..9"]);
        assert_eq!(responses[0].range, Some((vec![1].into(), vec![3].into())));
        assert_eq!(responses[2].range, None);
    }

    #[tokio::test]
    async fn test_paging_region_error_retries_remaining_range() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_c = attempts.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_c = requests.clone();
        let pd = mock_pd(cop_hook(move |req| {
            requests_c.lock().unwrap().push(describe(req));
            if attempts_c.fetch_add(1, Ordering::SeqCst) == 1 {
                return region_error(errorpb::Error {
                    region_not_found: Some(errorpb::RegionNotFound::default()),
                    ..Default::default()
                });
            }
            paging_response(req)
        }));
        let responses = run(pd, request(vec![range(&[1], &[9])]).paging(2, 8))
            .await
            .unwrap();
        // The failed second page is retried from where the first page ended, with its size.
        assert_eq!(datas(&responses), vec!["2:1..3", "4:3..7", "8:7..9"]);
        assert_eq!(
            *requests.lock().unwrap(),
            vec!["1|[1]-[9]", "1|[3]-[9]", "1|[3]-[9]", "1|[7]-[9]"]
        );
    }

    #[test]
    fn test_calculate_remain() {
        let ranges = vec![kr(&[1], &[3]), kr(&[5], &[8]), kr(&[10], &[])];
        assert_eq!(
            calculate_remain(ranges.clone(), &kr(&[1], &[6]), false),
            vec![kr(&[6], &[8]), kr(&[10], &[])]
        );
        assert_eq!(
            calculate_remain(ranges.clone(), &kr(&[1], &[3]), false),
            vec![kr(&[5], &[8]), kr(&[10], &[])]
        );
        assert_eq!(
            calculate_remain(ranges.clone(), &kr(&[1], &[]), false),
            vec![]
        );
        assert_eq!(
            calculate_remain(ranges.clone(), &kr(&[12], &[20]), true),
            vec![kr(&[1], &[3]), kr(&[5], &[8]), kr(&[10], &[12])]
        );
        assert_eq!(
            calculate_remain(ranges.clone(), &kr(&[6], &[20]), true),
            vec![kr(&[1], &[3]), kr(&[5], &[6])]
        );
        assert_eq!(calculate_remain(ranges, &kr(&[1], &[20]), true), vec![]);
    }
}
