// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The `BatchCommands` transport, after client-go's batch client
//! (`internal/client/client_batch.go` and `client.go` of client-go `c3b5079`, the version TiDB
//! v8.5.8 pins).
//!
//! One [`BatchConn`] per store address (client-go `batchConn`): callers put their request on a
//! bounded channel and wait for the answer on a oneshot. One send-loop task per store collects
//! what is pending into a `BatchCommandsRequest` (client-go `batchSendLoop`) and writes it to
//! one of the store's connections, round-robin (`getClientAndSend`). Each connection carries
//! one long-lived `BatchCommands` stream with its own receive-loop task
//! (`batchCommandsClient.batchRecvLoop`) that hands each response to its caller by request id
//! and recreates the stream after a failure (`recreateStreamingClient`).
//!
//! Cancellation: a caller that stops waiting (timeout, or its future dropped) drops its
//! oneshot receiver; the send loop then skips the entry (client-go `entry.canceled`) and the
//! receive loop drops its response.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc as stream_mpsc;
use log::info;
use log::warn;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tonic::transport::Channel;
use tonic::Status;

use crate::backoff::Backoff;
use crate::proto::tikvpb::batch_commands_request;
use crate::proto::tikvpb::batch_commands_response;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::proto::tikvpb::BatchCommandsRequest;
use crate::proto::tikvpb::BatchCommandsResponse;

/// The command of one request in a batch.
pub type BatchCmd = batch_commands_request::request::Cmd;
/// The command of one response in a batch.
pub type BatchRespCmd = batch_commands_response::response::Cmd;

/// client-go `config.BatchPolicyBasic` / `Standard` / `Positive` / `Custom`
/// (config/client.go:55-62).
pub const BATCH_POLICY_BASIC: &str = "basic";
pub const BATCH_POLICY_STANDARD: &str = "standard";
pub const BATCH_POLICY_POSITIVE: &str = "positive";
pub const BATCH_POLICY_CUSTOM: &str = "custom";

/// The batch knobs of client-go's `TiKVClient` config, with its defaults
/// (config/client.go:50-51, :88-97, :115-117, :170-175, :194).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BatchConfig {
    /// `max-batch-size`: the most requests in one `BatchCommandsRequest`; 0 sends every
    /// request as its own unary RPC. Default 128.
    pub max_batch_size: usize,
    /// `batch-policy`: `basic`, `standard` (default), `positive` or `custom {json}`.
    pub batch_policy: String,
    /// `max-batch-wait-time`: how long to wait for more requests when TiKV reports its
    /// transport layer overloaded (above `overload_threshold`). Default 0: never.
    pub max_batch_wait_time: Duration,
    /// `batch-wait-size`: how many requests to wait for in that case. Default 8.
    pub batch_wait_size: usize,
    /// `overload-threshold`: TiKV's transport-layer load above which the client waits.
    /// Default 200.
    pub overload_threshold: u64,
    /// `max-concurrency-request-limit`: requests in flight per connection. Default
    /// `i64::MAX` (no limit).
    pub max_concurrency_request_limit: i64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        BatchConfig {
            max_batch_size: 128,
            batch_policy: BATCH_POLICY_STANDARD.to_owned(),
            max_batch_wait_time: Duration::ZERO,
            batch_wait_size: 8,
            overload_threshold: 200,
            max_concurrency_request_limit: i64::MAX,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The caller's side.

/// One request waiting to be sent (client-go `batchCommandsEntry`, client_batch.go:144-161).
struct Entry {
    cmd: BatchCmd,
    /// The caller's slot; closed once the caller stopped waiting (`entry.canceled`).
    res: oneshot::Sender<Result<BatchRespCmd, Status>>,
    start: Instant,
}

impl Entry {
    fn is_canceled(&self) -> bool {
        self.res.is_closed()
    }
}

/// The batch transport to one store (client-go `batchConn`). Dropping the last handle stops
/// its tasks and fails what is still in flight (client-go `batchConn.Close`).
pub struct BatchConn {
    tx: mpsc::Sender<Entry>,
    _closed: watch::Sender<()>,
}

impl BatchConn {
    /// Start the send loop over `clients` (one per connection to the store at `target`).
    pub(crate) fn start(target: &str, clients: &[TikvClient<Channel>], cfg: BatchConfig) -> Self {
        let (closed_tx, closed_rx) = watch::channel(());
        let max_batch_size = cfg.max_batch_size.max(1);
        let (tx, rx) = mpsc::channel(max_batch_size);
        let load = Arc::new(AtomicU64::new(0));
        let metrics = Arc::new(crate::metrics::BatchMetrics::new(target));
        let clients: Arc<[Arc<BatchCommandsClient>]> = clients
            .iter()
            .map(|grpc| {
                Arc::new(BatchCommandsClient {
                    target: target.to_owned(),
                    grpc: grpc.clone(),
                    state: Mutex::new(StreamState::default()),
                    batched: Mutex::new(HashMap::new()),
                    sent: AtomicI64::new(0),
                    limit: cfg.max_concurrency_request_limit,
                    tikv_load: load.clone(),
                    track_load: !cfg.max_batch_wait_time.is_zero(),
                    closed: closed_rx.clone(),
                })
            })
            .collect();
        let send_loop = SendLoop {
            rx,
            clients,
            builder: Builder::new(max_batch_size),
            index: 0,
            load,
            metrics,
            cfg,
        };
        tokio::spawn(send_loop.run());
        BatchConn {
            tx,
            _closed: closed_tx,
        }
    }

    /// client-go `sendBatchRequest` (client_batch.go:1209-1282): wait at most `timeout` for
    /// the send loop to take the request and for its response.
    pub async fn send(&self, cmd: BatchCmd, timeout: Duration) -> Result<BatchRespCmd, Status> {
        let start = Instant::now();
        let deadline = tokio::time::Instant::from_std(start + timeout);
        let (res, rx) = oneshot::channel();
        let entry = Entry { cmd, res, start };
        match tokio::time::timeout_at(deadline, self.tx.send(entry)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Status::unavailable("batchConn closed")),
            Err(_) => return Err(Status::deadline_exceeded("wait sendLoop")),
        }
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Status::unavailable("batchConn closed")),
            Err(_) => Err(Status::deadline_exceeded(format!(
                "wait recvLoop timeout, timeout:{timeout:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The send loop.

/// client-go `batchCommandsBuilder` (client_batch.go:192-317), without priorities (this client
/// sends no resource-control priority, so every entry is a normal-priority task and the queue
/// is first in, first out).
struct Builder {
    id_alloc: u64,
    entries: VecDeque<Entry>,
    latest_req_start: Option<Instant>,
}

impl Builder {
    fn new(max_batch_size: usize) -> Self {
        Builder {
            id_alloc: 0,
            entries: VecDeque::with_capacity(max_batch_size),
            latest_req_start: None,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn push(&mut self, entry: Entry) {
        if self.latest_req_start.is_none_or(|t| entry.start > t) {
            self.latest_req_start = Some(entry.start);
        }
        self.entries.push_back(entry);
    }

    /// client-go `reset`: drop canceled entries, keep the rest for the next batch.
    fn reset(&mut self) {
        self.entries.retain(|e| !e.is_canceled());
    }

    /// client-go `buildWithLimit`: take up to `limit` live entries, in arrival order, giving
    /// each the next request id; the rest stay for the next batch.
    fn build_with_limit(&mut self, limit: i64, mut collect: impl FnMut(u64, Entry)) {
        let mut count = 0i64;
        while count < limit {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            if entry.is_canceled() {
                continue;
            }
            count += 1;
            collect(self.id_alloc, entry);
            self.id_alloc += 1;
        }
    }

    /// client-go `cancel`: fail every entry.
    fn cancel(&mut self, err: &Status) {
        for entry in self.entries.drain(..) {
            let _ = entry.res.send(Err(err.clone()));
        }
    }
}

/// client-go `turboBatchOptions` (client_batch.go:502-520).
#[derive(Clone, Copy, Debug, Default, PartialEq, Deserialize)]
#[serde(default)]
struct TurboBatchOptions {
    v: i32,
    n: i32,
    t: f64,
    w: f64,
    p: f64,
    q: f64,
}

const TURBO_BATCH_ALWAYS: i32 = 0;
const TURBO_BATCH_TIME_BASED: i32 = 1;
const TURBO_BATCH_PROB_BASED: i32 = 2;

/// client-go `presetBatchPolicies` (client_batch.go:488-494).
fn preset_batch_policy(policy: &str) -> Option<TurboBatchOptions> {
    match policy {
        BATCH_POLICY_BASIC => Some(TurboBatchOptions::default()),
        BATCH_POLICY_STANDARD => Some(TurboBatchOptions {
            v: TURBO_BATCH_TIME_BASED,
            t: 0.0001,
            n: 5,
            w: 0.2,
            p: 0.8,
            q: 0.8,
        }),
        BATCH_POLICY_POSITIVE => Some(TurboBatchOptions {
            v: TURBO_BATCH_ALWAYS,
            t: 0.0001,
            ..Default::default()
        }),
        _ => None,
    }
}

/// client-go `turboBatchTrigger` (client_batch.go:522-603).
#[derive(Debug, Default)]
struct TurboBatchTrigger {
    opts: TurboBatchOptions,
    est_fetch_more_prob: f64,
    est_arrival_interval: f64,
    max_arrival_interval: f64,
}

impl TurboBatchTrigger {
    /// client-go `newTurboBatchTriggerFromPolicy`: `false` when the policy is invalid and the
    /// default one is used instead.
    fn from_policy(policy: &str) -> (TurboBatchTrigger, bool) {
        if let Some(opts) = preset_batch_policy(policy) {
            return (
                TurboBatchTrigger {
                    opts,
                    ..Default::default()
                },
                true,
            );
        }
        let raw = policy.strip_prefix(BATCH_POLICY_CUSTOM).unwrap_or(policy);
        match serde_json::from_str::<TurboBatchOptions>(raw.trim()) {
            Ok(opts) => (
                TurboBatchTrigger {
                    opts,
                    ..Default::default()
                },
                true,
            ),
            Err(_) => (
                TurboBatchTrigger {
                    opts: preset_batch_policy(BATCH_POLICY_STANDARD).expect("preset"),
                    ..Default::default()
                },
                false,
            ),
        }
    }

    fn turbo_wait_seconds(&self) -> f64 {
        self.opts.t
    }

    fn turbo_wait_time(&self) -> Duration {
        Duration::from_secs_f64(self.opts.t.max(0.0))
    }

    fn need_fetch_more(&mut self, req_arrival_interval: Duration) -> bool {
        match self.opts.v {
            TURBO_BATCH_TIME_BASED => {
                let mut this = req_arrival_interval.as_secs_f64();
                if self.max_arrival_interval == 0.0 {
                    self.max_arrival_interval = self.turbo_wait_seconds() * self.opts.n as f64;
                }
                if this > self.max_arrival_interval {
                    this = self.max_arrival_interval;
                }
                if self.est_arrival_interval == 0.0 {
                    self.est_arrival_interval = this;
                } else {
                    self.est_arrival_interval =
                        self.opts.w * this + (1.0 - self.opts.w) * self.est_arrival_interval;
                }
                self.est_arrival_interval < self.turbo_wait_seconds() * self.opts.p
            }
            TURBO_BATCH_PROB_BASED => {
                let this = if req_arrival_interval.as_secs_f64() < self.turbo_wait_seconds() {
                    1.0
                } else {
                    0.0
                };
                self.est_fetch_more_prob =
                    self.opts.w * this + (1.0 - self.opts.w) * self.est_fetch_more_prob;
                self.est_fetch_more_prob > self.opts.p
            }
            _ => true,
        }
    }

    fn preferred_batch_wait_size(
        &self,
        avg_batch_wait_size: f64,
        def_batch_wait_size: usize,
    ) -> usize {
        if self.opts.v == TURBO_BATCH_ALWAYS {
            return def_batch_wait_size;
        }
        let n = avg_batch_wait_size.trunc();
        let m = avg_batch_wait_size - n;
        let mut size = n as usize;
        if m >= self.opts.q {
            size += 1;
        }
        size
    }
}

struct SendLoop {
    rx: mpsc::Receiver<Entry>,
    clients: Arc<[Arc<BatchCommandsClient>]>,
    builder: Builder,
    index: usize,
    load: Arc<AtomicU64>,
    metrics: Arc<crate::metrics::BatchMetrics>,
    cfg: BatchConfig,
}

impl SendLoop {
    /// client-go `batchSendLoop` (client_batch.go:610-677). Ends when every [`BatchConn`]
    /// handle is dropped.
    async fn run(mut self) {
        let (mut trigger, ok) = TurboBatchTrigger::from_policy(&self.cfg.batch_policy);
        if !ok {
            warn!(
                "fallback to default batch policy due to invalid value {:?}",
                self.cfg.batch_policy
            );
        }
        let turbo_wait = trigger.turbo_wait_time();
        let max_batch_size = self.cfg.max_batch_size.max(1);
        let mut avg_batch_wait_size = self.cfg.batch_wait_size as f64;
        loop {
            self.builder.reset();
            let Some(head_arrival_interval) = self.fetch_all_pending(max_batch_size).await else {
                // Every handle is gone: fail what is left (client-go `batchConn.Close`).
                self.builder
                    .cancel(&Status::unavailable("batchConn closed"));
                return;
            };
            let batch_size = self.builder.len();
            if batch_size < max_batch_size {
                if !self.cfg.max_batch_wait_time.is_zero()
                    && self.load.load(Ordering::Relaxed) > self.cfg.overload_threshold
                {
                    // The target TiKV is overloaded: wait a while to collect more requests.
                    self.metrics.wait_overload();
                    self.fetch_more_pending(
                        max_batch_size,
                        self.cfg.batch_wait_size,
                        self.cfg.max_batch_wait_time,
                    )
                    .await;
                } else if !turbo_wait.is_zero()
                    && !head_arrival_interval.is_zero()
                    && trigger.need_fetch_more(head_arrival_interval)
                {
                    let wait_size = trigger
                        .preferred_batch_wait_size(avg_batch_wait_size, self.cfg.batch_wait_size);
                    self.fetch_more_pending(max_batch_size, wait_size, turbo_wait)
                        .await;
                }
            }
            let length = self.builder.len();
            self.metrics.pending(self.rx.len() + length);
            if length == 0 {
                continue;
            }
            avg_batch_wait_size = 0.2 * length as f64 + 0.8 * avg_batch_wait_size;
            self.get_client_and_send();
        }
    }

    /// client-go `fetchAllPendingRequests` (client_batch.go:389-431): wait for the first
    /// request, then take what else is already queued. `None` once every handle is dropped;
    /// otherwise the head request's arrival interval since the newest request of the previous
    /// batch (zero for the first one).
    async fn fetch_all_pending(&mut self, max_batch_size: usize) -> Option<Duration> {
        let latest = self.builder.latest_req_start;
        let head = self.rx.recv().await?;
        let interval = latest.map_or(Duration::ZERO, |l| head.start.saturating_duration_since(l));
        self.builder.push(head);
        while self.builder.len() < max_batch_size {
            match self.rx.try_recv() {
                Ok(e) => self.builder.push(e),
                Err(_) => break,
            }
        }
        Some(interval)
    }

    /// client-go `fetchMorePendingRequests` (client_batch.go:433-483): wait up to `max_wait`
    /// for `batch_wait_size` requests, then take what else is queued, yielding once.
    ///
    /// Waits under a millisecond (the `standard` policy waits 100 µs) poll the queue and yield
    /// between polls: the runtime's timers have a resolution of one millisecond.
    async fn fetch_more_pending(
        &mut self,
        max_batch_size: usize,
        batch_wait_size: usize,
        max_wait: Duration,
    ) {
        let deadline = Instant::now() + max_wait;
        let precise = max_wait < Duration::from_millis(1);
        while self.builder.len() < batch_wait_size {
            match self.rx.try_recv() {
                Ok(e) => {
                    self.builder.push(e);
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => return,
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            if Instant::now() >= deadline {
                break;
            }
            if precise {
                tokio::task::yield_now().await;
            } else {
                match tokio::time::timeout_at(deadline.into(), self.rx.recv()).await {
                    Ok(Some(e)) => self.builder.push(e),
                    Ok(None) => return,
                    Err(_) => break,
                }
            }
        }
        // One more non-blocking try up to `max_batch_size`, so that the observed batch sizes
        // can raise `batch_wait_size`.
        let mut yielded = false;
        while self.builder.len() < max_batch_size {
            match self.rx.try_recv() {
                Ok(e) => self.builder.push(e),
                Err(mpsc::error::TryRecvError::Empty) if !yielded => {
                    tokio::task::yield_now().await;
                    yielded = true;
                }
                Err(_) => return,
            }
        }
    }

    /// client-go `getClientAndSend` (client_batch.go:684-748).
    fn get_client_and_send(&mut self) {
        let clients = self.clients.clone();
        let n = clients.len();
        let mut chosen = None;
        let mut reasons = Vec::new();
        for _ in 0..n {
            self.index = (self.index + 1) % n;
            let c = &clients[self.index];
            if c.available() > 0 {
                // client-go `tryLockForSend`: wait for the lock, fail if the stream is being
                // recreated.
                let state = c.state.lock().unwrap();
                if !state.recreating {
                    chosen = Some((c, state));
                    break;
                }
                reasons.push("tryLockForSend fail");
            } else {
                reasons.push("concurrency limit exceeded");
            }
        }
        let Some((cli, mut state)) = chosen else {
            info!(
                "no available connections, target {}, reasons {:?}",
                clients[self.index].target, reasons
            );
            crate::metrics::batch_no_available_connection();
            if self.cfg.max_concurrency_request_limit == i64::MAX {
                // client-go cancels only without a concurrency limit; with one, the requests
                // wait for their timeout.
                self.builder
                    .cancel(&Status::unavailable("no available connections"));
            }
            return;
        };
        let available = cli.available();
        let len = self.builder.len();
        let mut req = BatchCommandsRequest {
            requests: Vec::with_capacity(len),
            request_ids: Vec::with_capacity(len),
            client_send_time_ns: 0,
        };
        {
            let mut batched = cli.batched.lock().unwrap();
            self.builder.build_with_limit(available, |id, entry| {
                req.request_ids.push(id);
                req.requests.push(batch_commands_request::Request {
                    cmd: Some(entry.cmd),
                });
                batched.insert(id, entry.res);
            });
        }
        if req.request_ids.is_empty() {
            return;
        }
        cli.sent
            .fetch_add(req.request_ids.len() as i64, Ordering::Relaxed);
        self.metrics.batch_size(req.request_ids.len());
        cli.send(&mut state, req);
    }
}

// ---------------------------------------------------------------------------------------------
// One connection's stream.

#[derive(Default)]
struct StreamState {
    /// Feeds the current `BatchCommands` stream; `None` before the first send and while the
    /// stream is being recreated.
    stream: Option<stream_mpsc::UnboundedSender<BatchCommandsRequest>>,
    /// client-go `tryLock.reCreating`: the send loop skips this connection meanwhile.
    recreating: bool,
}

/// client-go `batchCommandsClient` (client_batch.go:823-863), one per connection.
struct BatchCommandsClient {
    target: String,
    grpc: TikvClient<Channel>,
    /// client-go `tryLock`: held by the send loop while it sends, and by the receive loop to
    /// mark the stream as being recreated.
    state: Mutex<StreamState>,
    /// client-go `batched`: the callers waiting for a response, by request id.
    batched: Mutex<HashMap<u64, oneshot::Sender<Result<BatchRespCmd, Status>>>>,
    sent: AtomicI64,
    limit: i64,
    tikv_load: Arc<AtomicU64>,
    track_load: bool,
    closed: watch::Receiver<()>,
}

impl BatchCommandsClient {
    /// client-go `available` (client_batch.go:869-880).
    fn available(&self) -> i64 {
        let sent = self.sent.load(Ordering::Relaxed);
        if sent > 0 {
            if self.limit > sent {
                return self.limit - sent;
            }
            return 0;
        }
        self.limit
    }

    fn is_stopped(&self) -> bool {
        self.closed.has_changed().is_err()
    }

    /// client-go `batchCommandsClient.send` (client_batch.go:882-911), with the stream created
    /// on first use (`initBatchClient`, :1172-1195).
    fn send(self: &Arc<Self>, state: &mut StreamState, req: BatchCommandsRequest) {
        if state.stream.is_none() {
            let (tx, rx) = stream_mpsc::unbounded();
            state.stream = Some(tx);
            tokio::spawn(self.clone().recv_loop(rx));
        }
        let stream = state.stream.as_ref().expect("stream");
        if let Err(e) = stream.unbounded_send(req) {
            let req = e.into_inner();
            info!(
                "sending batch commands meets error, target {}, request ids {:?}",
                self.target, req.request_ids
            );
            self.fail_requests_by_ids(
                &Status::unavailable("batch commands stream closed"),
                &req.request_ids,
            );
        }
    }

    /// client-go `failPendingRequests` (client_batch.go:924-934).
    fn fail_pending_requests(&self, err: &Status) {
        let pending: Vec<_> = self.batched.lock().unwrap().drain().collect();
        self.sent.fetch_sub(pending.len() as i64, Ordering::Relaxed);
        for (_, res) in pending {
            let _ = res.send(Err(err.clone()));
        }
    }

    /// client-go `failRequestsByIDs` (client_batch.go:950-958).
    fn fail_requests_by_ids(&self, err: &Status, ids: &[u64]) {
        let mut batched = self.batched.lock().unwrap();
        for id in ids {
            if let Some(res) = batched.remove(id) {
                self.sent.fetch_sub(1, Ordering::Relaxed);
                let _ = res.send(Err(err.clone()));
            }
        }
    }

    /// Hand the responses of one batch to their callers (client_batch.go:1069-1097).
    fn on_response(&self, resp: BatchCommandsResponse) {
        let BatchCommandsResponse {
            responses,
            request_ids,
            transport_layer_load,
            ..
        } = resp;
        let mut done = Vec::with_capacity(request_ids.len());
        {
            let mut batched = self.batched.lock().unwrap();
            for (id, resp) in request_ids.into_iter().zip(responses) {
                match batched.remove(&id) {
                    Some(res) => done.push((res, resp)),
                    None => warn!("batchRecvLoop receives outdated response, request id {id}"),
                }
            }
        }
        self.sent.fetch_sub(done.len() as i64, Ordering::Relaxed);
        for (res, resp) in done {
            if !res.is_closed() {
                let result = resp
                    .cmd
                    .ok_or_else(|| Status::internal("Unknown command response"));
                let _ = res.send(result);
            }
        }
        if transport_layer_load > 0 && self.track_load {
            // The load matters only when the batch-wait strategy is on.
            self.tikv_load
                .store(transport_layer_load, Ordering::Relaxed);
        }
    }

    /// Run one stream until it fails; `None` once the store's [`BatchConn`] is dropped.
    async fn run_stream(
        &self,
        rx: stream_mpsc::UnboundedReceiver<BatchCommandsRequest>,
        healthy: &mut bool,
    ) -> Option<Status> {
        let mut closed = self.closed.clone();
        let mut grpc = self.grpc.clone();
        let call = grpc.batch_commands(rx);
        let resp = tokio::select! {
            r = call => r,
            _ = closed.changed() => return None,
        };
        let mut streaming = match resp {
            Ok(r) => r.into_inner(),
            Err(status) => return Some(status),
        };
        loop {
            let msg = tokio::select! {
                biased;
                m = streaming.message() => m,
                _ = closed.changed() => return None,
            };
            match msg {
                Ok(Some(resp)) => {
                    *healthy = true;
                    self.on_response(resp);
                }
                Ok(None) => {
                    return Some(Status::unavailable(
                        "batch commands stream closed by the server",
                    ))
                }
                Err(status) => return Some(status),
            }
        }
    }

    /// client-go `batchRecvLoop` (client_batch.go:1019-1100) with `recreateStreamingClient`
    /// (:1108-1162).
    async fn recv_loop(self: Arc<Self>, rx: stream_mpsc::UnboundedReceiver<BatchCommandsRequest>) {
        let mut rx = Some(rx);
        // client-go retries the stream with `BoTiKVRPC` (100 ms → 2 s, equal jitter,
        // config/retry/config.go:122) until it is recreated.
        let mut backoff = Backoff::equal_jitter_backoff(100, 2000, u32::MAX);
        let mut failed_unused = false;
        loop {
            let mut healthy = false;
            let err = self
                .run_stream(rx.take().expect("stream"), &mut healthy)
                .await;
            let Some(err) = err else {
                self.fail_pending_requests(&Status::unavailable("batch client closed"));
                return;
            };
            if self.is_stopped() {
                self.fail_pending_requests(&Status::unavailable("batch client closed"));
                return;
            }
            info!(
                "batchRecvLoop fails when receiving, needs to reconnect, target {}: {err:?}",
                self.target
            );
            let unavailable_since = Instant::now();
            {
                let mut state = self.state.lock().unwrap();
                state.recreating = true;
                state.stream = None;
            }
            self.fail_pending_requests(&err);
            // client-go recreates the stream at once when the connection is still up
            // (`waitConnReady` returns immediately) and otherwise retries with backoff. A
            // stream here cannot tell before it is used: the first recreation is immediate,
            // and a recreated stream that fails again before any response is taken as a
            // connection that is not ready.
            if healthy {
                backoff = Backoff::equal_jitter_backoff(100, 2000, u32::MAX);
                failed_unused = false;
            } else if !failed_unused {
                failed_unused = true;
            } else if let Some(delay) = backoff.next_delay_duration() {
                let mut closed = self.closed.clone();
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = closed.changed() => return,
                }
            }
            let (tx, new_rx) = stream_mpsc::unbounded();
            rx = Some(new_rx);
            {
                let mut state = self.state.lock().unwrap();
                state.stream = Some(tx);
                state.recreating = false;
            }
            crate::metrics::batch_client_unavailable(unavailable_since.elapsed());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(start: Instant) -> (Entry, oneshot::Receiver<Result<BatchRespCmd, Status>>) {
        let (res, rx) = oneshot::channel();
        (
            Entry {
                cmd: BatchCmd::Get(Default::default()),
                res,
                start,
            },
            rx,
        )
    }

    #[test]
    fn builder_skips_canceled_and_respects_the_limit() {
        let now = Instant::now();
        let mut b = Builder::new(8);
        let mut receivers = Vec::new();
        for _ in 0..4 {
            let (e, r) = entry(now);
            b.push(e);
            receivers.push(Some(r));
        }
        receivers[1] = None; // the caller of the second one stopped waiting
        let mut ids = Vec::new();
        b.build_with_limit(2, |id, _| ids.push(id));
        // The first and third entries, numbered 0 and 1; the fourth is left for the next batch.
        assert_eq!(ids, [0, 1]);
        assert_eq!(b.len(), 1);
        receivers[3] = None;
        b.reset();
        assert_eq!(b.len(), 0);
        let (e, _r) = entry(now);
        b.push(e);
        let mut ids = Vec::new();
        b.build_with_limit(i64::MAX, |id, _| ids.push(id));
        assert_eq!(ids, [2]);
    }

    #[test]
    fn turbo_trigger_follows_client_go() {
        let (mut t, ok) = TurboBatchTrigger::from_policy(BATCH_POLICY_STANDARD);
        assert!(ok);
        assert_eq!(t.turbo_wait_time(), Duration::from_micros(100));
        // Arrivals 10 µs apart: below p * t = 80 µs.
        assert!(t.need_fetch_more(Duration::from_micros(10)));
        // A long gap is capped at n * t = 500 µs; the estimate moves by w = 0.2.
        assert!(!t.need_fetch_more(Duration::from_secs(1)));
        assert!((t.est_arrival_interval - (0.2 * 0.0005 + 0.8 * 0.00001)).abs() < 1e-12);
        assert_eq!(t.preferred_batch_wait_size(3.79, 8), 3);
        assert_eq!(t.preferred_batch_wait_size(3.85, 8), 4);

        let (basic, ok) = TurboBatchTrigger::from_policy(BATCH_POLICY_BASIC);
        assert!(ok);
        assert!(basic.turbo_wait_time().is_zero());

        let (positive, _) = TurboBatchTrigger::from_policy(BATCH_POLICY_POSITIVE);
        assert_eq!(positive.preferred_batch_wait_size(3.9, 8), 8);

        let (custom, ok) =
            TurboBatchTrigger::from_policy(r#"custom {"v":2,"t":0.001,"w":0.5,"p":0.4}"#);
        assert!(ok);
        assert_eq!(custom.opts.v, TURBO_BATCH_PROB_BASED);
        assert_eq!(custom.opts.t, 0.001);

        let (bad, ok) = TurboBatchTrigger::from_policy("nonsense");
        assert!(!ok);
        assert_eq!(
            bad.opts,
            preset_batch_policy(BATCH_POLICY_STANDARD).unwrap()
        );
    }

    // -----------------------------------------------------------------------------------------
    // Against a fake TiKV that serves `BatchCommands`.

    use std::convert::Infallible;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::task::Context;
    use std::task::Poll;

    use futures::StreamExt;
    use tonic::codegen::http;
    use tonic::codegen::Body;
    use tonic::codegen::BoxFuture;
    use tonic::codegen::Service;
    use tonic::codegen::StdError;
    use tonic::server::NamedService;
    use tonic::server::StreamingService;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::Server;
    use tonic::Code;

    use crate::proto::kvrpcpb;
    use crate::store::KvClient;
    use crate::store::KvConnect;
    use crate::store::Request;
    use crate::store::TikvConnect;
    use crate::Error;
    use crate::SecurityManager;

    const BIG: usize = 6 * 1024 * 1024;

    /// A TiKV whose `BatchCommands` answers each `Get` with its key as the value, except:
    /// key `slow` is never answered, key `drop` ends the stream with an error, key `big` gets a
    /// 6 MiB value. Responses of one batch come back one by one, in reverse order.
    #[derive(Clone, Default)]
    struct FakeTikv {
        streams: Arc<AtomicUsize>,
        open: Arc<AtomicUsize>,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
        paths: Arc<Mutex<Vec<String>>>,
    }

    impl NamedService for FakeTikv {
        const NAME: &'static str = "tikvpb.Tikv";
    }

    type RespStream =
        Pin<Box<dyn futures::Stream<Item = Result<BatchCommandsResponse, Status>> + Send>>;

    struct Batch(FakeTikv);

    impl StreamingService<BatchCommandsRequest> for Batch {
        type Response = BatchCommandsResponse;
        type ResponseStream = RespStream;
        type Future = BoxFuture<tonic::Response<RespStream>, Status>;

        fn call(
            &mut self,
            req: tonic::Request<tonic::Streaming<BatchCommandsRequest>>,
        ) -> Self::Future {
            let fake = self.0.clone();
            fake.streams.fetch_add(1, Ordering::SeqCst);
            fake.open.fetch_add(1, Ordering::SeqCst);
            let mut inbound = req.into_inner();
            let (tx, rx) = stream_mpsc::unbounded();
            tokio::spawn(async move {
                struct Open(Arc<AtomicUsize>);
                impl Drop for Open {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                let _open = Open(fake.open.clone());
                while let Some(Ok(batch)) = inbound.next().await {
                    fake.batch_sizes.lock().unwrap().push(batch.requests.len());
                    for (id, req) in batch.request_ids.iter().zip(batch.requests).rev() {
                        let key = match req.cmd {
                            Some(BatchCmd::Get(get)) => get.key,
                            _ => panic!("unexpected command"),
                        };
                        let value = match key.as_slice() {
                            b"slow" => continue,
                            b"drop" => {
                                let _ = tx.unbounded_send(Err(Status::unavailable("dropped")));
                                return;
                            }
                            b"big" => vec![7; BIG],
                            _ => key,
                        };
                        let resp = BatchCommandsResponse {
                            responses: vec![batch_commands_response::Response {
                                cmd: Some(BatchRespCmd::Get(kvrpcpb::GetResponse {
                                    value,
                                    ..Default::default()
                                })),
                            }],
                            request_ids: vec![*id],
                            ..Default::default()
                        };
                        if tx.unbounded_send(Ok(resp)).is_err() {
                            return;
                        }
                    }
                }
            });
            Box::pin(async move { Ok(tonic::Response::new(Box::pin(rx) as RespStream)) })
        }
    }

    impl<B> Service<http::Request<B>> for FakeTikv
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let path = req.uri().path().to_owned();
            self.paths.lock().unwrap().push(path.clone());
            let fake = self.clone();
            Box::pin(async move {
                if path == "/tikvpb.Tikv/BatchCommands" {
                    Ok(
                        tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                            .streaming(Batch(fake), req)
                            .await,
                    )
                } else {
                    Ok(Status::unimplemented(path).into_http())
                }
            })
        }
    }

    async fn start_fake(fake: FakeTikv) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            Server::builder()
                .add_service(fake)
                .serve_with_incoming(incoming),
        );
        format!("127.0.0.1:{}", addr.port())
    }

    async fn connect(addr: &str, max_decoding: usize) -> super::super::client::KvRpcClient {
        TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(10),
            max_decoding,
        )
        .connect(addr)
        .await
        .unwrap()
    }

    fn get(key: &str) -> kvrpcpb::GetRequest {
        kvrpcpb::GetRequest {
            key: key.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    async fn value_of(client: &impl KvClient, req: &dyn Request) -> crate::Result<Vec<u8>> {
        let resp = client.dispatch(req).await?;
        Ok(resp.downcast::<kvrpcpb::GetResponse>().unwrap().value)
    }

    fn grpc_code(err: &Error) -> Code {
        match err {
            Error::GrpcAPI(status) => status.code(),
            other => panic!("not a gRPC status: {other:?}"),
        }
    }

    /// Concurrent requests share batches and each caller gets its own response, though the
    /// responses come back out of order.
    #[tokio::test]
    async fn concurrent_requests_are_batched_and_demultiplexed() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        let calls = (0..64).map(|i| {
            let client = client.clone();
            async move {
                let key = format!("k{i}");
                assert_eq!(
                    value_of(&client, &get(&key)).await.unwrap(),
                    key.into_bytes()
                );
            }
        });
        futures::future::join_all(calls).await;
        let sizes = fake.batch_sizes.lock().unwrap().clone();
        assert_eq!(sizes.iter().sum::<usize>(), 64);
        assert!(sizes.len() < 64, "no batching: {sizes:?}");
        assert!(sizes.iter().all(|n| *n <= 128));
        assert_eq!(fake.streams.load(Ordering::SeqCst), 1);
        // Only the stream: no unary call.
        assert!(fake
            .paths
            .lock()
            .unwrap()
            .iter()
            .all(|p| p == "/tikvpb.Tikv/BatchCommands"));
    }

    /// A request without an answer fails at its deadline (client-go "wait recvLoop timeout"),
    /// and the stream keeps serving others; so does a caller that gave up on its own.
    #[tokio::test]
    async fn a_request_times_out_and_the_stream_stays_usable() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        let mut slow = get("slow");
        slow.set_max_execution_duration(Duration::from_millis(100));
        let start = Instant::now();
        let err = value_of(&client, &slow).await.unwrap_err();
        assert_eq!(grpc_code(&err), Code::DeadlineExceeded);
        assert!(format!("{err:?}").contains("wait recvLoop timeout"));
        assert!(start.elapsed() < Duration::from_secs(5));

        // A caller that is dropped while waiting.
        let gave_up =
            tokio::time::timeout(Duration::from_millis(50), value_of(&client, &get("slow"))).await;
        assert!(gave_up.is_err());

        assert_eq!(value_of(&client, &get("a")).await.unwrap(), b"a");
        assert_eq!(fake.streams.load(Ordering::SeqCst), 1);
    }

    /// When the stream fails, every request in flight on it fails at once with the stream's
    /// error (client-go `failPendingRequests`), and the stream is recreated for the next ones.
    #[tokio::test]
    async fn a_failed_stream_fails_pending_requests_and_is_recreated() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        let waiting = {
            let client = client.clone();
            tokio::spawn(async move { value_of(&client, &get("slow")).await })
        };
        // Let the slow request reach the server first.
        while fake.batch_sizes.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let start = Instant::now();
        let err = value_of(&client, &get("drop")).await.unwrap_err();
        assert_eq!(grpc_code(&err), Code::Unavailable);
        let err = waiting.await.unwrap().unwrap_err();
        assert_eq!(grpc_code(&err), Code::Unavailable, "{err:?}");
        assert!(start.elapsed() < Duration::from_secs(5));

        assert_eq!(value_of(&client, &get("b")).await.unwrap(), b"b");
        assert_eq!(fake.streams.load(Ordering::SeqCst), 2);
    }

    /// Requests that client-go never batches are unary RPCs, also with batching on.
    #[tokio::test]
    async fn unbatched_requests_use_unary_rpcs() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        let err = client
            .dispatch(&kvrpcpb::RawGetKeyTtlRequest::default())
            .await
            .unwrap_err();
        assert_eq!(grpc_code(&err), Code::Unimplemented);
        assert_eq!(
            fake.paths.lock().unwrap().clone(),
            ["/tikvpb.Tikv/RawGetKeyTTL"]
        );
    }

    /// No receive limit on the stream either (client-go `MaxRecvMsgSize`); an explicit limit
    /// fails the stream, and with it the request.
    #[tokio::test]
    async fn batch_responses_over_4_mib() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        assert_eq!(value_of(&client, &get("big")).await.unwrap().len(), BIG);

        let limited = connect(&addr, 4 * 1024 * 1024).await;
        let err = value_of(&limited, &get("big")).await.unwrap_err();
        assert_eq!(grpc_code(&err), Code::OutOfRange, "{err:?}");
    }

    /// Dropping the last handle stops the store's tasks and closes its streams.
    #[tokio::test]
    async fn dropping_the_client_closes_the_stream() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = connect(&addr, usize::MAX).await;
        assert_eq!(value_of(&client, &get("a")).await.unwrap(), b"a");
        let conn = Arc::downgrade(client.batch.as_ref().unwrap());
        assert_eq!(fake.open.load(Ordering::SeqCst), 1);
        drop(client);
        assert!(conn.upgrade().is_none());
        // The server sees the stream end.
        tokio::time::timeout(Duration::from_secs(5), async {
            while fake.open.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the stream is closed");
    }

    /// Many callers on several threads over several connections: every caller gets its own
    /// answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn many_callers_over_several_connections() {
        let fake = FakeTikv::default();
        let addr = start_fake(fake.clone()).await;
        let client = TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(10),
            usize::MAX,
        )
        .with_connection_count(4)
        .connect(&addr)
        .await
        .unwrap();
        let tasks: Vec<_> = (0..32)
            .map(|t| {
                let client = client.clone();
                tokio::spawn(async move {
                    for i in 0..200 {
                        let key = format!("t{t}-{i}");
                        assert_eq!(
                            value_of(&client, &get(&key)).await.unwrap(),
                            key.into_bytes()
                        );
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(
            fake.batch_sizes.lock().unwrap().iter().sum::<usize>(),
            32 * 200
        );
        assert!(fake.streams.load(Ordering::SeqCst) <= 4);
    }
}
