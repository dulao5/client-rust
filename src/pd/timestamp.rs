// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is the low-level mechanisms for getting timestamps from a PD
//! cluster. It should be used via the `get_timestamp` API in `PdClient`.
//!
//! Once a `TimestampOracle` is created, there will be two futures running in a background working
//! thread created automatically. The `get_timestamp` method creates a oneshot channel whose
//! transmitter is served as a `TimestampRequest`. `TimestampRequest`s are sent to the working
//! thread through a bounded multi-producer, single-consumer channel. Every time the first future
//! is polled, it tries to exhaust the channel to get as many requests as possible and sends a
//! single `TsoRequest` to the PD server. The other future receives `TsoResponse`s from the PD
//! server and allocates timestamps for the requests.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::prelude::*;
use futures::task::AtomicWaker;
use futures::task::Context;
use futures::task::Poll;
use log::debug;
use log::info;
use log::warn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::transport::Channel;

use crate::internal_err;
use crate::proto::pdpb::pd_client::PdClient;
use crate::proto::pdpb::*;
use crate::Result;

/// It is an empirical value.
const MAX_BATCH_SIZE: usize = 64;

/// TODO: This value should be adjustable.
const MAX_PENDING_COUNT: usize = 1 << 16;

/// Consecutive TSO streams that broke without delivering a single response before the oracle
/// gives up: its pending requests then fail and the oracle stops, so that the caller's
/// `RetryClient` reconnects to PD (possibly a new leader) and builds a new oracle.
const MAX_STREAM_FAILURES: u32 = 3;
/// Backoff between two TSO streams: doubled per consecutive failure, capped.
const STREAM_RETRY_BASE: Duration = Duration::from_millis(10);
const STREAM_RETRY_CAP: Duration = Duration::from_secs(1);
/// A batch PD has not answered within this time breaks the stream (client-go PD client:
/// `deadlineWatcher.Start(ctx, option.Timeout, cancel)` per batch, clients/tso/dispatcher.go:324;
/// `option.Timeout` defaults to 3 s, opt/option.go:27, and TiDB passes its `pd-server-timeout`,
/// also 3 s by default).
const TSO_BATCH_DEADLINE: Duration = Duration::from_secs(3);
/// The longest `get_timestamp` waits, a backstop so that a request never hangs: longer than the
/// oracle's own recovery (MAX_STREAM_FAILURES + 1 streams of TSO_BATCH_DEADLINE each, plus their
/// backoffs, about 12.2 s), after which `RetryClient` reconnects. client-go bounds the wait by
/// the statement's context instead.
const TSO_WAIT_LIMIT: Duration = Duration::from_secs(15);

type TimestampRequest = oneshot::Sender<Timestamp>;

/// The timestamp oracle (TSO) which provides monotonically increasing timestamps.
#[derive(Clone)]
pub(crate) struct TimestampOracle {
    /// The transmitter of a bounded channel which transports requests of getting a single
    /// timestamp to the TSO working thread. A bounded channel is used to prevent using
    /// too much memory unexpectedly.
    /// In the working thread, the `TimestampRequest`, which is actually a one channel sender,
    /// is used to send back the timestamp result.
    request_tx: mpsc::Sender<TimestampRequest>,
}

impl TimestampOracle {
    pub(crate) fn new(cluster_id: u64, pd_client: &PdClient<Channel>) -> Result<TimestampOracle> {
        let pd_client = pd_client.clone();
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);

        // Start a background thread to handle TSO requests and responses
        tokio::spawn(run_tso(cluster_id, pd_client, request_rx));

        Ok(TimestampOracle { request_tx })
    }

    pub(crate) async fn get_timestamp(self) -> Result<Timestamp> {
        debug!("getting current timestamp");
        let (request, response) = oneshot::channel();
        self.request_tx
            .send(request)
            .await
            .map_err(|_| internal_err!("TimestampRequest channel is closed"))?;
        match tokio::time::timeout(TSO_WAIT_LIMIT, response).await {
            Ok(ts) => Ok(ts?),
            Err(_) => Err(internal_err!(
                "no timestamp from PD within {:?}",
                TSO_WAIT_LIMIT
            )),
        }
    }
}

/// What the TSO streams of one oracle share. A broken stream is replaced by a new one (client-go
/// PD client's `tsoDispatcher` / `tsoStream`, which reconnects and retries rather than failing
/// the waiting requests); `generation` retires the old stream, so that it no longer takes
/// requests if tonic polls it once more.
struct Shared {
    request_rx: std::sync::Mutex<mpsc::Receiver<TimestampRequest>>,
    /// Requests sent on a stream that broke before answering them, sent first on the next.
    retry: std::sync::Mutex<VecDeque<TimestampRequest>>,
    /// The `TimestampRequest`s which are waiting for the responses from the PD server.
    ///
    /// A blocking mutex, never held across an `.await`: `run_tso` and the request stream only
    /// hold it to look at, push or pop a group. An async mutex let `poll_next` return `Pending`
    /// when `run_tso` held the lock, after the request channel had spent its waker on the
    /// arrival of the request; nothing woke the stream again (no batch was in flight, so no
    /// response came), and the request waited for `TSO_WAIT_LIMIT` (tidb-rs#225).
    pending_requests: std::sync::Mutex<VecDeque<RequestGroup>>,
    /// When there are too many pending requests, the `send_request` future will refuse to fetch
    /// more requests from the bounded channel. This waker is used to wake up the sending future
    /// if the queue containing pending requests is no longer full.
    sending_future_waker: AtomicWaker,
    generation: AtomicU64,
    /// Every `TimestampOracle` is gone: no request will come any more.
    closed: AtomicBool,
    /// Test hook: runs in `poll_next` between the first generation check and taking the
    /// `pending_requests` lock (the window of #17 review item 1).
    #[cfg(test)]
    before_lock: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Shared {
    fn lock_pending(&self) -> std::sync::MutexGuard<'_, VecDeque<RequestGroup>> {
        self.pending_requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Stop taking requests for good and fail the ones still buffered in the channel: the
    /// oracle gives up or is dropped. Closing explicitly matters because tonic may keep an old
    /// request stream (and with it `Shared` and the receiver) alive: new requests would then
    /// be accepted and never answered (#17 review item 2).
    fn close_requests(&self) {
        let mut rx = self.request_rx.lock().unwrap_or_else(|e| e.into_inner());
        rx.close();
        while rx.try_recv().is_ok() {}
        self.retry.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

async fn run_tso(
    cluster_id: u64,
    mut pd_client: PdClient<Channel>,
    request_rx: mpsc::Receiver<TimestampRequest>,
) {
    let shared = Arc::new(Shared {
        request_rx: std::sync::Mutex::new(request_rx),
        retry: std::sync::Mutex::new(VecDeque::new()),
        pending_requests: std::sync::Mutex::new(VecDeque::with_capacity(MAX_PENDING_COUNT)),
        sending_future_waker: AtomicWaker::new(),
        generation: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        #[cfg(test)]
        before_lock: std::sync::Mutex::new(None),
    });
    let mut failures = 0u32;
    loop {
        let generation = shared.generation.load(Ordering::Acquire);
        let request_stream = TsoRequestStream {
            cluster_id,
            generation,
            shared: shared.clone(),
        };
        let mut answered = false;
        match pd_client.tso(request_stream).await {
            Ok(responses) => {
                let mut responses = responses.into_inner();
                loop {
                    // The deadline of the oldest unanswered batch (TSO_BATCH_DEADLINE).
                    let oldest = shared.lock_pending().front().map(|g| g.sent);
                    let wait = oldest.map_or(TSO_BATCH_DEADLINE, |sent| {
                        TSO_BATCH_DEADLINE.saturating_sub(sent.elapsed())
                    });
                    let next = match tokio::time::timeout(wait, responses.next()).await {
                        Ok(next) => next,
                        Err(_) if oldest.is_some() => {
                            warn!(
                                "TSO stream: no answer within {TSO_BATCH_DEADLINE:?}; reopening it"
                            );
                            break;
                        }
                        // Nothing was pending: look again.
                        Err(_) => continue,
                    };
                    match next {
                        Some(Ok(resp)) => {
                            let allocated = {
                                let mut pending = shared.lock_pending();
                                allocate_timestamps(&resp, &mut pending)
                            };
                            if let Err(e) = allocated {
                                warn!("TSO stream: {e:?}; reopening it");
                                break;
                            }
                            answered = true;
                            // Wake up the sending future blocked by too many pending requests.
                            shared.sending_future_waker.wake();
                        }
                        Some(Err(status)) => {
                            warn!("TSO stream failed: {status}");
                            break;
                        }
                        None => break,
                    }
                }
            }
            Err(status) => warn!("cannot open a TSO stream: {status}"),
        }

        // Retire this stream; what it sent and PD did not answer goes out again on the next
        // one. Sending a request twice is safe: PD only ever hands out larger timestamps.
        shared.generation.fetch_add(1, Ordering::AcqRel);
        let unanswered: Vec<RequestGroup> = shared.lock_pending().drain(..).collect();
        {
            let mut retry = shared.retry.lock().unwrap_or_else(|e| e.into_inner());
            for group in unanswered.into_iter().rev() {
                for request in group.requests.into_iter().rev() {
                    retry.push_front(request);
                }
            }
            if shared.closed.load(Ordering::Acquire) && retry.is_empty() {
                drop(retry);
                info!("TSO stream terminated: the oracle is dropped");
                shared.close_requests();
                return;
            }
        }

        // A stream that answered something resets the count (to 1: this break is one failure,
        // so the first backoff is 20 ms).
        failures = if answered { 1 } else { failures + 1 };
        if !answered && failures > MAX_STREAM_FAILURES {
            // Give up on this connection: the waiting requests fail (their senders are dropped)
            // and so do new ones (the channel closes when this task ends); `RetryClient`
            // reconnects to PD and builds a new oracle.
            warn!("TSO stream failed {failures} times in a row; giving up on this PD connection");
            shared.close_requests();
            return;
        }
        let backoff = STREAM_RETRY_BASE
            .saturating_mul(1 << failures.min(10))
            .min(STREAM_RETRY_CAP);
        info!("reopening the TSO stream in {backoff:?}");
        tokio::time::sleep(backoff).await;
    }
}

struct RequestGroup {
    tso_request: TsoRequest,
    requests: Vec<TimestampRequest>,
    /// When the batch was handed to the stream (PD client's `requestDurationTSO` is the
    /// stream round trip of one batch).
    sent: std::time::Instant,
}

struct TsoRequestStream {
    cluster_id: u64,
    generation: u64,
    shared: Arc<Shared>,
}

impl Stream for TsoRequestStream {
    type Item = TsoRequest;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let shared = &this.shared;
        if shared.generation.load(Ordering::Acquire) != this.generation {
            // A newer stream replaced this one.
            return Poll::Ready(None);
        }

        #[cfg(test)]
        if let Some(hook) = shared.before_lock.lock().unwrap().take() {
            hook();
        }

        let mut pending_requests = shared.lock_pending();
        // Invariant: a group is only pushed into `pending_requests` under its lock, by the
        // stream of the current generation. `run_tso` retires a stream (bumps the generation)
        // and then drains under the same lock, so checking again here closes the window
        // between the check above and the lock: a retired stream can no longer push a group
        // that PD will never answer on the new stream (#17 review item 1).
        if shared.generation.load(Ordering::Acquire) != this.generation {
            return Poll::Ready(None);
        }
        let mut requests = Vec::new();

        {
            let mut retry = shared.retry.lock().unwrap_or_else(|e| e.into_inner());
            while requests.len() < MAX_BATCH_SIZE {
                match retry.pop_front() {
                    Some(r) => requests.push(r),
                    None => break,
                }
            }
        }
        {
            let mut request_rx = shared.request_rx.lock().unwrap_or_else(|e| e.into_inner());
            while requests.len() < MAX_BATCH_SIZE && pending_requests.len() < MAX_PENDING_COUNT {
                match request_rx.poll_recv(cx) {
                    Poll::Ready(Some(sender)) => {
                        requests.push(sender);
                    }
                    Poll::Ready(None) => {
                        shared.closed.store(true, Ordering::Release);
                        if requests.is_empty() {
                            return Poll::Ready(None);
                        }
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }

        if !requests.is_empty() {
            let req = TsoRequest {
                header: Some(RequestHeader {
                    cluster_id: this.cluster_id,
                    ..Default::default()
                }),
                count: requests.len() as u32,
                dc_location: String::new(),
            };

            let request_group = RequestGroup {
                tso_request: req.clone(),
                requests,
                sent: std::time::Instant::now(),
            };
            pending_requests.push_back(request_group);

            Poll::Ready(Some(req))
        } else {
            // Set the waker to the context, then the stream can be waked up after the pending queue
            // is no longer full.
            shared.sending_future_waker.register(cx.waker());
            Poll::Pending
        }
    }
}

fn allocate_timestamps(
    resp: &TsoResponse,
    pending_requests: &mut VecDeque<RequestGroup>,
) -> Result<()> {
    // PD returns the timestamp with the biggest logical value. We can send back timestamps
    // whose logical value is from `logical - count + 1` to `logical` using the senders
    // in `pending`.
    let tail_ts = resp
        .timestamp
        .as_ref()
        .ok_or_else(|| internal_err!("No timestamp in TsoResponse"))?;

    let mut offset = resp.count;
    if let Some(RequestGroup {
        tso_request,
        requests,
        sent,
    }) = pending_requests.pop_front()
    {
        crate::metrics::pd_request_tso(sent.elapsed(), true);
        if tso_request.count != offset {
            return Err(internal_err!(
                "PD gives different number of timestamps than expected"
            ));
        }

        for request in requests {
            offset -= 1;
            let ts = Timestamp {
                physical: tail_ts.physical,
                logical: tail_ts.logical - offset as i64,
                suffix_bits: tail_ts.suffix_bits,
            };
            let _ = request.send(ts);
        }
    } else {
        return Err(internal_err!("PD gives more TsoResponse than expected"));
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;
    use std::task::Context;
    use std::task::Poll;

    use futures::stream::BoxStream;
    use tonic::codegen::http;
    use tonic::codegen::Body;
    use tonic::codegen::BoxFuture;
    use tonic::codegen::Service;
    use tonic::codegen::StdError;
    use tonic::server::NamedService;
    use tonic::server::StreamingService;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::Endpoint;
    use tonic::transport::Server;
    use tonic::Status;

    use super::*;

    /// A PD whose TSO streams answer `answers` batches and then fail (`None`: never fail).
    /// `fail_from_stream`: streams before it behave normally.
    #[derive(Clone)]
    struct MockPd {
        streams: Arc<AtomicUsize>,
        next_logical: Arc<AtomicU64>,
        answers: Option<usize>,
        failing_streams: usize,
        /// The first streams that take requests and never answer.
        silent_streams: usize,
    }

    impl NamedService for MockPd {
        const NAME: &'static str = "pdpb.PD";
    }

    struct Tso {
        fail_after: Option<usize>,
        silent: bool,
        next_logical: Arc<AtomicU64>,
    }

    impl StreamingService<TsoRequest> for Tso {
        type Response = TsoResponse;
        type ResponseStream = BoxStream<'static, std::result::Result<TsoResponse, Status>>;
        type Future =
            future::Ready<std::result::Result<tonic::Response<Self::ResponseStream>, Status>>;

        fn call(&mut self, req: tonic::Request<tonic::Streaming<TsoRequest>>) -> Self::Future {
            let next = self.next_logical.clone();
            let fail_after = self.fail_after;
            if self.silent {
                // Read the requests, answer nothing, keep the stream open.
                let mut inbound = req.into_inner();
                let out = stream::once(async move {
                    while inbound.next().await.is_some() {}
                    futures::future::pending::<()>().await;
                    unreachable!()
                });
                return future::ready(Ok(tonic::Response::new(out.boxed())));
            }
            let out = stream::unfold(
                (req.into_inner(), 0usize, false),
                move |(mut inbound, answered, done)| {
                    let next = next.clone();
                    async move {
                        if done {
                            return None;
                        }
                        let req = match inbound.next().await {
                            Some(Ok(req)) => req,
                            _ => return None,
                        };
                        if fail_after == Some(answered) {
                            // Break the stream with this batch (and whatever follows) unanswered.
                            let err = Status::unavailable("mock PD: TSO stream broken");
                            return Some((Err(err), (inbound, answered, true)));
                        }
                        let logical =
                            next.fetch_add(req.count as u64, Ordering::SeqCst) + req.count as u64;
                        let resp = TsoResponse {
                            count: req.count,
                            timestamp: Some(Timestamp {
                                physical: 1,
                                logical: logical as i64,
                                suffix_bits: 0,
                            }),
                            ..Default::default()
                        };
                        Some((Ok(resp), (inbound, answered + 1, false)))
                    }
                },
            );
            future::ready(Ok(tonic::Response::new(out.boxed())))
        }
    }

    impl<B> Service<http::Request<B>> for MockPd
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            assert_eq!(req.uri().path(), "/pdpb.PD/Tso");
            let n = self.streams.fetch_add(1, Ordering::SeqCst);
            let svc = Tso {
                fail_after: if n < self.failing_streams {
                    self.answers
                } else {
                    None
                },
                silent: n < self.silent_streams,
                next_logical: self.next_logical.clone(),
            };
            Box::pin(async move {
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                Ok(grpc.streaming(svc, req).await)
            })
        }
    }

    async fn start(pd: MockPd) -> PdClient<Channel> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            Server::builder()
                .add_service(pd)
                .serve_with_incoming(incoming),
        );
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        PdClient::new(channel)
    }

    fn shared(rx: mpsc::Receiver<TimestampRequest>) -> Arc<Shared> {
        Arc::new(Shared {
            request_rx: std::sync::Mutex::new(rx),
            retry: std::sync::Mutex::new(VecDeque::new()),
            pending_requests: std::sync::Mutex::new(VecDeque::new()),
            sending_future_waker: AtomicWaker::new(),
            generation: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            before_lock: std::sync::Mutex::new(None),
        })
    }

    /// #17 review item 1: `run_tso` retires the stream (bumps the generation) between the
    /// stream's first generation check and its taking the `pending_requests` lock. The retired
    /// stream must not take the request and push a group that PD will never answer on the new
    /// stream.
    #[tokio::test]
    async fn retired_stream_takes_nothing_after_the_lock() {
        let (tx, rx) = mpsc::channel(MAX_BATCH_SIZE);
        let shared = shared(rx);
        let (request, _response) = oneshot::channel();
        tx.send(request).await.unwrap();
        let retire = shared.clone();
        *shared.before_lock.lock().unwrap() = Some(Box::new(move || {
            retire.generation.fetch_add(1, Ordering::AcqRel);
        }));
        let mut old = TsoRequestStream {
            cluster_id: 1,
            generation: 0,
            shared: shared.clone(),
        };
        assert_eq!(futures::poll!(old.next()), Poll::Ready(None));
        assert!(shared.lock_pending().is_empty());
        // The request is still in the channel, for the new stream.
        assert!(shared.request_rx.lock().unwrap().try_recv().is_ok());
    }

    /// #225 (tidb-rs): `run_tso` holds the `pending_requests` lock (its per-response check of
    /// the oldest batch) just when a request arrives and tonic polls the request stream. The
    /// stream must not give up on that poll without a way to be woken again: the channel's
    /// waker was spent on the arrival, and no TSO response will come since nothing is in
    /// flight, so the request would sit in the channel until `TSO_WAIT_LIMIT` (15 s).
    #[tokio::test]
    async fn busy_pending_lock_does_not_strand_a_request() {
        let (tx, rx) = mpsc::channel(MAX_BATCH_SIZE);
        let shared = shared(rx);
        let mut stream = TsoRequestStream {
            cluster_id: 1,
            generation: 0,
            shared: shared.clone(),
        };
        let woken = Arc::new(Flag(AtomicBool::new(false)));
        let waker = futures::task::waker(woken.clone());
        let mut cx = Context::from_waker(&waker);
        // Idle: the stream waits on the request channel.
        assert!(Pin::new(&mut stream).poll_next(&mut cx).is_pending());
        // A request arrives and wakes the stream (spending the channel's waker).
        let (request, _response) = oneshot::channel();
        tx.send(request).await.unwrap();
        assert!(woken.0.swap(false, Ordering::SeqCst));
        // `run_tso` holds the lock on another thread while tonic polls the stream.
        let holder_shared = shared.clone();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let guard = holder_shared.lock_pending();
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(100));
            drop(guard);
        });
        locked_rx.recv().unwrap();
        let polled = Pin::new(&mut stream).poll_next(&mut cx);
        holder.join().unwrap();
        assert!(
            polled.is_ready() || woken.0.load(Ordering::SeqCst),
            "the request is stranded: the stream returned Pending and nothing will wake it"
        );
    }

    struct Flag(AtomicBool);

    impl futures::task::ArcWake for Flag {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.store(true, Ordering::SeqCst);
        }
    }

    /// #17 review item 2: giving up closes the request channel even while an old request
    /// stream (holding `Shared`) is alive: buffered requests fail and new ones fail at once.
    #[tokio::test]
    async fn giving_up_closes_the_channel_despite_a_live_stream() {
        let (tx, rx) = mpsc::channel(MAX_BATCH_SIZE);
        let shared = shared(rx);
        let _old = TsoRequestStream {
            cluster_id: 1,
            generation: 0,
            shared: shared.clone(),
        };
        let (buffered, buffered_rx) = oneshot::channel();
        tx.send(buffered).await.unwrap();
        shared.close_requests();
        assert!(buffered_rx.await.is_err(), "the buffered request fails");
        let (late, _) = oneshot::channel();
        assert!(tx.send(late).await.is_err(), "a new request fails at once");
    }

    /// The TSO stream breaks after two batches, with requests in flight: the oracle reopens it
    /// and sends them again, so every concurrent request gets a timestamp, all distinct (as
    /// client-go's PD client: `tsoDispatcher` reconnects and retries).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn broken_tso_stream_is_reopened_and_requests_resent() {
        let streams = Arc::new(AtomicUsize::new(0));
        let client = start(MockPd {
            streams: streams.clone(),
            next_logical: Arc::new(AtomicU64::new(0)),
            answers: Some(2),
            failing_streams: 2,
            silent_streams: 0,
        })
        .await;
        let oracle = TimestampOracle::new(1, &client).unwrap();
        let handles: Vec<_> = (0..500)
            .map(|_| tokio::spawn(oracle.clone().get_timestamp()))
            .collect();
        let mut seen = HashSet::new();
        for h in handles {
            let ts = h.await.unwrap().expect("every request gets a timestamp");
            assert!(seen.insert((ts.physical, ts.logical)), "duplicate {ts:?}");
        }
        assert!(
            streams.load(Ordering::SeqCst) >= 3,
            "the stream was reopened"
        );
    }

    /// The first stream takes the request and never answers: after TSO_BATCH_DEADLINE the oracle
    /// gives up on it and sends the request again on a new stream (client-go's per-batch
    /// deadline, clients/tso/dispatcher.go:324).
    #[tokio::test]
    async fn silent_tso_stream_hits_the_batch_deadline() {
        let streams = Arc::new(AtomicUsize::new(0));
        let client = start(MockPd {
            streams: streams.clone(),
            next_logical: Arc::new(AtomicU64::new(0)),
            answers: None,
            failing_streams: 0,
            silent_streams: 1,
        })
        .await;
        let oracle = TimestampOracle::new(1, &client).unwrap();
        let started = std::time::Instant::now();
        oracle
            .get_timestamp()
            .await
            .expect("answered on the second stream");
        assert!(started.elapsed() >= TSO_BATCH_DEADLINE);
        assert_eq!(streams.load(Ordering::SeqCst), 2);
    }

    /// Every stream fails at once: the oracle gives up after MAX_STREAM_FAILURES (so that the
    /// caller's `RetryClient` reconnects) instead of retrying forever; requests fail, not hang.
    #[tokio::test]
    async fn oracle_gives_up_on_a_pd_that_keeps_failing() {
        let streams = Arc::new(AtomicUsize::new(0));
        let client = start(MockPd {
            streams: streams.clone(),
            next_logical: Arc::new(AtomicU64::new(0)),
            answers: Some(0),
            failing_streams: usize::MAX,
            silent_streams: 0,
        })
        .await;
        let oracle = TimestampOracle::new(1, &client).unwrap();
        let res = tokio::time::timeout(Duration::from_secs(10), oracle.clone().get_timestamp())
            .await
            .expect("does not hang");
        assert!(res.is_err());
        assert_eq!(
            streams.load(Ordering::SeqCst),
            MAX_STREAM_FAILURES as usize + 1
        );
        // The oracle is gone; later requests fail at once and RetryClient reconnects.
        assert!(oracle.get_timestamp().await.is_err());
    }
}
