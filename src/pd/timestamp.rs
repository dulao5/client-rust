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

use futures::pin_mut;
use futures::prelude::*;
use futures::task::AtomicWaker;
use futures::task::Context;
use futures::task::Poll;
use log::debug;
use log::info;
use log::warn;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
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
        Ok(response.await?)
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
    pending_requests: Mutex<VecDeque<RequestGroup>>,
    /// When there are too many pending requests, the `send_request` future will refuse to fetch
    /// more requests from the bounded channel. This waker is used to wake up the sending future
    /// if the queue containing pending requests is no longer full.
    sending_future_waker: AtomicWaker,
    generation: AtomicU64,
    /// Every `TimestampOracle` is gone: no request will come any more.
    closed: AtomicBool,
}

async fn run_tso(
    cluster_id: u64,
    mut pd_client: PdClient<Channel>,
    request_rx: mpsc::Receiver<TimestampRequest>,
) {
    let shared = Arc::new(Shared {
        request_rx: std::sync::Mutex::new(request_rx),
        retry: std::sync::Mutex::new(VecDeque::new()),
        pending_requests: Mutex::new(VecDeque::with_capacity(MAX_PENDING_COUNT)),
        sending_future_waker: AtomicWaker::new(),
        generation: AtomicU64::new(0),
        closed: AtomicBool::new(false),
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
                    match responses.next().await {
                        Some(Ok(resp)) => {
                            let allocated = {
                                let mut pending = shared.pending_requests.lock().await;
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
        let unanswered: Vec<RequestGroup> =
            shared.pending_requests.lock().await.drain(..).collect();
        {
            let mut retry = shared.retry.lock().unwrap_or_else(|e| e.into_inner());
            for group in unanswered.into_iter().rev() {
                for request in group.requests.into_iter().rev() {
                    retry.push_front(request);
                }
            }
            if shared.closed.load(Ordering::Acquire) && retry.is_empty() {
                info!("TSO stream terminated: the oracle is dropped");
                return;
            }
        }

        failures = if answered { 1 } else { failures + 1 };
        if !answered && failures > MAX_STREAM_FAILURES {
            // Give up on this connection: the waiting requests fail (their senders are dropped)
            // and so do new ones (the channel closes when this task ends); `RetryClient`
            // reconnects to PD and builds a new oracle.
            warn!("TSO stream failed {failures} times in a row; giving up on this PD connection");
            shared
                .retry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
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

        let pending_requests = shared.pending_requests.lock();
        pin_mut!(pending_requests);
        let mut pending_requests = if let Poll::Ready(pending_requests) = pending_requests.poll(cx)
        {
            pending_requests
        } else {
            shared.sending_future_waker.register(cx.waker());
            return Poll::Pending;
        };
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
    }

    impl NamedService for MockPd {
        const NAME: &'static str = "pdpb.PD";
    }

    struct Tso {
        fail_after: Option<usize>,
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
