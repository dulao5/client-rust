// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use super::Request;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Result;
use crate::SecurityManager;

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    grpc_max_decoding_message_size: usize,
    #[new(value = "1")]
    connection_count: usize,
}

impl TikvConnect {
    /// Open `count` connections (at least one) to each store.
    #[must_use]
    pub fn with_connection_count(mut self, count: usize) -> Self {
        self.connection_count = count.max(1);
        self
    }
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        let mut clients = Vec::with_capacity(self.connection_count);
        for _ in 0..self.connection_count.max(1) {
            let client = self
                .security_mgr
                .connect_with_timeout(address, self.timeout, move |channel| {
                    TikvClient::new(channel)
                        .max_decoding_message_size(self.grpc_max_decoding_message_size)
                        .accept_compressed(CompressionEncoding::Gzip)
                })
                .await?;
            clients.push(client);
        }
        Ok(KvRpcClient::with_clients(clients))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
/// Requests are spread round-robin over one or more connections to the node. Each request is
/// sent with client-go's RPC timeout for its kind ([`Request::default_timeout`]) or its own
/// deadline.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_clients: Arc<[TikvClient<Channel>]>,
    next: Arc<AtomicUsize>,
}

impl KvRpcClient {
    pub fn new(rpc_client: TikvClient<Channel>) -> Self {
        Self::with_clients(vec![rpc_client])
    }

    /// A client over several connections to the same node (`rpc_clients` must not be empty).
    pub fn with_clients(rpc_clients: Vec<TikvClient<Channel>>) -> Self {
        assert!(!rpc_clients.is_empty(), "at least one connection");
        KvRpcClient {
            rpc_clients: rpc_clients.into(),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn pick(&self) -> &TikvClient<Channel> {
        let n = self.rpc_clients.len();
        if n == 1 {
            return &self.rpc_clients[0];
        }
        &self.rpc_clients[self.next.fetch_add(1, Ordering::Relaxed) % n]
    }
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        request
            .dispatch(self.pick(), request.default_timeout())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::task::Context;
    use std::task::Poll;

    use tonic::codegen::http;
    use tonic::codegen::Body;
    use tonic::codegen::BoxFuture;
    use tonic::codegen::Service;
    use tonic::codegen::StdError;
    use tonic::server::NamedService;
    use tonic::server::UnaryService;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::Server;

    use super::*;
    use crate::proto::kvrpcpb;
    use crate::Config;

    /// What the fake TiKV saw of one request: its method, `Context.max_execution_duration_ms`
    /// and its `grpc-timeout` header.
    type Seen = Arc<std::sync::Mutex<Vec<(String, u64, Option<String>)>>>;

    /// A TiKV that answers every `KvGet` with a value of `value_len` bytes (and `KvBatchGet`
    /// with nothing), recording what it saw.
    #[derive(Clone)]
    struct BigGetTikv {
        value_len: usize,
        seen: Seen,
    }

    struct KvBatchGet {
        seen: Seen,
    }

    impl UnaryService<kvrpcpb::BatchGetRequest> for KvBatchGet {
        type Response = kvrpcpb::BatchGetResponse;
        type Future =
            std::future::Ready<std::result::Result<tonic::Response<Self::Response>, tonic::Status>>;

        fn call(&mut self, req: tonic::Request<kvrpcpb::BatchGetRequest>) -> Self::Future {
            let ms = req
                .get_ref()
                .context
                .as_ref()
                .map_or(0, |c| c.max_execution_duration_ms);
            self.seen
                .lock()
                .unwrap()
                .push(("KvBatchGet".to_owned(), ms, None));
            std::future::ready(Ok(tonic::Response::new(Default::default())))
        }
    }

    impl NamedService for BigGetTikv {
        const NAME: &'static str = "tikvpb.Tikv";
    }

    struct KvGet {
        value_len: usize,
        seen: Seen,
    }

    impl UnaryService<kvrpcpb::GetRequest> for KvGet {
        type Response = kvrpcpb::GetResponse;
        type Future =
            std::future::Ready<std::result::Result<tonic::Response<Self::Response>, tonic::Status>>;

        fn call(&mut self, req: tonic::Request<kvrpcpb::GetRequest>) -> Self::Future {
            let ms = req
                .get_ref()
                .context
                .as_ref()
                .map_or(0, |c| c.max_execution_duration_ms);
            self.seen
                .lock()
                .unwrap()
                .push(("KvGet".to_owned(), ms, None));
            std::future::ready(Ok(tonic::Response::new(kvrpcpb::GetResponse {
                value: vec![7; self.value_len],
                ..Default::default()
            })))
        }
    }

    impl<B> Service<http::Request<B>> for BigGetTikv
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
            let timeout = req
                .headers()
                .get("grpc-timeout")
                .map(|v| v.to_str().unwrap().to_owned());
            let path = req.uri().path().to_owned();
            let seen = self.seen.clone();
            let value_len = self.value_len;
            Box::pin(async move {
                let resp = match path.as_str() {
                    "/tikvpb.Tikv/KvGet" => {
                        let svc = KvGet {
                            value_len,
                            seen: seen.clone(),
                        };
                        tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                            .unary(svc, req)
                            .await
                    }
                    "/tikvpb.Tikv/KvBatchGet" => {
                        tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                            .unary(KvBatchGet { seen: seen.clone() }, req)
                            .await
                    }
                    other => panic!("unexpected method {other}"),
                };
                // The header of the request just recorded.
                if let Some(last) = seen.lock().unwrap().last_mut() {
                    last.2 = timeout;
                }
                Ok(resp)
            })
        }
    }

    /// Starts the fake TiKV and returns its address.
    async fn start_big_get_tikv(value_len: usize) -> String {
        start_tikv(value_len, Seen::default()).await
    }

    async fn start_tikv(value_len: usize, seen: Seen) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            Server::builder()
                .add_service(BigGetTikv { value_len, seen })
                .serve_with_incoming(incoming),
        );
        format!("127.0.0.1:{}", addr.port())
    }

    async fn kv_get(config: &Config, addr: &str) -> Result<kvrpcpb::GetResponse> {
        let connect = TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(10),
            config.grpc_max_decoding_message_size,
        );
        let client = connect.connect(addr).await?;
        let resp = client.dispatch(&kvrpcpb::GetRequest::default()).await?;
        Ok(*resp.downcast::<kvrpcpb::GetResponse>().unwrap())
    }

    /// P0-3: like client-go (`MaxRecvMsgSize = math.MaxInt64 - 1`), the default config accepts
    /// responses over tonic's 4 MiB default (a coprocessor response over wide rows is ~6 MiB).
    #[tokio::test]
    async fn default_config_receives_responses_over_4_mib() {
        let value_len = 6 * 1024 * 1024;
        let addr = start_big_get_tikv(value_len).await;

        let resp = kv_get(&Config::default(), &addr).await.unwrap();
        assert_eq!(resp.value.len(), value_len);

        // An explicit limit is still honoured.
        let limited = Config::default().with_grpc_max_decoding_message_size(4 * 1024 * 1024);
        let err = kv_get(&limited, &addr).await.unwrap_err();
        assert!(
            format!("{err:?}").contains("OutOfRange"),
            "unexpected error: {err:?}"
        );
    }

    /// tonic's `grpc-timeout` header (a number and a unit) in milliseconds.
    fn header_ms(header: &str) -> u64 {
        let (n, unit) = header.split_at(header.len() - 1);
        let n: u64 = n.parse().unwrap();
        match unit {
            "H" => n * 3_600_000,
            "M" => n * 60_000,
            "S" => n * 1000,
            "m" => n,
            "u" => n / 1000,
            "n" => n / 1_000_000,
            _ => panic!("unit {unit}"),
        }
    }

    /// P0-2: TiKV requests are sent with client-go's RPC timeouts (`ReadTimeoutShort` 30 s for
    /// a Get, `ReadTimeoutMedium` 60 s for a BatchGet; internal/client/client.go:87-88), not
    /// `Config::timeout` (2 s), and carry the timeout to TiKV as
    /// `Context.max_execution_duration_ms` (region_request.go:1393-1397). A request with its
    /// own deadline is sent with that deadline as its timeout.
    #[tokio::test]
    async fn requests_use_client_go_timeouts_and_send_the_deadline() {
        let seen = Seen::default();
        let addr = start_tikv(1, seen.clone()).await;
        let connect = TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(2),
            usize::MAX,
        );
        let client = connect.connect(&addr).await.unwrap();
        client
            .dispatch(&kvrpcpb::GetRequest::default())
            .await
            .unwrap();
        client
            .dispatch(&kvrpcpb::BatchGetRequest::default())
            .await
            .unwrap();
        let mut with_deadline = kvrpcpb::GetRequest::default();
        with_deadline.set_max_execution_duration(Duration::from_millis(150));
        client.dispatch(&with_deadline).await.unwrap();

        let seen = seen.lock().unwrap().clone();
        let got: Vec<(&str, u64)> = seen.iter().map(|(m, ms, _)| (m.as_str(), *ms)).collect();
        assert_eq!(
            got,
            [("KvGet", 30_000), ("KvBatchGet", 60_000), ("KvGet", 150)]
        );
        let timeouts: Vec<u64> = seen
            .iter()
            .map(|(_, _, t)| header_ms(t.as_deref().expect("grpc-timeout")))
            .collect();
        assert_eq!(timeouts, [30_000, 60_000, 150]);
    }
}
