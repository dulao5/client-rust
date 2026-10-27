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
        Ok(KvRpcClient::with_clients(clients, self.timeout))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
/// Requests are spread round-robin over one or more connections to the node.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_clients: Arc<[TikvClient<Channel>]>,
    next: Arc<AtomicUsize>,
    timeout: Duration,
}

impl KvRpcClient {
    pub fn new(rpc_client: TikvClient<Channel>, timeout: Duration) -> Self {
        Self::with_clients(vec![rpc_client], timeout)
    }

    /// A client over several connections to the same node (`rpc_clients` must not be empty).
    pub fn with_clients(rpc_clients: Vec<TikvClient<Channel>>, timeout: Duration) -> Self {
        assert!(!rpc_clients.is_empty(), "at least one connection");
        KvRpcClient {
            rpc_clients: rpc_clients.into(),
            next: Arc::new(AtomicUsize::new(0)),
            timeout,
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
        request.dispatch(self.pick(), self.timeout).await
    }
}
