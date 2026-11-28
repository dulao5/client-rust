// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::internal_err;
use crate::metrics;
use crate::metrics::PdCmd;
use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats::pd_stats;
use crate::Error;
use crate::Result;
use crate::SecurityManager;

// FIXME: these numbers and how they are used are all just cargo-culted in, there
// may be more optimal values.
const RECONNECT_INTERVAL_SEC: u64 = 1;
const MAX_REQUEST_COUNT: usize = 5;
const LEADER_CHANGE_RETRY: usize = 10;

#[async_trait]
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_timestamp_silent(self: Arc<Self>) -> Result<Timestamp>;

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}
/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: RwLock<(Cl, Instant)>,
    connection: Connection,
    timeout: Duration,
}

#[cfg(test)]
impl<Cl> RetryClient<Cl> {
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster: RwLock::new((cluster, Instant::now())),
            connection,
            timeout,
        }
    }
}

/// The sleep after the `attempt`-th failed PD request: 10 ms, doubled, at most
/// RECONNECT_INTERVAL_SEC (the attempts span about 4 s in all).
fn retry_backoff(attempt: usize) -> Duration {
    Duration::from_millis(10u64 << attempt.min(10)).min(Duration::from_secs(RECONNECT_INTERVAL_SEC))
}

macro_rules! retry_core {
    ($self: ident, $tag: literal, $cmd: expr, $call: expr) => {{
        let stats = pd_stats($tag);
        // PD client's `cmdDuration` / `cmdFailedDuration`: the whole call, retries included.
        let start = Instant::now();
        let result = async {
            let mut last_err = Ok(());
            for attempt in 0..LEADER_CHANGE_RETRY {
                let res = $call;

                match stats.done(res) {
                    Ok(r) => return Ok(r),
                    Err(e) => last_err = Err(e),
                }

                let mut reconnect_count = MAX_REQUEST_COUNT;
                while let Err(e) = $self.reconnect(RECONNECT_INTERVAL_SEC).await {
                    reconnect_count -= 1;
                    if reconnect_count == 0 {
                        return Err(e);
                    }
                    sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
                }
                // Wait before the next attempt. `reconnect` does nothing within
                // RECONNECT_INTERVAL_SEC of the last reconnect, so without a sleep a burst of
                // failures (a broken TSO stream) used up all attempts in milliseconds and
                // reached the caller (tidb-rs weekly build, wide_rows). client-go's PD client
                // retries with a backoff too.
                if attempt + 1 < LEADER_CHANGE_RETRY {
                    sleep(retry_backoff(attempt)).await;
                }
            }

            last_err?;
            unreachable!();
        }
        .await;
        metrics::pd_cmd($cmd, start.elapsed(), result.is_ok());
        result
    }};
}

macro_rules! retry_mut {
    ($self: ident, $tag: literal, $cmd: expr, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, $cmd, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &mut $self.cluster.write().await.0;
            $call.await
        })
    }};
}

macro_rules! retry {
    ($self: ident, $tag: literal, $cmd: expr, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, $cmd, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let $cluster = &$self.cluster.read().await.0;
            $call.await
        })
    }};
}

impl RetryClient<Cluster> {
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<RetryClient> {
        let connection = Connection::new(security_mgr);
        let cluster = RwLock::new((
            connection.connect_cluster(endpoints, timeout).await?,
            Instant::now(),
        ));
        Ok(RetryClient {
            cluster,
            connection,
            timeout,
        })
    }
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region", PdCmd::GetRegion, |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_region(key.clone(), self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        // PD returning an empty `region` field is a valid answer that the region ID no longer
        // exists (e.g. merged away), not a PD transport/leader failure. Do not retry or
        // reconnect PD on it, matching client-go `RegionCache.loadRegionByID`
        // (`internal/locate/region_cache.go:2210-2212`).
        let resp = retry_mut!(
            self,
            "get_region_by_id",
            PdCmd::GetRegionById,
            |cluster| async { cluster.get_region_by_id(region_id, self.timeout).await }
        )?;
        region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store> {
        retry_mut!(self, "get_store", PdCmd::GetStore, |cluster| async {
            cluster.get_store(id, self.timeout).await.and_then(|resp| {
                resp.store
                    .ok_or_else(|| internal_err!("store {} not found in response", id))
            })
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry_mut!(
            self,
            "get_all_stores",
            PdCmd::GetAllStores,
            |cluster| async {
                cluster
                    .get_all_stores(self.timeout)
                    .await
                    .map(|resp| resp.stores)
            }
        )
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", PdCmd::Tso, |cluster| cluster
            .get_timestamp())
    }

    async fn get_timestamp_silent(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", PdCmd::Tso, |cluster| cluster
            .get_timestamp_silent())
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        retry_mut!(
            self,
            "update_gc_safepoint",
            PdCmd::UpdateGcSafePoint,
            |cluster| async {
                cluster
                    .update_safepoint(safepoint, self.timeout)
                    .await
                    .map(|resp| resp.new_safe_point == safepoint)
            }
        )
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry_mut!(
            self,
            "load_keyspace",
            PdCmd::LoadKeyspace,
            |cluster| async { cluster.load_keyspace(keyspace, self.timeout).await }
        )
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn region_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    let region = resp.region.take().ok_or_else(err)?;
    Ok(RegionWithLeader::new(region, resp.leader.take()))
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    async fn reconnect(&self, interval_sec: u64) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    async fn reconnect(&self, interval_sec: u64) -> Result<()> {
        let reconnect_begin = Instant::now();
        let mut lock = self.cluster.write().await;
        let (cluster, last_connected) = &mut *lock;
        // If `last_connected + interval_sec` is larger or equal than reconnect_begin,
        // a concurrent reconnect is just succeed when this thread trying to get write lock
        let should_connect = reconnect_begin > *last_connected + Duration::from_secs(interval_sec);
        if should_connect {
            self.connection.reconnect(cluster, self.timeout).await?;
            *last_connected = Instant::now();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: u64) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            retry_mut!(client, "test", PdCmd::GetRegion, |_c| ready(Err(
                internal_err!("whoops")
            )))
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", PdCmd::GetRegion, |_c| ready(
                Ok::<_, Error>(())
            ))
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                reconnect_count: AtomicUsize::new(0),
                cluster: RwLock::new(((), Instant::now())),
            });

            assert!(retry_err(client.clone()).await.is_err());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                MAX_REQUEST_COUNT
            );

            client
                .reconnect_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            assert!(retry_ok(client.clone()).await.is_ok());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        })
    }

    // Paused time: the retries now sleep between attempts.
    #[tokio::test(start_paused = true)]
    async fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry_mut!(client, "test", PdCmd::GetRegion, |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", PdCmd::GetRegion, |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        {
            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(1000));

            assert!(retry_max_err(client.clone(), max_retries).await.is_err());
            assert_eq!(
                client.cluster.read().await.0.load(Ordering::SeqCst),
                LEADER_CHANGE_RETRY
            );

            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(2));

            assert!(retry_max_ok(client.clone(), max_retries).await.is_ok());
            assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn test_get_region_by_id_not_found_does_not_retry_or_reconnect() {
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

        #[derive(Clone)]
        struct MissingRegionPd {
            addr: String,
            get_members_calls: Arc<AtomicUsize>,
            get_region_by_id_calls: Arc<AtomicUsize>,
        }

        impl NamedService for MissingRegionPd {
            const NAME: &'static str = "pdpb.PD";
        }

        struct GetMembersSvc(String, Arc<AtomicUsize>);
        impl UnaryService<pdpb::GetMembersRequest> for GetMembersSvc {
            type Response = pdpb::GetMembersResponse;
            type Future = futures::future::Ready<
                std::result::Result<tonic::Response<Self::Response>, tonic::Status>,
            >;

            fn call(&mut self, _req: tonic::Request<pdpb::GetMembersRequest>) -> Self::Future {
                self.1.fetch_add(1, Ordering::SeqCst);
                let member = pdpb::Member {
                    member_id: 1,
                    client_urls: vec![self.0.clone()],
                    ..Default::default()
                };
                futures::future::ready(Ok(tonic::Response::new(pdpb::GetMembersResponse {
                    header: Some(pdpb::ResponseHeader {
                        cluster_id: 42,
                        ..Default::default()
                    }),
                    members: vec![member.clone()],
                    leader: Some(member),
                    ..Default::default()
                })))
            }
        }

        struct GetRegionByIdSvc(Arc<AtomicUsize>);
        impl UnaryService<pdpb::GetRegionByIdRequest> for GetRegionByIdSvc {
            type Response = pdpb::GetRegionResponse;
            type Future = futures::future::Ready<
                std::result::Result<tonic::Response<Self::Response>, tonic::Status>,
            >;

            fn call(&mut self, _req: tonic::Request<pdpb::GetRegionByIdRequest>) -> Self::Future {
                self.0.fetch_add(1, Ordering::SeqCst);
                // Healthy PD response for a merged-away region: header ok, region is None.
                futures::future::ready(Ok(tonic::Response::new(pdpb::GetRegionResponse {
                    header: Some(pdpb::ResponseHeader {
                        cluster_id: 42,
                        ..Default::default()
                    }),
                    region: None,
                    leader: None,
                    ..Default::default()
                })))
            }
        }

        impl<B> Service<http::Request<B>> for MissingRegionPd
        where
            B: Body + Send + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;

            fn poll_ready(
                &mut self,
                _: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Infallible>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let path = req.uri().path().to_owned();
                let addr = self.addr.clone();
                let members = self.get_members_calls.clone();
                let by_id = self.get_region_by_id_calls.clone();
                Box::pin(async move {
                    let resp = match path.as_str() {
                        "/pdpb.PD/GetMembers" => {
                            let mut grpc =
                                tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                            grpc.unary(GetMembersSvc(addr, members), req).await
                        }
                        "/pdpb.PD/GetRegionByID" => {
                            let mut grpc =
                                tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                            grpc.unary(GetRegionByIdSvc(by_id), req).await
                        }
                        other => panic!("unexpected PD path: {other}"),
                    };
                    Ok(resp)
                })
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("{addr}");
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        let get_members_calls = Arc::new(AtomicUsize::new(0));
        let get_region_by_id_calls = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(
            Server::builder()
                .add_service(MissingRegionPd {
                    addr: endpoint.clone(),
                    get_members_calls: get_members_calls.clone(),
                    get_region_by_id_calls: get_region_by_id_calls.clone(),
                })
                .serve_with_incoming(incoming),
        );

        let client = Arc::new(
            RetryClient::connect(
                &[endpoint],
                Arc::new(SecurityManager::default()),
                Duration::from_secs(5),
            )
            .await
            .unwrap(),
        );
        let members_after_connect = get_members_calls.load(Ordering::SeqCst);

        let start = Instant::now();
        let err = client.get_region_by_id(470).await.unwrap_err();
        let elapsed = start.elapsed();

        server.abort();

        assert!(
            matches!(err, Error::RegionNotFoundInResponse { region_id: 470 }),
            "expected RegionNotFoundInResponse(470), got {err:?}"
        );
        assert_eq!(
            get_region_by_id_calls.load(Ordering::SeqCst),
            1,
            "missing region ID must not be retried"
        );
        assert_eq!(
            get_members_calls.load(Ordering::SeqCst),
            members_after_connect,
            "missing region ID must not trigger PD reconnects"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "missing region ID must fail immediately without retry backoff, took {elapsed:?}"
        );
    }
}
