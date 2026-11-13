// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Various mock versions of the various clients and other objects.
//!
//! The goal is to be able to test functionality independently of the rest of
//! the system, in particular without requiring a TiKV or PD server, or RPC layer.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use derive_new::new;

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::metapb::RegionEpoch;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::StoreLiveness;
use crate::store::{KvClient, Store};
use crate::Config;
use crate::Error;
use crate::Key;
use crate::Result;
use crate::Timestamp;

/// Create a `PdRpcClient` with it's internals replaced with mocks so that the
/// client can be tested without doing any RPC calls.
pub async fn pd_rpc_client() -> PdRpcClient<MockKvConnect, MockCluster> {
    let config = Config::default();
    PdRpcClient::new(
        config.clone(),
        |_| MockKvConnect,
        |sm| {
            futures::future::ok(RetryClient::new_with_cluster(
                sm,
                config.timeout,
                MockCluster,
            ))
        },
        false,
    )
    .await
    .unwrap()
}

#[allow(clippy::type_complexity)]
#[derive(new, Default, Clone)]
pub struct MockKvClient {
    pub addr: String,
    dispatch: Option<Arc<dyn Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static>>,
}

impl MockKvClient {
    pub fn with_dispatch_hook<F>(dispatch: F) -> MockKvClient
    where
        F: Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    {
        MockKvClient {
            addr: String::new(),
            dispatch: Some(Arc::new(dispatch)),
        }
    }
}

pub struct MockKvConnect;

pub struct MockCluster;

#[allow(clippy::type_complexity)]
#[derive(new)]
pub struct MockPdClient {
    client: MockKvClient,
    /// Shared lock-resolution state, as the real client has one per cluster client.
    #[new(default)]
    lock_resolver: crate::transaction::LockResolverState,
    /// As the real client: refreshed by `get_timestamp`.
    #[new(default)]
    low_resolution_ts: crate::pd::LowResolutionTs,
    /// Optional override for `get_timestamp` (default: `Timestamp::default()`).
    #[new(default)]
    timestamp_hook: Option<Arc<dyn Fn() -> Timestamp + Send + Sync + 'static>>,
    /// Optional override for `map_region_to_store`.
    #[new(default)]
    map_region_to_store_hook:
        Option<Arc<dyn Fn(RegionWithLeader) -> Result<RegionStore> + Send + Sync + 'static>>,
    /// Optional override for `region_for_key`, e.g. to simulate PD failing to
    /// locate a region for a key.
    #[new(default)]
    region_for_key_hook:
        Option<Arc<dyn Fn(&Key) -> Result<RegionWithLeader> + Send + Sync + 'static>>,
    /// Optional observer/override for leader cache updates.
    #[new(default)]
    update_leader_hook:
        Option<Arc<dyn Fn(RegionVerId, metapb::Peer) -> Result<()> + Send + Sync + 'static>>,
    /// Optional observer for region cache invalidations.
    #[new(default)]
    invalidate_region_hook: Option<Arc<dyn Fn(RegionVerId) + Send + Sync + 'static>>,
    /// Optional store liveness state (default: every store reachable, never probed).
    #[new(default)]
    store_liveness: Option<StoreLiveness>,
}

#[async_trait]
impl KvClient for MockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match &self.dispatch {
            Some(f) => f(req.as_any()),
            None => panic!("no dispatch hook set"),
        }
    }
}

#[async_trait]
impl KvConnect for MockKvConnect {
    type KvClient = MockKvClient;

    async fn connect(&self, address: &str) -> Result<Self::KvClient> {
        Ok(MockKvClient {
            addr: address.to_owned(),
            dispatch: None,
        })
    }
}

impl MockPdClient {
    pub fn default() -> MockPdClient {
        MockPdClient::new(MockKvClient::default())
    }

    /// Override `get_timestamp` (a TSO clock the test controls).
    pub fn with_timestamp_hook<F>(mut self, hook: F) -> MockPdClient
    where
        F: Fn() -> Timestamp + Send + Sync + 'static,
    {
        self.timestamp_hook = Some(Arc::new(hook));
        self
    }

    pub fn with_map_region_to_store_hook<F>(mut self, hook: F) -> MockPdClient
    where
        F: Fn(RegionWithLeader) -> Result<RegionStore> + Send + Sync + 'static,
    {
        self.map_region_to_store_hook = Some(Arc::new(hook));
        self
    }

    /// Override `region_for_key` with a custom hook, leaving the rest of the
    /// mock's behavior untouched.
    pub fn with_region_for_key_hook<F>(mut self, hook: F) -> MockPdClient
    where
        F: Fn(&Key) -> Result<RegionWithLeader> + Send + Sync + 'static,
    {
        self.region_for_key_hook = Some(Arc::new(hook));
        self
    }

    pub fn with_update_leader_hook<F>(mut self, hook: F) -> MockPdClient
    where
        F: Fn(RegionVerId, metapb::Peer) -> Result<()> + Send + Sync + 'static,
    {
        self.update_leader_hook = Some(Arc::new(hook));
        self
    }

    pub fn with_invalidate_region_hook<F>(mut self, hook: F) -> MockPdClient
    where
        F: Fn(RegionVerId) + Send + Sync + 'static,
    {
        self.invalidate_region_hook = Some(Arc::new(hook));
        self
    }

    /// Track store liveness with `store_liveness` (whose probe the test controls).
    pub fn with_store_liveness(mut self, store_liveness: StoreLiveness) -> MockPdClient {
        self.store_liveness = Some(store_liveness);
        self
    }

    pub fn region1() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.start_key = vec![];
        region.region.end_key = vec![10];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 41,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region2() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 2;
        region.region.start_key = vec![10];
        region.region.end_key = vec![250, 250];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region3() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 3;
        region.region.start_key = vec![250, 250];
        region.region.end_key = vec![];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 43,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }
}

#[async_trait]
impl PdClient for MockPdClient {
    type KvClient = MockKvClient;

    fn lock_resolver_state(&self) -> Option<&crate::transaction::LockResolverState> {
        Some(&self.lock_resolver)
    }

    fn low_resolution_ts(&self) -> Option<&crate::pd::LowResolutionTs> {
        Some(&self.low_resolution_ts)
    }

    fn store_liveness(&self) -> Option<&StoreLiveness> {
        self.store_liveness.as_ref()
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        if let Some(hook) = &self.map_region_to_store_hook {
            return hook(region);
        }
        Ok(RegionStore::new(region, Arc::new(self.client.clone())))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        if let Some(hook) = &self.region_for_key_hook {
            return hook(key);
        }
        let bytes: &[_] = key.into();
        let region = if bytes.is_empty() || bytes < &[10][..] {
            Self::region1()
        } else if bytes >= &[10][..] && bytes < &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };

        Ok(region)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        match id {
            1 => Ok(Self::region1()),
            2 => Ok(Self::region2()),
            3 => Ok(Self::region3()),
            _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
        }
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![Store::new(Arc::new(self.client.clone()))])
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        let ts = self
            .timestamp_hook
            .as_ref()
            .map_or_else(Timestamp::default, |hook| hook());
        self.low_resolution_ts.set_last_ts(&ts);
        Ok(ts)
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        unimplemented!()
    }

    async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        match &self.update_leader_hook {
            Some(hook) => hook(ver_id, leader),
            None => todo!(),
        }
    }

    async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        if let Some(hook) = &self.invalidate_region_hook {
            hook(ver_id);
        }
    }

    async fn invalidate_store_cache(&self, _store_id: crate::region::StoreId) {}

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        unimplemented!()
    }
}

/// A one-region cluster for the store-liveness tests of the single-target read paths
/// (coprocessor tasks, scanner pages): region 1 covers every key, with peers on stores 41
/// (the leader PD reports), 44 and 45. Store 41 is down until `down` is cleared, as the
/// liveness probe sees it. The region cache's leader starts at 41 and follows
/// `update_leader`; [`DeadLeaderFixture::reload_from_pd`] puts it back to 41.
#[cfg(test)]
pub(crate) struct DeadLeaderFixture {
    pub down: Arc<std::sync::atomic::AtomicBool>,
    pub probes: Arc<std::sync::atomic::AtomicUsize>,
    leader: Arc<std::sync::Mutex<crate::region::StoreId>>,
    pub leader_updates: Arc<std::sync::Mutex<Vec<crate::region::StoreId>>>,
}

#[cfg(test)]
impl DeadLeaderFixture {
    pub const DEAD_LEADER: crate::region::StoreId = 41;

    pub fn new() -> DeadLeaderFixture {
        DeadLeaderFixture {
            down: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            probes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            leader: Arc::new(std::sync::Mutex::new(Self::DEAD_LEADER)),
            leader_updates: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn region(leader_store_id: crate::region::StoreId) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 1,
            version: 1,
        });
        region.region.peers = [41, 44, 45]
            .iter()
            .enumerate()
            .map(|(index, store_id)| metapb::Peer {
                id: index as u64 + 1,
                store_id: *store_id,
                ..Default::default()
            })
            .collect();
        region.leader = region
            .region
            .peers
            .iter()
            .find(|peer| peer.store_id == leader_store_id)
            .cloned();
        region
    }

    /// The region cache drops its leader update and reloads the region from PD, which still
    /// reports the dead store as the leader.
    pub fn reload_from_pd(&self) {
        *self.leader.lock().unwrap() = Self::DEAD_LEADER;
    }

    pub fn pd_client(&self, client: MockKvClient) -> MockPdClient {
        use std::sync::atomic::Ordering;

        use futures::FutureExt;

        use crate::store::Liveness;

        let leader = self.leader.clone();
        let update_leader = self.leader.clone();
        let leader_updates = self.leader_updates.clone();
        let probe_down = self.down.clone();
        let probes = self.probes.clone();
        MockPdClient::new(client)
            .with_region_for_key_hook(move |_| Ok(Self::region(*leader.lock().unwrap())))
            .with_update_leader_hook(move |_, peer| {
                *update_leader.lock().unwrap() = peer.store_id;
                leader_updates.lock().unwrap().push(peer.store_id);
                Ok(())
            })
            .with_store_liveness(StoreLiveness::new(Arc::new(move |store_id| {
                probes.fetch_add(1, Ordering::SeqCst);
                let down = store_id == Self::DEAD_LEADER && probe_down.load(Ordering::SeqCst);
                futures::future::ready(Some(if down {
                    Liveness::Unreachable
                } else {
                    Liveness::Reachable
                }))
                .boxed()
            })))
    }
}
