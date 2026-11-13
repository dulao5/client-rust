// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Store liveness: client-go's per-store `livenessState` (internal/locate/store_cache.go).
//!
//! When a request to a store fails at the connection level, client-go asks the store whether
//! it is alive (`requestLivenessAndStartHealthCheckLoopIfNeeded`, store_cache.go:631) with the
//! gRPC health service and a 1 s timeout (`storeLivenessTimeout`, :589; `invokeKVStatusAPI`).
//! A store that does not answer is marked unreachable once, and one background loop per
//! store (`startHealthCheckLoop`) probes it every second until it answers again. Meanwhile
//! the replica selector does not pick an unreachable leader (`isLeaderCandidate`,
//! replica_selector.go:233-248) and tries the other peers instead, so requests stop
//! rediscovering the dead store one connection failure (and one backoff) at a time.
//!
//! [`StoreLiveness`] holds that state for one cluster client, keyed by store id. The probe
//! itself is supplied by the owner (the PD client resolves the store's address and sends
//! `grpc.health.v1.Health/Check`, see `TikvConnect::check_liveness`); concurrent checks of
//! one store share a single probe (client-go's `livenessSf` singleflight).

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::future::Shared;
use log::info;

use crate::region::StoreId;

/// client-go `storeLivenessTimeout` (internal/locate/store_cache.go:589).
pub const STORE_LIVENESS_TIMEOUT: Duration = Duration::from_secs(1);

/// The period of the health-check loop of an unreachable store (client-go schedules
/// `startHealthCheckLoop`'s check every second, store_cache.go:685).
pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// client-go `livenessState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Liveness {
    Reachable,
    Unreachable,
    /// The store answered but could not say (health status `UNKNOWN` / `SERVICE_UNKNOWN`),
    /// or its address could not be resolved.
    Unknown,
}

/// Probes one store, without a timeout of its own ([`StoreLiveness`] applies
/// [`STORE_LIVENESS_TIMEOUT`]). `None`: the store no longer exists (tombstone), stop
/// tracking it.
pub type LivenessProbe =
    Arc<dyn Fn(StoreId) -> BoxFuture<'static, Option<Liveness>> + Send + Sync + 'static>;

type SharedProbe = Shared<BoxFuture<'static, Option<Liveness>>>;

/// See the module documentation.
pub struct StoreLiveness {
    inner: Arc<Inner>,
}

struct Inner {
    probe: LivenessProbe,
    timeout: Duration,
    interval: Duration,
    next_generation: AtomicU64,
    /// Only stores that are not reachable or have a probe in flight have an entry.
    stores: Mutex<HashMap<StoreId, Entry>>,
}

struct Entry {
    state: Liveness,
    /// The probe in flight (singleflight), with the generation that identifies it.
    probe: Option<(u64, SharedProbe)>,
}

impl StoreLiveness {
    pub fn new(probe: LivenessProbe) -> StoreLiveness {
        StoreLiveness::with_timing(probe, STORE_LIVENESS_TIMEOUT, HEALTH_CHECK_INTERVAL)
    }

    pub fn with_timing(probe: LivenessProbe, timeout: Duration, interval: Duration) -> Self {
        StoreLiveness {
            inner: Arc::new(Inner {
                probe,
                timeout,
                interval,
                next_generation: AtomicU64::new(0),
                stores: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The cached state of a store (client-go `getLivenessState`): reachable unless a check
    /// found otherwise and the health-check loop has not seen it come back yet.
    pub fn get(&self, store_id: StoreId) -> Liveness {
        self.inner
            .lock()
            .get(&store_id)
            .map_or(Liveness::Reachable, |entry| entry.state)
    }

    /// Check a store after a connection-level failure (client-go
    /// `requestLivenessAndStartHealthCheckLoopIfNeeded`): probe it, and if it is not
    /// reachable, mark it so and start its health-check loop. Concurrent checks share one
    /// probe; a store already marked is not probed again (its loop is running).
    pub async fn check(&self, store_id: StoreId) -> Liveness {
        let (generation, probe) = {
            let mut stores = self.inner.lock();
            let entry = stores.entry(store_id).or_insert(Entry {
                state: Liveness::Reachable,
                probe: None,
            });
            if entry.state != Liveness::Reachable {
                return entry.state;
            }
            match &entry.probe {
                Some((generation, probe)) => (*generation, probe.clone()),
                None => {
                    let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
                    let probe = self.inner.probe_once(store_id).boxed().shared();
                    entry.probe = Some((generation, probe.clone()));
                    (generation, probe)
                }
            }
        };

        let result = probe.await;

        let mut stores = self.inner.lock();
        if let Some(entry) = stores.get_mut(&store_id) {
            // The first waiter to finish applies the probe's result.
            if matches!(&entry.probe, Some((g, _)) if *g == generation) {
                entry.probe = None;
                if let Some(liveness) = result.filter(|l| *l != Liveness::Reachable) {
                    if entry.state == Liveness::Reachable {
                        info!(
                            "[health check] store {} is {:?}, start its health check loop",
                            store_id, liveness
                        );
                        entry.state = liveness;
                        self.inner.spawn_health_check_loop(store_id);
                    }
                }
            }
            if entry.state == Liveness::Reachable && entry.probe.is_none() {
                stores.remove(&store_id);
            }
        }
        result.unwrap_or(Liveness::Unknown)
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, HashMap<StoreId, Entry>> {
        self.stores.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// One probe, bounded by the liveness timeout: a store that does not answer in time is
    /// unreachable (client-go's health check fails with its context's deadline).
    fn probe_once(&self, store_id: StoreId) -> impl std::future::Future<Output = Option<Liveness>> {
        let probe = (self.probe)(store_id);
        let timeout = self.timeout;
        async move {
            tokio::time::timeout(timeout, probe)
                .await
                .unwrap_or(Some(Liveness::Unreachable))
        }
    }

    /// client-go `startHealthCheckLoop`: probe the store every `interval` until it is
    /// reachable again (or gone), then forget it. The loop holds the state weakly: it ends
    /// with the cluster client.
    fn spawn_health_check_loop(self: &Arc<Self>, store_id: StoreId) {
        let weak = Arc::downgrade(self);
        let interval = self.interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let result = inner.probe_once(store_id).await;
                let mut stores = inner.lock();
                let Some(entry) = stores.get_mut(&store_id) else {
                    return;
                };
                match result {
                    Some(Liveness::Reachable) | None => {
                        info!(
                            "[health check] store {} became {}",
                            store_id,
                            if result.is_some() {
                                "reachable"
                            } else {
                                "invalid (tombstone), stop checking"
                            }
                        );
                        entry.state = Liveness::Reachable;
                        if entry.probe.is_none() {
                            stores.remove(&store_id);
                        }
                        return;
                    }
                    Some(liveness) => entry.state = liveness,
                }
            }
        });
    }
}

/// `grpc.health.v1.HealthCheckRequest` (grpc/health/v1/health.proto).
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    pub service: String,
}

/// `grpc.health.v1.HealthCheckResponse`; `status` is its `ServingStatus` enum.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct HealthCheckResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
}

pub(crate) const HEALTH_CHECK_PATH: &str = "/grpc.health.v1.Health/Check";
pub(crate) const SERVING_STATUS_UNKNOWN: i32 = 0;
pub(crate) const SERVING_STATUS_SERVING: i32 = 1;
pub(crate) const SERVING_STATUS_SERVICE_UNKNOWN: i32 = 3;

/// client-go `invokeKVStatusAPI`'s reading of a health response.
pub(crate) fn liveness_of_status(status: i32) -> Liveness {
    match status {
        SERVING_STATUS_SERVING => Liveness::Reachable,
        SERVING_STATUS_UNKNOWN | SERVING_STATUS_SERVICE_UNKNOWN => Liveness::Unknown,
        _ => Liveness::Unreachable,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A probe answering from a shared state, counting its calls.
    fn probe(state: Arc<Mutex<Liveness>>, calls: Arc<AtomicUsize>) -> LivenessProbe {
        Arc::new(move |_| {
            let state = state.clone();
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                // A probe that takes some time, so that concurrent checks overlap.
                tokio::time::sleep(Duration::from_millis(100)).await;
                Some(*state.lock().unwrap())
            }
            .boxed()
        })
    }

    #[tokio::test(start_paused = true)]
    async fn unreachable_store_is_marked_once_and_the_loop_flips_it_back() {
        let state = Arc::new(Mutex::new(Liveness::Unreachable));
        let calls = Arc::new(AtomicUsize::new(0));
        let liveness = StoreLiveness::new(probe(state.clone(), calls.clone()));
        assert_eq!(liveness.get(1), Liveness::Reachable);

        // Concurrent checks share one probe.
        let checks = futures::future::join_all((0..8).map(|_| liveness.check(1))).await;
        assert!(checks.iter().all(|l| *l == Liveness::Unreachable));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(liveness.get(1), Liveness::Unreachable);
        assert_eq!(liveness.get(2), Liveness::Reachable);

        // A store already marked is not probed again by requests: its loop is running.
        assert_eq!(liveness.check(1).await, Liveness::Unreachable);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // The loop probes every second while the store stays down.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(liveness.get(1), Liveness::Unreachable);

        // The store comes back: the next tick marks it reachable and the loop stops.
        *state.lock().unwrap() = Liveness::Reachable;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(liveness.get(1), Liveness::Reachable);
        let after = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(calls.load(Ordering::SeqCst), after, "the loop has stopped");
        assert!(liveness.inner.lock().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reachable_probe_changes_nothing() {
        let state = Arc::new(Mutex::new(Liveness::Reachable));
        let calls = Arc::new(AtomicUsize::new(0));
        let liveness = StoreLiveness::new(probe(state, calls.clone()));
        assert_eq!(liveness.check(1).await, Liveness::Reachable);
        assert_eq!(liveness.get(1), Liveness::Reachable);
        assert!(liveness.inner.lock().is_empty());
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no loop for a reachable store"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_hangs_times_out_as_unreachable() {
        let hanging: LivenessProbe = Arc::new(|_| futures::future::pending().boxed());
        let liveness = StoreLiveness::new(hanging);
        let start = tokio::time::Instant::now();
        assert_eq!(liveness.check(7).await, Liveness::Unreachable);
        assert_eq!(start.elapsed(), STORE_LIVENESS_TIMEOUT);
        assert_eq!(liveness.get(7), Liveness::Unreachable);
    }

    #[tokio::test(start_paused = true)]
    async fn a_tombstone_store_stops_the_loop() {
        let gone = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe_gone = gone.clone();
        let probe: LivenessProbe = Arc::new(move |_| {
            let gone = probe_gone.load(Ordering::SeqCst);
            async move { (!gone).then_some(Liveness::Unreachable) }.boxed()
        });
        let liveness = StoreLiveness::new(probe);
        assert_eq!(liveness.check(3).await, Liveness::Unreachable);
        gone.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(liveness.get(3), Liveness::Reachable);
        assert!(liveness.inner.lock().is_empty());
    }

    #[test]
    fn health_statuses() {
        assert_eq!(liveness_of_status(1), Liveness::Reachable);
        assert_eq!(liveness_of_status(0), Liveness::Unknown);
        assert_eq!(liveness_of_status(3), Liveness::Unknown);
        assert_eq!(liveness_of_status(2), Liveness::Unreachable);
    }
}
