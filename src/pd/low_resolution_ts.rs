// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The low-resolution timestamp of a cluster client: client-go's `pdOracle.lastTSMap`
//! (oracle/oracles/pd.go:281-333), the latest TSO the client has fetched.
//!
//! client-go refreshes it with every `GetTimestamp` (`setLastTS`, pd.go:220) and from a
//! background loop every `defaultOracleUpdateInterval` = 2 s (tikv/kv.go:107). Here only the
//! first: there is no background loop yet, and [`LowResolutionTs::low_resolution_ts`] reports
//! the timestamp as unavailable once it is older than [`LowResolutionTs::MAX_AGE`], so the
//! caller fetches a fresh TSO (which refreshes it). The staleness bound is the same as
//! client-go's; the cost differs: after 2 s without a TSO fetch, the first caller pays one TSO
//! round trip on its critical path, which client-go's background loop avoids. A background
//! refresher can later be added behind the same two methods (dulao5/tidb-rs N11).

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use crate::proto::pdpb::Timestamp;
use crate::timestamp::TimestampExt;

/// See the module documentation.
#[derive(Debug, Default)]
pub struct LowResolutionTs {
    last: Mutex<Option<(Timestamp, Instant)>>,
}

impl LowResolutionTs {
    /// The longest a timestamp is served without a refresh (client-go's update interval).
    pub const MAX_AGE: Duration = Duration::from_secs(2);

    /// Record a TSO the client fetched (client-go `setLastTS`: keeps the larger one).
    pub fn set_last_ts(&self, ts: &Timestamp) {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if last
            .as_ref()
            .is_none_or(|(t, _)| t.version() < ts.version())
        {
            *last = Some((ts.clone(), Instant::now()));
        }
    }

    /// The latest fetched TSO (client-go `GetLowResolutionTimestamp`, pd.go:577), or `None`
    /// if none was fetched within [`Self::MAX_AGE`].
    pub fn low_resolution_ts(&self) -> Option<Timestamp> {
        let last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        last.as_ref()
            .filter(|(_, at)| at.elapsed() < Self::MAX_AGE)
            .map(|(ts, _)| ts.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn keeps_the_latest_and_expires() {
        let lr = LowResolutionTs::default();
        assert!(lr.low_resolution_ts().is_none());
        lr.set_last_ts(&Timestamp::from_version(10));
        lr.set_last_ts(&Timestamp::from_version(5)); // older: ignored
        assert_eq!(lr.low_resolution_ts().unwrap().version(), 10);
        tokio::time::advance(LowResolutionTs::MAX_AGE).await;
        assert!(lr.low_resolution_ts().is_none());
        lr.set_last_ts(&Timestamp::from_version(11));
        assert_eq!(lr.low_resolution_ts().unwrap().version(), 11);
    }
}
