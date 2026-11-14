// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rand::thread_rng;
use rand::Rng;

use crate::metrics::BackoffType;
use crate::metrics::BACKOFF_COUNT;

pub const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);
pub const DEFAULT_STORE_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 1000, 10);
pub const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);
pub const PESSIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);

/// When a request is retried, we can backoff for some time to avoid saturating the network.
///
/// `Backoff` is an object which determines how long to wait for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    kind: BackoffKind,
    current_attempts: u32,
    max_attempts: u32,
    base_delay_ms: u64,
    current_delay_ms: u64,
    max_delay_ms: u64,
}

impl Backoff {
    // Returns the delay period for next retry. If the maximum retry count is hit returns None.
    //
    // A [`time_budget`](Backoff::time_budget) backoff has no single curve: this takes the
    // `regionMiss` one. The crate's own retry loops call `next_delay_for` with the error's
    // kind instead.
    pub fn next_delay_duration(&mut self) -> Option<Duration> {
        if let BackoffKind::TimeBudget(_) = self.kind {
            return self.next_delay_for(BackoffType::RegionMiss);
        }
        if self.current_attempts >= self.max_attempts {
            return None;
        }
        self.current_attempts += 1;

        match self.kind {
            BackoffKind::None => None,
            BackoffKind::TimeBudget(_) => unreachable!(),
            BackoffKind::NoJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::FullJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);

                let mut rng = thread_rng();
                let delay_ms: u64 = rng.gen_range(0..delay_ms);
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::EqualJitter => {
                let delay_ms = self.max_delay_ms.min(self.current_delay_ms);
                let half_delay_ms = delay_ms >> 1;

                let mut rng = thread_rng();
                let delay_ms: u64 = rng.gen_range(0..half_delay_ms) + half_delay_ms;
                self.current_delay_ms <<= 1;

                Some(Duration::from_millis(delay_ms))
            }
            BackoffKind::DecorrelatedJitter => {
                let mut rng = thread_rng();
                let delay_ms: u64 = rng
                    .gen_range(0..self.current_delay_ms * 3 - self.base_delay_ms)
                    + self.base_delay_ms;

                let delay_ms = delay_ms.min(self.max_delay_ms);
                self.current_delay_ms = delay_ms;

                Some(Duration::from_millis(delay_ms))
            }
        }
    }

    /// The delay before retrying after an error of kind `t`. A
    /// [`time_budget`](Backoff::time_budget) backoff uses client-go's curve for `t` and charges
    /// the delay to its budget; every other backoff ignores `t`
    /// ([`next_delay_duration`](Backoff::next_delay_duration)).
    pub(crate) fn next_delay_for(&mut self, t: BackoffType) -> Option<Duration> {
        self.next_delay_capped(t, None)
    }

    /// [`next_delay_for`](Backoff::next_delay_for), with the sleep capped at `cap`: client-go's
    /// `BackoffWithMaxSleepTxnLockFast` (backoff.go:127), which charges the capped sleep.
    pub(crate) fn next_delay_capped(
        &mut self,
        t: BackoffType,
        cap: Option<Duration>,
    ) -> Option<Duration> {
        let BackoffKind::TimeBudget(budget) = &mut self.kind else {
            let delay = self.next_delay_duration()?;
            return Some(cap.map_or(delay, |cap| delay.min(cap)));
        };
        let ms = budget.next(t, cap.map(|c| c.as_millis() as u64))?;
        self.current_attempts += 1;
        Some(Duration::from_millis(ms))
    }

    /// client-go `Backoffer.Backoff(cfg, err)`: sleeps for the next delay of kind `t`, or
    /// returns `err` as this budget's exhaustion ([`exhausted`](Backoff::exhausted)).
    pub async fn backoff(&mut self, t: BackoffType, err: crate::Error) -> crate::Result<()> {
        match self.next_delay_for(t) {
            Some(delay) => {
                crate::metrics::backoff(t, delay);
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Err(self.exhausted(err)),
        }
    }

    /// client-go `Backoffer.BackoffWithMaxSleepTxnLockFast(maxSleepMs, err)`.
    pub async fn backoff_with_max_sleep_txn_lock_fast(
        &mut self,
        max_sleep_ms: u64,
        err: crate::Error,
    ) -> crate::Result<()> {
        let t = BackoffType::TikvLockFast;
        match self.next_delay_capped(t, Some(Duration::from_millis(max_sleep_ms))) {
            Some(delay) => {
                crate::metrics::backoff(t, delay);
                tokio::time::sleep(delay).await;
                Ok(())
            }
            None => Err(self.exhausted(err)),
        }
    }

    /// client-go `Backoffer.GetTotalSleep()` in ms: what a time budget has slept so far
    /// (0 for other backoffs or before any sleep).
    pub fn total_sleep_ms(&self) -> u64 {
        match &self.kind {
            BackoffKind::TimeBudget(TimeBudget {
                state: Some(state), ..
            }) => {
                state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .total_sleep_ms
            }
            _ => 0,
        }
    }

    /// Whether this is a [`time_budget`](Backoff::time_budget) backoff.
    pub fn is_time_budget(&self) -> bool {
        matches!(self.kind, BackoffKind::TimeBudget(_))
    }

    /// A [`time_budget`](Backoff::time_budget) not [started](Backoff::start) yet (a template).
    pub(crate) fn is_unstarted_time_budget(&self) -> bool {
        matches!(&self.kind, BackoffKind::TimeBudget(budget) if budget.state.is_none())
    }

    /// The kind of error this budget slept on longest, `serverBusy` aside (client-go
    /// `longestSleepCfg`, backoff.go:368-383); `None` for other backoffs or before any sleep.
    pub(crate) fn longest_sleep(&self) -> Option<BackoffType> {
        let BackoffKind::TimeBudget(TimeBudget {
            state: Some(state), ..
        }) = &self.kind
        else {
            return None;
        };
        let state = state.lock().unwrap_or_else(|e| e.into_inner());
        BackoffType::ALL
            .into_iter()
            .filter(|t| *t != BackoffType::ServerBusy)
            .map(|t| (t, state.slept_ms[t as usize]))
            .filter(|(_, ms)| *ms > 0)
            .max_by_key(|(_, ms)| *ms)
            .map(|(t, _)| t)
    }

    /// The error to return when this backoff refuses another retry after `error`. A
    /// [`time_budget`](Backoff::time_budget) reports, as client-go does, the kind of error it
    /// slept on longest ([`Error::BackoffExhausted`]; backoff.go:175-180), with `error` as the
    /// last one. Any other backoff, or an undetermined outcome (which commit paths must
    /// recognise as such), returns `error` unchanged.
    pub(crate) fn exhausted(&self, error: crate::Error) -> crate::Error {
        if crate::request::plan::is_undetermined_region_error(&error) {
            return error;
        }
        match self.longest_sleep() {
            Some(kind) => crate::Error::BackoffExhausted {
                kind,
                last: Box::new(error),
            },
            None => error,
        }
    }

    /// A budget for one new request: for a [`time_budget`](Backoff::time_budget) backoff, a
    /// fresh budget (client-go's `NewBackofferWithVars` at the start of each read) whose
    /// clones all draw from it; any other backoff is just cloned.
    pub fn start(&self) -> Backoff {
        let mut b = self.clone();
        if let BackoffKind::TimeBudget(budget) = &mut b.kind {
            budget.state = Some(Arc::new(Mutex::new(BudgetState::default())));
        }
        b.current_attempts = 0;
        b
    }

    /// A budget for one of several requests sent in parallel on behalf of this one
    /// (client-go's `Backoffer.Fork`, backoff.go:278): it keeps the sleep spent so far but
    /// counts on its own from now on, and every kind's curve starts again from its base. Any
    /// other backoff is just cloned.
    pub fn fork(&self) -> Backoff {
        let mut b = self.clone();
        if let BackoffKind::TimeBudget(budget) = &mut b.kind {
            let forked = budget.state.as_ref().map(|state| {
                let state = state.lock().unwrap_or_else(|e| e.into_inner());
                BudgetState {
                    total_sleep_ms: state.total_sleep_ms,
                    excluded_sleep_ms: state.excluded_sleep_ms,
                    attempts: [0; BACKOFF_COUNT],
                    // `Fork` copies `backoffSleepMS` (backoff.go:287).
                    slept_ms: state.slept_ms,
                }
            });
            budget.state = forked.map(|state| Arc::new(Mutex::new(state)));
        }
        b
    }

    /// client-go's `Backoffer` (config/retry/backoff.go), the budget of TiDB's reads:
    /// - each kind of error has its own exponential curve and attempt count
    ///   (config/retry/config.go:122-140, see [`go_curve`]);
    /// - retries go on while the total **sleep time** stays under `max_sleep_ms`
    ///   (`BackoffWithCfgAndMaxSleep`, backoff.go:146), e.g. 20 000 ms for a point get
    ///   (`getMaxBackoff`, txnsnapshot/snapshot.go:698) and a coprocessor task
    ///   (`CopNextMaxBackoff`);
    /// - `serverBusy` sleeps are not counted, up to 10 minutes of them
    ///   (`isSleepExcluded`, config.go:144; backoff.go:148-150): a busy TiKV is waited for.
    ///
    /// The check comes before each sleep, as in client-go: the sleep that crosses the budget
    /// is still taken, the next one is refused.
    ///
    /// **Sharing.** Like client-go's `bo`, one budget covers a read and everything done on
    /// its behalf (its region retries, its lock resolution's requests and its lock waits):
    /// once [started](Backoff::start), all clones of a budget draw from the same total.
    /// Requests sent in parallel get a [fork](Backoff::fork). A budget not started yet (the
    /// template kept in `RetryOptions`) is independent per clone and starts on first use.
    pub const fn time_budget(max_sleep_ms: u64) -> Backoff {
        Backoff {
            kind: BackoffKind::TimeBudget(TimeBudget {
                max_sleep_ms,
                state: None,
            }),
            current_attempts: 0,
            max_attempts: u32::MAX,
            base_delay_ms: 0,
            current_delay_ms: 0,
            max_delay_ms: 0,
        }
    }

    /// True if we should not backoff at all (usually indicates that we should not retry a request).
    pub fn is_none(&self) -> bool {
        self.kind == BackoffKind::None
    }

    /// Returns the number of attempts
    pub fn current_attempts(&self) -> u32 {
        self.current_attempts
    }

    /// Don't wait. Usually indicates that we should not retry a request.
    pub const fn no_backoff() -> Backoff {
        Backoff {
            kind: BackoffKind::None,
            current_attempts: 0,
            max_attempts: 0,
            base_delay_ms: 0,
            current_delay_ms: 0,
            max_delay_ms: 0,
        }
    }

    // Exponential backoff means that the retry delay should multiply a constant
    // after each attempt, up to a maximum value. After each attempt, the new retry
    // delay should be:
    //
    // new_delay = min(max_delay, base_delay * 2 ** attempts)
    pub const fn no_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        Backoff {
            kind: BackoffKind::NoJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    // Adds Jitter to the basic exponential backoff. Returns a random value between
    // zero and the calculated exponential backoff:
    //
    // temp = min(max_delay, base_delay * 2 ** attempts)
    // new_delay = random_between(0, temp)
    pub fn full_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(
            base_delay_ms > 0 && max_delay_ms > 0,
            "Both base_delay_ms and max_delay_ms must be positive"
        );

        Backoff {
            kind: BackoffKind::FullJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    // Equal Jitter limits the random value should be equal or greater than half of
    // the calculated exponential backoff:
    //
    // temp = min(max_delay, base_delay * 2 ** attempts)
    // new_delay = random_between(temp / 2, temp)
    pub fn equal_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(
            base_delay_ms > 1 && max_delay_ms > 1,
            "Both base_delay_ms and max_delay_ms must be greater than 1"
        );

        Backoff {
            kind: BackoffKind::EqualJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }

    // Decorrelated Jitter is always calculated with the previous backoff
    // (the initial value is base_delay):
    //
    // temp = random_between(base_delay, previous_delay * 3)
    // new_delay = min(max_delay, temp)
    pub fn decorrelated_jitter_backoff(
        base_delay_ms: u64,
        max_delay_ms: u64,
        max_attempts: u32,
    ) -> Backoff {
        assert!(base_delay_ms > 0, "base_delay_ms must be positive");

        Backoff {
            kind: BackoffKind::DecorrelatedJitter,
            current_attempts: 0,
            max_attempts,
            base_delay_ms,
            current_delay_ms: base_delay_ms,
            max_delay_ms,
        }
    }
}

/// The pattern for computing backoff times.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BackoffKind {
    None,
    NoJitter,
    FullJitter,
    EqualJitter,
    DecorrelatedJitter,
    TimeBudget(TimeBudget),
}

/// A client-go `Backoffer`: the budget and, once started, the state its clones share.
#[derive(Debug, Clone)]
struct TimeBudget {
    max_sleep_ms: u64,
    state: Option<Arc<Mutex<BudgetState>>>,
}

/// Two budgets are equal when they allow the same (the state is not compared), so that
/// `RetryOptions` keep comparing by configuration.
impl PartialEq for TimeBudget {
    fn eq(&self, other: &Self) -> bool {
        self.max_sleep_ms == other.max_sleep_ms
    }
}

impl Eq for TimeBudget {}

/// client-go `Backoffer`'s counters: total and excluded sleep, attempts per kind of error
/// (its `fn` map of per-config backoff functions).
#[derive(Debug, Clone, Default)]
struct BudgetState {
    total_sleep_ms: u64,
    excluded_sleep_ms: u64,
    attempts: [u32; BACKOFF_COUNT],
    /// Sleep per kind (`backoffSleepMS`), for the error reported when the budget runs out.
    slept_ms: [u64; BACKOFF_COUNT],
}

/// `serverBusy` sleeps excluded from the budget, at most (`isSleepExcluded`, config.go:144).
const SERVER_BUSY_EXCLUDED_MS: u64 = 600_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Jitter {
    No,
    Equal,
}

/// client-go's curve for a kind of error: `(base ms, cap ms, jitter)`
/// (config/retry/config.go:122-140).
const fn go_curve(t: BackoffType) -> (u64, u64, Jitter) {
    match t {
        // BoTiKVRPC (config.go:122).
        BackoffType::TikvRpc => (100, 2000, Jitter::Equal),
        // BoTxnLockFast (config.go:140); its base comes from `tidb_backoff_lock_fast`,
        // default 10 (`DefBackoffLockFast`, kv/variables.go:70).
        BackoffType::TikvLockFast => (10, 3000, Jitter::Equal),
        // BoRegionMiss (config.go:127).
        BackoffType::RegionMiss => (2, 500, Jitter::No),
        // BoRegionScheduling (config.go:128).
        BackoffType::RegionScheduling => (2, 500, Jitter::No),
        // BoTiKVServerBusy (config.go:129).
        BackoffType::ServerBusy => (2000, 10000, Jitter::Equal),
        // BoStaleCmd (config.go:134).
        BackoffType::StaleCommand => (2, 1000, Jitter::No),
        // BoTxnNotFound (config.go:133).
        BackoffType::TxnNotFound => (2, 500, Jitter::No),
    }
}

impl TimeBudget {
    /// `Backoffer.BackoffWithCfgAndMaxSleep` (backoff.go:134-205) without the sleep itself:
    /// the sleep for `t`, at most `cap_ms`, charged to the budget.
    fn next(&mut self, t: BackoffType, cap_ms: Option<u64>) -> Option<u64> {
        let max_sleep_ms = self.max_sleep_ms;
        let state = self
            .state
            .get_or_insert_with(|| Arc::new(Mutex::new(BudgetState::default())));
        let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
        let excluded = t == BackoffType::ServerBusy;
        // backoff.go:146-151.
        let budget_spent = state.total_sleep_ms - state.excluded_sleep_ms >= max_sleep_ms;
        let excluded_spent = excluded
            && state.excluded_sleep_ms >= SERVER_BUSY_EXCLUDED_MS
            && state.excluded_sleep_ms >= max_sleep_ms;
        if max_sleep_ms > 0 && (budget_spent || excluded_spent) {
            return None;
        }
        // newBackoffFn (config.go:169-214), expo (:216): `base < 2` becomes 2; `expo` is
        // min(cap, base * 2^attempts); EqualJitter is v/2 + rand(v/2).
        let (base, cap, jitter) = go_curve(t);
        let base = base.max(2);
        let attempts = &mut state.attempts[t as usize];
        let v = base
            .checked_shl(*attempts)
            .filter(|v| *v >> *attempts == base)
            .map_or(cap, |v| v.min(cap));
        let sleep = match jitter {
            Jitter::No => v,
            Jitter::Equal => v / 2 + thread_rng().gen_range(0..(v / 2).max(1)),
        };
        *attempts += 1;
        // `maxSleepMs` caps the sleep actually taken (config.go:196-199), which is what is
        // charged (backoff.go:200-203).
        let sleep = cap_ms.map_or(sleep, |cap| sleep.min(cap));
        state.total_sleep_ms += sleep;
        state.slept_ms[t as usize] += sleep;
        if excluded {
            state.excluded_sleep_ms += sleep;
        }
        Some(sleep)
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_no_jitter_backoff() {
        // Tests for zero attempts.
        let mut backoff = Backoff::no_jitter_backoff(0, 0, 0);
        assert_eq!(backoff.next_delay_duration(), None);

        let mut backoff = Backoff::no_jitter_backoff(2, 7, 3);
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(2))
        );
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(4))
        );
        assert_eq!(
            backoff.next_delay_duration(),
            Some(Duration::from_millis(7))
        );
        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    fn test_full_jitter_backoff() {
        let mut backoff = Backoff::full_jitter_backoff(2, 7, 3);
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(2));
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(4));
        assert!(backoff.next_delay_duration().unwrap() <= Duration::from_millis(7));
        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be positive")]
    fn test_full_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::full_jitter_backoff(0, 7, 3);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be positive")]
    fn test_full_jitter_backoff_with_invalid_max_delay_ms() {
        Backoff::full_jitter_backoff(2, 0, 3);
    }

    #[test]
    fn test_equal_jitter_backoff() {
        let mut backoff = Backoff::equal_jitter_backoff(2, 7, 3);

        let first_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(first_delay_dur >= Duration::from_millis(1));
        assert!(first_delay_dur <= Duration::from_millis(2));

        let second_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(second_delay_dur >= Duration::from_millis(2));
        assert!(second_delay_dur <= Duration::from_millis(4));

        let third_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(third_delay_dur >= Duration::from_millis(3));
        assert!(third_delay_dur <= Duration::from_millis(6));

        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be greater than 1")]
    fn test_equal_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::equal_jitter_backoff(1, 7, 3);
    }

    #[test]
    #[should_panic(expected = "Both base_delay_ms and max_delay_ms must be greater than 1")]
    fn test_equal_jitter_backoff_with_invalid_max_delay_ms() {
        Backoff::equal_jitter_backoff(2, 1, 3);
    }

    #[test]
    fn test_decorrelated_jitter_backoff() {
        let mut backoff = Backoff::decorrelated_jitter_backoff(2, 7, 3);

        let first_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(first_delay_dur >= Duration::from_millis(2));
        assert!(first_delay_dur <= Duration::from_millis(6));

        let second_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(second_delay_dur >= Duration::from_millis(2));
        let cap_ms = 7u64.min((first_delay_dur.as_millis() * 3).try_into().unwrap());
        assert!(second_delay_dur <= Duration::from_millis(cap_ms));

        let third_delay_dur = backoff.next_delay_duration().unwrap();
        assert!(third_delay_dur >= Duration::from_millis(2));
        let cap_ms = 7u64.min((second_delay_dur.as_millis() * 3).try_into().unwrap());
        assert!(second_delay_dur <= Duration::from_millis(cap_ms));

        assert_eq!(backoff.next_delay_duration(), None);
    }

    #[test]
    #[should_panic(expected = "base_delay_ms must be positive")]
    fn test_decorrelated_jitter_backoff_with_invalid_base_delay_ms() {
        Backoff::decorrelated_jitter_backoff(0, 7, 3);
    }

    /// client-go's curves per kind (config/retry/config.go:122-140), each with its own
    /// attempt count.
    #[test]
    fn test_time_budget_curves() {
        let mut b = Backoff::time_budget(1_000_000);
        let ms = |d: Option<Duration>| d.unwrap().as_millis() as u64;
        // regionMiss 2/500 ms, no jitter.
        let region_miss: Vec<u64> = (0..10)
            .map(|_| ms(b.next_delay_for(BackoffType::RegionMiss)))
            .collect();
        assert_eq!(region_miss, [2, 4, 8, 16, 32, 64, 128, 256, 500, 500]);
        // staleCommand 2/1000 ms starts at its own base.
        let stale: Vec<u64> = (0..11)
            .map(|_| ms(b.next_delay_for(BackoffType::StaleCommand)))
            .collect();
        assert_eq!(stale, [2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 1000]);
        // tikvRPC 100/2000 ms, equal jitter: in [v/2, v).
        for v in [100, 200, 400, 800, 1600, 2000, 2000] {
            let d = ms(b.next_delay_for(BackoffType::TikvRpc));
            assert!(v / 2 <= d && d < v, "{d} not in [{}, {v})", v / 2);
        }
        // serverBusy 2000/10000 ms, equal jitter.
        for v in [2000, 4000, 8000, 10000, 10000] {
            let d = ms(b.next_delay_for(BackoffType::ServerBusy));
            assert!(v / 2 <= d && d < v, "{d} not in [{}, {v})", v / 2);
        }
        // regionMiss went on from where it was.
        assert_eq!(ms(b.next_delay_for(BackoffType::RegionMiss)), 500);
        // `next_delay_duration` is regionMiss.
        assert_eq!(ms(b.next_delay_duration()), 500);
    }

    /// The budget is total sleep time, checked before each sleep (backoff.go:146): the sleep
    /// that crosses it is taken, the next is refused.
    #[test]
    fn test_time_budget_limits_total_sleep() {
        let mut b = Backoff::time_budget(20_000);
        let mut total = 0;
        let mut last = 0;
        while let Some(d) = b.next_delay_for(BackoffType::RegionMiss) {
            last = d.as_millis() as u64;
            total += last;
        }
        assert!(
            total >= 20_000 && total - last < 20_000,
            "total {total}, last {last}"
        );
        // Every kind is refused now.
        assert_eq!(b.next_delay_for(BackoffType::TikvRpc), None);
        // 2+4+...+256 = 510 ms in 8 steps, then 39 steps of 500 ms to reach 20 s: 47
        // regionMiss attempts, against the 10 of `DEFAULT_REGION_BACKOFF`.
        assert_eq!(b.current_attempts(), 47);
    }

    /// serverBusy sleeps are not charged, up to 10 minutes of them (config.go:144,
    /// backoff.go:148-150): a busy TiKV is waited for, and other errors keep their budget.
    #[test]
    fn test_time_budget_excludes_server_busy() {
        let mut b = Backoff::time_budget(20_000);
        let mut busy = 0;
        while let Some(d) = b.next_delay_for(BackoffType::ServerBusy) {
            busy += d.as_millis() as u64;
        }
        assert!(
            (SERVER_BUSY_EXCLUDED_MS..SERVER_BUSY_EXCLUDED_MS + 10_000).contains(&busy),
            "{busy}"
        );
        // The non-excluded budget is untouched: regionMiss still gets its 20 s.
        let mut other = 0;
        while let Some(d) = b.next_delay_for(BackoffType::RegionMiss) {
            other += d.as_millis() as u64;
        }
        assert!(other >= 20_000, "{other}");
    }

    fn drain(b: &mut Backoff, t: BackoffType) -> u64 {
        let mut total = 0;
        while let Some(d) = b.next_delay_for(t) {
            total += d.as_millis() as u64;
        }
        total
    }

    /// Once started, a budget is one client-go `bo`: every clone draws from it.
    #[test]
    fn test_time_budget_started_clones_share() {
        let mut read = Backoff::time_budget(1000).start();
        let mut resolution = read.clone();
        assert!(drain(&mut resolution, BackoffType::StaleCommand) >= 1000);
        assert_eq!(read.next_delay_for(BackoffType::RegionMiss), None);
        // A new read starts a new budget.
        assert!(read
            .start()
            .next_delay_for(BackoffType::RegionMiss)
            .is_some());
    }

    /// The template kept in `RetryOptions` is not shared: each clone starts on first use.
    #[test]
    fn test_time_budget_template_clones_are_independent() {
        let template = Backoff::time_budget(1000);
        assert!(drain(&mut template.clone(), BackoffType::RegionMiss) >= 1000);
        assert_eq!(
            template.clone().next_delay_for(BackoffType::RegionMiss),
            Some(Duration::from_millis(2))
        );
    }

    /// `fork` (client-go `Fork` / `Clone`, backoff.go:259-292): keeps the sleep spent so far,
    /// restarts every curve, and counts on its own afterwards.
    #[test]
    fn test_time_budget_fork() {
        let mut read = Backoff::time_budget(1000).start();
        for _ in 0..8 {
            read.next_delay_for(BackoffType::RegionMiss); // 2 + 4 + ... + 256 = 510 ms
        }
        let mut fork = read.fork();
        assert_eq!(
            fork.next_delay_for(BackoffType::RegionMiss),
            Some(Duration::from_millis(2))
        );
        // The fork has 1000 - 512 ms left.
        let forked = drain(&mut fork, BackoffType::RegionMiss);
        assert!((488..1000).contains(&forked), "{forked}");
        // The read itself is not charged for the fork's sleeps.
        assert_eq!(
            read.next_delay_for(BackoffType::RegionMiss),
            Some(Duration::from_millis(500))
        );
    }

    /// `next_delay_capped` (`BackoffWithMaxSleepTxnLockFast`): the capped sleep is what is
    /// charged, and the curve still advances.
    #[test]
    fn test_time_budget_capped_charges_capped_sleep() {
        let mut b = Backoff::time_budget(100).start();
        let cap = Some(Duration::from_millis(1));
        for _ in 0..100 {
            assert_eq!(
                b.next_delay_capped(BackoffType::TikvLockFast, cap),
                Some(Duration::from_millis(1))
            );
        }
        assert_eq!(b.next_delay_capped(BackoffType::TikvLockFast, cap), None);
    }

    /// `longest_sleep` (client-go `longestSleepCfg`, backoff.go:368-383): the kind slept on
    /// longest, `serverBusy` aside; forks keep what was slept.
    #[test]
    fn test_time_budget_longest_sleep() {
        let mut b = Backoff::time_budget(1_000_000).start();
        assert_eq!(b.longest_sleep(), None);
        for _ in 0..3 {
            b.next_delay_for(BackoffType::RegionMiss); // 2 + 4 + 8 ms
        }
        assert_eq!(b.longest_sleep(), Some(BackoffType::RegionMiss));
        b.next_delay_for(BackoffType::TikvRpc); // >= 50 ms
        assert_eq!(b.longest_sleep(), Some(BackoffType::TikvRpc));
        b.next_delay_for(BackoffType::ServerBusy); // >= 1000 ms, but excluded
        assert_eq!(b.longest_sleep(), Some(BackoffType::TikvRpc));
        assert_eq!(b.fork().longest_sleep(), Some(BackoffType::TikvRpc));
        // Count-based backoffs report nothing.
        let mut count = Backoff::no_jitter_backoff(2, 500, 10);
        count.next_delay_duration();
        assert_eq!(count.longest_sleep(), None);
    }
}
