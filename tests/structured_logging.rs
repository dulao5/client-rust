// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;
use std::sync::Once;

use tikv_client::metrics::BackoffType;
use tikv_client::Backoff;
use tikv_client::Error;

#[derive(Debug, Clone)]
struct RecordedLog {
    level: log::Level,
    message: String,
    kvs: Vec<(String, String)>,
}

impl RecordedLog {
    fn kv(&self, key: &str) -> Option<&str> {
        self.kvs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

struct KvVisitor(Vec<(String, String)>);

impl<'kvs> log::kv::VisitSource<'kvs> for KvVisitor {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        self.0.push((key.as_str().to_owned(), value.to_string()));
        Ok(())
    }
}

static RECORDS: Mutex<Vec<RecordedLog>> = Mutex::new(Vec::new());
static LOGGER: TestLogger = TestLogger;
static INIT: Once = Once::new();

struct TestLogger;

impl log::Log for TestLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let mut visitor = KvVisitor(Vec::new());
        let _ = record.key_values().visit(&mut visitor);
        if let Ok(mut records) = RECORDS.lock() {
            records.push(RecordedLog {
                level: record.level(),
                message: record.args().to_string(),
                kvs: visitor.0,
            });
        }
    }

    fn flush(&self) {}
}

fn checkpoint() -> usize {
    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("install test logger");
        log::set_max_level(log::LevelFilter::Trace);
    });
    RECORDS.lock().unwrap().len()
}

fn since(idx: usize) -> Vec<RecordedLog> {
    RECORDS.lock().unwrap()[idx..].to_vec()
}

#[tokio::test]
async fn backoff_emits_structured_retry_later_and_max_sleep_warn() {
    let start = checkpoint();
    let mut bo = Backoff::time_budget(10).start().with_txn_start_ts(12345);
    let err_marker = "integration-backoff-err-460";

    while bo
        .backoff(
            BackoffType::RegionMiss,
            Error::StringError(err_marker.to_owned()),
        )
        .await
        .is_ok()
    {}

    let records = since(start);
    assert!(
        records.iter().any(|r| {
            r.level == log::Level::Debug
                && r.message == "backoff"
                && r.kv("base") == Some("2")
                && r.kv("sleep") == Some("2")
                && r.kv("attempts") == Some("0")
        }),
        "expected structured 'backoff' DEBUG log"
    );

    let retry_later = records
        .iter()
        .find(|r| {
            r.level == log::Level::Debug
                && r.message == "retry later"
                && r.kv("error").is_some_and(|e| e.contains(err_marker))
        })
        .expect("expected structured 'retry later' DEBUG log");
    assert_eq!(retry_later.kv("type"), Some("regionMiss"));
    assert_eq!(retry_later.kv("maxSleep"), Some("10"));
    assert_eq!(retry_later.kv("excludedSleep"), Some("0"));
    assert_eq!(retry_later.kv("txnStartTS"), Some("12345"));
    assert!(retry_later.kv("totalSleep").is_some());

    let max_sleep_warn = records
        .iter()
        .find(|r| {
            r.level == log::Level::Warn
                && r.message
                    .starts_with("regionMiss backoffer.maxSleep 10ms is exceeded, errors:")
                && r.message.contains(err_marker)
        })
        .expect("expected Go-formatted backoffer.maxSleep WARN log");
    assert!(
        max_sleep_warn.kvs.is_empty(),
        "backoffer.maxSleep WARN must not attach KV fields, got {:?}",
        max_sleep_warn.kvs
    );
    assert!(
        max_sleep_warn
            .message
            .contains(&format!("{err_marker} at ")),
        "expected 'at <RFC3339>' error entry in {:?}",
        max_sleep_warn.message
    );
    let expected_tail = format!(
        "\ntotal-backoff-times: 3, backoff-detail: regionMiss:3, maxBackoffTimeExceeded: true, maxExcludedTimeExceeded: false\nlongest sleep type: regionMiss, time: {}ms",
        bo.total_sleep_ms()
    );
    assert!(
        max_sleep_warn.message.ends_with(&expected_tail),
        "unexpected WARN tail in {:?}",
        max_sleep_warn.message
    );
}

#[tokio::test]
async fn backoff_lock_fast_logs_none_txn_start_ts_when_unset() {
    let start = checkpoint();
    let mut bo = Backoff::time_budget(100).start();
    assert_eq!(bo.txn_start_ts(), None);
    let err_marker = "integration-lock-fast-err-461";

    bo.backoff_with_max_sleep_txn_lock_fast(1, Error::StringError(err_marker.to_owned()))
        .await
        .unwrap();

    let records = since(start);
    let retry_later = records
        .iter()
        .find(|r| {
            r.level == log::Level::Debug
                && r.message == "retry later"
                && r.kv("error").is_some_and(|e| e.contains(err_marker))
        })
        .expect("expected 'retry later' log for txnLockFast");
    assert_eq!(retry_later.kv("type"), Some("txnLockFast"));
    assert_eq!(retry_later.kv("maxSleep"), Some("100"));
    assert_eq!(retry_later.kv("totalSleep"), Some("1"));
    assert_eq!(retry_later.kv("excludedSleep"), Some("0"));
    assert_eq!(retry_later.kv("txnStartTS"), Some("None"));
}
