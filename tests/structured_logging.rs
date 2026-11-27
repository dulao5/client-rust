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
    let mut bo = Backoff::time_budget(10).start();
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
    assert!(retry_later.kv("totalSleep").is_some());

    let max_sleep_warn = records
        .iter()
        .find(|r| {
            r.level == log::Level::Warn
                && r.message == "regionMiss backoffer.maxSleep 10ms is exceeded, errors:"
                && r.kv("error").is_some_and(|e| e.contains(err_marker))
        })
        .expect("expected structured backoffer.maxSleep WARN log");
    assert_eq!(max_sleep_warn.kv("type"), Some("regionMiss"));
    assert_eq!(max_sleep_warn.kv("maxSleep"), Some("10"));
    assert_eq!(max_sleep_warn.kv("longestSleepType"), Some("regionMiss"));
    assert_eq!(
        max_sleep_warn.kv("totalSleep"),
        Some(bo.total_sleep_ms().to_string().as_str())
    );
    assert_eq!(
        max_sleep_warn.kv("longestSleepMs"),
        Some(bo.total_sleep_ms().to_string().as_str())
    );
    assert_eq!(max_sleep_warn.kv("excludedSleep"), Some("0"));
}
