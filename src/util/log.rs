// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Write as _;
use std::time::Duration;

/// Format a non-negative `Duration` using Go's `time.Duration.String()` format
/// (e.g. `0s`, `500ns`, `12.345µs`, `35ms`, `1.5s`, `1m2s`, `1h0m0s`).
pub(crate) fn format_go_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let nanos = d.subsec_nanos();
    if total_secs == 0 {
        if nanos == 0 {
            return "0s".to_owned();
        }
        if nanos < 1_000 {
            return format!("{nanos}ns");
        }
        if nanos < 1_000_000 {
            return format_scaled(nanos / 1_000, nanos % 1_000, 3, "µs");
        }
        return format_scaled(nanos / 1_000_000, nanos % 1_000_000, 6, "ms");
    }
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let mut out = String::new();
    if hours > 0 {
        let _ = write!(out, "{hours}h{mins}m");
    } else if mins > 0 {
        let _ = write!(out, "{mins}m");
    }
    out.push_str(&format_scaled(secs as u32, nanos, 9, "s"));
    out
}

fn format_scaled(whole: u32, frac: u32, width: usize, unit: &str) -> String {
    if frac == 0 {
        return format!("{whole}{unit}");
    }
    let frac_str = format!("{frac:0width$}");
    let trimmed = frac_str.trim_end_matches('0');
    format!("{whole}.{trimmed}{unit}")
}

#[cfg(test)]
pub(crate) mod test_logger {
    use std::sync::Mutex;
    use std::sync::Once;

    #[derive(Debug, Clone)]
    pub(crate) struct CapturedRecord {
        pub(crate) level: log::Level,
        pub(crate) message: String,
        pub(crate) kvs: Vec<(String, String)>,
    }

    impl CapturedRecord {
        pub(crate) fn kv(&self, key: &str) -> Option<&str> {
            self.kvs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str())
        }
    }

    static CAPTURED_RECORDS: Mutex<Vec<CapturedRecord>> = Mutex::new(Vec::new());
    static LOGGER: CaptureLogger = CaptureLogger;
    static LOGGER_INIT: Once = Once::new();

    struct KvCollector(Vec<(String, String)>);

    impl<'kvs> log::kv::VisitSource<'kvs> for KvCollector {
        fn visit_pair(
            &mut self,
            key: log::kv::Key<'kvs>,
            value: log::kv::Value<'kvs>,
        ) -> Result<(), log::kv::Error> {
            self.0.push((key.as_str().to_owned(), value.to_string()));
            Ok(())
        }
    }

    struct CaptureLogger;

    impl log::Log for CaptureLogger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            let mut collector = KvCollector(Vec::new());
            let _ = record.key_values().visit(&mut collector);
            let captured = CapturedRecord {
                level: record.level(),
                message: record.args().to_string(),
                kvs: collector.0,
            };
            if let Ok(mut logs) = CAPTURED_RECORDS.lock() {
                logs.push(captured);
            }
        }

        fn flush(&self) {}
    }

    pub(crate) fn install() {
        LOGGER_INIT.call_once(|| {
            let _ = log::set_logger(&LOGGER);
            log::set_max_level(log::LevelFilter::Trace);
        });
    }

    pub(crate) fn checkpoint() -> usize {
        install();
        CAPTURED_RECORDS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub(crate) fn since(idx: usize) -> Vec<CapturedRecord> {
        CAPTURED_RECORDS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(idx..)
            .unwrap_or(&[])
            .to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::format_go_duration;

    #[test]
    fn test_format_go_duration() {
        assert_eq!(format_go_duration(Duration::ZERO), "0s");
        assert_eq!(format_go_duration(Duration::from_nanos(500)), "500ns");
        assert_eq!(format_go_duration(Duration::from_nanos(1_000)), "1µs");
        assert_eq!(format_go_duration(Duration::from_nanos(12_345)), "12.345µs");
        assert_eq!(format_go_duration(Duration::from_nanos(12_300)), "12.3µs");
        assert_eq!(format_go_duration(Duration::from_nanos(12_005)), "12.005µs");
        assert_eq!(format_go_duration(Duration::from_millis(35)), "35ms");
        assert_eq!(
            format_go_duration(Duration::from_micros(12_345)),
            "12.345ms"
        );
        assert_eq!(format_go_duration(Duration::from_secs(1)), "1s");
        assert_eq!(format_go_duration(Duration::from_millis(1_500)), "1.5s");
        assert_eq!(format_go_duration(Duration::from_secs(60)), "1m0s");
        assert_eq!(format_go_duration(Duration::from_secs(62)), "1m2s");
        assert_eq!(format_go_duration(Duration::from_secs(3_600)), "1h0m0s");
        assert_eq!(
            format_go_duration(Duration::from_secs(3_661) + Duration::from_millis(250)),
            "1h1m1.25s"
        );
    }
}
