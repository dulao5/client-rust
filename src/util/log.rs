// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Zero-allocation formatter for a non-negative `Duration` using Go's `time.Duration.String()`
/// format (e.g. `0s`, `500ns`, `12.345µs`, `35ms`, `1.5s`, `1m2s`, `1h0m0s`).
#[derive(Clone, Copy, Debug)]
pub struct GoDuration(pub Duration);

impl fmt::Display for GoDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.0;
        let total_secs = d.as_secs();
        let nanos = d.subsec_nanos();
        if total_secs == 0 {
            if nanos == 0 {
                return f.write_str("0s");
            }
            if nanos < 1_000 {
                return write!(f, "{nanos}ns");
            }
            if nanos < 1_000_000 {
                return write_scaled(f, nanos / 1_000, nanos % 1_000, 3, "µs");
            }
            return write_scaled(f, nanos / 1_000_000, nanos % 1_000_000, 6, "ms");
        }
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        if hours > 0 {
            write!(f, "{hours}h{mins}m")?;
        } else if mins > 0 {
            write!(f, "{mins}m")?;
        }
        write_scaled(f, secs as u32, nanos, 9, "s")
    }
}

/// Format a non-negative `Duration` using Go's `time.Duration.String()` format.
#[allow(dead_code)]
pub(crate) fn format_go_duration(d: Duration) -> String {
    GoDuration(d).to_string()
}

fn write_scaled(
    f: &mut fmt::Formatter<'_>,
    whole: u32,
    mut frac: u32,
    mut width: usize,
    unit: &str,
) -> fmt::Result {
    write!(f, "{whole}")?;
    if frac != 0 {
        while width > 0 && frac.is_multiple_of(10) {
            frac /= 10;
            width -= 1;
        }
        write!(f, ".{frac:0width$}")?;
    }
    f.write_str(unit)
}

/// Zero-allocation formatter for `SystemTime` in UTC matching Go's `time.RFC3339Nano`
/// (`YYYY-MM-DDTHH:MM:SSZ` or `YYYY-MM-DDTHH:MM:SS.<trimmed_nanos>Z`).
///
/// Formats in UTC (`Z`) rather than local offset because Rust `std` exposes no local timezone
/// API without `unsafe` FFI or adding an external timezone crate dependency.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Rfc3339NanoUtc(pub(crate) SystemTime);

impl fmt::Display for Rfc3339NanoUtc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dur = self.0.duration_since(UNIX_EPOCH).unwrap_or_default();
        let total_secs = dur.as_secs();
        let nanos = dur.subsec_nanos();
        let days = (total_secs / 86_400) as i64;
        let rem = total_secs % 86_400;
        let hour = rem / 3_600;
        let min = (rem % 3_600) / 60;
        let sec = rem % 60;
        let (year, month, day) = civil_from_days(days);
        write!(
            f,
            "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}"
        )?;
        if nanos != 0 {
            let mut frac = nanos;
            let mut width = 9usize;
            while width > 0 && frac.is_multiple_of(10) {
                frac /= 10;
                width -= 1;
            }
            write!(f, ".{frac:0width$}")?;
        }
        f.write_str("Z")
    }
}

/// Convert days since Unix epoch (`1970-01-01`) to `(year, month, day)` in the proleptic
/// Gregorian calendar using Howard Hinnant's `civil_from_days` algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = y + i64::from(m <= 2);
    (year, m, d)
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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{format_go_duration, GoDuration, Rfc3339NanoUtc};

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
        // Parity between zero-alloc Display and convenience String helper.
        for d in [
            Duration::ZERO,
            Duration::from_nanos(500),
            Duration::from_nanos(12_300),
            Duration::from_millis(35),
            Duration::from_secs(3_661) + Duration::from_millis(250),
        ] {
            assert_eq!(GoDuration(d).to_string(), format_go_duration(d));
        }
    }

    #[test]
    fn test_rfc3339_nano_utc() {
        assert_eq!(
            Rfc3339NanoUtc(UNIX_EPOCH).to_string(),
            "1970-01-01T00:00:00Z"
        );
        // 2024-02-29T23:59:59.5Z (leap day, 1709251199s after epoch)
        let leap = UNIX_EPOCH + Duration::new(1_709_251_199, 500_000_000);
        assert_eq!(Rfc3339NanoUtc(leap).to_string(), "2024-02-29T23:59:59.5Z");
        // 2024-03-01T00:00:00.000123Z
        let next_day = UNIX_EPOCH + Duration::new(1_709_251_200, 123_000);
        assert_eq!(
            Rfc3339NanoUtc(next_day).to_string(),
            "2024-03-01T00:00:00.000123Z"
        );
        // 2000-02-29T00:00:00Z (400-year leap day, 11016 days after 1970-01-01)
        let y2k_leap = UNIX_EPOCH + Duration::from_secs(11_016 * 86_400);
        assert_eq!(Rfc3339NanoUtc(y2k_leap).to_string(), "2000-02-29T00:00:00Z");
        // Current wall clock produces valid RFC3339 UTC ending in Z.
        let now = Rfc3339NanoUtc(SystemTime::now()).to_string();
        assert!(now.ends_with('Z'));
        assert_eq!(now.as_bytes()[4], b'-');
        assert_eq!(now.as_bytes()[7], b'-');
        assert_eq!(now.as_bytes()[10], b'T');
        assert_eq!(now.as_bytes()[13], b':');
        assert_eq!(now.as_bytes()[16], b':');
    }

    #[test]
    fn test_go_duration_lazy_display_with_log_kv() {
        let start = super::test_logger::checkpoint();
        log::info!(
            spent:% = GoDuration(Duration::from_millis(35)),
            marker = "go-dur-kv-test";
            "go duration kv"
        );
        let records = super::test_logger::since(start);
        assert!(records.iter().any(|r| {
            r.level == log::Level::Info
                && r.message == "go duration kv"
                && r.kv("marker") == Some("go-dur-kv-test")
                && r.kv("spent") == Some("35ms")
        }));
    }
}
