//! A timestamp returned from the timestamp oracle.
//!
//! The version used in transactions can be converted from a timestamp.
//! The lower 18 (PHYSICAL_SHIFT_BITS) bits are the logical part of the timestamp.
//! The higher bits of the version are the physical part of the timestamp.

pub use crate::proto::pdpb::Timestamp;

const PHYSICAL_SHIFT_BITS: i64 = 18;
const LOGICAL_MASK: i64 = (1 << PHYSICAL_SHIFT_BITS) - 1;

/// A helper trait to convert a Timestamp to and from an u64.
///
/// Currently the only implmentation of this trait is [`Timestamp`](Timestamp) in TiKV.
/// It contains a physical part (first 46 bits) and a logical part (last 18 bits).
pub trait TimestampExt: Sized {
    /// Convert the timestamp to u64.
    fn version(&self) -> u64;
    /// Convert u64 to a timestamp.
    fn from_version(version: u64) -> Self;
    /// Convert u64 to an optional timestamp, where `0` represents no timestamp.
    fn try_from_version(version: u64) -> Option<Self>;
}

impl TimestampExt for Timestamp {
    fn version(&self) -> u64 {
        // `from_version(u64::MAX)` (TiDB's MaxTS, "read the latest committed version") stores
        // the bit pattern as i64 -1 parts; compose in u64 so it round-trips instead of
        // overflowing. Every timestamp PD issues is far below 2^63.
        let physical = self.physical as u64;
        let logical = self.logical as u64;
        (physical << PHYSICAL_SHIFT_BITS).wrapping_add(logical)
    }

    fn from_version(version: u64) -> Self {
        let version = version as i64;
        Self {
            physical: version >> PHYSICAL_SHIFT_BITS,
            logical: version & LOGICAL_MASK,
            // Now we only support global transactions: suffix_bits: 0,
            ..Default::default()
        }
    }

    fn try_from_version(version: u64) -> Option<Self> {
        if version == 0 {
            None
        } else {
            Some(Self::from_version(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_round_trip() {
        for v in [
            0,
            1,
            (1 << 18) - 1,
            1 << 18,
            449_000_000_000_000_000,
            u64::MAX,
        ] {
            assert_eq!(Timestamp::from_version(v).version(), v, "{v}");
        }
    }
}
