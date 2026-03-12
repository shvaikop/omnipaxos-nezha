//! Clock simulation for clock-assisted consensus.
//!
//! The clock periodically synchronizes to hardware time within a configured
//! uncertainty bound. Between synchronization points, drift is applied to
//! elapsed hardware time.

use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

/// Simulated clock used for clock-assisted consensus experiments.
pub struct Clock {
    /// Drift in microseconds per second.
    drift_us_per_sec: i64,

    /// Synchronization period in microseconds.
    sync_interval_us: u64,

    /// Maximum synchronization uncertainty in microseconds.
    sync_uncertainty_us: u64,

    /// Hardware time at the last synchronization point.
    last_sync_real_time_us: u64,

    /// Local clock time at the last synchronization point.
    base_clock_time_us: u64,

    /// Last returned clock value.
    ///
    /// Ensures the clock never goes backwards.
    last_returned_time_us: u64,
}

impl Clock {
    /// Creates a new simulated clock instance.
    pub fn new(
        drift_us_per_sec: i64,
        sync_interval_us: u64,
        sync_uncertainty_us: u64,
    ) -> Self {
        let real_now = Self::hardware_time_us();
        let base_now = Self::apply_sync_uncertainty(real_now, sync_uncertainty_us);

        Self {
            drift_us_per_sec,
            sync_interval_us,
            sync_uncertainty_us,
            last_sync_real_time_us: real_now,
            base_clock_time_us: base_now,
            last_returned_time_us: base_now,
        }
    }

    /// Returns the current simulated clock time in microseconds.
    pub fn now_us(&mut self) -> u64 {
        let real_now = Self::hardware_time_us();

        if real_now.saturating_sub(self.last_sync_real_time_us) >= self.sync_interval_us {
            self.resync(real_now);
        }

        let elapsed_real_time_us = real_now.saturating_sub(self.last_sync_real_time_us);
        let elapsed_clock_time_us = self.apply_drift(elapsed_real_time_us);

        let candidate = self
            .base_clock_time_us
            .saturating_add(elapsed_clock_time_us);

        let monotonic_time = candidate.max(self.last_returned_time_us);
        self.last_returned_time_us = monotonic_time;

        monotonic_time
    }

    /// Returns the current synchronization uncertainty in microseconds.
    pub fn uncertainty_us(&self) -> u64 {
        self.sync_uncertainty_us
    }

    /// Synchronizes the clock to hardware time within the configured uncertainty.
    fn resync(&mut self, real_now: u64) {
        let synced_time = Self::apply_sync_uncertainty(real_now, self.sync_uncertainty_us);
        let monotonic_synced_time = synced_time.max(self.last_returned_time_us);

        self.last_sync_real_time_us = real_now;
        self.base_clock_time_us = monotonic_synced_time;
        self.last_returned_time_us = monotonic_synced_time;
    }

    /// Applies drift to elapsed real time.
    fn apply_drift(&self, elapsed_real_time_us: u64) -> u64 {
        let drift_delta =
            elapsed_real_time_us.saturating_mul(self.drift_us_per_sec.unsigned_abs()) / 1_000_000;

        if self.drift_us_per_sec >= 0 {
            elapsed_real_time_us.saturating_add(drift_delta)
        } else {
            elapsed_real_time_us.saturating_sub(drift_delta)
        }
    }

    /// Returns the current hardware time in microseconds.
    fn hardware_time_us() -> u64 {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        duration.as_micros() as u64
    }

    /// Applies synchronization uncertainty to hardware time.
    fn apply_sync_uncertainty(real_time_us: u64, sync_uncertainty_us: u64) -> u64 {
        if sync_uncertainty_us == 0 {
            return real_time_us;
        }

        let mut rng = rand::thread_rng();
        let offset =
            rng.gen_range(-(sync_uncertainty_us as i64)..=(sync_uncertainty_us as i64));

        if offset >= 0 {
            real_time_us.saturating_add(offset as u64)
        } else {
            real_time_us.saturating_sub((-offset) as u64)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::Clock;
    use std::{thread, time::Duration};

    /// Test that the clock never goes backwards.
    #[test]
    fn clock_is_monotonic() {
        let mut clock = Clock::new(0, 10_000_000, 0);

        let t1 = clock.now_us();
        let t2 = clock.now_us();
        let t3 = clock.now_us();

        assert!(t2 >= t1);
        assert!(t3 >= t2);
    }

    /// Test that time increases over real time.
    #[test]
    fn clock_progresses_over_time() {
        let mut clock = Clock::new(0, 10_000_000, 0);

        let t1 = clock.now_us();

        thread::sleep(Duration::from_millis(10));

        let t2 = clock.now_us();

        assert!(t2 > t1);
    }

    /// Test that drift affects the clock rate.
    #[test]
    fn clock_with_drift_runs_faster() {
        let mut normal_clock = Clock::new(0, 10_000_000, 0);
        let mut drifted_clock = Clock::new(100_000, 10_000_000, 0);

        let start_normal = normal_clock.now_us();
        let start_drifted = drifted_clock.now_us();

        thread::sleep(Duration::from_millis(200));

        let end_normal = normal_clock.now_us();
        let end_drifted = drifted_clock.now_us();

        let normal_elapsed = end_normal - start_normal;
        let drifted_elapsed = end_drifted - start_drifted;

        assert!(drifted_elapsed > normal_elapsed);
    }
}