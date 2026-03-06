//! Clock simulation for clock-assisted consensus.
//!
//! Each clock reads the same hardware time and applies simulated
//! drift and uncertainty.

use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

/// Simulated clock used for clock-assisted consensus.
pub struct Clock {
    /// Drift factor applied to the hardware time.
    drift: f64,

    /// Maximum uncertainty in microseconds.
    uncertainty: u64,
}

impl Clock {
    /// Creates a new clock instance.
    pub fn new(drift: f64, uncertainty: u64) -> Self {
        Self { drift, uncertainty }
    }

    /// Returns the current simulated clock time in microseconds.
    pub fn now_us(&self) -> u64 {
        let hw_time = self.hardware_time_us();
        let drifted_time = self.apply_drift(hw_time);
        self.apply_uncertainty(drifted_time)
    }

    /// Returns the maximum clock uncertainty in microseconds.
    pub fn uncertainty_us(&self) -> u64 {
        self.uncertainty
    }

    /// Returns the current hardware time in microseconds.
    fn hardware_time_us(&self) -> u64 {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        duration.as_micros() as u64
    }

    /// Applies drift to the given hardware time.
    fn apply_drift(&self, hw_time: u64) -> u64 {
        ((hw_time as f64) * self.drift) as u64
    }

    /// Applies uncertainty to the given drifted time.
    fn apply_uncertainty(&self, drifted_time: u64) -> u64 {
        if self.uncertainty == 0 {
            return drifted_time;
        }

        let mut rng = rand::thread_rng();
        let offset = rng.gen_range(-(self.uncertainty as i64)..=(self.uncertainty as i64));

        if offset >= 0 {
            drifted_time.saturating_add(offset as u64)
        } else {
            drifted_time.saturating_sub((-offset) as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Clock;
    use std::{thread, time::Duration};

    #[test]
    fn new_sets_uncertainty() {
        let clock = Clock::new(1.0, 250);
        assert_eq!(clock.uncertainty_us(), 250);
    }

    #[test]
    fn apply_drift_scales_time() {
        let clock = Clock::new(1.5, 0);
        assert_eq!(clock.apply_drift(1_000), 1_500);

        let identity_clock = Clock::new(1.0, 0);
        assert_eq!(identity_clock.apply_drift(42), 42);
    }

    #[test]
    fn apply_uncertainty_zero_returns_input() {
        let clock = Clock::new(1.0, 0);
        assert_eq!(clock.apply_uncertainty(123_456), 123_456);
    }

    #[test]
    fn apply_uncertainty_stays_within_bounds() {
        let clock = Clock::new(1.0, 100);
        let base = 10_000u64;

        for _ in 0..500 {
            let t = clock.apply_uncertainty(base);
            assert!((base - 100..=base + 100).contains(&t));
        }
    }

    #[test]
    fn apply_uncertainty_saturates_at_zero() {
        let clock = Clock::new(1.0, 100);

        for _ in 0..500 {
            let t = clock.apply_uncertainty(0);
            assert!(t <= 100);
        }
    }

    #[test]
    fn now_us_progresses_without_uncertainty() {
        let clock = Clock::new(1.0, 0);
        let t1 = clock.now_us();
        thread::sleep(Duration::from_millis(2));
        let t2 = clock.now_us();
        assert!(t2 >= t1);
    }
}
