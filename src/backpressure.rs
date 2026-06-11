use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Admission decision for a device under current pressure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Admission {
    /// The device may accept new work.
    Allow,
    /// The device should reject new work for now.
    Backpressure,
}

/// Pressure signal categories with independent severity weights.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PressureSignal {
    /// Allocation path pressure (e.g. OOM or failed reservation).
    AllocationFailure,
    /// Execution enqueue pressure (e.g. saturated command queue).
    EnqueueFailure,
    /// Transfer path pressure (e.g. copy/map stalls or failures).
    TransferFailure,
    /// Generic saturation signal.
    Saturation,
}

impl PressureSignal {
    #[inline]
    fn weight(self) -> f32 {
        match self {
            Self::AllocationFailure => 1.0,
            Self::EnqueueFailure => 0.75,
            Self::TransferFailure => 0.5,
            Self::Saturation => 0.6,
        }
    }
}

/// Configuration for multi-device pressure tracking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackpressureConfig {
    /// Number of consecutive pressure signals before admission is denied.
    pub hard_threshold: u32,
    /// Number of consecutive success signals required to clear denial state.
    pub recover_threshold: u32,
    /// Score threshold above which admissions are denied.
    pub score_threshold: f32,
    /// Maximum concurrent admitted operations per device.
    pub max_inflight: u32,
    /// Exponential decay half-life for pressure score.
    pub decay_half_life: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            hard_threshold: 3,
            recover_threshold: 2,
            score_threshold: 1.5,
            max_inflight: 64,
            decay_half_life: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DeviceState {
    pressure_streak: u32,
    recovery_streak: u32,
    inflight: u32,
    score: f32,
    last_decay: Instant,
}

impl Default for DeviceState {
    fn default() -> Self {
        Self {
            pressure_streak: 0,
            recovery_streak: 0,
            inflight: 0,
            score: 0.0,
            last_decay: Instant::now(),
        }
    }
}

/// Multi-device backpressure manager.
///
/// This manager intentionally does not rely on device memory introspection APIs.
/// Instead it tracks operational pressure signals (e.g. allocation failures,
/// saturation) and uses those signals to gate new admissions per device.
#[derive(Clone)]
pub struct BackpressureManager {
    config: BackpressureConfig,
    state: Arc<Mutex<HashMap<String, DeviceState>>>,
}

impl BackpressureManager {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns whether the given `device` should accept new work.
    pub fn admit(&self, device: &str) -> Admission {
        let mut state = self.state.lock().expect("backpressure state");
        let status = state.entry(device.to_string()).or_default();
        decay(status, self.config);

        if status.inflight >= self.config.max_inflight
            || status.pressure_streak >= self.config.hard_threshold
            || status.score >= self.config.score_threshold
        {
            Admission::Backpressure
        } else {
            Admission::Allow
        }
    }

    /// Attempt to acquire an in-flight slot for `device`.
    /// Returns `false` when backpressure denies admission.
    pub fn try_acquire(&self, device: &str) -> bool {
        let mut state = self.state.lock().expect("backpressure state");
        let status = state.entry(device.to_string()).or_default();
        decay(status, self.config);

        if status.inflight >= self.config.max_inflight
            || status.pressure_streak >= self.config.hard_threshold
            || status.score >= self.config.score_threshold
        {
            return false;
        }

        status.inflight = status.inflight.saturating_add(1);
        true
    }

    /// Release an in-flight slot for `device`.
    pub fn release(&self, device: &str) {
        let mut state = self.state.lock().expect("backpressure state");
        let status = state.entry(device.to_string()).or_default();
        status.inflight = status.inflight.saturating_sub(1);
    }

    /// Record a pressure event for `device`.
    pub fn report_pressure(&self, device: &str) {
        self.report_signal(device, PressureSignal::Saturation)
    }

    /// Record a typed pressure signal for `device`.
    pub fn report_signal(&self, device: &str, signal: PressureSignal) {
        let mut state = self.state.lock().expect("backpressure state");
        let status = state.entry(device.to_string()).or_default();
        decay(status, self.config);

        status.score += signal.weight();
        status.pressure_streak = status.pressure_streak.saturating_add(1);
        status.recovery_streak = 0;
    }

    /// Record a successful operation for `device`.
    pub fn report_success(&self, device: &str) {
        let mut state = self.state.lock().expect("backpressure state");
        let status = state.entry(device.to_string()).or_default();
        decay(status, self.config);

        status.score = (status.score - 0.5).max(0.0);
        status.recovery_streak = status.recovery_streak.saturating_add(1);

        if status.recovery_streak >= self.config.recover_threshold {
            status.pressure_streak = status.pressure_streak.saturating_sub(1);
            status.recovery_streak = 0;
        }
    }
}

fn decay(state: &mut DeviceState, config: BackpressureConfig) {
    let now = Instant::now();
    let elapsed = now.saturating_duration_since(state.last_decay);

    if elapsed.is_zero() || config.decay_half_life.is_zero() || state.score <= 0.0 {
        state.last_decay = now;
        return;
    }

    let half_life = config.decay_half_life.as_secs_f32();
    let t = elapsed.as_secs_f32() / half_life;
    let factor = 2f32.powf(-t);
    state.score *= factor;
    state.last_decay = now;
}

impl Default for BackpressureManager {
    fn default() -> Self {
        Self::new(BackpressureConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, BackpressureConfig, BackpressureManager, PressureSignal};
    use std::time::Duration;

    #[test]
    fn blocks_after_pressure_threshold() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 2,
            recover_threshold: 2,
            score_threshold: 2.0,
            max_inflight: 64,
            decay_half_life: Duration::from_secs(1),
        });

        assert_eq!(mgr.admit("gpu0"), Admission::Allow);
        mgr.report_pressure("gpu0");
        assert_eq!(mgr.admit("gpu0"), Admission::Allow);
        mgr.report_pressure("gpu0");
        assert_eq!(mgr.admit("gpu0"), Admission::Backpressure);
    }

    #[test]
    fn recovers_with_success_signals() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 1,
            recover_threshold: 1,
            score_threshold: 1.0,
            max_inflight: 64,
            decay_half_life: Duration::from_secs(1),
        });

        mgr.report_pressure("gpu0");
        assert_eq!(mgr.admit("gpu0"), Admission::Backpressure);

        mgr.report_success("gpu0");
        assert_eq!(mgr.admit("gpu0"), Admission::Allow);
    }

    #[test]
    fn pressure_isolated_per_device() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 1,
            recover_threshold: 1,
            score_threshold: 1.0,
            max_inflight: 64,
            decay_half_life: Duration::from_secs(1),
        });

        mgr.report_pressure("gpu0");

        assert_eq!(mgr.admit("gpu0"), Admission::Backpressure);
        assert_eq!(mgr.admit("gpu1"), Admission::Allow);
    }

    #[test]
    fn inflight_budget_enforced() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 100,
            recover_threshold: 1,
            score_threshold: 100.0,
            max_inflight: 1,
            decay_half_life: Duration::from_secs(1),
        });

        assert!(mgr.try_acquire("gpu0"));
        assert!(!mgr.try_acquire("gpu0"));
        mgr.release("gpu0");
        assert!(mgr.try_acquire("gpu0"));
    }

    #[test]
    fn weighted_signals_affect_admission() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 100,
            recover_threshold: 1,
            score_threshold: 1.0,
            max_inflight: 64,
            decay_half_life: Duration::from_secs(10),
        });

        mgr.report_signal("gpu0", PressureSignal::TransferFailure);
        assert_eq!(mgr.admit("gpu0"), Admission::Allow);

        mgr.report_signal("gpu0", PressureSignal::EnqueueFailure);
        assert_eq!(mgr.admit("gpu0"), Admission::Backpressure);
    }

    #[test]
    fn score_decay_recovers_admission() {
        let mgr = BackpressureManager::new(BackpressureConfig {
            hard_threshold: 100,
            recover_threshold: 100,
            score_threshold: 0.3,
            max_inflight: 64,
            decay_half_life: Duration::from_millis(20),
        });

        mgr.report_signal("gpu0", PressureSignal::Saturation);
        assert_eq!(mgr.admit("gpu0"), Admission::Backpressure);

        std::thread::sleep(Duration::from_millis(120));
        assert_eq!(mgr.admit("gpu0"), Admission::Allow);
    }
}
