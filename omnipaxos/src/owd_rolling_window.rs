use std::collections::VecDeque;

pub(crate) struct OwdRollingWindowConfig {
    pub(crate) max_size: usize,
    pub(crate) min_size: usize,
    pub(crate) percentile: f64,
    pub(crate) recompute_interval: usize,
}

impl Default for OwdRollingWindowConfig {
    fn default() -> Self {
        Self {
            max_size: 300,
            min_size: 20,
            percentile: 0.95,
            recompute_interval: 20,
        }
    }
}

pub(crate) struct OwdRollingWindow {
    window: VecDeque<u64>,
    max_size: usize,
    min_size: usize,
    percentile: f64,
    recompute_interval: usize,
    cached_percentile_value: Option<u64>,
    pushes_since_recompute: usize,
}

impl OwdRollingWindow {

    pub(crate) fn with(config: OwdRollingWindowConfig) -> Self {
        Self {
            window: VecDeque::new(),
            max_size: config.max_size,
            min_size: config.min_size,
            percentile: config.percentile,
            recompute_interval: config.recompute_interval,
            pushes_since_recompute: 0,
            cached_percentile_value: None,
        }
    }

    pub(crate) fn push(&mut self, value: u64) {
        if self.window.len() >= self.max_size {
            self.window.pop_front();
        }

        self.window.push_back(value);
        self.pushes_since_recompute += 1;

        if self.pushes_since_recompute >= self.recompute_interval ||
            (self.window.len() >= self.min_size && self.cached_percentile_value.is_none()) {
            self.recompute_percentile();
            self.pushes_since_recompute = 0;
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.window.len()
    }

    pub(crate) fn percentile_value(&self) -> Option<u64> {
        self.cached_percentile_value
    }

    pub(crate) fn recompute_percentile(&mut self) {
        if self.window.len() < self.min_size {
            self.cached_percentile_value = None;
            return;
        }

        let mut v: Vec<_> = self.window.iter().copied().collect();
        v.sort_unstable();

        let idx = ((v.len() - 1) as f64 * self.percentile).round() as usize;
        self.cached_percentile_value = Some(v[idx]);
    }
}

impl Default for OwdRollingWindow {
    fn default() -> Self {
        Self::with(OwdRollingWindowConfig::default())
    }
}
