pub const DEFAULT_HOT_ALLOCATION: f64 = 0.97;
pub const DEFAULT_GHOST_ALLOCATION: f64 = 0.5;

/// Cache options. Built with [OptionsBuilder].
#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) shards: usize,
    pub(crate) hot_allocation: f64,
    pub(crate) ghost_allocation: f64,
    pub(crate) estimated_items_capacity: usize,
    pub(crate) weight_capacity: u64,
}

/// Builder for [Options].
///
/// # Example
///
/// ```rust
/// use quick_cache::{sync::{Cache, DefaultLifecycle}, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
///
/// Cache::<String, String>::with_options(
///   OptionsBuilder::new()
///     .estimated_items_capacity(10000)
///     .weight_capacity(10000)
///     .build()
///     .unwrap(),
///     UnitWeighter,
///     DefaultHashBuilder::default(),
///     DefaultLifecycle::default(),
/// );
/// ```
#[derive(Debug, Clone, Default)]
pub struct OptionsBuilder {
    shards: Option<usize>,
    hot_allocation: Option<f64>,
    ghost_allocation: Option<f64>,
    estimated_items_capacity: Option<usize>,
    weight_capacity: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Error(&'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {}

impl OptionsBuilder {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of internal shards. Each shard has independent synchronization
    /// and capacity. This means that the Cache can be used from multiple threads
    /// with little contetion but the capacity of each shard is a portion of the total.
    ///
    /// Defaults to: `number of detected cores * 4`
    ///
    /// Note that this number isn't enforced and will be adjusted internally to
    /// the next power of two. Too small shards (depending on estimated capacity)
    /// may also cause the actual shard count to decrease.
    #[inline]
    pub fn shards(&mut self, shards: usize) -> &mut Self {
        self.shards = Some(shards);
        self
    }

    /// The estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    /// An estimation within one or even two orders of magnitude of the real value is often good enough.
    ///
    /// This is used to estimate the maximum number of shards (to avoid shards that are too small)
    /// and to estimate space required to track items recently evicted from the cache.
    #[inline]
    pub fn estimated_items_capacity(&mut self, estimated_items_capacity: usize) -> &mut Self {
        self.estimated_items_capacity = Some(estimated_items_capacity);
        self
    }

    /// The max weight that the cache can hold.
    #[inline]
    pub fn weight_capacity(&mut self, weight_capacity: u64) -> &mut Self {
        self.weight_capacity = Some(weight_capacity);
        self
    }

    /// What percentage `[0..=1.0]` of the cache space to reserve for "hot" items.
    /// If your workload exhibit heavy bias towards recency instead of frequency try
    /// lowering this setting. In practice the useful ranges are between 50% to 99%
    /// (usually on the higher side).
    ///
    /// Defaults to: `0.97` (97%).
    #[inline]
    pub fn hot_allocation(&mut self, hot_allocation: f64) -> &mut Self {
        assert!(
            hot_allocation.clamp(0.0, 1.0) == hot_allocation,
            "hot_allocation must be within [0, 1]"
        );
        self.hot_allocation = Some(hot_allocation);
        self
    }

    /// The cache optimistically tracks recently seen keys that are not resident
    /// in the cache. These keys are called ghost keys. If a ghost key is seen
    /// again items it will be admitted as "hot".
    /// The ghost allocation percentage defines how much space to allocate for
    /// the ghost keys considering the `estimated_items_capacity`.
    ///
    /// Defaults to: `0.5` (50%).
    #[inline]
    pub fn ghost_allocation(&mut self, ghost_allocation: f64) -> &mut Self {
        assert!(
            ghost_allocation.clamp(0.0, 1.0) == ghost_allocation,
            "ghost_allocation must be within [0, 1]"
        );
        self.ghost_allocation = Some(ghost_allocation);
        self
    }

    /// Builds an `Option` struct which can be used in the `Cache::with_options` constructor.
    #[inline]
    pub fn build(&self) -> Result<Options, Error> {
        let shards = self.shards.unwrap_or_else(|| available_parallelism() * 4);
        let hot_allocation = self.hot_allocation.unwrap_or(DEFAULT_HOT_ALLOCATION);
        let ghost_allocation = self.ghost_allocation.unwrap_or(DEFAULT_GHOST_ALLOCATION);
        let weight_capacity = self
            .weight_capacity
            .ok_or(Error("weight_capacity is not set"))?;
        let estimated_items_capacity = self
            .estimated_items_capacity
            .ok_or(Error("estimated_items_capacity is not set"))?;
        Ok(Options {
            shards,
            hot_allocation,
            ghost_allocation,
            estimated_items_capacity,
            weight_capacity,
        })
    }
}

/// Memoized wrapper for `std::thread::available_parallelism`, which can be incredibly slow.
fn available_parallelism() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static AVAILABLE_PARALLELISM: AtomicUsize = AtomicUsize::new(0);
    let mut ap = AVAILABLE_PARALLELISM.load(Ordering::Relaxed);
    if ap == 0 {
        ap = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        AVAILABLE_PARALLELISM.store(ap, Ordering::Relaxed);
    }
    ap
}
