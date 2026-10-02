"""CRI per-container resource exporter.


Behavior contract implemented here:
- poll CRI ``RuntimeService.ContainerStats`` per target container at a bounded
  fixed tick (default 2 s), one RPC attempt per target per tick;
- expose cumulative CPU (``usageCoreNanoSeconds``) and memory fields as gauges
  carrying the CRI **source** timestamp as the explicit Prometheus timestamp;
- record ``usageNanoCores`` / ``writableLayer`` under distinct ``*_cached``
  metric names with their own source timestamps; they are NOT certified fresh
  (statsCollector / snapshot caches) and must not be presented as 2 s rates;
- no interpolation, no zero fill, no rate computation anywhere;
- missing fields stay missing, missed ticks stay missing, errors are counted
  and surfaced, never silently retried into nicer-looking data;
- ``cri_exporter_target_identity_changes_total`` is exporter-owned state (not
  a CRI field): it is emitted for every configured target from process start
  (0 before any change) and counts resolved-id changes per the change-tracking rule implemented in ``collector.py``. The no-zero-fill rule above governs
  CRI-sourced container gauges, not this counter;
- duplicate targets (two specs resolving to the same exposition key, however
  spelled - including two distinct ids sharing the 12-hex prefix) are refused
  at startup : two states on one key would emit duplicate series while
  the losing state inflates ``cri_exporter_stale_targets`` despite successful
  collection, and silently deduplicating would mask the configuration error.

"""

__version__ = "0.3.0-m1-cri-exporter"
