# freqfs Roadmap

> **Non-normative:** this file tracks unimplemented work and cannot override
> this repository's implemented behavior or local contracts.

This roadmap captures planned work for `freqfs` as a cache/persistence primitive used across TinyChain services.

## Current focus

1. Preserve correctness-first behavior for cache eviction, persistence atomicity, and backpressure signaling.
2. Keep API semantics stable for downstream crates (`txfs`, `b-tree`, `b-table`, `fensor`, and related services).

## Deferred deep-dive: memory-mapped file I/O

Status: deferred planning only. Do not implement yet.

### Why defer now

Memory-mapped file I/O can reduce serialization and copy overhead for large
workloads, but it is a high-risk optimization. Existing persistence and cache
behavior should be fully covered before adding mmap-specific complexity.

### Preconditions before implementation begins

1. Existing `freqfs` atomic persistence guarantees are covered by integration tests.
2. Cache eviction, cancellation, and resource accounting have regression coverage.
3. Performance baselines are captured for the supported platforms.

### Planned design direction (future)

1. Candidate crate: `memmap2`.
2. Introduce mmap support behind an opt-in feature flag (e.g. `mmap`).
3. Preserve current `FileLoad`/`FileSave` semantics as the canonical fallback path.
4. Keep temp-file + rename atomicity semantics unchanged.
5. Apply size-threshold routing so mmap targets large payloads while small files remain on buffered I/O.

### Risk register

1. Cross-platform mapping differences (Linux/macOS/Windows behavior and limits).
2. Flush/sync ordering mistakes causing visibility/corruption issues.
3. Lock-lifetime hazards with mutable mappings.
4. Potential regressions for small files due to page-fault overhead.

### Required test gates before merge

1. Atomicity regression: failures during write never corrupt existing persisted files.
2. Concurrency visibility: readers observe either old or new full contents, never partial writes.
3. Eviction/reload parity: mmap on/off paths yield identical logical results.
4. Backpressure compatibility: cache eviction signaling and throttling semantics remain unchanged.
5. Cross-platform CI: Linux required first, then macOS and Windows before any default-on decision.
6. Benchmark gate: measurable copy/CPU reduction for large-file sync workloads versus baseline.

### Rollout plan

1. Phase A: feature-flagged experimental support for byte-oriented payloads.
2. Phase B: optional extensions for structured codec paths where safe.
3. Phase C: default-on decision only after reliability + performance evidence across supported platforms.
