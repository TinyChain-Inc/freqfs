# freqfs
An in-memory filesystem cache layer based on tokio::fs, with least-frequently-used eviction

Roadmap and planning notes are tracked in `ROADMAP.md`.

Memory-mapped file I/O support is intentionally deferred and documented as planning-only in `ROADMAP.md` under "Deferred deep-dive: memory-mapped file I/O". Do not implement this optimization until the listed TinyChain ecosystem validation preconditions and test gates are satisfied.
