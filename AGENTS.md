# freqfs Agent Notes

- `freqfs` owns cache-size, file-handle, and filesystem-I/O backpressure. A
  bootstrap-created `Cache` accounts actual retained bytes and handles and owns
  one finite, coalescing eviction signal; do not introduce score/decay heuristics,
  process-global managers, or unbounded eviction queues.
- Admit work before allocating or spawning it. Preserve saturation and I/O errors
  for callers; do not panic on ordinary disk-full/resource exhaustion or hide it
  behind retries, alternate roots, or eager memory buffering.
- Keep reads, writes, and eviction incremental and explicitly bounded. Cancellation
  or drop must release file handles, in-flight permits, and pending cleanup state.
- After a root is loaded, mutate it only through `freqfs` handles so accounting,
  cache state, and filesystem state remain coherent.
