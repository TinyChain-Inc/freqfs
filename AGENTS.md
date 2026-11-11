# freqfs Agent Notes

- `freqfs` owns cache-size and active filesystem-I/O backpressure. Cached guards
  pin memory, not file-operation permits or OS descriptors. A bootstrap-created
  `Cache` accounts actual retained bytes, bounds active file work, and owns
  one finite, coalescing eviction signal; do not introduce score/decay heuristics,
  process-global managers, or unbounded eviction queues.
- Decode or clone once, measure actual retained size, and admit complete values
  before publication. Mutable guards account actual size on release. Temporary
  decoded/cloned values and mutation growth can exceed the cache target; it is
  not a strict allocation ceiling or host OOM guarantee. Preserve saturation and I/O errors
  for callers; do not panic on ordinary disk-full/resource exhaustion or hide it
  behind retries, alternate roots, or eager memory buffering.
- Keep reads, writes, and eviction incremental and explicitly bounded. Cancellation
  or drop must release file handles, in-flight permits, and pending cleanup state.
- After a root is loaded, mutate it only through `freqfs` handles so accounting,
  cache state, and filesystem state remain coherent.
- Directories own pending native removals. Deleted file handles reject public
  synchronization; only directory cleanup may remove their native names.
- Borrowed guards borrow their cache; owned guards retain it. Release native
  guards before notifying cache waiters.

- Keep unit tests, fixtures, observation state, and test-only helpers under
  `tests/unit/`, included through conditional module declarations in `src`.
  Fixtures must not add fields or test APIs to production types.

- Poisoned bookkeeping is an invariant failure and panics. Ordinary I/O errors
  remain fallible; the application owns panic strategy and recovery policy.
