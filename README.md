# freqfs
An in-memory filesystem cache layer based on tokio::fs, with one-position cache promotion per hit

Roadmap and planning notes are tracked in `ROADMAP.md`.

Cache entries start at the front. Each hit moves an entry one position toward
the front; eviction scans from the back. Removal preserves the order of the
remaining entries. This policy is neither exact LFU nor LRU.
The private cache priority map owns this ordering; it is not a public collection
API. Its borrowed cursors do not retain keys, and mutation uses exclusive access
under the existing cache mutex. The implementation is adapted from ds-ext under
the Apache-2.0 license and requires Rust 1.86 or newer for disjoint map access.

`sync()` writes cached changes to the filesystem without forcing durability.
Dirty eviction uses the same buffered writeback. Neither acknowledges survival
of power loss. Directory synchronization preserves existing empty directories and
materializes declared empty descendants. Only explicit deletion removes them;
`truncate_and_sync` explicitly removes its root.

`sync_all()` explicitly synchronizes file contents and directory publication,
including writes already evicted from memory. Directory calls batch publication
barriers across their subtree. Every explicit call synchronizes the selected
file contents, including unchanged files. No separate durability-dirty state is
maintained: the persistence owner selects when and what to synchronize.
The caller supplies durably established cache roots and coordinates concurrent
mutations. Synchronize from the containing root when publishing multiple new
directory levels. Durable file synchronization retains exclusive state access
through writeback and the durability barriers; concurrent writers wait.

`Dir::sync_deleted()` (also available on `DirLock`) applies pending deletions and
durably synchronizes their containing directory. It retains that directory and
does not write back or synchronize surviving children. Use this for reclamation
after the owning durability protocol has published the removal of references.

For publication records, use `FileLock::replace_all(value)` instead of a
cached write followed by synchronization. As with file creation, freqfs measures
the complete value and admits its actual retained size before publication begins.
The final cache charge is the value's actual `GetSize`, independent of encoded bytes.
This operation excludes eviction, synchronizes
the temporary replacement before rename, and then synchronizes its parent. An
error or cancellation after publication begins makes that handle unusable until
reopening through a new cache; it does not promise rollback. Synchronization errors propagate, including
unsupported directory barriers. These are per-file durability primitives, not a
transaction protocol or a multi-file recovery guarantee.

Filename extensions `_freqfs` and extensions ending in `__freqfs` are reserved
for replacement temporaries. Loading hides abandoned replacements from the
logical directory and retains them as pending deletions. Coordinated cleanup
uses the existing writeback or `sync_deleted()` path; loading itself deletes
nothing and never treats a temporary file as the publication record.

Run `cargo test --all-targets --all-features` for the cache and durability tests.
The syscall-failure test requires Linux, `strace` with syscall injection support,
and permission to trace child processes. The ordinary test driver runs the
replacement probe under injected `fsync` failures at the temporary-file and
parent-directory barriers, verifies the actual syscall trace, and fails if any
prerequisite is unavailable. These are syscall-failure tests, not simulated
power-loss tests.

Memory-mapped file I/O support is deferred planning described in `ROADMAP.md`.
Any implementation must satisfy this repository's atomicity, backpressure,
portability, and performance gates.

## Directory names

Directory lookup and deletion borrow exact textual names as `&str`. Their
lexical comparison matches the directory index: `"2"` and `"02"` are distinct,
and UUID spelling is not normalized. Borrow existing strings or IDs directly;
format typed identifiers into caller-owned stack buffers before lookup. Only
insertion needs an owned name. Existing filenames are unchanged.

Creation and copying accept one native filename component, never a path. Empty
names, `.`, `..`, absolute paths, separators, and NUL bytes reject with
`InvalidInput` before publication or mutation. File replacement temporary names
retain their existing reservation.

Directories use the standard ordered map for membership and pending deletions.
File creation and copying reject directory destinations, including directories
awaiting deletion, with `AlreadyExists`. Complete deletion with `sync()` or
`sync_deleted()` before reusing a directory name for a file. Copying over an
existing file remains supported; rejected copies preserve pending cleanup.

The containing directory owns pending native removals until synchronization
succeeds. `truncate()` retains that directory while removing its entries;
synchronization completes their native removal before reopening. Deleted file
handles return `NotFound` from `sync()` and `sync_all()`, including after their
name is reused. Retaining an old handle grants no authority over its replacement.
Cleanup tolerates already-absent files on retry and preserves integrity errors
from invalidated file ownership.

Deleted directory handles retain metadata access but cannot create, copy,
synchronize, or trim entries: these operations return `NotFound`, including
after the same name is reused. Removing a directory invalidates its descendants.
Ordinary truncation leaves the containing directory usable;
`truncate_and_sync` removes it and invalidates that handle.

Directory copying never waits for a conflicting source lock while holding its
destination. It returns `WouldBlock` instead. Copying into the source's own
subtree rejects before mutation. A later failure may leave a partial destination;
the caller retains responsibility for interrupted mutations.

`Cache::load` claims its root before scanning metadata. Concurrent overlapping
loads are rejected while unrelated roots can load independently. A returned load
error discards unpublished cache entries and releases the claim for retry.
Root-path comparison remains textual; callers coordinate path aliases and
separate cache instances.

`Dir::max_file_handles()` exposes the configured cache-wide admission limit.
File and directory creation and copying share the filename validator, including
the reservation of writeback temporary names. Rejection precedes admission or
source locking. Exact lookup and startup inspection of existing entries are
unchanged.
Directory truncation admits at most that many child futures per pass. This
bounds pending lifecycle work, not total memory across concurrent callers.
Loading, copying, writeback, and synchronization share one file-operation
semaphore. Cached read/write guards pin memory through their locks, not OS file
descriptors or I/O capacity. Clean eviction needs no I/O permit; prepared
evictions remain bounded by the configured limit. Completed load I/O releases
admission before waiting for memory capacity and returns a cached guard without
reacquiring it. Replacement and resident-copy memory waits likewise hold no
file-operation permit, allowing eviction to make progress.
Cancellation releases guards and permits but can leave partially applied changes;
the caller owns cleanup and safe reuse after interrupted mutations.

## Filesystem codecs

File entries implement `freqfs::FileLoad` and `FileSave`. Loads reconstruct the
same entry type that saves write; typed access validates the resulting entry via
`AsType`. Adapters must preserve payload identity across persistence rather than
reinterpret bytes as whichever type a reader requests.

Codec selection belongs entirely to the calling code. freqfs has no `stream`
feature, codec dependencies, or blanket `FileLoad`/`FileSave` implementations.
Implement these traits explicitly for the entry type, streaming bytes through
the codec you choose. Tests and examples demonstrate caller-owned TBON adapters.

## Retained allocation and admission

`FileLoad::load` decodes once under the existing file-state lock and file-handle
permit. The cache measures the complete entry with `GetSize`, admits that actual
size, then publishes it. Resident copying likewise measures the completed clone
before admission and replacement. Failed or cancelled admission drops the temporary
value without publishing it. `FileSave` reports bytes written; those bytes never
replace the retained-memory charge, which includes spare container capacity.

Decoded and cloned values awaiting admission already occupy memory outside the
cache target. File-handle permits bound concurrent operations, not their combined
bytes. Adapters retain structural decoding limits; cache admission does not
provide a decode-time memory ceiling or guarantee against host OOM.

The configured minimum free-disk threshold checks current filesystem free space
before writeback. It is separate from retained-memory admission and neither
estimates encoded size nor reserves space for future writes. Native write errors
propagate to the caller.

Mutable file guards account actual retained size when released, including unused
container capacity. Growth may temporarily exceed the cache target; it does not
invalidate or discard the payload. Eviction reclaims eligible entries and subsequent
admissions apply backpressure. Concurrent mutations and caller-owned buffers are
not bounded by the cache target, which is not a host OOM guarantee.

Creation and durable replacement measure complete entries and admit that actual
size before publication. Creation requires an absent name and returns `AlreadyExists`
without changing existing handles. Cancellation before admission publishes nothing.
An entry grown beyond total cache capacity can be synchronized and evicted.
Reloading requires the actual decoded size to fit the budget; serialization may
discard unused in-memory container capacity.

The eviction worker keeps a weak cache reference while idle. Cache entries retain
payload state without retaining their owning cache; owned guards keep the cache
alive for exactly their lifetime. Borrowed write guards borrow their cache and
do not increment its reference count. Eviction failures retain the original I/O error
and its typed causes for the next admission attempt. Uncached copies enforce the
same free-disk threshold as writeback. Save and native filesystem errors propagate
without conversion to diagnostic strings, preserving local integrity provenance.

An empty eviction pass does not repeatedly wake admission. Releasing capacity,
guards, or permits signals progress after the relevant resources are released.
A failed pass stops automatic retry and retains its error; a later admission
reports that error, and fresh demand can request another pass. Cache pressure,
pending cleanup, and retained errors share the existing bookkeeping mutex.

Unsupported filesystem entries return `InvalidData`; loading never treats them
as files or silently skips them. Poisoned cache bookkeeping is an invariant failure
and panics; it is not an ordinary I/O error. Callers must stop using the cache after such a failure.
The application owns termination and recovery policy, including the panic
strategy of its tasks. This library does not explicitly terminate the host.

### Storage integrity errors

`IntegrityError` preserves native I/O kinds and causes while identifying detected
stored-data inconsistency or invalidated file ownership. Libraries return these
errors; their caller decides whether to stop the application and recover. Input
validation and ordinary I/O failures do not acquire this marker. Poisoned
bookkeeping remains an invariant panic, under the application's panic strategy.
