# freqfs
An in-memory filesystem cache layer based on tokio::fs, with least-frequently-used eviction

Roadmap and planning notes are tracked in `ROADMAP.md`.

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
directory levels. A file modified during synchronization returns an error rather than
acknowledging unsynchronized contents.

`Dir::sync_deleted()` (also available on `DirLock`) applies pending deletions and
durably synchronizes their containing directory. It retains that directory and
does not write back or synchronize surviving children. Use this for reclamation
after the owning durability protocol has published the removal of references.

For publication records, use `FileLock::replace_all(value, retained_bound)` instead of a
cached write followed by synchronization. As with file creation, the caller supplies
a retained-allocation bound; cache capacity is admitted before publication begins.
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

`FileLoad::load_size` inspects encoded data with bounded scratch and reports an upper
bound for the decoded allocation before `load` runs. The cache reserves this bound,
rewinds the file, decodes, validates `GetSize`, and immediately refunds unused
capacity. `FileSave` reports bytes written; those bytes never replace the cache's
retained-memory charge. Adapters own both codec-specific size inspection and actual
capacity accounting, including spare capacity in vectors and other containers.

The configured minimum free-disk threshold checks current filesystem free space
before writeback. It is separate from retained-memory admission and neither
estimates encoded size nor reserves space for future writes. Native write errors
propagate to the caller.

Mutable file methods take a retained bound. The existing allocation remains
admitted, so `write(0)` permits mutation without growth. A write guard's
`reserve(bound)` admits additional capacity before the caller allocates it. Guard
drop reconciles the actual charge. A payload exceeding the admitted bound invalidates
the file and is discarded; later operations fail until caller-coordinated reopening.
Creation and replacement likewise validate supplied bounds. Creation requires an
absent name and returns `AlreadyExists` without changing existing handles; mutate
or replace an existing payload through its file handle. `Dir::create_empty_file`
derives the actual retained size and uses the same asynchronous admission,
awaiting reclaimable cache space before publishing the entry. Cancellation before
admission publishes no entry; unavailable pinned capacity still returns a bounded
admission error.

The eviction worker keeps a weak cache reference while idle. Cache entries retain
payload state without retaining their owning cache; owned guards keep the cache
alive for exactly their lifetime. Eviction failures retain the original I/O error
kind and message for the next admission attempt.
