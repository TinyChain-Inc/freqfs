# freqfs
An in-memory filesystem cache layer based on tokio::fs, with least-frequently-used eviction

Roadmap and planning notes are tracked in `ROADMAP.md`.

`sync()` writes cached changes to the filesystem without forcing durability.
Dirty eviction uses the same buffered writeback. Neither acknowledges survival
of power loss.

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

For publication records, use `FileLock::replace_all(value, size_hint)` instead of a
cached write followed by synchronization. As with file creation, the caller supplies
the retained size; cache capacity is admitted before publication begins.
This operation excludes eviction, synchronizes
the temporary replacement before rename, and then synchronizes its parent. An
error or cancellation after publication begins makes that handle unusable until
reopening through a new cache; it does not promise rollback. Synchronization errors propagate, including
unsupported directory barriers. These are per-file durability primitives, not a
transaction protocol or a multi-file recovery guarantee.

Run `cargo test --all-targets --all-features` for the cache and durability tests.
On Linux, compile the unit tests with `cargo test --lib --all-features --no-run`
and use the printed test executable to check syscall failures:

```sh
strace -f -e inject=fsync:error=EIO:when=1 TEST_EXECUTABLE durable_replacement_syscall_failure_requires_reopen --ignored
strace -f -e inject=fsync:error=EIO:when=2 TEST_EXECUTABLE durable_replacement_syscall_failure_requires_reopen --ignored
```

These exercise failure before rename and failure to durably publish the rename.
They are syscall-failure tests, not simulated power-loss tests.

Memory-mapped file I/O support is deferred planning described in `ROADMAP.md`.
Any implementation must satisfy this repository's atomicity, backpressure,
portability, and performance gates.
