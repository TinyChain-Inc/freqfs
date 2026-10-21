use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::Duration;

use super::dir::DirLock;
use super::file::{CachedFile, FileLock, FileSave};
use super::Result;

const MAX_FILE_HANDLES: usize = 512;

type Lfu<FE> = ds_ext::LinkedHashMap<PathBuf, CachedFile<FE>>;

struct State<FE> {
    files: Lfu<FE>,
    size: usize,
    roots: Vec<PathBuf>,
}

impl<FE> State<FE> {
    fn new() -> Self {
        Self {
            size: 0,
            files: Lfu::new(),
            roots: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct Evict;

/// An in-memory cache layer over [`tokio::fs`] with least-frequently-used (LFU) eviction.
pub struct Cache<FE> {
    gc_pending: AtomicBool,
    eviction_error: Mutex<Option<io::Error>>,
    requested: AtomicUsize,
    capacity: usize,
    max_file_handles: usize,
    minimum_free_disk_bytes: u64,
    file_handles: Arc<Semaphore>,
    capacity_released: Notify,
    handle_wait: Duration,
    state: Mutex<State<FE>>,
    tx: Sender<Evict>,
}

impl<FE> Cache<FE> {
    #[inline]
    fn check(&self, state: MutexGuard<State<FE>>) {
        if (state.size > self.capacity || self.requested.load(Ordering::Acquire) > 0)
            && self
                .gc_pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            match self.tx.try_send(Evict) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Closed(_)) => {
                    self.gc_pending.store(false, Ordering::Release);
                    *self.eviction_error.lock().expect("eviction error") = Some(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "cache cleanup task stopped",
                    ));
                    self.capacity_released.notify_waiters();
                }
            }
        }
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, State<FE>> {
        self.state.lock().expect("file cache state")
    }

    pub(crate) fn bump(&self, path: &PathBuf, file_size: Option<usize>) -> bool {
        let mut state = self.lock();

        if let Some(file_size) = file_size {
            state.size += file_size;
        }

        let exists = state.files.bump(path);
        self.check(state);
        exists
    }

    pub(crate) fn validate_size(&self, bytes: usize) -> Result<()> {
        if bytes > self.capacity {
            Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "retained file exceeds cache capacity",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation<FE>> {
        let deadline = tokio::time::Instant::now() + self.handle_wait;
        loop {
            // Register before attempting admission so a release cannot be missed.
            let notified = self.capacity_released.notified();
            if let Some(reservation) = self.reserve_if_available(bytes)? {
                return Ok(reservation);
            }

            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::ResourceBusy, "cache capacity is exhausted")
                })?;
        }
    }

    // None means capacity pressure; eviction errors retain their original kind.
    fn reserve_if_available(self: &Arc<Self>, bytes: usize) -> Result<Option<Reservation<FE>>> {
        self.validate_size(bytes)?;
        if let Some(cause) = self.eviction_error.lock().expect("eviction error").take() {
            return Err(cause);
        }

        let mut state = self.lock();
        if state
            .size
            .checked_add(bytes)
            .is_none_or(|size| size > self.capacity)
        {
            self.requested.fetch_max(bytes, Ordering::AcqRel);
            self.check(state);
            return Ok(None);
        }

        state.size += bytes;
        Ok(Some(Reservation {
            cache: Arc::clone(self),
            bytes,
            committed: false,
        }))
    }

    // Mutable payloads may exceed the cache target. Account them without discarding
    // contents; subsequent admissions apply backpressure while eviction catches up.
    pub(crate) fn reconcile(&self, previous: usize, actual: usize) {
        if actual == previous {
            return;
        }

        let mut state = self.lock();
        state.size = state.size - previous + actual;
        self.check(state);
        if actual < previous {
            self.capacity_released.notify_waiters();
        }
    }

    pub(crate) fn release(&self, bytes: usize) {
        if bytes > 0 {
            self.lock().size -= bytes;
            self.capacity_released.notify_waiters();
        }
    }

    pub(crate) fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.lock();
        state.files.insert(path, file.cached());
        state.size += file_size;

        self.check(state)
    }

    pub(crate) fn insert_reserved(
        &self,
        path: PathBuf,
        file: FileLock<FE>,
        reservation: Reservation<FE>,
    ) {
        let mut state = self.lock();
        state.files.insert(path, file.cached());
        reservation.commit();
    }

    pub(crate) fn remove(&self, path: &PathBuf, size: usize) {
        let mut state = self.lock();

        if state.files.remove(path).is_some() {
            state.size -= size;
            self.capacity_released.notify_waiters();
        }

        self.check(state)
    }

    pub(crate) async fn acquire_file_handle(&self) -> Result<OwnedSemaphorePermit> {
        tokio::time::timeout(
            self.handle_wait,
            Arc::clone(&self.file_handles).acquire_owned(),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::ResourceBusy, "file handle limit reached"))?
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "file handle admission closed"))
    }

    pub(crate) fn try_acquire_file_handle(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.file_handles)
            .try_acquire_owned()
            .map_err(|cause| match cause {
                TryAcquireError::NoPermits => {
                    io::Error::new(io::ErrorKind::ResourceBusy, "file handle limit reached")
                }
                TryAcquireError::Closed => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "file handle admission closed")
                }
            })
    }

    // Check the current filesystem floor before writeback. Encoded sizes are
    // adapter-owned, so this neither estimates nor reserves future disk space.
    pub(crate) fn ensure_disk_space(&self, path: &std::path::Path) -> Result<()> {
        let mut root = path.parent().unwrap_or(path);
        while !root.exists() {
            root = root.parent().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "no filesystem root for cache path")
            })?;
        }
        let available = fs2::available_space(root)?;
        if available < self.minimum_free_disk_bytes {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "filesystem free space is below the configured reserve of {} bytes",
                    self.minimum_free_disk_bytes
                ),
            ));
        }
        Ok(())
    }
}

impl<FE> Cache<FE>
where
    FE: FileSave,
{
    /// Initialize the cache.
    ///
    /// `max_file_handles` specifies how many files are allowed to be evicted at once.
    /// If not specified, `max_file_handles` will default to 512.
    ///
    /// `minimum_free_disk_bytes` checks current filesystem free space before a
    /// write. It is not an encoded-size estimate or a reservation for that write.
    ///
    /// Panics: if `max_file_handles` is `Some(0)`
    pub fn new(
        capacity: usize,
        max_file_handles: Option<usize>,
        minimum_free_disk_bytes: u64,
        handle_wait: Duration,
    ) -> Arc<Self> {
        let max_file_handles = max_file_handles.unwrap_or(MAX_FILE_HANDLES);

        assert!(
            max_file_handles > 0,
            "invalid config for max_file_handles: {}",
            max_file_handles
        );

        // Eviction requests are coalesced by the single in-flight GC permit.
        let (tx, rx) = mpsc::channel(1);
        let cache = Arc::new(Self {
            gc_pending: AtomicBool::new(false),
            eviction_error: Mutex::new(None),
            requested: AtomicUsize::new(0),
            capacity,
            max_file_handles,
            minimum_free_disk_bytes,
            file_handles: Arc::new(Semaphore::new(max_file_handles)),
            capacity_released: Notify::new(),
            handle_wait,
            state: Mutex::new(State::new()),
            tx,
        });

        spawn_cleanup_thread(Arc::downgrade(&cache), rx);

        cache
    }

    /// Load a filesystem directory into the cache.
    ///
    /// After loading, all interactions with files under this directory should go through
    /// a [`DirLock`] or [`FileLock`].
    pub fn load(self: Arc<Self>, path: PathBuf) -> Result<DirLock<FE>> {
        {
            let state = self.lock();

            for root in &state.roots {
                if root.starts_with(&path) || path.starts_with(root) {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "called Cache::load on a directory that's already loaded: {:?}",
                            path
                        ),
                    ));
                }
            }
        }

        let dir = DirLock::load(self.clone(), path.clone())?;

        let mut state = self.lock();
        state.roots.push(path);

        Ok(dir)
    }

    fn gc_for(
        self: &Arc<Self>,
        requested: usize,
    ) -> FuturesUnordered<impl Future<Output = Result<()>> + Send> {
        let evictions = FuturesUnordered::new();

        let state = self.lock();

        if state.size.saturating_add(requested) <= self.capacity {
            return evictions;
        }

        let mut over = state.size.saturating_add(requested) - self.capacity;

        for (_path, file) in state.files.iter().rev() {
            if let Some((size, eviction)) = file.with_cache(Arc::clone(self)).evict() {
                over = over.saturating_sub(size);
                evictions.push(eviction);
            }

            if over == 0 || evictions.len() >= self.max_file_handles {
                break;
            }
        }

        evictions
    }
}

pub(crate) struct Reservation<FE> {
    cache: Arc<Cache<FE>>,
    bytes: usize,
    committed: bool,
}

impl<FE> Reservation<FE> {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl<FE> Drop for Reservation<FE> {
    fn drop(&mut self) {
        if !self.committed {
            self.cache.release(self.bytes);
        }
    }
}

fn spawn_cleanup_thread<FE>(
    cache: Weak<Cache<FE>>,
    mut rx: Receiver<Evict>,
) -> tokio::task::JoinHandle<()>
where
    FE: FileSave,
{
    tokio::spawn(async move {
        while let Some(Evict) = rx.recv().await {
            let Some(cache) = cache.upgrade() else {
                break;
            };
            let requested = cache.requested.swap(0, Ordering::AcqRel);
            let mut evictions = cache.gc_for(requested);
            let mut evicted = false;

            while let Some(result) = evictions.next().await {
                evicted = true;
                match result {
                    Ok(()) => {}
                    Err(cause) => {
                        #[cfg(feature = "logging")]
                        log::error!("failed to evict cached file: {cause}");
                        let mut error = cache.eviction_error.lock().expect("eviction error");
                        if error.is_none() {
                            *error = Some(cause);
                        }
                    }
                }
            }

            cache.gc_pending.store(false, Ordering::Release);
            cache.capacity_released.notify_waiters();
            if evicted {
                cache.check(cache.lock());
            }
        }
    })
}

#[cfg(test)]
#[path = "../tests/unit/cache.rs"]
mod tests;
