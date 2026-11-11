use std::io;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::Duration;

use self::priority::PriorityMap;
use super::dir::DirLock;
use super::file::{CachedFile, FileLock, FileSave};
use super::Result;

mod priority;

// Default to 512 concurrent file operations per cache to give bulk reads and
// writeback room to overlap. Callers can lower the limit on smaller hosts.
const MAX_FILE_HANDLES: usize = 512;

struct State<FE> {
    files: PriorityMap<CachedFile<FE>>,
    size: usize,
    roots: Vec<PathBuf>,
    gc_pending: bool,
    requested: usize,
    eviction_error: Option<io::Error>,
}

impl<FE> State<FE> {
    fn new() -> Self {
        Self {
            size: 0,
            files: PriorityMap::new(),
            roots: Vec::new(),
            gc_pending: false,
            requested: 0,
            eviction_error: None,
        }
    }
}

#[derive(Debug)]
struct Evict;

/// An in-memory cache layer over [`tokio::fs`] with one-position promotion per hit and eviction from the back.
pub struct Cache<FE> {
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
    /// The configured concurrency limit for file operations and lifecycle work.
    pub(crate) fn max_file_handles(&self) -> usize {
        self.max_file_handles
    }

    fn request_cleanup(&self, state: &mut State<FE>) {
        state.gc_pending = true;
        match self.tx.try_send(Evict) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Closed(_)) => {
                state.gc_pending = false;
                state.eviction_error = Some(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "cache cleanup task stopped",
                ));
                self.capacity_released.notify_waiters();
            }
        }
    }

    #[inline]
    fn check(&self, mut state: MutexGuard<State<FE>>) {
        if !state.gc_pending
            && state.eviction_error.is_none()
            && (state.size > self.capacity || state.requested > 0)
        {
            self.request_cleanup(&mut state);
        }
    }

    fn released(&self) {
        let mut state = self.lock();
        // A release can race an active pass's scan. The bounded channel retains
        // one rescan, rather than waking admission after a no-progress pass.
        if state.eviction_error.is_none() && (state.size > self.capacity || state.requested > 0) {
            self.request_cleanup(&mut state);
        }
        drop(state);
        self.capacity_released.notify_waiters();
    }

    pub(crate) fn on_release(&self) -> BorrowedRelease<'_>
    where
        FE: Send + Sync,
    {
        Release(self)
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, State<FE>> {
        self.state.lock().expect("file cache state")
    }

    pub(crate) fn bump(&self, path: &PathBuf) {
        let mut state = self.lock();
        state.files.bump(path);
        self.check(state);
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

        let mut state = self.lock();
        if let Some(cause) = state.eviction_error.take() {
            return Err(cause);
        }

        if state
            .size
            .checked_add(bytes)
            .is_none_or(|size| size > self.capacity)
        {
            state.requested = state.requested.max(bytes);
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
        // The writer's release field signals after its native guards drop.
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
        // Ready cached reads must not cooperatively yield with exclusive state.
        match self.try_acquire_file_handle() {
            Ok(permit) => return Ok(permit),
            Err(error) if error.kind() == io::ErrorKind::ResourceBusy => {}
            Err(error) => return Err(error),
        }

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
    /// `max_file_handles` bounds active file operations, including dirty eviction,
    /// using one shared semaphore. Cached guards pin memory without I/O admission.
    /// If not specified, `max_file_handles` will default to 512.
    ///
    /// `minimum_free_disk_bytes` checks current filesystem free space before a
    /// write. It is not an encoded-size estimate or a reservation for that write.
    ///
    /// Native construction requires an active Tokio runtime and a positive
    /// `max_file_handles` no larger than `tokio::sync::Semaphore::MAX_PERMITS`.
    /// Invalid native configuration panics; hosts accepting configuration from
    /// untrusted callers must validate it before construction.
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
            let mut state = self.lock();

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

            // The same claim excludes overlap during loading and after publication.
            state.roots.push(path.clone());
        }

        #[cfg(test)]
        tests::root_claimed();

        match DirLock::load(self.clone(), path.clone()) {
            Ok(dir) => Ok(dir),
            Err(cause) => {
                // Loading discards unpublished file registrations before returning.
                self.lock().roots.retain(|root| root != &path);
                Err(cause)
            }
        }
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
            // Bound prepared work independently of I/O admission: clean entries
            // remain reclaimable even while every I/O permit is occupied.
            if evictions.len() >= self.max_file_handles() {
                break;
            }

            if let Some((size, eviction)) = file.evict(self) {
                over = over.saturating_sub(size);
                evictions.push(eviction);
            }

            if over == 0 {
                break;
            }
        }

        evictions
    }
}

// Only borrowed read guards erase the entry type, preserving their public
// signature. Both borrowed and owned release fields drop after native guards;
// notification itself allocates nothing and acquires no resource.
pub(crate) trait Released {
    fn released(&self);
}

impl<FE> Released for Cache<FE> {
    fn released(&self) {
        Cache::released(self);
    }
}

pub(crate) struct Release<C: Deref>(pub(crate) C)
where
    C::Target: Released;

pub(crate) type BorrowedRelease<'a> = Release<&'a (dyn Released + Sync)>;
pub(crate) type OwnedRelease<FE> = Release<Arc<Cache<FE>>>;

impl<C: Deref> Drop for Release<C>
where
    C::Target: Released,
{
    fn drop(&mut self) {
        self.0.released();
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
    #[cfg(test)]
    let observation = tests::cleanup_observer();

    tokio::spawn(async move {
        while let Some(Evict) = rx.recv().await {
            let Some(cache) = cache.upgrade() else {
                break;
            };
            let requested = {
                let mut state = cache.lock();
                if state.eviction_error.is_some() {
                    state.gc_pending = false;
                    continue;
                }
                state.gc_pending = true;
                std::mem::take(&mut state.requested)
            };

            let mut evictions = cache.gc_for(requested);
            let mut reclaimed = false;
            let mut failure = None;

            while let Some(result) = evictions.next().await {
                match result {
                    Ok(()) => reclaimed = true,
                    Err(cause) => {
                        #[cfg(feature = "logging")]
                        log::error!("failed to evict cached file: {cause}");
                        if failure.is_none() {
                            failure = Some(cause);
                        }
                    }
                }
            }

            let mut state = cache.lock();
            state.gc_pending = false;
            if let Some(cause) = failure {
                state.eviction_error = Some(cause);
                drop(state);
                cache.capacity_released.notify_waiters();
            } else if reclaimed || state.requested > 0 {
                cache.check(state);
            } else {
                drop(state);
            }

            #[cfg(test)]
            tests::cleanup_pass_finished(observation.as_deref());
        }
    })
}

#[cfg(test)]
#[path = "../tests/unit/cache.rs"]
pub(crate) mod tests;
