use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender};
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Duration;

use super::dir::DirLock;
use super::file::{FileLock, FileSave};
use super::Result;

const GC_CYCLE_TIME: Duration = Duration::from_millis(10);
const MAX_FILE_HANDLES: usize = 512;

type Lfu<FE> = ds_ext::LinkedHashMap<PathBuf, FileLock<FE>>;

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
    eviction_failed: AtomicBool,
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
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

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
                    panic!("cache cleanup thread");
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

    pub(crate) async fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation<FE>> {
        if bytes > self.capacity {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "retained file exceeds cache capacity",
            ));
        }

        let deadline = tokio::time::Instant::now() + self.handle_wait;
        loop {
            if self.eviction_failed.swap(false, Ordering::AcqRel) {
                return Err(io::Error::other("cache eviction failed"));
            }
            let notified = self.capacity_released.notified();
            {
                let mut state = self.lock();
                if state.size.saturating_add(bytes) <= self.capacity {
                    state.size += bytes;
                    return Ok(Reservation {
                        cache: Arc::clone(self),
                        bytes,
                        committed: false,
                    });
                }
            }

            self.requested.fetch_max(bytes, Ordering::AcqRel);
            self.check(self.lock());

            tokio::time::timeout_at(deadline, notified)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::ResourceBusy, "cache capacity is exhausted")
                })?;
        }
    }

    pub(crate) fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.lock();
        state.files.insert(path, file);
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
        state.files.insert(path, file);
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

    pub(crate) async fn resize(self: &Arc<Self>, old_size: usize, new_size: usize) -> Result<()> {
        if new_size > old_size {
            self.reserve(new_size - old_size).await?.commit();
        } else if old_size > new_size {
            let mut state = self.lock();
            state.size -= old_size - new_size;
            self.capacity_released.notify_waiters();
        }

        Ok(())
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
            .map_err(|_| io::Error::new(io::ErrorKind::ResourceBusy, "file handle limit reached"))
    }

    pub(crate) fn ensure_disk_capacity(&self, path: &std::path::Path, bytes: u64) -> Result<()> {
        let mut root = path.parent().unwrap_or(path);
        while !root.exists() {
            root = root.parent().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "no filesystem root for cache path")
            })?;
        }
        let available = fs2::available_space(root)?;
        let required = self.minimum_free_disk_bytes.saturating_add(bytes);
        if available < required {
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
    FE: FileSave + Clone,
{
    /// Initialize the cache.
    ///
    /// `cleanup_interval` specifies how often cache cleanup should run in the background.
    /// `max_file_handles` specifies how many files are allowed to be evicted at once.
    /// If not specified, `max_file_handles` will default to 512.
    ///
    /// This function should only be called once.
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
            eviction_failed: AtomicBool::new(false),
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

        spawn_cleanup_thread(cache.clone(), rx);

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

    #[cfg(test)]
    fn gc(&self) -> FuturesUnordered<impl Future<Output = Result<()>> + Send> {
        self.gc_for(0)
    }

    fn gc_for(
        &self,
        requested: usize,
    ) -> FuturesUnordered<impl Future<Output = Result<()>> + Send> {
        let evictions = FuturesUnordered::new();

        let state = self.lock();

        if state.size.saturating_add(requested) <= self.capacity {
            return evictions;
        }

        let mut over = state.size.saturating_add(requested) - self.capacity;

        for (_path, file) in state.files.iter().rev() {
            if let Some((size, eviction)) = file.clone().evict() {
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
            let mut state = self.cache.lock();
            state.size -= self.bytes;
            self.cache.capacity_released.notify_waiters();
        }
    }
}

fn spawn_cleanup_thread<FE>(
    cache: Arc<Cache<FE>>,
    mut rx: Receiver<Evict>,
) -> tokio::task::JoinHandle<()>
where
    FE: FileSave + Clone,
{
    tokio::spawn(async move {
        while let Some(Evict) = rx.recv().await {
            let requested = cache.requested.swap(0, Ordering::AcqRel);
            let mut evictions = cache.gc_for(requested);
            let mut evicted = false;

            while let Some(result) = evictions.next().await {
                evicted = true;
                match result {
                    Ok(()) => {}
                    Err(cause) => {
                        cache.eviction_failed.store(true, Ordering::Release);
                        #[cfg(feature = "logging")]
                        log::error!("failed to evict cached file: {cause}");
                        #[cfg(not(feature = "logging"))]
                        let _ = cause;
                    }
                }
            }

            cache.gc_pending.store(false, Ordering::Release);
            cache.capacity_released.notify_waiters();
            if evicted {
                cache.check(cache.lock());
            }

            // let the filesystem catch up in case there's another gc cycle immediately after this
            tokio::time::sleep(GC_CYCLE_TIME).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{AtomicUsize, Cache, Notify, State, MAX_FILE_HANDLES};
    use crate::file::FileSave;
    use crate::FileLock;
    use futures::StreamExt;
    use safecast::as_type;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;
    use tokio::sync::Semaphore;

    #[derive(Clone)]
    enum Entry {
        Bin(Vec<u8>),
    }

    impl FileSave for Entry {
        async fn save(&self, file: &mut tokio::fs::File) -> crate::Result<u64> {
            match self {
                Self::Bin(bytes) => {
                    file.write_all(bytes).await?;
                    Ok(bytes.len() as u64)
                }
            }
        }
    }

    as_type!(Entry, Bin, Vec<u8>);

    #[cfg(not(feature = "stream"))]
    impl crate::file::FileLoad for Vec<u8> {
        async fn load(
            _path: &std::path::Path,
            mut file: tokio::fs::File,
            _metadata: std::fs::Metadata,
        ) -> crate::Result<Self> {
            use tokio::io::AsyncReadExt;

            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await?;
            Ok(bytes)
        }
    }

    fn unique_tmp_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("freqfs_test_cache_{}", uuid::Uuid::new_v4()));
        path
    }

    #[tokio::test]
    async fn lfu_eviction_prefers_least_used() -> std::io::Result<()> {
        let tmp = unique_tmp_dir();
        tokio::fs::create_dir(&tmp).await?;

        let (tx, _rx) = mpsc::channel(1);

        let cache = Arc::new(Cache {
            gc_pending: AtomicBool::new(false),
            eviction_failed: AtomicBool::new(false),
            requested: AtomicUsize::new(0),
            capacity: 10,
            max_file_handles: MAX_FILE_HANDLES,
            minimum_free_disk_bytes: 0,
            file_handles: Arc::new(Semaphore::new(MAX_FILE_HANDLES)),
            capacity_released: Notify::new(),
            handle_wait: Duration::from_secs(3),
            state: Mutex::new(State::new()),
            tx,
        });

        let path_a = tmp.join("a.bin");
        let path_b = tmp.join("b.bin");

        let file_a: FileLock<Entry> =
            FileLock::new(cache.clone(), path_a.clone(), vec![1u8; 10], 10);
        let file_b: FileLock<Entry> =
            FileLock::new(cache.clone(), path_b.clone(), vec![2u8; 10], 10);

        cache.insert(path_a.clone(), file_a.clone(), 10);
        cache.insert(path_b.clone(), file_b.clone(), 10);

        // make `a.bin` the most frequently used
        cache.bump(&path_a, None);

        let mut evictions = cache.gc();
        while let Some(result) = evictions.next().await {
            result?;
        }

        assert!(file_a.try_read::<Vec<u8>>().is_ok());

        let evicted = file_b.try_read::<Vec<u8>>().unwrap_err();
        assert_eq!(evicted.kind(), std::io::ErrorKind::WouldBlock);

        let _ = tokio::fs::remove_dir_all(&tmp).await;
        Ok(())
    }

    #[test]
    fn tiny_capacity_coalesces_duplicate_evict_signals() {
        let tmp = unique_tmp_dir();
        std::fs::create_dir_all(&tmp).expect("create test dir");

        let (tx, mut rx) = mpsc::channel(1);

        let cache = Arc::new(Cache {
            gc_pending: AtomicBool::new(false),
            eviction_failed: AtomicBool::new(false),
            requested: AtomicUsize::new(0),
            capacity: 1,
            max_file_handles: MAX_FILE_HANDLES,
            minimum_free_disk_bytes: 0,
            file_handles: Arc::new(Semaphore::new(MAX_FILE_HANDLES)),
            capacity_released: Notify::new(),
            handle_wait: Duration::from_secs(3),
            state: Mutex::new(State::new()),
            tx,
        });

        let path_a = tmp.join("a.bin");
        let path_b = tmp.join("b.bin");
        let path_c = tmp.join("c.bin");

        let file_a: FileLock<Entry> = FileLock::new(cache.clone(), path_a.clone(), vec![1u8; 2], 2);
        let file_b: FileLock<Entry> = FileLock::new(cache.clone(), path_b.clone(), vec![2u8; 2], 2);
        let file_c: FileLock<Entry> = FileLock::new(cache.clone(), path_c.clone(), vec![3u8; 2], 2);

        cache.insert(path_a, file_a, 2);
        assert!(rx.try_recv().is_ok());

        cache.insert(path_b, file_b, 2);
        assert!(rx.try_recv().is_err());

        cache.gc_pending.store(false, Ordering::Release);

        cache.insert(path_c, file_c, 2);
        assert!(rx.try_recv().is_ok());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn disk_reserve_rejects_before_a_write() {
        let tmp = unique_tmp_dir();
        std::fs::create_dir_all(&tmp).expect("create test dir");
        let cache = Cache::<Entry>::new(10, Some(1), u64::MAX, Duration::from_secs(3));

        let err = cache
            .ensure_disk_capacity(&tmp.join("data.bin"), 1)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::StorageFull);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn file_handle_admission_waits_and_recovers() {
        let cache = Cache::<Entry>::new(10, Some(1), 0, Duration::from_secs(1));
        let first = FileLock::new(
            cache.clone(),
            PathBuf::from("first.bin"),
            Entry::Bin(vec![1]),
            1,
        );
        let second = FileLock::new(cache, PathBuf::from("second.bin"), Entry::Bin(vec![2]), 1);
        let first_guard = first.read::<Vec<u8>>().await.unwrap();
        let waiting = second.read::<Vec<u8>>();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());

        drop(first_guard);
        let second_guard = waiting.await.unwrap();
        assert_eq!(&*second_guard, &[2]);
    }

    #[tokio::test]
    async fn byte_admission_waits_at_limit_and_recovers_on_release() {
        let cache = Cache::<Entry>::new(10, Some(1), 0, Duration::from_secs(1));
        let first = cache.reserve(10).await.unwrap();
        let waiting = cache.reserve(1);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());

        drop(first);
        let second = waiting.await.unwrap();
        second.commit();
        assert_eq!(cache.lock().size, 1);
    }
}
