use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender};
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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

    pub(crate) fn validate_bound(&self, bytes: usize) -> Result<()> {
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

    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation<FE>> {
        self.reserve_if_available(bytes)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::ResourceBusy, "cache capacity is exhausted")
        })
    }

    // None means capacity pressure; eviction errors retain their original kind.
    fn reserve_if_available(self: &Arc<Self>, bytes: usize) -> Result<Option<Reservation<FE>>> {
        self.validate_bound(bytes)?;
        if let Some(cause) = self.eviction_error.lock().expect("eviction error").take() {
            return Err(cause);
        }

        let mut state = self.lock();
        if state.size.saturating_add(bytes) > self.capacity {
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
            .map_err(|_| io::Error::new(io::ErrorKind::ResourceBusy, "file handle limit reached"))
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

    #[cfg(test)]
    fn gc(self: &Arc<Self>) -> FuturesUnordered<impl Future<Output = Result<()>> + Send> {
        self.gc_for(0)
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

    pub(crate) fn shrink(&mut self, actual: usize) {
        assert!(actual <= self.bytes, "validated retained size");
        self.cache.release(self.bytes - actual);
        self.bytes = actual;
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
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::StreamExt;
    use safecast::as_type;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;
    use tokio::sync::Semaphore;

    use super::{AtomicUsize, Cache, Notify, State, MAX_FILE_HANDLES};
    use crate::file::FileSave;
    use crate::FileLock;

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

    impl get_size::GetSize for Entry {
        fn get_size(&self) -> usize {
            match self {
                Self::Bin(bytes) => bytes.capacity(),
            }
        }
    }

    impl crate::file::FileLoad for Entry {
        async fn load_size(
            _: &std::path::Path,
            _: &mut tokio::fs::File,
            metadata: &std::fs::Metadata,
        ) -> crate::Result<usize> {
            usize::try_from(metadata.len()).map_err(std::io::Error::other)
        }

        async fn load(
            _path: &std::path::Path,
            mut file: tokio::fs::File,
            metadata: std::fs::Metadata,
        ) -> crate::Result<Self> {
            use tokio::io::AsyncReadExt;
            let mut bytes = vec![0; metadata.len() as usize];
            file.read_exact(&mut bytes).await?;
            Ok(Self::Bin(bytes))
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
            eviction_error: Mutex::new(None),
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
            eviction_error: Mutex::new(None),
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

        let root = cache.load(tmp.clone()).unwrap();
        let file = root
            .write()
            .await
            .create_file("data.bin".into(), vec![1_u8], 1)
            .await
            .unwrap();
        for result in [
            file.sync().await,
            file.replace_all(Entry::Bin(vec![2]), 1).await,
            file.clone().evict().unwrap().1.await,
        ] {
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::StorageFull);
        }
        assert!(!file.path().exists(), "disk rejection precedes publication");
        assert_eq!(&*file.read::<Vec<u8>>().await.unwrap(), &[1]);

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
        assert_eq!(
            cache.try_reserve(1).err().unwrap().kind(),
            std::io::ErrorKind::ResourceBusy
        );
        let waiting = cache.reserve(1);
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());

        drop(first);
        let second = waiting.await.unwrap();
        second.commit();
        assert_eq!(cache.lock().size, 1);
    }

    #[tokio::test]
    async fn initial_creation_reclaims_unlocked_payloads() -> std::io::Result<()> {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        let cache = Cache::<Entry>::new(128, Some(2), 0, Duration::from_secs(1));
        let root = cache.clone().load(path.clone())?;
        let first = root
            .write()
            .await
            .create_empty_file("first".into(), vec![0u8; 96])
            .await?;
        let second = root
            .write()
            .await
            .create_empty_file("second".into(), vec![0u8; 64])
            .await?;
        assert_eq!(cache.lock().size, 64);
        assert_eq!(cache.file_handles.available_permits(), 2);
        assert_eq!(tokio::fs::metadata(first.path()).await?.len(), 96);
        assert_eq!(second.read::<Vec<u8>>().await?.len(), 64);
        assert_eq!(first.read::<Vec<u8>>().await?.len(), 96);
        root.write().await.truncate_and_sync().await?;
        Ok(())
    }

    #[tokio::test]
    async fn initial_creation_timeout_and_cancellation_publish_nothing() -> std::io::Result<()> {
        for cancel in [false, true] {
            let path = unique_tmp_dir();
            tokio::fs::create_dir(&path).await?;
            let cache = Cache::<Entry>::new(64, Some(2), 0, Duration::from_millis(30));
            let root = cache.clone().load(path.clone())?;
            let first = root
                .write()
                .await
                .create_empty_file("first".into(), vec![0u8; 64])
                .await?;
            let pinned = first.read::<Vec<u8>>().await?;
            let mut contents = root.write().await;
            {
                let waiting = contents.create_empty_file("second".into(), vec![0u8; 16]);
                futures::pin_mut!(waiting);
                assert!(futures::poll!(&mut waiting).is_pending());
                if !cancel {
                    assert_eq!(
                        waiting.await.err().unwrap().kind(),
                        std::io::ErrorKind::ResourceBusy
                    );
                }
            }
            assert!(!contents.contains("second"));
            assert_eq!(cache.lock().size, 64);
            assert_eq!(cache.file_handles.available_permits(), 1);
            drop(pinned);
            contents
                .create_empty_file("second".into(), vec![0u8; 16])
                .await?;
            assert!(contents.contains("second"));
            assert_eq!(cache.lock().size, 16);
            assert_eq!(cache.file_handles.available_permits(), 2);
            drop(contents);
            root.write().await.truncate_and_sync().await?;
        }
        Ok(())
    }

    // An eight-byte length expands into a zero-filled vector. Only admission tests
    // need encoded size to differ from retained allocation and decoding to be detectable.
    struct ExpandedPayload(Vec<u8>);

    impl get_size::GetSize for ExpandedPayload {
        fn get_size(&self) -> usize {
            self.0.capacity()
        }
    }

    impl safecast::AsType<ExpandedPayload> for ExpandedPayload {
        fn into_type(self) -> Option<ExpandedPayload> {
            Some(self)
        }

        fn as_type(&self) -> Option<&ExpandedPayload> {
            Some(self)
        }

        fn as_type_mut(&mut self) -> Option<&mut ExpandedPayload> {
            Some(self)
        }
    }

    impl crate::FileLoad for ExpandedPayload {
        async fn load_size(
            _: &std::path::Path,
            file: &mut tokio::fs::File,
            _: &std::fs::Metadata,
        ) -> crate::Result<usize> {
            use tokio::io::AsyncReadExt;
            let len = usize::try_from(file.read_u64_le().await?).map_err(std::io::Error::other)?;
            len.checked_add(8)
                .ok_or_else(|| std::io::Error::other("size overflow"))
        }

        async fn load(
            _: &std::path::Path,
            mut file: tokio::fs::File,
            _: std::fs::Metadata,
        ) -> crate::Result<Self> {
            use tokio::io::AsyncReadExt;
            let len = usize::try_from(file.read_u64_le().await?).map_err(std::io::Error::other)?;
            if len > 128 {
                return Err(std::io::Error::other("oversized decoder was called"));
            }
            Ok(Self(vec![0; len]))
        }
    }

    impl FileSave for ExpandedPayload {
        async fn save(&self, file: &mut tokio::fs::File) -> crate::Result<u64> {
            file.write_u64_le(self.0.len() as u64).await?;
            Ok(8)
        }
    }

    #[tokio::test]
    async fn retained_allocation_is_independent_of_encoded_size() -> std::io::Result<()> {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        let cache = Cache::<ExpandedPayload>::new(128, Some(2), 0, Duration::from_millis(50));
        let root = cache.clone().load(path.clone())?;
        let file = root
            .write()
            .await
            .create_file("expanded".into(), ExpandedPayload(vec![0; 64]), 80)
            .await?;
        assert_eq!(cache.lock().size, 64);
        let duplicate = root
            .write()
            .await
            .create_file("expanded".into(), ExpandedPayload(vec![0; 16]), 16)
            .await
            .err()
            .unwrap();
        assert_eq!(duplicate.kind(), std::io::ErrorKind::AlreadyExists);
        let duplicate = root
            .write()
            .await
            .create_empty_file("expanded".into(), ExpandedPayload(vec![0; 16]))
            .await
            .err()
            .unwrap();
        assert_eq!(duplicate.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(file.read::<ExpandedPayload>().await?.0.len(), 64);
        assert_eq!(cache.lock().size, 64);
        file.sync().await?;
        assert_eq!(tokio::fs::metadata(file.path()).await?.len(), 8);
        assert_eq!(cache.lock().size, 64);
        file.clone().evict().unwrap().1.await?;
        assert_eq!(cache.lock().size, 0);
        assert_eq!(file.read::<ExpandedPayload>().await?.0.len(), 64);
        assert_eq!(cache.lock().size, 64, "preflight slack must be refunded");
        {
            let mut guard = file.write::<ExpandedPayload>(80).await?;
            assert_eq!(cache.lock().size, 80);
            guard.reserve(96).await?;
            assert_eq!(cache.lock().size, 96);
            assert_eq!(
                guard.reserve(160).await.err().unwrap().kind(),
                std::io::ErrorKind::OutOfMemory
            );
            assert_eq!(cache.lock().size, 96);
            guard.0 = vec![0; 72];
        }
        assert_eq!(cache.lock().size, 72);
        file.sync().await?;
        assert_eq!(cache.lock().size, 72);
        {
            let mut guard = file.write_owned::<ExpandedPayload>(80).await?;
            guard.reserve(96).await?;
            guard.reserve(80).await?;
            assert_eq!(cache.lock().size, 96);
        }
        assert_eq!(cache.lock().size, 72);
        root.write().await.delete("expanded").await;
        assert_eq!(cache.lock().size, 0);
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn admission_precedes_decode_and_bound_violation_fails_closed() -> std::io::Result<()> {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        tokio::fs::write(path.join("oversized"), 4096u64.to_le_bytes()).await?;
        let cache = Cache::<ExpandedPayload>::new(128, Some(2), 0, Duration::from_millis(30));
        let root = cache.clone().load(path.clone())?;
        let oversized = root.read().await.get_file("oversized").unwrap().clone();
        assert_eq!(
            oversized
                .read::<ExpandedPayload>()
                .await
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::OutOfMemory
        );
        assert_eq!(cache.lock().size, 0);
        let file = root
            .write()
            .await
            .create_empty_file("small".into(), ExpandedPayload(vec![0; 16]))
            .await?;
        assert_eq!(cache.lock().size, 16);
        assert!(root
            .write()
            .await
            .create_file("bad".into(), ExpandedPayload(vec![0; 32]), 16)
            .await
            .is_err());
        assert!(!root.read().await.contains("bad"));
        assert_eq!(cache.lock().size, 16);
        {
            let mut guard = file.write::<ExpandedPayload>(16).await?;
            guard.0 = vec![0; 32]; // Deliberately violate the caller admission contract.
        }
        assert!(file.read::<ExpandedPayload>().await.is_err());
        assert!(file.sync().await.is_err());
        assert_eq!(cache.lock().size, 0);
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn owned_guard_retains_cache_without_worker_or_entry_cycles() -> std::io::Result<()> {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        let cache = Cache::<Entry>::new(64, Some(2), 0, Duration::from_millis(30));
        let weak = Arc::downgrade(&cache);
        let root = cache.clone().load(path.clone())?;
        let file = root
            .write()
            .await
            .create_empty_file("small".into(), vec![0u8; 16])
            .await?;
        let guard = file.read_owned::<Vec<u8>>().await?;
        drop(file);
        drop(root);
        drop(cache);
        assert!(weak.upgrade().is_some());
        drop(guard);
        assert!(weak.upgrade().is_none());
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn eviction_preserves_original_io_error() -> std::io::Result<()> {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        let cache = Cache::<Entry>::new(16, Some(2), u64::MAX, Duration::from_millis(100));
        let root = cache.clone().load(path.clone())?;
        root.write()
            .await
            .create_empty_file("small".into(), vec![0u8; 16])
            .await?;
        let cause = cache.reserve(1).await.err().expect("eviction must fail");
        assert_eq!(cause.kind(), std::io::ErrorKind::StorageFull);
        assert!(cause.to_string().contains("filesystem free space"));
        assert_eq!(cache.lock().size, 16);

        // ResourceBusy from I/O is an error, not a signal to wait for capacity.
        for wait in [false, true] {
            *cache.eviction_error.lock().unwrap() = Some(std::io::Error::new(
                std::io::ErrorKind::ResourceBusy,
                "original eviction error",
            ));
            let result = if wait {
                cache.reserve(1).await
            } else {
                cache.try_reserve(1)
            };
            let cause = result.err().unwrap();
            assert_eq!(cause.kind(), std::io::ErrorKind::ResourceBusy);
            assert_eq!(cause.to_string(), "original eviction error");
        }
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn copy_admits_its_clone_before_replacing_the_old_payload() -> std::io::Result<()> {
        for capacity in [64, 96] {
            let path = unique_tmp_dir();
            tokio::fs::create_dir(&path).await?;
            let cache = Cache::<Entry>::new(capacity, Some(2), 0, Duration::from_millis(20));
            let root = cache.clone().load(path.clone())?;
            let source = root
                .write()
                .await
                .create_empty_file("source".into(), vec![1u8; 32])
                .await?;
            let target = root
                .write()
                .await
                .create_empty_file("target".into(), vec![2u8; 32])
                .await?;
            let result = target.overwrite(&source).await;
            if capacity == 64 {
                assert_eq!(
                    result.err().unwrap().kind(),
                    std::io::ErrorKind::ResourceBusy
                );
                assert_eq!(target.read::<Vec<u8>>().await?[0], 2);
            } else {
                result?;
                assert_eq!(target.read::<Vec<u8>>().await?[0], 1);
            }
            // A completed pressure request may evict either unlocked payload.
            // Reacquire both before asserting the live retained charge.
            let source_guard = source.read::<Vec<u8>>().await?;
            let target_guard = target.read::<Vec<u8>>().await?;
            assert_eq!(cache.lock().size, 64);
            drop((source_guard, target_guard));
            tokio::fs::remove_dir_all(path).await?;
        }
        Ok(())
    }
}
