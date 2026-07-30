use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::future::Future;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::dir::DirLock;
use super::file::{FileLock, FileSave};
use super::Result;

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
    capacity: usize,
    max_file_handles: usize,
    state: Mutex<State<FE>>,
    tx: UnboundedSender<Evict>,
}

impl<FE> Cache<FE> {
    #[inline]
    fn check(&self, state: MutexGuard<State<FE>>) {
        if state.size > self.capacity {
            self.tx.send(Evict).expect("cache cleanup thread");
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

    pub(crate) fn insert(&self, path: PathBuf, file: FileLock<FE>, file_size: usize) {
        let mut state = self.lock();
        state.files.insert(path, file);
        state.size += file_size;

        self.check(state)
    }

    pub(crate) fn remove(&self, path: &PathBuf, size: usize) {
        let mut state = self.lock();

        if state.files.remove(path).is_some() {
            state.size -= size;
        }

        self.check(state)
    }

    pub(crate) fn resize(&self, old_size: usize, new_size: usize) {
        let mut state = self.lock();
        state.size += new_size;
        state.size -= old_size;

        self.check(state)
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
    pub fn new(capacity: usize, max_file_handles: Option<usize>) -> Arc<Self> {
        let max_file_handles = max_file_handles.unwrap_or(MAX_FILE_HANDLES);

        assert!(
            max_file_handles > 0,
            "invalid config for max_file_handles: {}",
            max_file_handles
        );

        let (tx, rx) = mpsc::unbounded_channel();

        let cache = Arc::new(Self {
            capacity,
            max_file_handles,
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

    fn gc(&self) -> FuturesUnordered<impl Future<Output = Result<()>> + Send> {
        let evictions = FuturesUnordered::new();

        let mut state = self.lock();

        if state.size < self.capacity {
            return evictions;
        }

        let mut over = state.size as i64 - self.capacity as i64;
        let mut total_evicted = 0usize;

        for (_path, file) in state.files.iter().rev() {
            if let Some((size, eviction)) = file.clone().evict() {
                total_evicted += size;
                over -= size as i64;
                evictions.push(eviction);
            }

            if over <= 0 || evictions.len() >= self.max_file_handles {
                break;
            }
        }

        // Update the cache size now, while we still hold Cache::state.
        // This ensures the lock acquisition order is strictly Cache -> File
        // (LIFO: acquire Cache first, then File inside evict(), then release File,
        // then release Cache) and avoids a File -> Cache inversion in the
        // eviction future that runs *after* Cache::state is released.
        state.size -= total_evicted;

        // state (Cache::state guard) is dropped here, before eviction futures run.
        // Since size was already adjusted above, the eviction futures do not need
        // to call cache.resize() and therefore never acquire Cache::state.
        evictions
    }
}

fn spawn_cleanup_thread<FE>(
    cache: Arc<Cache<FE>>,
    mut rx: UnboundedReceiver<Evict>,
) -> tokio::task::JoinHandle<()>
where
    FE: FileSave + Clone,
{
    tokio::spawn(async move {
        while let Some(Evict) = rx.recv().await {
            // Coalesce: drain any Evict signals that accumulated while we were
            // processing the previous cycle, so we don't run redundant GC passes.
            while rx.try_recv().is_ok() {}

            loop {
                let mut evictions = cache.gc();

                if evictions.is_empty() {
                    // Cache is within capacity, or no evictable files remain
                    // (all candidates are currently locked). In either case
                    // there's nothing more to do until a new Evict signal arrives.
                    break;
                }

                while let Some(result) = evictions.next().await {
                    match result {
                        Ok(()) => {}
                        Err(cause) => panic!("failed to evict file from cache: {}", cause),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{Cache, State, MAX_FILE_HANDLES};
    use crate::file::FileSave;
    use crate::FileLock;
    use futures::StreamExt;
    use safecast::as_type;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;

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

        let (tx, _rx) = mpsc::unbounded_channel();

        let cache = Arc::new(Cache {
            capacity: 10,
            max_file_handles: MAX_FILE_HANDLES,
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
}
