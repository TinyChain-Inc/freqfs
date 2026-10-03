use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use futures::{Future, TryFutureExt};
use get_size::GetSize;
use safecast::AsType;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock, RwLockReadGuard,
    RwLockWriteGuard,
};

use super::cache::Cache;
use super::Result;

const TMP: &str = "_freqfs";

pub(crate) fn temporary_name(name: &str) -> bool {
    Path::new(name)
        .extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| ext.strip_suffix(TMP))
        .is_some_and(|prefix| prefix.is_empty() || prefix.ends_with('_'))
}

fn interrupted() -> io::Error {
    io::Error::other("file ownership invalidated; reopen the cache")
}

pub(crate) async fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path).await?.sync_all().await
}

pub struct FileReadGuard<'a, F> {
    guard: RwLockReadGuard<'a, F>,
    _permit: OwnedSemaphorePermit,
}

pub struct FileReadGuardOwned<FE, F> {
    guard: OwnedRwLockReadGuard<Option<FE>, F>,
    _cache: Arc<Cache<FE>>,
    _permit: OwnedSemaphorePermit,
}

/// An admitted mutable payload. Dropping the guard reconciles retained allocation.
/// Exceeding the admitted bound invalidates the file rather than publishing unaccounted data.
pub struct FileWriteGuard<'a, FE: GetSize, F> {
    guard: RwLockWriteGuard<'a, Option<FE>>,
    state: RwLockWriteGuard<'a, FileLockState>,
    cache: Arc<Cache<FE>>,
    bound: usize,
    _payload: PhantomData<F>,
    _permit: OwnedSemaphorePermit,
}

pub struct FileWriteGuardOwned<FE: GetSize, F> {
    guard: OwnedRwLockWriteGuard<Option<FE>>,
    state: OwnedRwLockWriteGuard<FileLockState>,
    cache: Arc<Cache<FE>>,
    bound: usize,
    _payload: PhantomData<F>,
    _permit: OwnedSemaphorePermit,
}

impl<FE: GetSize, F> FileWriteGuard<'_, FE, F> {
    /// Admit a larger retained allocation before growing the payload.
    pub async fn reserve(&mut self, retained_bound: usize) -> Result<()> {
        reserve_write(&self.cache, &mut self.bound, retained_bound).await
    }
}

impl<FE: GetSize, F> FileWriteGuardOwned<FE, F> {
    /// Admit a larger retained allocation before growing the payload.
    pub async fn reserve(&mut self, retained_bound: usize) -> Result<()> {
        reserve_write(&self.cache, &mut self.bound, retained_bound).await
    }
}

async fn reserve_write<FE>(
    cache: &Arc<Cache<FE>>,
    bound: &mut usize,
    retained_bound: usize,
) -> Result<()> {
    if retained_bound > *bound {
        cache.validate_bound(retained_bound)?;
        cache.reserve(retained_bound - *bound).await?.commit();
        *bound = retained_bound;
    }

    Ok(())
}

fn finish_write<FE: GetSize>(
    contents: &mut Option<FE>,
    state: &mut FileLockState,
    cache: &Cache<FE>,
    bound: usize,
) {
    let actual = contents.as_ref().expect("file").get_size();

    if actual > bound {
        *state = FileLockState::Failed;
        *contents = None;
        cache.release(bound);
    } else {
        *state = FileLockState::Modified(actual);
        cache.release(bound - actual);
    }
}

impl<FE: GetSize, F> Drop for FileWriteGuard<'_, FE, F> {
    fn drop(&mut self) {
        finish_write(&mut self.guard, &mut self.state, &self.cache, self.bound);
    }
}

impl<FE: GetSize, F> Drop for FileWriteGuardOwned<FE, F> {
    fn drop(&mut self) {
        finish_write(&mut self.guard, &mut self.state, &self.cache, self.bound);
    }
}

impl<'a, F> Deref for FileReadGuard<'a, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<FE, F> Deref for FileReadGuardOwned<FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<FE: GetSize + AsType<F>, F> Deref for FileWriteGuard<'_, FE, F> {
    type Target = F;

    fn deref(&self) -> &F {
        self.guard
            .as_ref()
            .expect("file")
            .as_type()
            .expect("validated payload type")
    }
}

impl<FE: GetSize + AsType<F>, F> DerefMut for FileWriteGuard<'_, FE, F> {
    fn deref_mut(&mut self) -> &mut F {
        self.guard
            .as_mut()
            .expect("file")
            .as_type_mut()
            .expect("validated payload type")
    }
}

impl<FE: GetSize + AsType<F>, F> Deref for FileWriteGuardOwned<FE, F> {
    type Target = F;

    fn deref(&self) -> &F {
        self.guard
            .as_ref()
            .expect("file")
            .as_type()
            .expect("validated payload type")
    }
}

impl<FE: GetSize + AsType<F>, F> DerefMut for FileWriteGuardOwned<FE, F> {
    fn deref_mut(&mut self) -> &mut F {
        self.guard
            .as_mut()
            .expect("file")
            .as_type_mut()
            .expect("validated payload type")
    }
}

impl<F: fmt::Debug> fmt::Debug for FileReadGuard<'_, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl<FE, F: fmt::Debug> fmt::Debug for FileReadGuardOwned<FE, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl<FE: GetSize + AsType<F>, F: fmt::Debug> fmt::Debug for FileWriteGuard<'_, FE, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl<FE: GetSize + AsType<F>, F: fmt::Debug> fmt::Debug for FileWriteGuardOwned<FE, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

/// A helper trait to coerce container types like [`Arc`] into a borrowed file.
pub trait FileDeref {
    /// The type of file referenced
    type File;

    /// Borrow this instance as a [`Self::File`]
    fn as_file(&self) -> &Self::File;
}

impl<'a, F> FileDeref for FileReadGuard<'a, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

impl<'a, F> FileDeref for Arc<FileReadGuard<'a, F>> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref().as_file()
    }
}

impl<FE, F> FileDeref for FileReadGuardOwned<FE, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

impl<FE, F> FileDeref for Arc<FileReadGuardOwned<FE, F>> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref().as_file()
    }
}

impl<FE: GetSize + AsType<F>, F> FileDeref for FileWriteGuard<'_, FE, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

impl<FE: GetSize + AsType<F>, F> FileDeref for FileWriteGuardOwned<FE, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

/// Load a file-backed data structure.
#[trait_variant::make(Send)]
pub trait FileLoad: GetSize + Send + Sync + Sized + 'static {
    /// Inspect the encoded file using bounded scratch and return an upper bound on
    /// the decoded payload's retained allocation. No payload allocation is admitted yet.
    /// The file is rewound before `load`; serialized length is not retained size.
    async fn load_size(
        path: &Path,
        file: &mut fs::File,
        metadata: &std::fs::Metadata,
    ) -> Result<usize>;

    /// Load this state from the given `file`.
    async fn load(path: &Path, file: fs::File, metadata: std::fs::Metadata) -> Result<Self>;
}

/// Write a file-backed data structure to the filesystem.
#[trait_variant::make(Send)]
pub trait FileSave: Send + Sync + Sized + 'static {
    /// Save this state to the given `file`.
    async fn save(&self, file: &mut fs::File) -> Result<u64>;
}

#[derive(Copy, Clone)]
enum FileLockState {
    Pending,
    Read(usize),
    Modified(usize),
    Deleted(bool),
    Failed,
}

impl FileLockState {
    fn check_available(&self) -> Result<()> {
        match self {
            Self::Failed => Err(interrupted()),
            Self::Deleted(_) => Err(deleted()),
            _ => Ok(()),
        }
    }

    fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
}

/// A futures-aware read-write lock on a file
pub struct FileLock<FE> {
    cache: Arc<Cache<FE>>,
    path: Arc<PathBuf>,
    state: Arc<RwLock<FileLockState>>,
    contents: Arc<RwLock<Option<FE>>>,
}

// Cache entries retain file state, but never the Cache that owns the entry.
pub(crate) struct CachedFile<FE> {
    path: Arc<PathBuf>,
    state: Arc<RwLock<FileLockState>>,
    contents: Arc<RwLock<Option<FE>>>,
}

impl<FE> CachedFile<FE> {
    pub(crate) fn with_cache(&self, cache: Arc<Cache<FE>>) -> FileLock<FE> {
        FileLock {
            cache,
            path: self.path.clone(),
            state: self.state.clone(),
            contents: self.contents.clone(),
        }
    }
}

impl<FE> Clone for FileLock<FE> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            path: self.path.clone(),
            state: self.state.clone(),
            contents: self.contents.clone(),
        }
    }
}

impl<FE> FileLock<FE> {
    pub(crate) fn cached(&self) -> CachedFile<FE> {
        CachedFile {
            path: self.path.clone(),
            state: self.state.clone(),
            contents: self.contents.clone(),
        }
    }

    pub(crate) fn abandoned(cache: Arc<Cache<FE>>, path: PathBuf) -> Self {
        Self {
            cache,
            path: Arc::new(path),
            state: Arc::new(RwLock::new(FileLockState::Deleted(true))),
            contents: Arc::new(RwLock::new(None)),
        }
    }

    async fn load_reserved(&self) -> Result<(usize, FE, crate::cache::Reservation<FE>)>
    where
        FE: FileLoad,
    {
        let (mut file, metadata) = open(&self.path).await?;
        let bound = FE::load_size(&self.path, &mut file, &metadata).await?;
        let mut reservation = self.cache.reserve(bound).await?;
        file.rewind().await?;
        let entry = FE::load(&self.path, file, metadata).await?;
        let actual = entry.get_size();
        validate_size(actual, bound)?;
        reservation.shrink(actual);
        Ok((actual, entry, reservation))
    }

    /// Create a new [`FileLock`].
    pub(crate) fn new<F>(cache: Arc<Cache<FE>>, path: PathBuf, contents: F, size: usize) -> Self
    where
        FE: From<F>,
    {
        Self {
            cache,
            path: Arc::new(path),
            state: Arc::new(RwLock::new(FileLockState::Modified(size))),
            contents: Arc::new(RwLock::new(Some(contents.into()))),
        }
    }

    /// Borrow the [`Path`] of this [`FileLock`].
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Load a new [`FileLock`].
    pub(crate) fn load<F>(cache: Arc<Cache<FE>>, path: PathBuf) -> Self
    where
        FE: From<F>,
    {
        Self {
            cache,
            path: Arc::new(path),
            state: Arc::new(RwLock::new(FileLockState::Pending)),
            contents: Arc::new(RwLock::new(None)),
        }
    }

    /// Replace this file from another cached payload or its persisted contents.
    pub async fn overwrite(&self, other: &Self) -> Result<()>
    where
        FE: GetSize + Clone,
    {
        if Arc::ptr_eq(&self.state, &other.state) {
            return Ok(());
        }
        let _permit = self.cache.acquire_file_handle().await?;
        // Reciprocal copies acquire the same first state lock. The Arc keeps
        // its identity stable for the entire acquisition and copy.
        let (mut this, that) = if Arc::as_ptr(&self.state) < Arc::as_ptr(&other.state) {
            let this = self.state.write().await;
            let that = other.state.read().await;
            (this, that)
        } else {
            let that = other.state.read().await;
            let this = self.state.write().await;
            (this, that)
        };
        this.check_available()?;
        that.check_available()?;
        let old_size = match *this {
            FileLockState::Read(size) | FileLockState::Modified(size) => size,
            _ => 0,
        };
        let mut contents = self.contents.write().await;
        match *that {
            FileLockState::Pending => {
                create_dir(self.path.parent().expect("file parent dir")).await?;
                fs::copy(other.path.as_path(), self.path.as_path()).await?;
                *contents = None;
                *this = FileLockState::Pending;
                self.cache.release(old_size);
            }
            FileLockState::Read(size) | FileLockState::Modified(size) => {
                // Cloning retains the old destination until the clone succeeds.
                // Admit that temporary allocation in full before invoking Clone.
                let reservation = self.cache.reserve(size).await?;
                let source = other.contents.read().await;
                let value = source.as_ref().expect("file").clone();
                let actual = value.get_size();
                validate_size(actual, size)?;
                *contents = Some(value);
                *this = FileLockState::Modified(actual);
                reservation.commit();
                self.cache.release(old_size + size - actual);
            }
            _ => unreachable!("validated source state"),
        }
        Ok(())
    }

    /// Lock this file for reading.
    pub async fn read<F>(&self) -> Result<FileReadGuard<'_, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self.contents.try_write().expect("file contents");
            let (size, entry, reservation) = self.load_reserved().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            // Do not cooperatively suspend a cached read while retaining the
            // exclusive state guard: another consumer may drive the next read
            // before polling this buffered future again.
            match self.contents.try_read() {
                Ok(contents) => contents,
                Err(_) => self.contents.read().await,
            }
        };

        read_type(guard).map(|guard| FileReadGuard {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read<F>(&self) -> Result<FileReadGuard<'_, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
            FileLockState::Failed => Err(interrupted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path, None);
                let guard = self.contents.try_read().map_err(would_block)?;
                read_type(guard).map(|guard| FileReadGuard {
                    guard,
                    _permit: permit,
                })
            }
        }
    }

    /// Lock this file for reading.
    pub async fn read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");

            let (size, entry, reservation) = self.load_reserved().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            match self.contents.clone().try_read_owned() {
                Ok(contents) => contents,
                Err(_) => self.contents.clone().read_owned().await,
            }
        };

        read_type_owned(guard).map(|guard| FileReadGuardOwned {
            guard,
            _cache: Arc::clone(&self.cache),
            _permit: permit,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
            FileLockState::Failed => Err(interrupted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path, None);
                let guard = self
                    .contents
                    .clone()
                    .try_read_owned()
                    .map_err(would_block)?;

                read_type_owned(guard).map(|guard| FileReadGuardOwned {
                    guard,
                    _cache: Arc::clone(&self.cache),
                    _permit: permit,
                })
            }
        }
    }

    /// Lock this file for reading, without borrowing.
    pub async fn into_read<F>(self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");
            let (size, entry, reservation) = self.load_reserved().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            match self.contents.clone().try_read_owned() {
                Ok(contents) => contents,
                Err(_) => self.contents.read_owned().await,
            }
        };

        read_type_owned(guard).map(|guard| FileReadGuardOwned {
            guard,
            _cache: Arc::clone(&self.cache),
            _permit: permit,
        })
    }

    /// Admit at least `retained_bound` bytes and lock this file for mutation.
    /// Existing retained allocation remains admitted; zero permits no additional growth.
    pub async fn write<F>(&self, retained_bound: usize) -> Result<FileWriteGuard<'_, FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;
        state.check_available()?;
        let mut guard = self.contents.write().await;
        if state.is_pending() {
            let (size, entry, reservation) = self.load_reserved().await?;
            *guard = Some(entry);
            *state = FileLockState::Read(size);
            reservation.commit();
        }

        let current = check_write::<FE, F>(&guard, &state)?;
        let retained_bound = retained_bound.max(current);
        self.cache.validate_bound(retained_bound)?;
        self.cache.reserve(retained_bound - current).await?.commit();
        self.cache.bump(&self.path, None);

        Ok(FileWriteGuard {
            guard,
            state,
            cache: Arc::clone(&self.cache),
            bound: retained_bound,
            _payload: PhantomData,
            _permit: permit,
        })
    }

    /// Admit and lock a cached file immediately, or return an admission/lock error.
    pub fn try_write<F>(&self, retained_bound: usize) -> Result<FileWriteGuard<'_, FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = self.state.try_write().map_err(would_block)?;
        state.check_available()?;
        let guard = self.contents.try_write().map_err(would_block)?;

        let current = check_write::<FE, F>(&guard, &state)?;
        let retained_bound = retained_bound.max(current);
        self.cache.validate_bound(retained_bound)?;
        self.cache.try_reserve(retained_bound - current)?.commit();
        self.cache.bump(&self.path, None);

        Ok(FileWriteGuard {
            guard,
            state,
            cache: Arc::clone(&self.cache),
            bound: retained_bound,
            _payload: PhantomData,
            _permit: permit,
        })
    }

    /// Admit at least `retained_bound` bytes and lock this file for owned mutation.
    pub async fn write_owned<F>(&self, retained_bound: usize) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = Arc::clone(&self.state).write_owned().await;
        state.check_available()?;
        let mut guard = Arc::clone(&self.contents).write_owned().await;
        if state.is_pending() {
            let (size, entry, reservation) = self.load_reserved().await?;
            *guard = Some(entry);
            *state = FileLockState::Read(size);
            reservation.commit();
        }

        let current = check_write::<FE, F>(&guard, &state)?;
        let retained_bound = retained_bound.max(current);
        self.cache.validate_bound(retained_bound)?;
        self.cache.reserve(retained_bound - current).await?.commit();
        self.cache.bump(&self.path, None);

        Ok(FileWriteGuardOwned {
            guard,
            state,
            cache: Arc::clone(&self.cache),
            bound: retained_bound,
            _payload: PhantomData,
            _permit: permit,
        })
    }

    /// Admit and lock a cached file immediately, or return an admission/lock error.
    pub fn try_write_owned<F>(&self, retained_bound: usize) -> Result<FileWriteGuardOwned<FE, F>>
    where
        FE: GetSize + AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = Arc::clone(&self.state)
            .try_write_owned()
            .map_err(would_block)?;
        state.check_available()?;
        let guard = Arc::clone(&self.contents)
            .try_write_owned()
            .map_err(would_block)?;

        let current = check_write::<FE, F>(&guard, &state)?;
        let retained_bound = retained_bound.max(current);
        self.cache.validate_bound(retained_bound)?;
        self.cache.try_reserve(retained_bound - current)?.commit();
        self.cache.bump(&self.path, None);

        Ok(FileWriteGuardOwned {
            guard,
            state,
            cache: Arc::clone(&self.cache),
            bound: retained_bound,
            _payload: PhantomData,
            _permit: permit,
        })
    }

    /// Admit and lock this file for mutation without borrowing the file handle.
    pub async fn into_write<F>(self, retained_bound: usize) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        self.write_owned(retained_bound).await
    }

    /// Admit and lock this file for mutation immediately without borrowing.
    pub fn try_into_write<F>(self, retained_bound: usize) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        self.try_write_owned(retained_bound)
    }

    /// Write buffered contents to the filesystem without a durability barrier.
    pub async fn sync(&self) -> Result<()>
    where
        FE: FileSave,
    {
        let mut state = self.state.write().await;

        let new_state = match &*state {
            FileLockState::Failed => return Err(interrupted()),
            FileLockState::Pending => FileLockState::Pending,
            FileLockState::Read(size) => FileLockState::Read(*size),
            FileLockState::Modified(old_size) => {
                #[cfg(feature = "logging")]
                log::trace!("sync modified file {}...", self.path.display());

                let contents = self.contents.read().await;
                let contents = contents.as_ref().expect("file");

                self.cache.ensure_disk_space(&self.path)?;
                persist_with(self.path.clone(), contents, false).await?;
                FileLockState::Read(*old_size)
            }
            FileLockState::Deleted(pending_delete) => {
                if *pending_delete {
                    delete_file(&self.path).await?;
                }

                FileLockState::Deleted(false)
            }
        };

        *state = new_state;

        Ok(())
    }

    /// Make this file's contents and its directory entry durable, including prior eviction.
    pub async fn sync_all(&self) -> Result<()>
    where
        FE: FileSave,
    {
        self.sync_durable(true).await
    }

    pub(crate) async fn sync_durable(&self, parent: bool) -> Result<()>
    where
        FE: FileSave,
    {
        let _permit = self.cache.acquire_file_handle().await?;
        self.sync().await?;
        let state = self.state.write().await;
        if matches!(*state, FileLockState::Failed) {
            return Err(interrupted());
        }
        // A writer can enter between writeback and this lock. Do not acknowledge its data.
        if matches!(*state, FileLockState::Modified(_)) {
            return Err(would_block("file modified during durable synchronization"));
        }
        if !state.is_deleted() {
            fs::OpenOptions::new()
                .write(true)
                .open(self.path())
                .await?
                .sync_all()
                .await?;
        }
        // A prior subtree barrier may have synchronized contents before failing
        // on its directory. Always complete publication for standalone calls.
        if parent {
            sync_directory(self.path.parent().expect("file parent")).await?;
        }
        Ok(())
    }

    /// Atomically publish a durable replacement, admitting `retained_bound` cache
    /// bytes before work (as with directory file creation). Once publication begins,
    /// error or cancellation requires reopening; access and eviction fail closed.
    /// Encoding borrows the supplied replacement under exclusive ownership.
    pub async fn replace_all(&self, value: FE, retained_bound: usize) -> Result<()>
    where
        FE: FileSave + GetSize,
    {
        let actual = value.get_size();
        validate_size(actual, retained_bound)?;
        self.cache.validate_bound(retained_bound)?;
        let _permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;
        state.check_available()?;
        let old_size = match *state {
            FileLockState::Read(size) | FileLockState::Modified(size) => size,
            _ => 0,
        };
        let mut contents = self.contents.write().await;
        let reservation = self
            .cache
            .reserve(retained_bound.saturating_sub(old_size))
            .await?;
        self.cache.ensure_disk_space(&self.path)?;
        *state = FileLockState::Failed;
        persist_with(self.path.clone(), &value, true).await?;
        *contents = Some(value);
        reservation.commit();
        *state = FileLockState::Read(actual);
        self.cache.release(old_size.max(retained_bound) - actual);
        Ok(())
    }

    pub(crate) async fn delete(&self, file_only: bool) {
        let mut file_state = self.state.write().await;

        let size = match &*file_state {
            FileLockState::Failed => return,
            FileLockState::Pending => 0,
            FileLockState::Read(size) => *size,
            FileLockState::Modified(size) => *size,
            FileLockState::Deleted(_) => return,
        };

        *self.contents.write().await = None;
        self.cache.remove(&self.path, size);
        *file_state = FileLockState::Deleted(file_only);
    }

    pub(crate) fn evict(self) -> Option<(usize, impl Future<Output = Result<()>> + Send)>
    where
        FE: FileSave + 'static,
    {
        // if this file is in use, don't evict it
        let mut state = self.state.try_write_owned().ok()?;

        let (old_size, mut contents, modified) = match &*state {
            FileLockState::Pending => {
                // in this case there's nothing to evict
                return None;
            }
            FileLockState::Read(size) => {
                let contents = self.contents.try_write_owned().ok()?;
                (*size, contents, false)
            }
            FileLockState::Modified(size) => {
                let contents = self.contents.try_write_owned().ok()?;
                (*size, contents, true)
            }
            FileLockState::Failed => return None,
            FileLockState::Deleted(_) => unreachable!("evict a deleted file"),
        };

        let eviction = async move {
            if modified {
                let contents = contents.as_ref().expect("file");
                self.cache.ensure_disk_space(&self.path)?;
                persist_with(self.path.clone(), contents, false).await?;
            }

            *contents = None;
            *state = FileLockState::Pending;
            // Pending readers require exclusive contents access. Release it before
            // publishing Pending by unlocking state, including to other workers.
            drop(contents);
            drop(state);

            // Shrinking accounting cannot suspend. Release native guards first.
            self.cache.release(old_size);
            Ok(())
        };

        Some((old_size, eviction))
    }
}

impl<FE> fmt::Debug for FileLock<FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        #[cfg(debug_assertions)]
        write!(f, "file at {}", self.path.display())?;

        #[cfg(not(debug_assertions))]
        f.write_str("a file lock")?;

        Ok(())
    }
}

async fn open(path: &Path) -> Result<(fs::File, std::fs::Metadata)> {
    let file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
            #[cfg(debug_assertions)]
            let message = format!("there is no file at {}", path.display());

            #[cfg(not(debug_assertions))]
            let message = "the requested file is not in cache and does not exist on the filesystem";

            return Err(io::Error::new(io::ErrorKind::NotFound, message));
        }
        Err(cause) => return Err(cause),
    };

    let metadata = file.metadata().await?;
    Ok((file, metadata))
}

#[cfg(test)]
async fn persist<FE: FileSave>(path: Arc<PathBuf>, file: FE) -> Result<u64> {
    persist_with(path, &file, false).await
}

async fn persist_with<FE: FileSave>(path: Arc<PathBuf>, file: &FE, durable: bool) -> Result<u64> {
    let tmp = if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
        path.with_extension(format!("{}_{}", ext, TMP))
    } else {
        path.with_extension(TMP)
    };

    let size = {
        let mut tmp_file = match fs::File::create(tmp.as_path()).await {
            Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
                create_dir(tmp.parent().expect("dir")).await?;
                fs::File::create(tmp.as_path()).await
            }
            result => result,
        }
        .map_err(|cause| {
            io::Error::new(
                cause.kind(),
                format!("failed to create tmp file: {}", cause),
            )
        })?;

        let size = file
            .save(&mut tmp_file)
            .map_err(|cause| {
                io::Error::new(cause.kind(), format!("failed to save tmp file: {}", cause))
            })
            .await?;
        // Complete Tokio's buffered writes before publishing the name. This is
        // writeback, not a durability barrier, and also propagates delayed errors.
        tmp_file.flush().await?;
        if durable {
            tmp_file.sync_all().await?;
        }
        size
    };

    tokio::fs::rename(tmp.as_path(), path.as_path())
        .map_err(|cause| {
            io::Error::new(
                cause.kind(),
                format!("failed to rename tmp file: {}", cause),
            )
        })
        .await?;

    if durable {
        sync_directory(path.parent().expect("file parent")).await?;
    }
    Ok(size)
}

async fn create_dir(path: &Path) -> Result<()> {
    if path.exists() {
        Ok(())
    } else {
        match tokio::fs::create_dir_all(path).await {
            Ok(()) => Ok(()),
            Err(cause) => {
                if path.exists() && path.is_dir() {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        cause.kind(),
                        format!("failed to create directory: {}", cause),
                    ))
                }
            }
        }
    }
}

#[inline]
fn read_type<F, T>(maybe_file: RwLockReadGuard<Option<F>>) -> Result<RwLockReadGuard<T>>
where
    F: AsType<T>,
{
    match RwLockReadGuard::try_map(maybe_file, |file| file.as_ref().expect("file").as_type()) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

#[inline]
fn read_type_owned<F, T>(
    maybe_file: OwnedRwLockReadGuard<Option<F>>,
) -> Result<OwnedRwLockReadGuard<Option<F>, T>>
where
    F: AsType<T>,
{
    match OwnedRwLockReadGuard::try_map(maybe_file, |file| file.as_ref().expect("file").as_type()) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

fn check_write<FE: AsType<F>, F>(contents: &Option<FE>, state: &FileLockState) -> Result<usize> {
    let current = match state {
        FileLockState::Read(size) | FileLockState::Modified(size) => *size,
        _ => return Err(would_block("this file is not in the cache")),
    };
    if contents.as_ref().expect("file").as_type().is_none() {
        return Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        )));
    }
    Ok(current)
}

pub(crate) fn validate_size(actual: usize, bound: usize) -> Result<()> {
    if actual > bound {
        Err(invalid_data(
            "retained payload exceeds its admitted allocation bound",
        ))
    } else {
        Ok(())
    }
}

async fn delete_file(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
            // no-op
            Ok(())
        }
        Err(cause) => Err(cause),
    }
}

#[inline]
fn deleted() -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, "this file has been deleted")
}

#[inline]
fn invalid_data<E>(cause: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, cause)
}

#[inline]
fn would_block<E>(cause: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::WouldBlock, cause)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    use super::{persist, FileLoad, FileLockState, FileSave};

    fn unique_tmp_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("freqfs_test_file_{}", uuid::Uuid::new_v4()));
        path
    }

    #[derive(Clone)]
    struct Data {
        bytes: Vec<u8>,
        pause: Option<Arc<tokio::sync::Notify>>,
        fail: bool,
        unlink: Option<PathBuf>,
    }

    impl Data {
        fn new(bytes: &[u8]) -> Self {
            Self {
                bytes: bytes.to_vec(),
                pause: None,
                fail: false,
                unlink: None,
            }
        }
    }

    impl safecast::AsType<Data> for Data {
        fn as_type(&self) -> Option<&Data> {
            Some(self)
        }

        fn as_type_mut(&mut self) -> Option<&mut Data> {
            Some(self)
        }

        fn into_type(self) -> Option<Data> {
            Some(self)
        }
    }

    impl get_size::GetSize for Data {
        fn get_size(&self) -> usize {
            self.bytes.capacity()
        }
    }

    impl FileLoad for Data {
        async fn load_size(
            _: &std::path::Path,
            _: &mut fs::File,
            metadata: &std::fs::Metadata,
        ) -> crate::Result<usize> {
            usize::try_from(metadata.len()).map_err(std::io::Error::other)
        }

        async fn load(
            _: &std::path::Path,
            mut file: fs::File,
            metadata: std::fs::Metadata,
        ) -> crate::Result<Self> {
            use tokio::io::AsyncReadExt;
            let mut bytes = vec![0; metadata.len() as usize];
            file.read_exact(&mut bytes).await?;
            Ok(Self {
                bytes,
                pause: None,
                fail: false,
                unlink: None,
            })
        }
    }

    impl FileSave for Data {
        async fn save(&self, file: &mut fs::File) -> crate::Result<u64> {
            file.write_all(&self.bytes).await?;
            if let Some(path) = &self.unlink {
                fs::remove_file(path).await?;
            }
            if let Some(started) = &self.pause {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            if self.fail {
                return Err(std::io::Error::other("injected save failure"));
            }
            Ok(self.bytes.len() as u64)
        }
    }

    async fn overwrite_files(
        cached: bool,
    ) -> crate::Result<(PathBuf, super::FileLock<Data>, super::FileLock<Data>)> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        fs::write(path.join("a"), b"one").await?;
        fs::write(path.join("b"), b"two").await?;
        let root = crate::Cache::<Data>::new(16, Some(2), 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let (a, b) = {
            let dir = root.read().await;
            (
                dir.get_file("a").unwrap().clone(),
                dir.get_file("b").unwrap().clone(),
            )
        };
        if cached {
            a.read::<Data>().await?;
            b.read::<Data>().await?;
        }
        let (low, high) = if Arc::as_ptr(&a.state) < Arc::as_ptr(&b.state) {
            (a, b)
        } else {
            (b, a)
        };
        Ok((path, low, high))
    }

    #[tokio::test]
    async fn reciprocal_overwrites_acquire_states_in_one_order() -> crate::Result<()> {
        for cached in [false, true] {
            let (path, low, high) = overwrite_files(cached).await?;
            {
                let barrier = low.state.write().await;
                let reverse = high.overwrite(&low);
                let forward = low.overwrite(&high);
                futures::pin_mut!(reverse, forward);
                assert!(futures::poll!(&mut reverse).is_pending());
                // A copy waiting on the first state may not hold the second.
                assert!(high.state.try_write().is_ok());
                assert!(futures::poll!(&mut forward).is_pending());
                drop(barrier);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    futures::try_join!(reverse, forward)
                })
                .await
                .expect("reciprocal copies must complete")?;
            }
            let left = low.read::<Data>().await?.bytes.clone();
            let right = high.read::<Data>().await?.bytes.clone();
            assert_eq!(left, right);
            assert!(left == b"one" || left == b"two");
            drop(low);
            drop(high);
            fs::remove_dir_all(path).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_overwrites_release_states_and_capacity() -> crate::Result<()> {
        for cached in [false, true] {
            for reverse in [false, true] {
                let (path, low, high) = overwrite_files(cached).await?;
                let barrier = high.state.write().await;
                {
                    let copy = if reverse {
                        high.overwrite(&low)
                    } else {
                        low.overwrite(&high)
                    };
                    futures::pin_mut!(copy);
                    assert!(futures::poll!(&mut copy).is_pending());
                    assert!(low.state.try_write().is_err());
                }
                assert!(low.state.try_write().is_ok());
                drop(barrier);
                assert!(high.state.try_write().is_ok());
                // Both handle permits are available after cancellation.
                let left = low.read::<Data>().await?;
                let right = high.read::<Data>().await?;
                assert_ne!(left.bytes, right.bytes);
                assert_eq!(fs::read(path.join("a")).await?, b"one");
                assert_eq!(fs::read(path.join("b")).await?, b"two");
                drop(left);
                drop(right);
                drop(low);
                drop(high);
                fs::remove_dir_all(path).await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn overwrite_self_needs_no_locks_or_admission() -> crate::Result<()> {
        for cached in [false, true] {
            let (path, file, other) = overwrite_files(cached).await?;
            {
                let _state = file.state.write().await;
                let _first = file.cache.acquire_file_handle().await?;
                let _second = file.cache.acquire_file_handle().await?;
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    file.overwrite(&file.clone()),
                )
                .await
                .expect("self-copy must not acquire capacity or locks")?;
            }
            assert_eq!(fs::read(path.join("a")).await?, b"one");
            assert_eq!(fs::read(path.join("b")).await?, b"two");
            drop(file);
            drop(other);
            fs::remove_dir_all(path).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn buffered_writeback_and_eviction_have_no_durability_barriers() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(1024, None, 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let file = root
            .write()
            .await
            .create_file("data".into(), Data::new(b"old"), 3)
            .await?;
        root.sync().await?;
        *file.write::<Data>(3).await? = Data::new(b"new");
        file.clone().evict().unwrap().1.await?;
        assert!(file.contents.try_read().unwrap().is_none());
        assert_eq!(fs::read(path.join("data")).await?, b"new");
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_readers_observe_released_eviction_contents() -> crate::Result<()> {
        use futures::FutureExt;

        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(3, Some(2), 0, std::time::Duration::from_secs(3));
        let root = cache.load(path.clone())?;
        let file = root
            .write()
            .await
            .create_file("data".into(), Data::new(b"old"), 3)
            .await?;
        file.sync().await?;
        for _ in 0..256 {
            let eviction = file.clone().evict().unwrap().1;
            let reader = file.read::<Data>();
            futures::pin_mut!(reader);
            assert!(reader.as_mut().now_or_never().is_none());
            let eviction = tokio::spawn(eviction);
            assert_eq!(reader.await?.bytes, b"old");
            eviction.await.unwrap()?;
        }
        // Eviction failure or cancellation retains the resident data and state.
        for failure in [true, false] {
            let started = Arc::new(tokio::sync::Notify::new());
            let mut replacement = Data::new(b"new");
            replacement.fail = failure;
            replacement.pause = (!failure).then(|| Arc::clone(&started));
            *file.write::<Data>(3).await? = replacement;
            let eviction = file.clone().evict().unwrap().1;
            if failure {
                assert!(eviction.await.is_err());
            } else {
                let task = tokio::spawn(eviction);
                started.notified().await;
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
            assert!(matches!(
                *file.state.read().await,
                FileLockState::Modified(3)
            ));
            assert_eq!(
                file.contents.try_read().unwrap().as_ref().unwrap().bytes,
                b"new"
            );
            assert_eq!(file.read::<Data>().await?.bytes, b"new");
        }
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn reopening_defers_abandoned_replacement_cleanup() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir_all(&path).await?;
        fs::write(path.join("data"), b"old").await?;
        fs::write(path.join("data._freqfs"), b"incomplete").await?;
        let cache = crate::Cache::<Data>::new(4096, None, 0, std::time::Duration::from_secs(3));
        let dir = cache.load(path.clone())?;
        assert_eq!(dir.read().await.len(), 1);
        assert!(path.join("data._freqfs").exists());
        assert!(dir
            .write()
            .await
            .create_empty_file("reserved._freqfs".into(), Data::new(b"bad"))
            .await
            .is_err());
        dir.sync_deleted().await?;
        assert!(!path.join("data._freqfs").exists());
        assert_eq!(fs::read(path.join("data")).await?, b"old");
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn explicit_durable_sync_covers_eviction_and_repeated_calls() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(1024, Some(1), 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let file = root
            .write()
            .await
            .create_file("data".into(), Data::new(b"old"), 3)
            .await?;
        file.sync().await?;
        file.clone().evict().unwrap().1.await?;
        assert!(matches!(*file.state.read().await, FileLockState::Pending));
        file.sync_all().await?;
        file.sync_all().await?;
        *file.write::<Data>(3).await? = Data::new(b"new");
        file.clone().evict().unwrap().1.await?;
        root.sync_all().await?;
        assert_eq!(fs::read(path.join("data")).await?, b"new");
        root.write().await.delete("data").await;
        root.sync_all().await?;
        assert!(path.is_dir());
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn durable_sync_waits_for_an_active_writer() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(1024, Some(2), 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let file = root
            .write()
            .await
            .create_file("data".into(), Data::new(b"old"), 3)
            .await?;
        let mut writer = file.write::<Data>(3).await?;
        *writer = Data::new(b"new");
        let mut task = tokio::spawn({
            let file = file.clone();
            async move { file.sync_all().await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
                .await
                .is_err()
        );
        drop(writer);
        task.await.unwrap()?;
        assert_eq!(fs::read(path.join("data")).await?, b"new");
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn durable_directory_batches_file_publication() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(1024, Some(1), 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let nested = root.write().await.create_dir("nested".into())?;
        for name in ["a", "b"] {
            nested
                .write()
                .await
                .create_file(name.into(), Data::new(b"data"), 4)
                .await?;
        }
        root.sync_all().await?;
        root.sync_all().await?;
        let survivor = nested.read().await.get_file("b").unwrap().clone();
        *survivor.write::<Data>(4).await? = Data::new(b"next");
        nested.write().await.delete("a").await;
        nested.sync_deleted().await?;
        assert!(!path.join("nested/a").exists());
        assert_eq!(fs::read(path.join("nested/b")).await?, b"data");
        assert_eq!(survivor.read::<Data>().await?.bytes, b"next");
        root.write().await.delete("nested").await;
        root.sync_deleted().await?;
        assert!(!path.join("nested").exists());
        assert!(path.exists());
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[test]
    fn durable_replacement_syscall_failure_requires_reopen() -> crate::Result<()> {
        const CHILD: &str = "FREQFS_FSYNC_FAILURE_CHILD";
        if let Some(position) = std::env::var_os(CHILD) {
            let position: usize = position
                .to_str()
                .expect("UTF-8 child marker")
                .parse()
                .expect("numeric child marker");
            assert!(matches!(position, 1 | 2), "invalid child marker");
            return tokio::runtime::Builder::new_current_thread()
                .enable_all()
                // strace counts each syscall separately for each tracee. Keep
                // both sequential fsyncs on the same blocking worker thread.
                .max_blocking_threads(1)
                .build()?
                .block_on(durable_replacement_failure_probe(position));
        }

        for position in [1, 2] {
            let output = std::process::Command::new("strace")
                .args(["-f", "-y", "-e", "trace=fsync", "-e"])
                .arg(format!("inject=fsync:error=EIO:when={position}"))
                .arg(std::env::current_exe()?)
                .args([
                    "--exact", "file::tests::durable_replacement_syscall_failure_requires_reopen",
                    "--nocapture", "--test-threads=1",
                ])
                .env(CHILD, position.to_string())
                .output()
                .expect("mandatory syscall fault test requires strace and permission to trace child processes");
            let trace = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "fsync position {position}: {}\n{trace}",
                String::from_utf8_lossy(&output.stdout)
            );
            let mut injected = trace
                .lines()
                .filter(|line| line.contains("fsync(") && line.contains("(INJECTED)"));
            let failure = injected
                .next()
                .expect("strace must inject an fsync failure");
            assert!(
                injected.next().is_none(),
                "exactly one failure expected: {trace}"
            );
            assert!(
                failure.contains("EIO"),
                "unexpected injected error: {trace}"
            );
            assert!(
                failure.contains("freqfs_test_file_"),
                "unexpected fsync target: {trace}"
            );
            assert_eq!(
                failure.contains("._freqfs"),
                position == 1,
                "wrong publication phase: {trace}"
            );
        }
        Ok(())
    }

    async fn durable_replacement_failure_probe(position: usize) -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        fs::write(path.join("data"), b"old").await?;
        let cache = crate::Cache::<Data>::new(1024, None, 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let file = root.read().await.get_file("data").unwrap().clone();
        let error = file.replace_all(Data::new(b"new"), 3).await.unwrap_err();
        assert_eq!(error.raw_os_error(), Some(5), "expected Linux EIO: {error}");
        assert!(file.read::<Data>().await.is_err());
        assert!(file.clone().evict().is_none());
        let contents = fs::read(path.join("data")).await?;
        assert_eq!(contents, if position == 1 { b"old" } else { b"new" });
        let reopened = crate::Cache::<Data>::new(1024, None, 0, std::time::Duration::from_secs(1))
            .load(path.clone())?;
        let current = reopened.read().await.get_file("data").unwrap().clone();
        assert_eq!(current.read::<Data>().await?.bytes, contents);
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn reopened_file_can_be_evicted_for_durable_replacement() -> crate::Result<()> {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        fs::write(path.join("data"), b"before").await?;
        let cache = crate::Cache::<Data>::new(8, None, 0, std::time::Duration::from_secs(1));
        let root = cache.load(path.clone())?;
        let file = root.read().await.get_file("data").unwrap().clone();
        assert_eq!(file.read::<Data>().await?.bytes, b"before");

        // The old contents and replacement exceed capacity together. The
        // reopened file must participate in eviction to admit the replacement.
        file.replace_all(Data::new(b"next"), 4).await?;
        assert_eq!(file.read::<Data>().await?.bytes, b"next");
        assert_eq!(fs::read(path.join("data")).await?, b"next");
        fs::remove_dir_all(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn durable_replacement_excludes_eviction_and_fails_closed() -> crate::Result<()> {
        for failure in 0..3 {
            let path = unique_tmp_dir();
            fs::create_dir(&path).await?;
            let cache = crate::Cache::<Data>::new(1024, None, 0, std::time::Duration::from_secs(1));
            let root = cache.load(path.clone())?;
            let file = root
                .write()
                .await
                .create_file("data".into(), Data::new(b"old"), 3)
                .await?;
            file.sync_all().await?;
            assert!(file.replace_all(Data::new(b"new"), 2048).await.is_err());
            assert_eq!(file.read::<Data>().await?.bytes, b"old");
            assert!(!path.join("data._freqfs").exists());
            let started = Arc::new(tokio::sync::Notify::new());
            let mut replacement = Data::new(b"new");
            replacement.fail = failure == 0;
            replacement.unlink = (failure == 1).then(|| path.join("data._freqfs"));
            replacement.pause = (failure == 2).then(|| started.clone());
            if failure == 2 {
                let task = tokio::spawn({
                    let file = file.clone();
                    async move { file.replace_all(replacement, 3).await }
                });
                started.notified().await;
                assert_eq!(Arc::strong_count(&started), 2);
                assert!(file.clone().evict().is_none());
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(file.replace_all(replacement, 3).await.is_err());
            }
            assert!(file.clone().evict().is_none());
            assert!(file.read::<Data>().await.is_err());
            assert!(file.write::<Data>(3).await.is_err());
            assert!(file.sync_all().await.is_err());
            assert_eq!(fs::read(path.join("data")).await?, b"old");
            fs::remove_dir_all(path).await?;
        }
        Ok(())
    }

    struct PartialThenFail {
        bytes: Vec<u8>,
        kind: std::io::ErrorKind,
    }

    impl FileSave for PartialThenFail {
        async fn save(&self, file: &mut fs::File) -> crate::Result<u64> {
            file.write_all(&self.bytes).await?;
            Err(std::io::Error::new(self.kind, "intentional failure"))
        }
    }

    #[tokio::test]
    async fn persist_does_not_corrupt_existing_file_on_save_error() -> std::io::Result<()> {
        let tmp = unique_tmp_dir();
        fs::create_dir(&tmp).await?;

        let path = tmp.join("data.txt");
        fs::write(&path, b"original").await?;

        let err = persist(
            Arc::new(path.clone()),
            PartialThenFail {
                bytes: b"new".to_vec(),
                kind: std::io::ErrorKind::Other,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(fs::read(&path).await?, b"original");

        persist(Arc::new(path.clone()), Data::new(b"x")).await?;
        assert_eq!(fs::read(&path).await?, b"x");
        assert!(!path.with_extension("txt_freqfs").exists());

        let _ = fs::remove_dir_all(&tmp).await;
        Ok(())
    }
}
