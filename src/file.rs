use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use futures::Future;
use get_size::GetSize;
use safecast::AsType;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use super::cache::{BorrowedRelease, Cache, OwnedRelease, Release};
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
    crate::IntegrityError::mark(io::Error::other(
        "file ownership invalidated; reopen the cache",
    ))
}

pub(crate) async fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path).await?.sync_all().await
}

pub struct FileReadGuard<'a, F> {
    guard: RwLockReadGuard<'a, F>,
    _release: BorrowedRelease<'a>,
}

pub struct FileReadGuardOwned<FE, F> {
    guard: OwnedRwLockReadGuard<Option<FE>, F>,
    _cache: OwnedRelease<FE>,
}

/// A mutable payload. Dropping the guard accounts its actual retained allocation.
pub struct FileWriteGuard<'a, FE: GetSize, F> {
    guard: RwLockWriteGuard<'a, Option<FE>>,
    state: RwLockWriteGuard<'a, FileLockState>,
    previous: usize,
    _payload: PhantomData<F>,
    cache: Release<&'a Cache<FE>>,
}

pub struct FileWriteGuardOwned<FE: GetSize, F> {
    guard: OwnedRwLockWriteGuard<Option<FE>>,
    state: OwnedRwLockWriteGuard<FileLockState>,
    previous: usize,
    _payload: PhantomData<F>,
    cache: OwnedRelease<FE>,
}

fn finish_write<FE: GetSize>(
    contents: &mut Option<FE>,
    state: &mut FileLockState,
    cache: &Cache<FE>,
    previous: usize,
) {
    let actual = contents.as_ref().expect("file").get_size();

    *state = FileLockState::Modified(actual);
    cache.reconcile(previous, actual);
}

impl<FE: GetSize, F> Drop for FileWriteGuard<'_, FE, F> {
    fn drop(&mut self) {
        finish_write(
            &mut self.guard,
            &mut self.state,
            self.cache.0,
            self.previous,
        );
    }
}

impl<FE: GetSize, F> Drop for FileWriteGuardOwned<FE, F> {
    fn drop(&mut self) {
        finish_write(
            &mut self.guard,
            &mut self.state,
            &self.cache.0,
            self.previous,
        );
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
    /// Decode this state once from the given `file`.
    ///
    /// The cache admits its actual `GetSize` before publication. Decoding and
    /// waiting for admission may retain temporary memory outside the cache target;
    /// adapters remain responsible for structural decoding limits.
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
    Deleted,
    Failed,
}

impl FileLockState {
    fn check_available(&self) -> Result<()> {
        match self {
            Self::Failed => Err(interrupted()),
            Self::Deleted => Err(deleted()),
            _ => Ok(()),
        }
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
    pub(crate) fn evict(
        &self,
        cache: &Arc<Cache<FE>>,
    ) -> Option<(usize, impl Future<Output = Result<()>> + Send)>
    where
        FE: FileSave + 'static,
    {
        // if this file is in use, don't evict it
        let state = self.state.clone().try_write_owned().ok()?;

        let (old_size, contents, modified) = match &*state {
            FileLockState::Pending => {
                // in this case there's nothing to evict
                return None;
            }
            FileLockState::Read(size) => {
                let contents = self.contents.clone().try_write_owned().ok()?;
                (*size, contents, false)
            }
            FileLockState::Modified(size) => {
                let contents = self.contents.clone().try_write_owned().ok()?;
                (*size, contents, true)
            }
            FileLockState::Failed => return None,
            FileLockState::Deleted => unreachable!("evict a deleted file"),
        };

        // Only writeback needs I/O admission; clean eviction just releases memory.
        let permit = if modified {
            Some(cache.try_acquire_file_handle().ok()?)
        } else {
            None
        };

        let cache = Arc::clone(cache);
        let path = self.path.clone();
        let eviction = async move {
            let _release = cache.on_release();
            let _permit = permit;
            let mut state = state;
            let mut contents = contents;
            if modified {
                let contents = contents.as_ref().expect("file");
                cache.ensure_disk_space(&path)?;
                persist_with(path, contents, false).await?;
            }

            *contents = None;
            *state = FileLockState::Pending;
            // Pending readers require exclusive contents access. Release it before
            // publishing Pending by unlocking state, including to other workers.
            drop(contents);
            drop(state);

            // Shrinking accounting cannot suspend. Release native guards first.
            cache.release(old_size);
            Ok(())
        };

        Some((old_size, eviction))
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
            state: Arc::new(RwLock::new(FileLockState::Deleted)),
            contents: Arc::new(RwLock::new(None)),
        }
    }

    async fn load_reserved(
        cache: &Arc<Cache<FE>>,
        path: &Path,
    ) -> Result<(usize, FE, crate::cache::Reservation<FE>)>
    where
        FE: FileLoad,
    {
        let permit = cache.acquire_file_handle().await?;
        let (file, metadata) = open(path).await?;
        // This decoder reads an existing stored entry, not an application body.
        let entry = FE::load(path, file, metadata)
            .await
            .map_err(crate::IntegrityError::from_read)?;
        let actual = entry.get_size();
        drop(permit);

        let reservation = cache.reserve(actual).await?;
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

        let _release = Release(&*self.cache);
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
        let permit = self.cache.acquire_file_handle().await?;
        match *that {
            FileLockState::Pending => {
                self.cache.ensure_disk_space(&self.path)?;
                create_dir(self.path.parent().expect("file parent dir")).await?;
                fs::copy(other.path.as_path(), self.path.as_path()).await?;
                *contents = None;
                *this = FileLockState::Pending;
                self.cache.release(old_size);
            }
            FileLockState::Read(_) | FileLockState::Modified(_) => {
                // Keep the old destination until the clone is admitted.
                let source = other.contents.read().await;
                let value = source.as_ref().expect("file").clone();
                let actual = value.get_size();
                drop(permit);
                let reservation = self.cache.reserve(actual).await?;
                *contents = Some(value);
                *this = FileLockState::Modified(actual);
                reservation.commit();
                self.cache.release(old_size);
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
        // Preserve pull-driven cooperation before acquiring exclusive state.
        tokio::task::consume_budget().await;
        let release = self.cache.on_release();
        // A ready cached read must not yield with exclusive state access.
        // Async lock acquisition may consume its cooperative budget after locking.
        let mut state = match self.state.try_write() {
            Ok(state) => state,
            Err(_) => self.state.write().await,
        };

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self.contents.try_write().expect("file contents");
            let (size, entry, reservation) = Self::load_reserved(&self.cache, &self.path).await?;
            reservation.commit();
            self.cache.bump(&self.path);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path);
            // Do not cooperatively suspend a cached read while retaining the
            // exclusive state guard: another consumer may drive the next read
            // before polling this buffered future again.
            match self.contents.try_read() {
                Ok(contents) => contents,
                Err(_) => self.contents.read().await,
            }
        };

        let guard = read_type(guard)?;
        Ok(FileReadGuard {
            guard,
            _release: release,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read<F>(&self) -> Result<FileReadGuard<'_, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let release = self.cache.on_release();
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted => Err(deleted()),
            FileLockState::Failed => Err(interrupted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path);
                let guard = self.contents.try_read().map_err(would_block)?;
                let guard = read_type(guard)?;
                Ok(FileReadGuard {
                    guard,
                    _release: release,
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
        tokio::task::consume_budget().await;
        let cache = Release(Arc::clone(&self.cache));
        // A ready cached read must not yield with exclusive state access.
        // Async lock acquisition may consume its cooperative budget after locking.
        let mut state = match self.state.try_write() {
            Ok(state) => state,
            Err(_) => self.state.write().await,
        };

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");

            let (size, entry, reservation) = Self::load_reserved(&cache.0, &self.path).await?;
            reservation.commit();
            cache.0.bump(&self.path);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            cache.0.bump(&self.path);
            match self.contents.clone().try_read_owned() {
                Ok(contents) => contents,
                Err(_) => self.contents.clone().read_owned().await,
            }
        };

        let guard = read_type_owned(guard)?;
        Ok(FileReadGuardOwned {
            guard,
            _cache: cache,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let cache = Release(Arc::clone(&self.cache));
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted => Err(deleted()),
            FileLockState::Failed => Err(interrupted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                cache.0.bump(&self.path);
                let guard = self
                    .contents
                    .clone()
                    .try_read_owned()
                    .map_err(would_block)?;

                let guard = read_type_owned(guard)?;
                Ok(FileReadGuardOwned {
                    guard,
                    _cache: cache,
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
        tokio::task::consume_budget().await;
        let cache = Release(self.cache);
        // A ready cached read must not yield with exclusive state access.
        // Async lock acquisition may consume its cooperative budget after locking.
        let mut state = match self.state.try_write() {
            Ok(state) => state,
            Err(_) => self.state.write().await,
        };

        state.check_available()?;

        let guard = if state.is_pending() {
            let mut contents = self.contents.try_write_owned().expect("file contents");
            let (size, entry, reservation) = Self::load_reserved(&cache.0, &self.path).await?;
            reservation.commit();
            cache.0.bump(&self.path);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            cache.0.bump(&self.path);
            match self.contents.clone().try_read_owned() {
                Ok(contents) => contents,
                Err(_) => self.contents.read_owned().await,
            }
        };

        let guard = read_type_owned(guard)?;
        Ok(FileReadGuardOwned {
            guard,
            _cache: cache,
        })
    }

    /// Lock this file for mutation; account retained allocation when the guard drops.
    pub async fn write<F>(&self) -> Result<FileWriteGuard<'_, FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let release = Release(&*self.cache);
        let mut state = self.state.write().await;
        state.check_available()?;
        let mut guard = self.contents.write().await;
        if state.is_pending() {
            let (size, entry, reservation) = Self::load_reserved(&self.cache, &self.path).await?;
            *guard = Some(entry);
            *state = FileLockState::Read(size);
            reservation.commit();
        }

        let current = check_write::<FE, F>(&guard, &state)?;
        self.cache.bump(&self.path);

        Ok(FileWriteGuard {
            guard,
            state,
            cache: release,
            previous: current,
            _payload: PhantomData,
        })
    }

    /// Lock a cached file immediately, or return a lock or availability error.
    pub fn try_write<F>(&self) -> Result<FileWriteGuard<'_, FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        let release = Release(&*self.cache);
        let state = self.state.try_write().map_err(would_block)?;
        state.check_available()?;
        let guard = self.contents.try_write().map_err(would_block)?;

        let current = check_write::<FE, F>(&guard, &state)?;
        self.cache.bump(&self.path);

        Ok(FileWriteGuard {
            guard,
            state,
            cache: release,
            previous: current,
            _payload: PhantomData,
        })
    }

    /// Lock this file for owned mutation; account retained allocation on guard drop.
    pub async fn write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        Self::write_owned_inner(
            self.cache.clone(),
            &self.path,
            self.state.clone(),
            self.contents.clone(),
        )
        .await
    }

    async fn write_owned_inner<F>(
        cache: Arc<Cache<FE>>,
        path: &PathBuf,
        state: Arc<RwLock<FileLockState>>,
        contents: Arc<RwLock<Option<FE>>>,
    ) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        let cache = Release(cache);
        let mut state = state.write_owned().await;
        state.check_available()?;
        let mut guard = contents.write_owned().await;
        if state.is_pending() {
            let (size, entry, reservation) = Self::load_reserved(&cache.0, path).await?;
            *guard = Some(entry);
            *state = FileLockState::Read(size);
            reservation.commit();
        }

        let current = check_write::<FE, F>(&guard, &state)?;
        cache.0.bump(path);

        Ok(FileWriteGuardOwned {
            guard,
            state,
            cache,
            previous: current,
            _payload: PhantomData,
        })
    }

    /// Lock a cached file immediately, or return a lock or availability error.
    pub fn try_write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        FE: GetSize + AsType<F>,
    {
        Self::try_write_owned_inner(
            self.cache.clone(),
            &self.path,
            self.state.clone(),
            self.contents.clone(),
        )
    }

    fn try_write_owned_inner<F>(
        cache: Arc<Cache<FE>>,
        path: &PathBuf,
        state: Arc<RwLock<FileLockState>>,
        contents: Arc<RwLock<Option<FE>>>,
    ) -> Result<FileWriteGuardOwned<FE, F>>
    where
        FE: GetSize + AsType<F>,
    {
        let cache = Release(cache);
        let state = state.try_write_owned().map_err(would_block)?;
        state.check_available()?;
        let guard = contents.try_write_owned().map_err(would_block)?;

        let current = check_write::<FE, F>(&guard, &state)?;
        cache.0.bump(path);

        Ok(FileWriteGuardOwned {
            guard,
            state,
            cache,
            previous: current,
            _payload: PhantomData,
        })
    }

    /// Lock this file for mutation without borrowing the file handle.
    pub async fn into_write<F>(self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F> + From<F>,
    {
        Self::write_owned_inner(self.cache, &self.path, self.state, self.contents).await
    }

    /// Admit and lock this file for mutation immediately without borrowing.
    pub fn try_into_write<F>(self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        Self::try_write_owned_inner(self.cache, &self.path, self.state, self.contents)
    }

    /// Write buffered contents to the filesystem without a durability barrier.
    /// Deleted handles return `NotFound`; their directory owns native removal.
    pub async fn sync(&self) -> Result<()>
    where
        FE: FileSave,
    {
        let _release = self.cache.on_release();
        let mut state = self.state.write().await;
        state.check_available()?;
        let _permit = self.cache.acquire_file_handle().await?;
        self.sync_locked(&mut state).await
    }

    async fn sync_locked(&self, state: &mut FileLockState) -> Result<()>
    where
        FE: FileSave,
    {
        let new_state = match &*state {
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
            FileLockState::Deleted | FileLockState::Failed => {
                unreachable!("sync requires available file state")
            }
        };

        *state = new_state;

        Ok(())
    }

    /// Make this file's contents and its directory entry durable, including prior eviction.
    /// Deleted handles return `NotFound`; their directory owns native removal.
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
        let _release = self.cache.on_release();
        let mut state = self.state.write().await;
        state.check_available()?;
        let _permit = self.cache.acquire_file_handle().await?;
        self.sync_locked(&mut state).await?;
        // Retain exclusive state through writeback and the durability barriers.
        fs::OpenOptions::new()
            .write(true)
            .open(self.path())
            .await?
            .sync_all()
            .await?;
        // A prior subtree barrier may have synchronized contents before failing
        // on its directory. Always complete publication for standalone calls.
        if parent {
            sync_directory(self.path.parent().expect("file parent")).await?;
        }

        Ok(())
    }

    /// Atomically publish a durable replacement, admitting its actual retained size.
    /// Once publication begins,
    /// error or cancellation requires reopening; access and eviction fail closed.
    /// Encoding borrows the supplied replacement under exclusive ownership.
    pub async fn replace_all(&self, value: FE) -> Result<()>
    where
        FE: FileSave + GetSize,
    {
        let actual = value.get_size();
        self.cache.validate_size(actual)?;
        let _release = self.cache.on_release();
        let mut state = self.state.write().await;
        state.check_available()?;
        let old_size = match *state {
            FileLockState::Read(size) | FileLockState::Modified(size) => size,
            _ => 0,
        };

        let mut contents = self.contents.write().await;
        let reservation = if actual > old_size {
            Some(self.cache.reserve(actual - old_size).await?)
        } else {
            None
        };

        let _permit = self.cache.acquire_file_handle().await?;
        self.cache.ensure_disk_space(&self.path)?;
        *state = FileLockState::Failed;
        persist_with(self.path.clone(), &value, true)
            .await
            .map_err(crate::IntegrityError::mark)?;
        *contents = Some(value);
        if let Some(reservation) = reservation {
            reservation.commit();
        }
        *state = FileLockState::Read(actual);
        self.cache.release(old_size.saturating_sub(actual));
        Ok(())
    }

    // Only the owning directory's retained tombstone may remove the native name.
    pub(crate) async fn remove_from_disk(&self) -> Result<()>
    where
        FE: Send + Sync,
    {
        let _release = self.cache.on_release();
        let state = self.state.write().await;
        match *state {
            FileLockState::Failed => return Err(interrupted()),
            FileLockState::Deleted => {}
            _ => unreachable!("native removal requires a directory tombstone"),
        }

        let _permit = self.cache.acquire_file_handle().await?;
        delete_file(&self.path).await
    }

    pub(crate) async fn delete(&self) {
        let mut file_state = self.state.write().await;

        let size = match &*file_state {
            FileLockState::Failed => return,
            FileLockState::Pending => 0,
            FileLockState::Read(size) => *size,
            FileLockState::Modified(size) => *size,
            FileLockState::Deleted => return,
        };

        *self.contents.write().await = None;
        self.cache.remove(&self.path, size);
        *file_state = FileLockState::Deleted;
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
        }?;

        let size = file.save(&mut tmp_file).await?;
        // Complete Tokio's buffered writes before publishing the name. This is
        // writeback, not a durability barrier, and also propagates delayed errors.
        tmp_file.flush().await?;
        if durable {
            tmp_file.sync_all().await?;
        }
        size
    };

    tokio::fs::rename(tmp.as_path(), path.as_path()).await?;

    if durable {
        sync_directory(path.parent().expect("file parent")).await?;
    }

    Ok(size)
}

async fn create_dir(path: &Path) -> Result<()> {
    tokio::fs::create_dir_all(path).await
}

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
#[path = "../tests/unit/file.rs"]
mod tests;
