use futures::{join, Future, TryFutureExt};
use safecast::AsType;
use std::convert::TryInto;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};
use tokio::fs;
use tokio::sync::{
    OwnedRwLockMappedWriteGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit,
    RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};

use super::cache::Cache;
use super::Result;

const TMP: &str = "_freqfs";

pub struct FileReadGuard<'a, F> {
    guard: RwLockReadGuard<'a, F>,
    _permit: OwnedSemaphorePermit,
}

pub struct FileReadGuardOwned<FE, F> {
    guard: OwnedRwLockReadGuard<Option<FE>, F>,
    _permit: OwnedSemaphorePermit,
}

pub struct FileWriteGuard<'a, F> {
    guard: RwLockMappedWriteGuard<'a, F>,
    _permit: OwnedSemaphorePermit,
}

pub struct FileWriteGuardOwned<FE, F> {
    guard: OwnedRwLockMappedWriteGuard<Option<FE>, F>,
    _permit: OwnedSemaphorePermit,
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

impl<'a, F> Deref for FileWriteGuard<'a, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, F> DerefMut for FileWriteGuard<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<FE, F> Deref for FileWriteGuardOwned<FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<FE, F> DerefMut for FileWriteGuardOwned<FE, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
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

impl<F: fmt::Debug> fmt::Debug for FileWriteGuard<'_, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, formatter)
    }
}

impl<FE, F: fmt::Debug> fmt::Debug for FileWriteGuardOwned<FE, F> {
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

impl<'a, F> FileDeref for FileWriteGuard<'a, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

impl<FE, F> FileDeref for FileWriteGuardOwned<FE, F> {
    type File = F;

    fn as_file(&self) -> &F {
        self.deref()
    }
}

/// Load a file-backed data structure.
#[trait_variant::make(Send)]
pub trait FileLoad: Send + Sync + Sized + 'static {
    /// Load this state from the given `file`.
    async fn load(path: &Path, file: fs::File, metadata: std::fs::Metadata) -> Result<Self>;
}

/// Write a file-backed data structure to the filesystem.
#[trait_variant::make(Send)]
pub trait FileSave: Send + Sync + Sized + 'static {
    /// Save this state to the given `file`.
    async fn save(&self, file: &mut fs::File) -> Result<u64>;
}

#[cfg(feature = "stream")]
impl<T> FileLoad for T
where
    T: destream::de::FromStream<Context = ()> + Send + Sync + 'static,
{
    async fn load(_path: &Path, file: fs::File, _metadata: std::fs::Metadata) -> Result<Self> {
        tbon::de::read_from((), file)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))
            .await
    }
}

#[cfg(feature = "stream")]
impl<T> FileSave for T
where
    T: for<'en> destream::en::ToStream<'en> + Send + Sync + 'static,
{
    async fn save(&self, file: &mut fs::File) -> Result<u64> {
        use futures::TryStreamExt;

        let encoded = tbon::en::encode(self)
            .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause))?;

        let mut reader = tokio_util::io::StreamReader::new(
            encoded
                .map_ok(bytes::Bytes::from)
                .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause)),
        );

        tokio::io::copy(&mut reader, file).await
    }
}

#[derive(Copy, Clone)]
enum FileLockState {
    Pending,
    Read(usize),
    Modified(usize),
    Deleted(bool),
}

impl FileLockState {
    fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }

    fn is_loaded(&self) -> bool {
        matches!(self, Self::Read(_) | Self::Modified(_))
    }

    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    fn upgrade(&mut self) {
        let size = match self {
            Self::Read(size) | Self::Modified(size) => *size,
            _ => unreachable!("upgrade a file not in the cache"),
        };

        *self = Self::Modified(size);
    }
}

/// A futures-aware read-write lock on a file
pub struct FileLock<FE> {
    cache: Arc<Cache<FE>>,
    path: Arc<PathBuf>,
    state: Arc<RwLock<FileLockState>>,
    contents: Arc<RwLock<Option<FE>>>,
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
    async fn load_reserved<F>(&self) -> Result<(usize, FE, crate::cache::Reservation<FE>)>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let (file, metadata, size) = open(&self.path, self.cache.capacity()).await?;
        let reservation = self.cache.reserve(size).await?;
        let entry = decode::<F, FE>(&self.path, file, metadata).await?;
        Ok((size, entry, reservation))
    }

    /// Create a new [`FileLock`].
    pub fn new<F>(cache: Arc<Cache<FE>>, path: PathBuf, contents: F, size: usize) -> Self
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
    pub fn load<F>(cache: Arc<Cache<FE>>, path: PathBuf) -> Self
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

    /// Replace the contents of this [`FileLock`] with those of the `other` [`FileLock`],
    /// without reading from the filesystem.
    pub async fn overwrite(&self, other: &Self) -> Result<()>
    where
        FE: Clone,
    {
        let (mut this, that) = join!(self.state.write(), other.state.read());

        let old_size = match &*this {
            FileLockState::Pending | FileLockState::Deleted(_) => 0,
            FileLockState::Read(size) | FileLockState::Modified(size) => *size,
        };

        let new_size = match &*that {
            FileLockState::Pending => {
                debug_assert!(other.path.exists());

                create_dir(self.path.parent().expect("file parent dir")).await?;

                match fs::copy(other.path.as_path(), self.path.as_path()).await {
                    Ok(_) => {}
                    Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
                        #[cfg(debug_assertions)]
                        let message = format!(
                            "tried to copy a file from a nonexistent source: {}",
                            other.path.display()
                        );

                        #[cfg(not(debug_assertions))]
                        let message = "tried to copy a file from a nonexistent source";

                        return Err(io::Error::new(io::ErrorKind::NotFound, message));
                    }
                    Err(cause) => return Err(cause),
                }

                *this = FileLockState::Pending;
                0
            }
            FileLockState::Deleted(_sync) => {
                *this = FileLockState::Deleted(true);
                0
            }
            FileLockState::Read(size) | FileLockState::Modified(size) => {
                *this = FileLockState::Modified(*size);
                *size
            }
        };

        if this.is_loaded() {
            let (mut this_data, that_data) = join!(self.contents.write(), other.contents.read());
            let that_data = that_data.as_ref().expect("file");
            *this_data = Some(FE::clone(that_data));
        }

        self.cache.resize(old_size, new_size).await?;

        Ok(())
    }

    /// Lock this file for reading.
    pub async fn read<F>(&self) -> Result<FileReadGuard<'_, F>>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.contents.try_write().expect("file contents");
            let (size, entry, reservation) = self.load_reserved::<F>().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.contents.read().await
        };

        read_type(guard).map(|guard| FileReadGuard {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read<F>(&self) -> Result<FileReadGuard<'_, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
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
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");

            let (size, entry, reservation) = self.load_reserved::<F>().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.contents.clone().read_owned().await
        };

        read_type_owned(guard).map(|guard| FileReadGuardOwned {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for reading synchronously if possible, otherwise return an error.
    pub fn try_read_owned<F>(&self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let state = self.state.try_read().map_err(would_block)?;

        match &*state {
            FileLockState::Pending => Err(would_block("this file is not in the cache")),
            FileLockState::Deleted(_sync) => Err(deleted()),
            FileLockState::Read(_size) | FileLockState::Modified(_size) => {
                self.cache.bump(&self.path, None);
                let guard = self
                    .contents
                    .clone()
                    .try_read_owned()
                    .map_err(would_block)?;

                read_type_owned(guard).map(|guard| FileReadGuardOwned {
                    guard,
                    _permit: permit,
                })
            }
        }
    }

    /// Lock this file for reading, without borrowing.
    pub async fn into_read<F>(self) -> Result<FileReadGuardOwned<FE, F>>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");
            let (size, entry, reservation) = self.load_reserved::<F>().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Read(size);
            *contents = Some(entry);

            contents.downgrade()
        } else {
            self.cache.bump(&self.path, None);
            self.contents.read_owned().await
        };

        read_type_owned(guard).map(|guard| FileReadGuardOwned {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for writing.
    pub async fn write<F>(&self) -> Result<FileWriteGuard<'_, F>>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self.contents.try_write().expect("file contents");
            let (size, entry, reservation) = self.load_reserved::<F>().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Modified(size);
            *contents = Some(entry);

            contents
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            self.contents.write().await
        };

        write_type(guard).map(|guard| FileWriteGuard {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for writing synchronously if possible, otherwise return an error.
    pub fn try_write<F>(&self) -> Result<FileWriteGuard<'_, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let mut state = self.state.try_write().map_err(would_block)?;

        if state.is_pending() {
            Err(would_block("this file is not in the cache"))
        } else if state.is_deleted() {
            Err(deleted())
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            let guard = self.contents.try_write().map_err(would_block)?;
            write_type(guard).map(|guard| FileWriteGuard {
                guard,
                _permit: permit,
            })
        }
    }

    /// Lock this file for writing.
    pub async fn write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        let permit = self.cache.acquire_file_handle().await?;
        let mut state = self.state.write().await;

        if state.is_deleted() {
            return Err(deleted());
        }

        let guard = if state.is_pending() {
            let mut contents = self
                .contents
                .clone()
                .try_write_owned()
                .expect("file contents");

            let (size, entry, reservation) = self.load_reserved::<F>().await?;
            reservation.commit();
            self.cache.bump(&self.path, None);

            *state = FileLockState::Modified(size);
            *contents = Some(entry);

            contents
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);
            self.contents.clone().write_owned().await
        };

        write_type_owned(guard).map(|guard| FileWriteGuardOwned {
            guard,
            _permit: permit,
        })
    }

    /// Lock this file for writing synchronously if possible, otherwise return an error.
    pub fn try_write_owned<F>(&self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        FE: AsType<F>,
    {
        let permit = self.cache.try_acquire_file_handle()?;
        let mut state = self.state.try_write().map_err(would_block)?;

        if state.is_pending() {
            Err(would_block("this file is not in the cache"))
        } else if state.is_deleted() {
            Err(deleted())
        } else {
            state.upgrade();
            self.cache.bump(&self.path, None);

            let guard = self
                .contents
                .clone()
                .try_write_owned()
                .map_err(would_block)?;

            write_type_owned(guard).map(|guard| FileWriteGuardOwned {
                guard,
                _permit: permit,
            })
        }
    }

    /// Lock this file for writing, without borrowing.
    pub async fn into_write<F>(self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: FileLoad,
        FE: AsType<F> + From<F>,
    {
        self.write_owned().await
    }

    /// Lock this file for writing synchronously, if possible, without borrowing.
    pub fn try_into_write<F>(self) -> Result<FileWriteGuardOwned<FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        self.try_write_owned()
    }

    /// Back up this file's contents to the filesystem.
    pub async fn sync(&self) -> Result<()>
    where
        FE: FileSave + Clone,
    {
        let mut state = self.state.write().await;

        let new_state = match &*state {
            FileLockState::Pending => FileLockState::Pending,
            FileLockState::Read(size) => FileLockState::Read(*size),
            FileLockState::Modified(old_size) => {
                #[cfg(feature = "logging")]
                log::trace!("sync modified file {}...", self.path.display());

                let contents = self.contents.read().await;
                let contents = contents.as_ref().cloned().expect("file");

                self.cache
                    .ensure_disk_capacity(&self.path, *old_size as u64)?;
                let new_size = persist(self.path.clone(), contents).await?;

                self.cache.resize(*old_size, new_size as usize).await?;
                FileLockState::Read(new_size as usize)
            }
            FileLockState::Deleted(needs_sync) => {
                if *needs_sync && self.path.exists() {
                    delete_file(&self.path).await?;
                }

                FileLockState::Deleted(false)
            }
        };

        *state = new_state;

        Ok(())
    }

    pub(crate) async fn delete(&self, file_only: bool) {
        let mut file_state = self.state.write().await;

        let size = match &*file_state {
            FileLockState::Pending => 0,
            FileLockState::Read(size) => *size,
            FileLockState::Modified(size) => *size,
            FileLockState::Deleted(_) => return,
        };

        self.cache.remove(&self.path, size);

        *file_state = FileLockState::Deleted(file_only);
    }

    pub(crate) fn evict(self) -> Option<(usize, impl Future<Output = Result<()>> + Send)>
    where
        FE: FileSave + Clone + 'static,
    {
        // if this file is in use, don't evict it
        let mut state = self.state.try_write_owned().ok()?;

        let (old_size, contents, modified) = match &*state {
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
            FileLockState::Deleted(_) => unreachable!("evict a deleted file"),
        };

        let eviction = async move {
            if modified {
                let contents = contents.as_ref().cloned().expect("file");
                self.cache
                    .ensure_disk_capacity(&self.path, old_size as u64)?;
                persist(self.path.clone(), contents).await?;
            }

            *state = FileLockState::Pending;
            drop(state);
            drop(contents);

            // Never acquire Cache::state while holding a file or contents guard.
            self.cache.resize(old_size, 0).await?;
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

async fn open(path: &Path, capacity: usize) -> Result<(fs::File, std::fs::Metadata, usize)> {
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
    let size = match metadata.len().try_into() {
        Ok(size) => size,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "this file is too large to load into the cache",
            ))
        }
    };
    if size > capacity {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "this file exceeds the configured cache capacity",
        ));
    }

    Ok((file, metadata, size))
}

async fn decode<F: FileLoad, FE: From<F>>(
    path: &Path,
    file: fs::File,
    metadata: std::fs::Metadata,
) -> Result<FE> {
    F::load(path, file, metadata).await.map(FE::from)
}

// TODO: use borrowed rather than owned parameters
// when https://github.com/rust-lang/rust/issues/100013 is resolved
async fn persist<FE: FileSave>(path: Arc<PathBuf>, file: FE) -> Result<u64> {
    let tmp = if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
        path.with_extension(format!("{}_{}", ext, TMP))
    } else {
        path.with_extension(TMP)
    };

    let size = {
        let mut tmp_file = if tmp.exists() {
            fs::OpenOptions::new()
                .truncate(true)
                .write(true)
                .open(tmp.as_path())
                .await?
        } else {
            let parent = tmp.parent().expect("dir");
            create_dir(parent).await?;

            match fs::File::create(tmp.as_path()).await {
                Ok(file) => file,
                Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
                    // The parent directory may have been removed between
                    // create_dir and File::create (TOCTOU race).
                    // Retry directory creation then create the file.
                    create_dir(parent).await?;
                    fs::File::create(tmp.as_path())
                        .map_err(|cause| {
                            io::Error::new(
                                cause.kind(),
                                format!("failed to create tmp file: {}", cause),
                            )
                        })
                        .await?
                }
                Err(cause) => {
                    return Err(io::Error::new(
                        cause.kind(),
                        format!("failed to create tmp file: {}", cause),
                    ))
                }
            }
        };

        assert!(tmp.exists());
        assert!(!tmp.is_dir());

        file.save(&mut tmp_file)
            .map_err(|cause| {
                io::Error::new(cause.kind(), format!("failed to save tmp file: {}", cause))
            })
            .await?
    };

    tokio::fs::rename(tmp.as_path(), path.as_path())
        .map_err(|cause| {
            io::Error::new(
                cause.kind(),
                format!("failed to rename tmp file: {}", cause),
            )
        })
        .await?;

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

#[inline]
fn write_type<F, T>(maybe_file: RwLockWriteGuard<Option<F>>) -> Result<RwLockMappedWriteGuard<T>>
where
    F: AsType<T>,
{
    match RwLockWriteGuard::try_map(maybe_file, |file| {
        file.as_mut().expect("file").as_type_mut()
    }) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
    }
}

#[inline]
fn write_type_owned<F, T>(
    maybe_file: OwnedRwLockWriteGuard<Option<F>>,
) -> Result<OwnedRwLockMappedWriteGuard<Option<F>, T>>
where
    F: AsType<T>,
{
    match OwnedRwLockWriteGuard::try_map(maybe_file, |file| {
        file.as_mut().expect("file").as_type_mut()
    }) {
        Ok(file) => Ok(file),
        Err(_) => Err(invalid_data(format!(
            "invalid file type, expected {}",
            std::any::type_name::<F>()
        ))),
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
    use super::{persist, FileSave};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    fn unique_tmp_dir() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("freqfs_test_file_{}", uuid::Uuid::new_v4()));
        path
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

        let _ = fs::remove_dir_all(&tmp).await;
        Ok(())
    }
}
