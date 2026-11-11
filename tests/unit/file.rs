use std::path::PathBuf;
use std::sync::Arc;

use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::{persist_with, FileLoad, FileLockState, FileSave};

async fn persist<FE: FileSave>(path: Arc<PathBuf>, file: FE) -> crate::Result<u64> {
    persist_with(path, &file, false).await
}

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
    error: Option<fn() -> std::io::Error>,
    observation: Option<Arc<SaveObservation>>,
}

#[derive(Default)]
struct SaveObservation {
    attempts: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

impl Data {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            pause: None,
            fail: false,
            unlink: None,
            error: None,
            observation: None,
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
            error: None,
            observation: None,
        })
    }
}

impl FileSave for Data {
    async fn save(&self, file: &mut fs::File) -> crate::Result<u64> {
        if let Some(observation) = &self.observation {
            observation
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            observation.changed.notify_one();
        }
        if let Some(error) = self.error {
            return Err(error());
        }
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

#[derive(Debug)]
struct SaveError;

impl std::fmt::Display for SaveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("typed save failure")
    }
}

impl std::error::Error for SaveError {}

fn marked_save_error() -> std::io::Error {
    crate::IntegrityError::invalid_data(SaveError)
}

fn native_save_error() -> std::io::Error {
    std::io::Error::from_raw_os_error(13)
}

fn assert_save_error(error: &std::io::Error, marked: bool) {
    assert_eq!(crate::IntegrityError::is_marked(error), marked);
    if marked {
        let marker = error.get_ref().unwrap();
        let cause = marker
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert!(cause.get_ref().unwrap().is::<SaveError>());
    } else {
        assert_eq!(error.raw_os_error(), Some(13));
    }
}

#[tokio::test]
async fn writeback_preserves_typed_and_native_errors() -> crate::Result<()> {
    for (fault, marked) in [
        (marked_save_error as fn() -> std::io::Error, true),
        (native_save_error, false),
    ] {
        let path = unique_tmp_dir();
        fs::create_dir(&path).await?;
        let cache = crate::Cache::<Data>::new(1, Some(1), 0, std::time::Duration::from_secs(3));
        let root = cache.clone().load(path)?;
        let mut payload = Data::new(&[1]);
        payload.error = Some(fault);
        let file = root
            .write()
            .await
            .create_file("fault".into(), payload)
            .await?;
        assert_save_error(&file.sync().await.unwrap_err(), marked);
        assert_save_error(&file.sync_all().await.unwrap_err(), marked);
        assert_save_error(
            &file.cached().evict(&cache).unwrap().1.await.unwrap_err(),
            marked,
        );
        assert_save_error(&cache.reserve(1).await.err().unwrap(), marked);
        drop(cache.try_acquire_file_handle()?);
        assert_eq!(&file.read::<Data>().await?.bytes, &[1]);
        root.write().await.truncate_and_sync().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cold_copy_respects_disk_reserve_before_mutation() -> crate::Result<()> {
    let path = unique_tmp_dir();
    fs::create_dir(&path).await?;
    fs::write(path.join("source"), [7]).await?;
    fs::write(path.join("destination"), [3]).await?;
    let root = crate::Cache::<Data>::new(64, Some(1), u64::MAX, std::time::Duration::from_secs(3))
        .load(path.clone())?;
    let source = root.read().await.get_file("source").unwrap().clone();
    for name in ["destination", "absent"] {
        assert_eq!(
            root.write()
                .await
                .copy_file_from(name.into(), &source)
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::StorageFull
        );
    }
    assert_eq!(fs::read(path.join("destination")).await?, [3]);
    assert!(!path.join("absent").exists());
    assert!(!root.read().await.contains("absent"));
    root.write().await.truncate_and_sync().await?;
    Ok(())
}

#[tokio::test]
async fn synchronization_and_eviction_share_one_admission_limit() -> crate::Result<()> {
    use futures::FutureExt;

    let path = unique_tmp_dir();
    fs::create_dir(&path).await?;
    let cache = crate::Cache::<Data>::new(64, Some(1), 0, std::time::Duration::from_secs(3));
    let root = cache.clone().load(path)?;
    let first_started = Arc::new(tokio::sync::Notify::new());
    let second_started = Arc::new(tokio::sync::Notify::new());
    let mut a = Data::new(&[1]);
    a.pause = Some(first_started.clone());
    let mut b = Data::new(&[2]);
    b.pause = Some(second_started.clone());
    let first = root.write().await.create_file("first".into(), a).await?;
    let second = root.write().await.create_file("second".into(), b).await?;
    let mut initial = Box::pin(first.sync());
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::select! {
            _ = first_started.notified() => {}
            result = &mut initial => panic!("expected blocked save, got {result:?}"),
        }
    })
    .await
    .unwrap();
    assert!(cache.try_acquire_file_handle().is_err());
    assert!(second.cached().evict(&cache).is_none());
    let mut waiting = Box::pin(second.sync_all());
    assert!(futures::poll!(&mut waiting).is_pending());
    assert!(second_started.notified().now_or_never().is_none());
    drop(initial);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::select! {
            _ = second_started.notified() => {}
            result = &mut waiting => panic!("expected blocked second save, got {result:?}"),
        }
    })
    .await
    .unwrap();
    assert!(cache.try_acquire_file_handle().is_err());
    drop(waiting);
    drop(cache.try_acquire_file_handle()?);
    assert!(first.state.try_write().is_ok());
    assert!(second.state.try_write().is_ok());
    root.write().await.truncate_and_sync().await?;
    Ok(())
}

#[tokio::test]
async fn failed_oversized_eviction_stops_until_new_demand() -> crate::Result<()> {
    use std::sync::atomic::Ordering;

    let path = unique_tmp_dir();
    fs::create_dir(&path).await?;
    let cache = crate::Cache::<Data>::new(1, Some(1), 0, std::time::Duration::from_secs(3));
    let root = cache.clone().load(path)?;
    let observation = Arc::new(SaveObservation::default());
    let mut data = Data::new(&[1]);
    data.error = Some(marked_save_error);
    data.observation = Some(observation.clone());
    let file = root.write().await.create_file("grown".into(), data).await?;
    file.write::<Data>().await?.bytes = vec![1, 2];
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        observation.changed.notified(),
    )
    .await
    .unwrap();
    crate::cache::tests::cleanup_finished(&cache).await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(observation.attempts.load(Ordering::SeqCst), 1);
    assert_save_error(&cache.reserve(1).await.err().unwrap(), true);
    assert_eq!(observation.attempts.load(Ordering::SeqCst), 1);
    assert_save_error(&cache.reserve(1).await.err().unwrap(), true);
    assert_eq!(observation.attempts.load(Ordering::SeqCst), 2);
    drop(cache.try_acquire_file_handle()?);
    root.write().await.truncate_and_sync().await?;
    Ok(())
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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    root.sync().await?;
    *file.write::<Data>().await? = Data::new(b"new");
    file.cached().evict(&file.cache).unwrap().1.await?;
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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    file.sync().await?;
    for _ in 0..256 {
        let eviction = file.cached().evict(&file.cache).unwrap().1;
        let reader = file.read::<Data>();
        futures::pin_mut!(reader);
        assert!(reader.as_mut().now_or_never().is_none());
        let eviction = tokio::spawn(eviction);
        let read = reader.await?;
        assert_eq!(read.bytes, b"old");
        eviction.await.unwrap()?;
        // Keep the loaded value pinned until any pressure-triggered cleanup ends,
        // so the next manual eviction does not compete with the cache worker.
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            crate::cache::tests::cleanup_finished(&file.cache),
        )
        .await
        .unwrap();
        drop(read);
    }
    // Eviction failure or cancellation retains the resident data and state.
    for failure in [true, false] {
        let started = Arc::new(tokio::sync::Notify::new());
        let mut replacement = Data::new(b"new");
        replacement.fail = failure;
        replacement.pause = (!failure).then(|| Arc::clone(&started));
        *file.write::<Data>().await? = replacement;
        let eviction = file.cached().evict(&file.cache).unwrap().1;
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
        .create_file("reserved._freqfs".into(), Data::new(b"bad"))
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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    file.sync().await?;
    file.cached().evict(&file.cache).unwrap().1.await?;
    assert!(matches!(*file.state.read().await, FileLockState::Pending));
    file.sync_all().await?;
    file.sync_all().await?;
    *file.write::<Data>().await? = Data::new(b"new");
    file.cached().evict(&file.cache).unwrap().1.await?;
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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    let mut writer = file.write::<Data>().await?;
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
            .create_file(name.into(), Data::new(b"data"))
            .await?;
    }
    root.sync_all().await?;
    root.sync_all().await?;
    let survivor = nested.read().await.get_file("b").unwrap().clone();
    *survivor.write::<Data>().await? = Data::new(b"next");
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
    let error = file.replace_all(Data::new(b"new")).await.unwrap_err();
    assert!(crate::IntegrityError::is_marked(&error));
    let cause = error
        .get_ref()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.raw_os_error(), Some(5), "expected Linux EIO: {error}");
    assert!(file.read::<Data>().await.is_err());
    assert!(file.cached().evict(&file.cache).is_none());
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
    file.replace_all(Data::new(b"next")).await?;
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
            .create_file("data".into(), Data::new(b"old"))
            .await?;
        file.sync_all().await?;
        assert!(file.replace_all(Data::new(&[0; 2048])).await.is_err());
        assert_eq!(file.read::<Data>().await?.bytes, b"old");
        // Shrinking a resident oversized entry needs no additional admission.
        *file.write::<Data>().await? = Data::new(&[0; 2048]);
        file.replace_all(Data::new(b"old")).await?;
        assert!(!path.join("data._freqfs").exists());
        let started = Arc::new(tokio::sync::Notify::new());
        let mut replacement = Data::new(b"new");
        replacement.fail = failure == 0;
        replacement.unlink = (failure == 1).then(|| path.join("data._freqfs"));
        replacement.pause = (failure == 2).then(|| started.clone());
        if failure == 2 {
            let task = tokio::spawn({
                let file = file.clone();
                async move { file.replace_all(replacement).await }
            });
            started.notified().await;
            assert_eq!(Arc::strong_count(&started), 2);
            assert!(file.cached().evict(&file.cache).is_none());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            let error = file.replace_all(replacement).await.unwrap_err();
            assert!(crate::IntegrityError::is_marked(&error));
        }
        assert!(file.cached().evict(&file.cache).is_none());
        assert!(crate::IntegrityError::is_marked(
            &file.read::<Data>().await.err().expect("invalidated read")
        ));
        assert!(crate::IntegrityError::is_marked(
            &file.write::<Data>().await.err().expect("invalidated write")
        ));
        assert!(crate::IntegrityError::is_marked(
            &file.sync_all().await.unwrap_err()
        ));
        assert_eq!(fs::read(path.join("data")).await?, b"old");
        assert!(root.write().await.delete("data").await);
        assert!(crate::IntegrityError::is_marked(
            &root.sync_deleted().await.unwrap_err()
        ));
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

#[tokio::test]
async fn truncation_bounds_admitted_children_and_releases_cancelled_waits() -> crate::Result<()> {
    for synchronize in [false, true] {
        let path = unique_tmp_dir();
        std::fs::create_dir(&path)?;
        let cache = crate::Cache::<Data>::new(1024, Some(2), 0, std::time::Duration::from_secs(3));
        let root = cache.clone().load(path.clone())?;
        let mut files = Vec::new();
        {
            let mut dir = root.write().await;
            assert_eq!(dir.max_file_handles(), 2);
            for i in 0..5 {
                files.push(dir.create_file(i.to_string(), Data::new(&[1])).await?);
            }
        }
        let mut retained = Vec::new();
        for file in &files {
            retained.push(file.state.clone().read_owned().await);
        }
        let mut dir = root.write().await;
        let mut truncation = Box::pin(async {
            if synchronize {
                dir.truncate_and_sync().await
            } else {
                dir.truncate().await;
                Ok(())
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            std::future::poll_fn(
                |cx| match std::future::Future::poll(truncation.as_mut(), cx) {
                    std::task::Poll::Pending
                        if files
                            .iter()
                            .filter(|file| file.state.try_read().is_err())
                            .count()
                            == 2 =>
                    {
                        std::task::Poll::Ready(())
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                    std::task::Poll::Ready(result) => {
                        panic!("expected blocked truncation: {result:?}")
                    }
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            files
                .iter()
                .filter(|file| file.state.try_read().is_err())
                .count(),
            2
        );
        drop(truncation);
        assert!(files.iter().all(|file| file.state.try_read().is_ok()));
        let first = cache.try_acquire_file_handle()?;
        let second = cache.try_acquire_file_handle()?;
        assert!(cache.try_acquire_file_handle().is_err());
        drop((first, second));
        drop(dir);
        drop((retained, files, root, cache));
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}
