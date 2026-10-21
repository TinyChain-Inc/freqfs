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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    root.sync().await?;
    *file.write::<Data>().await? = Data::new(b"new");
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
        .create_file("data".into(), Data::new(b"old"))
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
        *file.write::<Data>().await? = replacement;
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
        .create_file("data".into(), Data::new(b"old"))
        .await?;
    file.sync().await?;
    file.clone().evict().unwrap().1.await?;
    assert!(matches!(*file.state.read().await, FileLockState::Pending));
    file.sync_all().await?;
    file.sync_all().await?;
    *file.write::<Data>().await? = Data::new(b"new");
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
            assert!(file.clone().evict().is_none());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert!(file.replace_all(replacement).await.is_err());
        }
        assert!(file.clone().evict().is_none());
        assert!(file.read::<Data>().await.is_err());
        assert!(file.write::<Data>().await.is_err());
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
