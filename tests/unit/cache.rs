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

fn try_reserve<FE>(cache: &Arc<Cache<FE>>, bytes: usize) -> crate::Result<super::Reservation<FE>> {
    cache.reserve_if_available(bytes)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::ResourceBusy,
            "cache capacity is exhausted",
        )
    })
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

    let file_a: FileLock<Entry> = FileLock::new(cache.clone(), path_a.clone(), vec![1u8; 10], 10);
    let file_b: FileLock<Entry> = FileLock::new(cache.clone(), path_b.clone(), vec![2u8; 10], 10);

    cache.insert(path_a.clone(), file_a.clone(), 10);
    cache.insert(path_b.clone(), file_b.clone(), 10);

    // make `a.bin` the most frequently used
    cache.bump(&path_a, None);

    let mut evictions = cache.gc_for(0);
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
        .create_file("data.bin".into(), vec![1_u8])
        .await
        .unwrap();
    for result in [
        file.sync().await,
        file.replace_all(Entry::Bin(vec![2])).await,
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
        try_reserve(&cache, 1).err().unwrap().kind(),
        std::io::ErrorKind::ResourceBusy
    );
    let waiting = cache.reserve(1);
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());

    drop(first);
    let second = waiting.await.unwrap();
    second.commit();
    assert_eq!(cache.lock().size, 1);

    // Exercise accounting overflow without allocating a payload.
    let cache = Cache::<Entry>::new(usize::MAX, Some(1), 0, Duration::from_secs(1));
    let full = try_reserve(&cache, usize::MAX).unwrap();

    assert_eq!(
        try_reserve(&cache, 1).err().unwrap().kind(),
        std::io::ErrorKind::ResourceBusy
    );
    assert_eq!(cache.lock().size, usize::MAX);

    drop(full);
    assert_eq!(cache.lock().size, 0);
    drop(try_reserve(&cache, 1).unwrap());
}

#[tokio::test]
async fn file_handle_admission_distinguishes_exhaustion_from_closure() {
    let cache = Cache::<Entry>::new(10, Some(1), 0, Duration::from_secs(1));
    let permit = cache.try_acquire_file_handle().unwrap();

    assert_eq!(
        cache.try_acquire_file_handle().unwrap_err().kind(),
        std::io::ErrorKind::ResourceBusy
    );

    drop(permit);
    drop(cache.try_acquire_file_handle().unwrap());

    cache.file_handles.close();
    assert_eq!(
        cache.try_acquire_file_handle().unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        cache.acquire_file_handle().await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
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

#[derive(Clone)]
struct DecodeObserver {
    started: Arc<Notify>,
    loads: Arc<AtomicUsize>,
    pause: bool,
}

// Test-only, task-scoped observation; no process-global observer state.
tokio::task_local! {
    static DECODE_OBSERVER: DecodeObserver;
}

impl Clone for ExpandedPayload {
    fn clone(&self) -> Self {
        let mut bytes = Vec::with_capacity(self.0.len() * 2);
        bytes.extend_from_slice(&self.0);
        Self(bytes)
    }
}

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
    async fn load(
        _: &std::path::Path,
        mut file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> crate::Result<Self> {
        use tokio::io::AsyncReadExt;
        let len = usize::try_from(file.read_u64_le().await?).map_err(std::io::Error::other)?;
        if len > 4096 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "test payload exceeds fixture limit",
            ));
        }
        let value = Self(vec![0; len]);
        if let Ok(DecodeObserver {
            started,
            loads,
            pause,
        }) = DECODE_OBSERVER.try_with(Clone::clone)
        {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            started.notify_one();
            if pause {
                std::future::pending::<()>().await;
            }
        }
        Ok(value)
    }
}

impl FileSave for ExpandedPayload {
    async fn save(&self, file: &mut tokio::fs::File) -> crate::Result<u64> {
        file.write_u64_le(self.0.len() as u64).await?;
        Ok(8)
    }
}

#[tokio::test]
async fn loading_cancellation_and_timeout_leave_pending_files_reusable() -> std::io::Result<()> {
    for (pause_decode, cancel) in [(true, true), (false, true), (false, false)] {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        tokio::fs::write(path.join("pending"), 32u64.to_le_bytes()).await?;
        let cache = Cache::<ExpandedPayload>::new(64, Some(3), 0, Duration::from_millis(30));
        let root = cache.clone().load(path.clone())?;
        let pending = root.read().await.get_file("pending").unwrap().clone();
        let resident = root
            .write()
            .await
            .create_file("resident".into(), ExpandedPayload(vec![1; 64]))
            .await?;
        let pinned = resident.read::<ExpandedPayload>().await?;
        let started = Arc::new(Notify::new());
        let loads = Arc::new(AtomicUsize::new(0));
        DECODE_OBSERVER.scope(DecodeObserver { started: started.clone(), loads: loads.clone(), pause: pause_decode }, async {
            let read = pending.read::<ExpandedPayload>();
            tokio::pin!(read);
            tokio::select! {
                _ = started.notified() => {},
                result = &mut read => panic!("read completed before observation: {:?}", result.err()),
            }
            assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(cache.lock().size, 64);
            if !cancel {
                assert_eq!(read.await.err().unwrap().kind(), std::io::ErrorKind::ResourceBusy);
            }
        }).await;
        assert_eq!(cache.file_handles.available_permits(), 2);
        assert_eq!(cache.lock().size, 64);
        drop(pinned);
        let (left, right) = DECODE_OBSERVER
            .scope(
                DecodeObserver {
                    started,
                    loads: loads.clone(),
                    pause: false,
                },
                async {
                    tokio::join!(
                        pending.read::<ExpandedPayload>(),
                        pending.read::<ExpandedPayload>()
                    )
                },
            )
            .await;
        assert_eq!(left?.0.len(), 32);
        assert_eq!(right?.0.len(), 32);
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "concurrent readers share the successful load"
        );
        root.write().await.truncate_and_sync().await?;
    }
    Ok(())
}

#[tokio::test]
async fn copy_accounts_clone_capacity_independently_of_source() -> std::io::Result<()> {
    for capacity in [256, 96] {
        let path = unique_tmp_dir();
        tokio::fs::create_dir(&path).await?;
        let cache = Cache::<ExpandedPayload>::new(capacity, Some(4), 0, Duration::from_millis(30));
        let root = cache.clone().load(path)?;
        let source = root
            .write()
            .await
            .create_file("source".into(), ExpandedPayload(vec![1; 32]))
            .await?;
        let target = root
            .write()
            .await
            .create_file("target".into(), ExpandedPayload(vec![2; 16]))
            .await?;
        let result = target.overwrite(&source).await;
        if capacity == 256 {
            result?;
            let guard = target.read::<ExpandedPayload>().await?;
            assert_eq!(guard.0.as_slice(), &[1; 32]);
            assert_eq!(guard.0.capacity(), 64);
            assert_eq!(cache.lock().size, 96);
        } else {
            assert_eq!(
                result.err().unwrap().kind(),
                std::io::ErrorKind::ResourceBusy
            );
            assert_eq!(
                target.read::<ExpandedPayload>().await?.0.as_slice(),
                &[2; 16]
            );
        }
        root.write().await.truncate_and_sync().await?;
    }
    Ok(())
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
        .create_file("expanded".into(), ExpandedPayload(vec![0; 64]))
        .await?;
    assert_eq!(cache.lock().size, 64);
    let duplicate = root
        .write()
        .await
        .create_file("expanded".into(), ExpandedPayload(vec![0; 16]))
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
    assert_eq!(
        cache.lock().size,
        64,
        "account the decoded retained capacity"
    );
    {
        let mut guard = file.write::<ExpandedPayload>().await?;
        assert_eq!(cache.lock().size, 64);
        guard.0 = vec![0; 72];
        assert_eq!(cache.lock().size, 64, "account on guard release");
    }
    assert_eq!(cache.lock().size, 72);
    file.sync().await?;
    {
        let mut guard = file.write_owned::<ExpandedPayload>().await?;
        guard.0 = Vec::with_capacity(96);
        guard.0.resize(8, 0);
    }
    assert_eq!(cache.lock().size, 96, "retained capacity, not length");
    {
        let mut guard = file.try_write::<ExpandedPayload>()?;
        guard.0 = vec![0; 16];
    }
    assert_eq!(cache.lock().size, 16);
    root.write().await.delete("expanded").await;
    assert_eq!(cache.lock().size, 0);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn loading_admission_and_actual_mutation_accounting() -> std::io::Result<()> {
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
        .create_file("bad".into(), ExpandedPayload(vec![0; 256]))
        .await
        .is_err());
    assert!(!root.read().await.contains("bad"));
    assert_eq!(cache.lock().size, 16);
    {
        let mut guard = file.write::<ExpandedPayload>().await?;
        guard.0 = vec![0; 32];
    }
    assert_eq!(file.read::<ExpandedPayload>().await?.0.len(), 32);
    file.sync().await?;
    assert_eq!(cache.lock().size, 32);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn mutation_growth_survives_unwind_cancellation_and_eviction() -> std::io::Result<()> {
    let path = unique_tmp_dir();
    tokio::fs::create_dir(&path).await?;
    let cache = Cache::<ExpandedPayload>::new(64, Some(4), 0, Duration::from_millis(100));
    let root = cache.clone().load(path.clone())?;
    let file = root
        .write()
        .await
        .create_file("data".into(), ExpandedPayload(vec![0; 16]))
        .await?;

    let mut guard = file.write::<ExpandedPayload>().await?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        guard.0 = vec![0; 32];
        panic!("mutation unwinding");
    }));
    assert!(result.is_err());
    assert_eq!(cache.lock().size, 32);
    assert_eq!(file.read::<ExpandedPayload>().await?.0.len(), 32);

    let started = Arc::new(Notify::new());
    let task = tokio::spawn({
        let file = file.clone();
        let started = started.clone();
        async move {
            let mut guard = file.write_owned::<ExpandedPayload>().await.unwrap();
            guard.0 = vec![0; 48];
            started.notify_one();
            std::future::pending::<()>().await;
        }
    });
    started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(cache.lock().size, 48);

    {
        let mut guard = file.try_write_owned::<ExpandedPayload>()?;
        guard.0 = vec![0; 96];
    }
    assert_eq!(cache.lock().size, 96);
    let pinned = file.read::<ExpandedPayload>().await?;
    assert_eq!(pinned.0.len(), 96);
    assert_eq!(cache.file_handles.available_permits(), 3);
    assert_eq!(
        cache.reserve(1).await.err().unwrap().kind(),
        std::io::ErrorKind::ResourceBusy
    );
    drop(pinned);

    // Demand admission must reclaim the oversized entry without discarding it.
    let next = root
        .write()
        .await
        .create_file("next".into(), ExpandedPayload(vec![0; 16]))
        .await?;
    assert_eq!(next.read::<ExpandedPayload>().await?.0.len(), 16);
    assert_eq!(
        file.read::<ExpandedPayload>().await.err().unwrap().kind(),
        std::io::ErrorKind::OutOfMemory
    );
    let reopened = Cache::<ExpandedPayload>::new(256, Some(4), 0, Duration::from_secs(1))
        .load(path.clone())?;
    let restored = reopened.read().await.get_file("data").unwrap().clone();
    assert_eq!(restored.read::<ExpandedPayload>().await?.0.len(), 96);
    root.write().await.truncate_and_sync().await?;
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
            try_reserve(&cache, 1)
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

        // Coordinate with any eviction still writing these payloads before
        // removing their directory; raw filesystem cleanup can race it.
        root.write().await.truncate_and_sync().await?;
        assert!(!path.exists());
    }
    Ok(())
}
