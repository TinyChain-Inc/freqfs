use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use safecast::as_type;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::{Cache, FileLoad, FileSave};

#[derive(Clone)]
enum Entry {
    Bytes(Vec<u8>),
}

as_type!(Entry, Bytes, Vec<u8>);

impl get_size::GetSize for Entry {
    fn get_size(&self) -> usize {
        let Self::Bytes(bytes) = self;
        bytes.capacity()
    }
}

impl FileLoad for Entry {
    async fn load(
        _: &std::path::Path,
        mut file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(Self::Bytes(bytes))
    }
}

impl FileSave for Entry {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let Self::Bytes(bytes) = self;
        file.write_all(bytes).await?;
        Ok(bytes.len() as u64)
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> io::Result<Self> {
        loop {
            let path = std::env::temp_dir().join(format!("freqfs_names_{}", Uuid::new_v4()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn insertion_names_cannot_escape_or_alias_the_directory() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let loaded = fixture.0.join("loaded");
    std::fs::create_dir(&loaded)?;
    let outside = fixture.0.join("outside");
    std::fs::write(&outside, [99])?;
    let cache = Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3));
    let root = cache.load(loaded.clone())?;
    let mut dir = root.write().await;
    let source = dir.create_dir("source".into())?;
    let file = source
        .write()
        .await
        .create_file("data".into(), vec![7])
        .await?;
    for name in [
        "",
        ".",
        "..",
        "./source",
        "source/",
        "nested/data",
        "../outside",
        "bad\0name",
        "value._freqfs",
        "value.data__freqfs",
        outside.to_str().unwrap(),
    ] {
        assert_eq!(
            dir.create_dir(name.into()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            dir.get_or_create_dir(name.into()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            dir.create_file(name.into(), vec![1])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            dir.copy_file_from(name.into(), &file)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            dir.copy_dir_from(name.into(), &source)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            dir.names().map(String::as_str).collect::<Vec<_>>(),
            ["source"]
        );
        assert_eq!(std::fs::read(&outside)?, [99]);
    }
    {
        let _source_write = source.write().await;
        assert_eq!(
            dir.copy_dir_from("blocked._freqfs".into(), &source)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    dir.create_file("ordinary".into(), vec![5]).await?;
    dir.sync().await?;
    assert!(!loaded.join("nested").exists());
    assert_eq!(std::fs::read(loaded.join("source/data"))?, [7]);
    assert_eq!(std::fs::read(loaded.join("ordinary"))?, [5]);
    Ok(())
}

#[tokio::test]
async fn deleted_directory_handles_reject_effects_after_name_reuse() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let root =
        Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3)).load(fixture.0.clone())?;
    let root_file = root
        .write()
        .await
        .create_file("root-before-truncate".into(), vec![4])
        .await?;
    let child = root.write().await.create_dir("child".into())?;
    let nested = child.write().await.create_dir("nested".into())?;
    let source = root.write().await.create_dir("source".into())?;
    let file = source
        .write()
        .await
        .create_file("data".into(), vec![8])
        .await?;
    root.sync().await?;
    assert!(root.write().await.delete("child").await);
    root.sync().await?;
    assert!(!fixture.0.join("child").exists());
    {
        let mut old = child.write().await;
        assert_eq!(old.path(), fixture.0.join("child"));
        assert!(old.is_empty());
        assert_eq!(
            old.create_dir("late".into()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.get_or_create_dir("late".into()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.create_file("late".into(), vec![1])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.copy_file_from("late".into(), &file)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.copy_dir_from("late".into(), &source)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.sync().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.sync_all().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.sync_deleted().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            old.truncate_and_sync().await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
    assert_eq!(
        child.trim().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        nested
            .write()
            .await
            .create_file("late".into(), vec![1])
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        nested.sync().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    let replacement = root.write().await.create_dir("child".into())?;
    replacement
        .write()
        .await
        .create_file("fresh".into(), vec![2])
        .await?;
    root.sync().await?;
    assert_eq!(
        child.sync().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(std::fs::read(fixture.0.join("child/fresh"))?, [2]);

    root.write().await.truncate().await;
    assert!(root.read().await.is_empty());
    root.sync().await?;
    assert!(fixture.0.is_dir());
    assert!(!fixture.0.join("root-before-truncate").exists());
    assert!(!fixture.0.join("child").exists());
    assert!(!fixture.0.join("source").exists());
    assert_eq!(
        root_file.sync().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    root.write()
        .await
        .create_file("root-still-live".into(), vec![3])
        .await?;
    root.sync().await?;
    assert_eq!(
        replacement.sync().await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    drop((root_file, child, nested, source, file, replacement, root));

    let reopened =
        Cache::<Entry>::new(1024, Some(1), 0, Duration::from_secs(3)).load(fixture.0.clone())?;
    let dir = reopened.read().await;
    assert_eq!(
        dir.names().map(String::as_str).collect::<Vec<_>>(),
        ["root-still-live"]
    );
    assert_eq!(&*dir.read_file::<Vec<u8>>("root-still-live").await?, &[3]);
    Ok(())
}

#[tokio::test]
async fn deleted_file_handles_cannot_remove_replacements() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let cache = Cache::<Entry>::new(128, Some(1), 0, Duration::from_secs(3));
    let root = cache.clone().load(fixture.0.clone())?;
    let stale = root
        .write()
        .await
        .create_file("data".into(), vec![1])
        .await?;
    root.sync_all().await?;
    assert!(root.write().await.delete("data").await);

    // Deleted handles reject before I/O admission, even before native removal.
    let permit = cache.try_acquire_file_handle()?;
    for result in [stale.sync().await, stale.sync_all().await] {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
    drop(permit);
    assert_eq!(std::fs::read(fixture.0.join("data"))?, [1]);

    let replacement = root
        .write()
        .await
        .create_file("data".into(), vec![9])
        .await?;
    replacement.sync_all().await?;
    replacement.cached().evict(&cache).unwrap().1.await?;

    let permit = cache.try_acquire_file_handle()?;
    for result in [stale.sync().await, stale.sync_all().await] {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
    drop(permit);
    assert_eq!(std::fs::read(fixture.0.join("data"))?, [9]);
    assert_eq!(&*replacement.read::<Vec<u8>>().await?, &[9]);
    root.sync_all().await?;
    drop((stale, replacement, root, cache));

    let reopened =
        Cache::<Entry>::new(128, Some(1), 0, Duration::from_secs(3)).load(fixture.0.clone())?;
    assert_eq!(
        &*reopened.read().await.read_file::<Vec<u8>>("data").await?,
        &[9]
    );
    Ok(())
}

#[tokio::test]
async fn failed_native_removal_retains_directory_cleanup_for_retry() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let cache = Cache::<Entry>::new(128, Some(1), 0, Duration::from_secs(3));
    let root = cache.clone().load(fixture.0.clone())?;
    {
        let mut dir = root.write().await;
        for name in ["a-removed", "z-blocked", "keep"] {
            dir.create_file(name.into(), vec![7]).await?;
        }
    }
    root.sync_all().await?;
    {
        let mut dir = root.write().await;
        assert!(dir.delete("a-removed").await);
        assert!(dir.delete("z-blocked").await);
    }

    // Isolated fault injection: make the second native unlink fail after the first succeeds.
    let blocked = fixture.0.join("z-blocked");
    std::fs::remove_file(&blocked)?;
    std::fs::create_dir(&blocked)?;
    let expected = std::fs::remove_file(&blocked).unwrap_err();
    let error = root.sync_deleted().await.unwrap_err();
    assert_eq!(error.kind(), expected.kind());
    assert_eq!(error.raw_os_error(), expected.raw_os_error());
    assert!(!crate::IntegrityError::is_marked(&error));
    assert!(!fixture.0.join("a-removed").exists());
    assert_eq!(root.read().await.deleted.len(), 2);
    drop(cache.try_acquire_file_handle()?);

    std::fs::remove_dir(&blocked)?;
    // Retrying includes the already-unlinked first file and the now-absent second file.
    root.sync_deleted().await?;
    root.sync_deleted().await?;
    assert!(root.read().await.deleted.is_empty());
    root.sync_all().await?;
    assert_eq!(std::fs::read(fixture.0.join("keep"))?, [7]);
    drop((root, cache));

    let reopened =
        Cache::<Entry>::new(128, Some(1), 0, Duration::from_secs(3)).load(fixture.0.clone())?;
    let dir = reopened.read().await;
    assert_eq!(
        dir.names().map(String::as_str).collect::<Vec<_>>(),
        ["keep"]
    );
    assert_eq!(&*dir.read_file::<Vec<u8>>("keep").await?, &[7]);
    Ok(())
}

#[tokio::test]
async fn directory_copy_rejects_self_ancestor_and_conflicting_sources() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let root =
        Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3)).load(fixture.0.clone())?;
    let a = root.write().await.create_dir("a".into())?;
    let b = root.write().await.create_dir("b".into())?;
    let mut left = a.write().await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            left.copy_dir_from("self".into(), &a)
        )
        .await
        .unwrap()
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            left.copy_dir_from("ancestor".into(), &root)
        )
        .await
        .unwrap()
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    let mut right = b.write().await;
    let (left_result, right_result) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            left.copy_dir_from("copy".into(), &b),
            right.copy_dir_from("copy".into(), &a)
        )
    })
    .await
    .unwrap();
    assert_eq!(left_result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(right_result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert!(left.is_empty());
    assert!(right.is_empty());
    Ok(())
}

#[tokio::test]
async fn textual_names_use_lexical_identity() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let cache = Cache::<Entry>::new(1024 * 1024, None, 0, Duration::from_secs(3));
    let root = cache.load(fixture.0.clone())?;

    {
        let mut dir = root.write().await;
        for (name, byte) in [
            ("1", 1),
            ("10", 10),
            ("2", 2),
            ("20", 20),
            ("01", 101),
            ("-1", 255),
            ("!note", 42),
            ("not-a-number", 0),
        ] {
            dir.create_file(name.into(), vec![byte]).await?;
        }
        dir.create_dir("02".into())?;

        let created = dir.get_or_create_dir("temporary".into())?;
        let existing = dir.get_or_create_dir("temporary".into())?;
        {
            let _write = created.write().await;
            assert_eq!(
                existing.try_read().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }
        assert_eq!(
            dir.create_dir("temporary".into()).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(dir.delete("temporary").await);
        assert_eq!(
            dir.get_or_create_dir("temporary".into())
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        let error = dir.get_or_create_dir("2".into()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(error.to_string(), "2");

        assert_eq!(
            dir.names().map(String::as_str).collect::<Vec<_>>(),
            [
                "!note",
                "-1",
                "01",
                "02",
                "1",
                "10",
                "2",
                "20",
                "not-a-number"
            ]
        );
        assert!(dir.get("2").is_some());
        assert!(dir.get_file("02").is_none());
        assert!(dir.get_dir("02").is_some());
        assert!(dir.get_dir("2").is_none());
        assert!(dir.contains(&String::from("10")));
        assert!(dir.contains(&Arc::new(String::from("01"))));

        assert_eq!(&*dir.read_file::<Vec<u8>>("2").await?, &[2]);
        assert_eq!(&*dir.read_file_owned::<Vec<u8>>("01").await?, &[101]);
        dir.write_file::<Vec<u8>>("2").await?[0] = 12;
        dir.write_file_owned::<Vec<u8>>("01").await?[0] = 111;
        assert_eq!(&*dir.read_file::<Vec<u8>>("2").await?, &[12]);
        assert_eq!(&*dir.read_file::<Vec<u8>>("01").await?, &[111]);

        assert!(!dir.contains("002"));
        assert!(dir.get("002").is_none());
        assert!(dir.get_dir("002").is_none());
        assert!(dir.get_file("002").is_none());
        for error in [
            dir.read_file::<Vec<u8>>("002").await.unwrap_err(),
            dir.read_file_owned::<Vec<u8>>("002").await.unwrap_err(),
            dir.write_file::<Vec<u8>>("002").await.unwrap_err(),
            dir.write_file_owned::<Vec<u8>>("002").await.unwrap_err(),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::NotFound);
            assert_eq!(error.to_string(), "002");
        }

        assert!(!dir.delete("002").await);
        assert!(dir.delete("2").await);
        assert!(dir.get("2").is_none());
        assert!(dir.contains("02"));
        assert_eq!(&*dir.read_file::<Vec<u8>>("20").await?, &[20]);
    }

    root.sync().await?;
    drop(root);
    let reopened = Cache::<Entry>::new(1024 * 1024, None, 0, Duration::from_secs(3))
        .load(fixture.0.clone())?;
    let dir = reopened.read().await;
    assert!(!dir.contains("2"));
    assert!(dir.get_dir("02").is_some());
    assert_eq!(&*dir.read_file::<Vec<u8>>("01").await?, &[111]);
    assert_eq!(&*dir.read_file::<Vec<u8>>("10").await?, &[10]);
    Ok(())
}

#[tokio::test]
async fn uuid_names_use_exact_spelling_and_stack_buffers() -> io::Result<()> {
    let fixture = TestDir::new()?;
    let cache = Cache::<Entry>::new(1024 * 1024, None, 0, Duration::from_secs(3));
    let root = cache.load(fixture.0.clone())?;
    let id = Uuid::from_u128(0xabcdef0123456789abcdef0123456789);
    let mut lower = Uuid::encode_buffer();
    let lower = id.hyphenated().encode_lower(&mut lower);
    let mut upper = Uuid::encode_buffer();
    let upper = id.hyphenated().encode_upper(&mut upper);

    {
        let mut dir = root.write().await;
        dir.create_file(lower.to_owned(), vec![1]).await?;
        assert!(dir.get_file(lower).is_some());
        assert!(!dir.contains(upper));
        assert!(dir.get_file(upper).is_none());
        assert!(dir.get_file("not-a-uuid").is_none());
        dir.create_file(upper.to_owned(), vec![2]).await?;
        assert_eq!(&*dir.read_file::<Vec<u8>>(lower).await?, &[1]);
        assert_eq!(&*dir.read_file::<Vec<u8>>(upper).await?, &[2]);
        assert!(dir.delete(lower).await);
        assert!(dir.get_file(lower).is_none());
        assert_eq!(&*dir.read_file::<Vec<u8>>(upper).await?, &[2]);

        let (id, file) = dir.create_file_unique(vec![3]).await?;
        let mut buffer = Uuid::encode_buffer();
        let name = id.hyphenated().encode_lower(&mut buffer);
        assert_eq!(dir.get_file(name).unwrap().path(), file.path());
        assert_eq!(&*dir.read_file::<Vec<u8>>(name).await?, &[3]);
        drop(file);
        assert!(dir.delete(name).await);

        let (id, child) = dir.create_dir_unique()?;
        let mut buffer = Uuid::encode_buffer();
        let name = id.hyphenated().encode_lower(&mut buffer);
        assert!(dir.get_dir(name).is_some());
        drop(child);
        assert!(dir.delete(name).await);
    }

    root.sync().await?;
    drop(root);
    let reopened = Cache::<Entry>::new(1024 * 1024, None, 0, Duration::from_secs(3))
        .load(fixture.0.clone())?;
    let dir = reopened.read().await;
    assert!(dir.get_file(lower).is_none());
    assert_eq!(&*dir.read_file::<Vec<u8>>(upper).await?, &[2]);
    Ok(())
}

#[tokio::test]
async fn file_publication_rejects_live_and_deleted_directories() -> io::Result<()> {
    for resident in [false, true] {
        for deleted in [false, true] {
            let fixture = TestDir::new()?;
            std::fs::create_dir(fixture.0.join("destination"))?;
            std::fs::write(fixture.0.join("destination/retained"), [1])?;
            std::fs::write(fixture.0.join("source"), [7, 8])?;
            let cache = Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3));
            let root = cache.load(fixture.0.clone())?;
            let source = root.read().await.get_file("source").unwrap().clone();
            if resident {
                assert_eq!(&*source.read::<Vec<u8>>().await?, &[7, 8]);
            }
            {
                let mut dir = root.write().await;
                if deleted {
                    assert!(dir.delete("destination").await);
                }
                let error = dir
                    .copy_file_from("destination".into(), &source)
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
                let error = dir
                    .create_file("destination".into(), vec![9])
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
                assert!(dir.get_file("destination").is_none());
                assert_eq!(dir.get_dir("destination").is_some(), !deleted);
            }
            assert_eq!(std::fs::read(fixture.0.join("destination/retained"))?, [1]);
            root.sync().await?;
            if deleted {
                assert!(!fixture.0.join("destination").exists());
                root.write()
                    .await
                    .create_file("destination".into(), vec![9])
                    .await?;
                root.sync().await?;
                assert_eq!(std::fs::read(fixture.0.join("destination"))?, [9]);
            } else {
                assert_eq!(std::fs::read(fixture.0.join("destination/retained"))?, [1]);
            }
            drop((source, root));
            let reopened = Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3))
                .load(fixture.0.clone())?;
            let dir = reopened.read().await;
            assert_eq!(dir.get_dir("destination").is_some(), !deleted);
            if deleted {
                assert_eq!(&*dir.read_file::<Vec<u8>>("destination").await?, &[9]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn copied_file_replaces_only_its_successful_pending_deletion() -> io::Result<()> {
    for fail in [false, true] {
        let fixture = TestDir::new()?;
        let cache = Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3));
        let root = cache.clone().load(fixture.0.clone())?;
        let (stale, source) = {
            let mut dir = root.write().await;
            let stale = dir.create_file("destination".into(), vec![1]).await?;
            let source = dir.create_file("source".into(), vec![7, 8]).await?;
            (stale, source)
        };
        root.sync().await?;
        let copied = {
            let mut dir = root.write().await;
            assert!(dir.delete("destination").await);
            if fail {
                assert!(dir.delete("source").await);
                assert!(dir
                    .copy_file_from("destination".into(), &source)
                    .await
                    .is_err());
                assert!(dir.get_file("destination").is_none());
                None
            } else {
                Some(dir.copy_file_from("destination".into(), &source).await?)
            }
        };
        if let Some(file) = &copied {
            // Write back and evict before deletion cleanup: stale tombstones would
            // otherwise delete this new value without another writeback.
            file.cached().evict(&cache).unwrap().1.await?;
        }
        for result in [stale.sync().await, stale.sync_all().await] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
        }
        root.sync().await?;
        if fail {
            assert!(!fixture.0.join("destination").exists());
        } else {
            assert_eq!(std::fs::read(fixture.0.join("destination"))?, [7, 8]);
        }
        drop((stale, copied, source, root, cache));
        let reopened = Cache::<Entry>::new(1024, Some(2), 0, Duration::from_secs(3))
            .load(fixture.0.clone())?;
        let dir = reopened.read().await;
        assert_eq!(dir.get_file("destination").is_some(), !fail);
        if !fail {
            assert_eq!(&*dir.read_file::<Vec<u8>>("destination").await?, &[7, 8]);
        }
    }
    Ok(())
}
