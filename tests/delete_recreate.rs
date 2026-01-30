#![cfg(feature = "stream")]

use std::io;
use std::path::PathBuf;

use destream::en;
use safecast::as_type;
use tokio::fs;

use freqfs::*;

#[derive(Clone)]
enum File {
    Text(String),
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Text(string) => string.to_stream(encoder),
        }
    }
}

as_type!(File, Text, String);

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "test_freqfs_delete_recreate_{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir(&path).await?;
    Ok(path)
}

#[tokio::test]
async fn delete_then_recreate_file_and_dir() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    let cache = Cache::<File>::new(1024 * 1024, None);
    let root = cache.load(path.clone())?;

    {
        let mut dir = root.write().await;

        // keep at least one entry so syncing deletions won't delete the root dir
        dir.create_file("keep.txt".to_string(), "keep".to_string(), 4)?;

        let file = dir.create_file("a.txt".to_string(), "first".to_string(), 5)?;
        file.sync().await?;
    }

    assert!(path.join("a.txt").exists());

    {
        let mut dir = root.write().await;
        assert!(dir.delete("a.txt").await);
        assert!(dir.get("a.txt").is_none());
    }

    root.sync().await?;
    assert!(!path.join("a.txt").exists());

    {
        let mut dir = root.write().await;
        let file = dir.create_file("a.txt".to_string(), "second".to_string(), 6)?;
        file.sync().await?;
    }

    assert!(path.join("a.txt").exists());

    {
        let dir = root.read().await;
        let file = dir.get_file("a.txt").expect("a.txt");
        let contents: FileReadGuard<String> = file.read().await?;
        assert_eq!(&*contents, "second");
    }

    {
        let mut dir = root.write().await;
        let subdir = dir.create_dir("d".to_string())?;

        {
            let mut subdir = subdir.write().await;
            let file = subdir.create_file("x.txt".to_string(), "x".to_string(), 1)?;
            file.sync().await?;
        }
    }

    assert!(path.join("d").exists());

    {
        let mut dir = root.write().await;
        assert!(dir.delete("d").await);
        assert!(dir.get("d").is_none());
    }

    root.sync().await?;
    assert!(!path.join("d").exists());

    {
        let mut dir = root.write().await;
        let subdir = dir.create_dir("d".to_string())?;

        let mut subdir = subdir.write().await;
        let file = subdir.create_file("y.txt".to_string(), "y".to_string(), 1)?;
        file.sync().await?;
    }

    assert!(path.join("d").exists());

    root.sync().await?;

    let _ = fs::remove_dir_all(&path).await;
    Ok(())
}
