#![cfg(feature = "stream")]

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use destream::en;
use safecast::as_type;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

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
    path.push(format!("test_freqfs_concurrency_{}", uuid::Uuid::new_v4()));

    fs::create_dir(&path).await?;

    let mut file_path = path.clone();
    file_path.push("hello.txt");

    let mut file = fs::File::create(&file_path).await?;
    file.write_all(b"\"Hello, world!\"").await?;
    file.sync_all().await?;

    Ok(path)
}

#[tokio::test]
async fn concurrent_read_write_does_not_deadlock() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;

    let file = {
        let root = root.read().await;
        root.get_file("hello.txt").expect("hello.txt").clone()
    };

    let mut join_set = JoinSet::new();

    for i in 0..32usize {
        let file_for_write = file.clone();
        join_set.spawn(async move {
            let mut contents: FileWriteGuard<String> = file_for_write.write().await?;
            *contents = format!("value-{i}");
            Ok::<(), io::Error>(())
        });

        let file_for_read = file.clone();
        join_set.spawn(async move {
            let _contents: FileReadGuard<String> = file_for_read.read().await?;
            Ok::<(), io::Error>(())
        });
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(result) = join_set.join_next().await {
            result.expect("task panicked")?;
        }
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "deadlock or excessive contention"))??;

    file.sync().await?;

    let contents: FileReadGuard<String> = file.read().await?;
    assert!(contents.starts_with("value-"));

    let _ = fs::remove_dir_all(&path).await;
    Ok(())
}
