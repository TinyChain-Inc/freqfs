use std::io;
use std::path::PathBuf;
use std::time::Duration;

use destream::{de, en};
use safecast::as_type;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

use freqfs::*;

#[derive(Clone)]
enum File {
    Text(String),
}

impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        decoder.decode_any(FileVisitor).await
    }
}

struct FileVisitor;

impl de::Visitor for FileVisitor {
    type Value = File;

    fn expecting() -> &'static str {
        "a filesystem entry"
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<File, E> {
        Ok(File::Text(value))
    }
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
            let mut contents: FileWriteGuard<File, String> = file_for_write.write(64).await?;
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

impl get_size::GetSize for File {
    fn get_size(&self) -> usize {
        match self {
            Self::Text(text) => text.capacity(),
        }
    }
}

impl freqfs::FileLoad for File {
    async fn load_size(
        _: &std::path::Path,
        _: &mut tokio::fs::File,
        metadata: &std::fs::Metadata,
    ) -> std::io::Result<usize> {
        // Test codec strings and byte vectors retain at most geometric Vec capacity.
        usize::try_from(metadata.len())
            .ok()
            .and_then(|len| len.max(8).checked_next_power_of_two())
            .ok_or_else(|| std::io::Error::other("payload size overflow"))
    }

    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

impl freqfs::FileSave for File {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use futures::TryStreamExt;
        use tokio::io::AsyncWriteExt;
        let mut stream = tbon::en::encode(self).map_err(std::io::Error::other)?;
        let mut size = 0;
        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }
        Ok(size)
    }
}
