use std::io;
use std::path::PathBuf;

use destream::{de, en};
use safecast::as_type;
use tokio::fs;

use freqfs::*;

#[derive(Clone)]
enum File {
    Bin(Vec<u8>),
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
    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<File, A::Error> {
        let mut bytes = Vec::new();
        while let Some(byte) = seq.next_element::<u8>(()).await? {
            bytes.push(byte);
        }
        Ok(File::Bin(bytes))
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Bin(bytes) => bytes.to_stream(encoder),
            Self::Text(string) => string.to_stream(encoder),
        }
    }
}

as_type!(File, Bin, Vec<u8>);
as_type!(File, Text, String);

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut path = std::env::temp_dir();
    path.push(format!("test_freqfs_type_safety_{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path).await?;
    Ok(path)
}

#[tokio::test]
async fn wrong_type_read_returns_invalid_data() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;

    let file = {
        let mut dir = root.write().await;
        dir.create_file("data.bin".to_string(), vec![1u8, 2, 3], 3)
            .await?
    };

    let err = file.read::<String>().await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let err = file.write::<String>().await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    file.sync().await?;
    drop(file);
    drop(root);
    let cache = Cache::<File>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;
    let file = root.read().await.get_file("data.bin").unwrap().clone();
    // Cold reads must load the saved entry type, not decode as the requested type.
    assert_eq!(
        file.read::<String>().await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(&*file.read::<Vec<u8>>().await?, &[1, 2, 3]);

    let _ = fs::remove_dir_all(&path).await;
    Ok(())
}

impl freqfs::FileLoad for File {
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
