#![cfg(feature = "stream")]

use std::io;
use std::path::PathBuf;

use destream::en;
use safecast::as_type;
use tokio::fs;

use freqfs::*;

#[derive(Clone)]
enum File {
    Bin(Vec<u8>),
    Text(String),
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

    let cache = Cache::<File>::new(1024 * 1024, None);
    let root = cache.load(path.clone())?;

    let file = {
        let mut dir = root.write().await;
        dir.create_file("data.bin".to_string(), vec![1u8, 2, 3], 3)?
    };

    let err = file.read::<String>().await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let err = file.write::<String>().await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let _ = fs::remove_dir_all(&path).await;
    Ok(())
}
