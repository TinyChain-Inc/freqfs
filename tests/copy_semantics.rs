#![cfg(feature = "stream")]

use std::io;
use std::path::PathBuf;
use std::time::Duration;

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
    path.push(format!("test_freqfs_copy_{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path).await?;
    Ok(path)
}

#[tokio::test]
async fn copy_dir_from_preserves_tree_and_contents() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    let cache = Cache::<File>::new(1024 * 1024, None);
    let root = cache.load(path.clone())?;

    let src = {
        let mut dir = root.write().await;
        dir.create_dir("src".to_string())?
    };

    {
        let mut src_dir = src.write().await;
        src_dir.create_file("a.txt".to_string(), "hello".to_string(), 5)?;
        src_dir.create_file("b.bin".to_string(), vec![1u8, 2, 3], 3)?;

        let nested = src_dir.create_dir("nested".to_string())?;
        let mut nested = nested.write().await;
        nested.create_file("c.txt".to_string(), "world".to_string(), 5)?;
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        let mut dir = root.write().await;
        dir.copy_dir_from("dst".to_string(), &src).await?;
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "copy_dir_from timed out"))??;

    let dst = {
        let dir = root.read().await;
        dir.get_dir("dst").expect("dst").clone()
    };

    {
        let dst_dir = dst.read().await;

        let a = dst_dir.get_file("a.txt").expect("a.txt");
        let a_contents: FileReadGuard<String> = a.read().await?;
        assert_eq!(&*a_contents, "hello");

        let b = dst_dir.get_file("b.bin").expect("b.bin");
        let b_contents: FileReadGuard<Vec<u8>> = b.read().await?;
        assert_eq!(&*b_contents, &[1u8, 2, 3]);

        let nested = dst_dir.get_dir("nested").expect("nested");
        let nested = nested.read().await;
        let c = nested.get_file("c.txt").expect("c.txt");
        let c_contents: FileReadGuard<String> = c.read().await?;
        assert_eq!(&*c_contents, "world");
    }

    // also verify copy_file_from overwrites an existing file
    {
        let src_dir = src.read().await;
        let src_a = src_dir.get_file("a.txt").expect("a.txt").clone();
        std::mem::drop(src_dir);

        let mut dst_dir = dst.write().await;
        dst_dir.create_file("a.txt".to_string(), "old".to_string(), 3)?;
        dst_dir.copy_file_from("a.txt".to_string(), &src_a).await?;

        let a = dst_dir.get_file("a.txt").expect("a.txt");
        let a_contents: FileReadGuard<String> = a.read().await?;
        assert_eq!(&*a_contents, "hello");
    }

    let _ = fs::remove_dir_all(&path).await;
    Ok(())
}
