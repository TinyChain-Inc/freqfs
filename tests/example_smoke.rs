#![cfg(feature = "stream")]

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use destream::en;
use rand::Rng;
use safecast::as_type;
use tokio::fs;
use tokio::io::AsyncWriteExt;

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
    let mut rng = rand::rng();
    loop {
        let rand: u32 = rng.random();
        let mut path = std::env::temp_dir();
        path.push(format!("test_freqfs_{}", rand));

        if !path.exists() {
            fs::create_dir(&path).await?;

            let mut file_path = path.clone();
            file_path.push("hello.txt");

            let mut file = fs::File::create(&file_path).await?;
            file.write_all(b"\"Hello, world!\"").await?;
            file.sync_all().await?;

            let mut subdir = path.clone();
            subdir.push("subdir");
            fs::create_dir(&subdir).await?;

            break Ok(path);
        }
    }
}

async fn run_example(cache: DirLock<File>) -> Result<(), io::Error> {
    let mut root = cache.write().await;

    assert_eq!(root.len(), 2);
    assert!(root.get("hello").is_none());

    {
        let text_file = root.get_file("hello.txt").expect("text file");

        {
            let mut contents: FileWriteGuard<String> = text_file.write().await?;
            assert_eq!(&*contents, "Hello, world!");
            *contents = "नमस्ते दुनिया!".to_string();
        }

        {
            let contents: FileReadGuard<String> = text_file.read().await?;
            assert_eq!(&*contents, "नमस्ते दुनिया!");
        }

        text_file.sync().await?;
    }

    {
        let mut sub_dir = root.get_dir("subdir").expect("subdirectory").write().await;

        let sub_sub_dir = sub_dir.create_dir("sub-subdir".to_string())?;
        let mut sub_sub_dir = sub_sub_dir.write().await;

        let binary_file =
            sub_sub_dir.create_file("vector.bin".to_string(), (0..25).collect::<Vec<u8>>(), 25)?;

        let binary_file: FileReadGuard<Vec<u8>> = binary_file.read().await?;

        tokio::time::sleep(Duration::from_millis(50)).await;

        std::mem::drop(binary_file);

        let text_file = root.get_file("hello.txt").expect("text file");
        let contents: FileReadGuard<String> = text_file.read().await?;
        assert_eq!(&*contents, "नमस्ते दुनिया!");

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(root.delete("hello.txt").await);
    assert!(root.get("hello.txt").is_none());
    assert!(root.delete("subdir").await);
    assert!(root.get("subdir").is_none());

    std::mem::drop(root);
    cache.sync().await?;

    Ok(())
}

#[tokio::test]
async fn example_smoke() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;

    let cache = Cache::new(40, None);
    let root = cache.load(path.clone())?;

    run_example(root).await?;

    let mut txt_file_path = path.clone();
    txt_file_path.push("hello.txt");
    assert!(!txt_file_path.exists());

    let mut sub_dir_path = path.clone();
    sub_dir_path.push("subdir");
    assert!(!sub_dir_path.exists());

    assert!(!path.exists());

    Ok(())
}
