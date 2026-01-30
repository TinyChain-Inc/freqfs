#![cfg(feature = "stream")]

use rand::Rng;
use std::path::PathBuf;
use tokio::fs;

use freqfs::{FileLoad, FileSave};

async fn unique_tmp_file(name: &str) -> std::io::Result<PathBuf> {
    let mut rng = rand::rng();
    loop {
        let rand: u32 = rng.random();
        let mut path = std::env::temp_dir();
        path.push(format!("freqfs_test_{}_{}", name, rand));

        if !path.exists() {
            return Ok(path);
        }
    }
}

#[tokio::test]
async fn stream_roundtrip_u64() -> std::io::Result<()> {
    let path = unique_tmp_file("u64").await?;

    let value: u64 = 0xDEAD_BEEF_DEAD_BEEF;

    {
        let mut file = fs::File::create(&path).await?;
        let _size = value.save(&mut file).await?;
        file.sync_all().await?;
    }

    {
        let file = fs::File::open(&path).await?;
        let metadata = std::fs::metadata(&path)?;
        let loaded = u64::load(&path, file, metadata).await?;
        assert_eq!(loaded, value);
    }

    let _ = fs::remove_file(&path).await;
    Ok(())
}
