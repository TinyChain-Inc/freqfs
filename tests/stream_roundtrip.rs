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

    let value = FileValue(0xDEAD_BEEF_DEAD_BEEF);

    {
        let mut file = fs::File::create(&path).await?;
        let _size = value.save(&mut file).await?;
        file.sync_all().await?;
    }

    {
        let file = fs::File::open(&path).await?;
        let metadata = std::fs::metadata(&path)?;
        let loaded = FileValue::load(&path, file, metadata).await?;
        assert_eq!(loaded, value);
    }

    let _ = fs::remove_file(&path).await;
    Ok(())
}

#[derive(Debug, PartialEq)]
struct FileValue(u64);

impl get_size::GetSize for FileValue {
    fn get_size(&self) -> usize {
        8
    }
}

impl freqfs::FileLoad for FileValue {
    async fn load_size(
        _: &std::path::Path,
        _: &mut tokio::fs::File,
        _: &std::fs::Metadata,
    ) -> std::io::Result<usize> {
        Ok(8)
    }

    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map(Self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

impl freqfs::FileSave for FileValue {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use futures::TryStreamExt;
        use tokio::io::AsyncWriteExt;
        let mut stream = tbon::en::encode(&self.0).map_err(std::io::Error::other)?;
        let mut size = 0;
        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }
        Ok(size)
    }
}
