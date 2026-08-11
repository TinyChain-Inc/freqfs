use std::io;
use std::path::PathBuf;

use tokio::fs;

use freqfs::{Cache, FileSave};

#[derive(Clone)]
struct Entry(Vec<u8>);

impl FileSave for Entry {
    async fn save(&self, file: &mut fs::File) -> std::result::Result<u64, std::io::Error> {
        use tokio::io::AsyncWriteExt;
        file.write_all(&self.0).await?;
        Ok(self.0.len() as u64)
    }
}

fn unique_tmp_dir() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("freqfs_test_load_{}", uuid::Uuid::new_v4()));
    path
}

#[tokio::test]
async fn cache_load_rejects_overlapping_roots() -> Result<(), io::Error> {
    let root = unique_tmp_dir();
    fs::create_dir(&root).await?;
    fs::create_dir(root.join("sub")).await?;

    let cache = Cache::<Entry>::new(1024 * 1024, None, 0, std::time::Duration::from_secs(3));

    let _dir = cache.clone().load(root.clone())?;

    let err = cache.clone().load(root.join("sub")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    let err = cache.clone().load(root.clone()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    let _ = fs::remove_dir_all(&root).await;
    Ok(())
}
