use anyhow::Result;
use std::{io::SeekFrom, path::Path, sync::Arc};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

#[derive(Clone)]
pub struct FileWriter {
    file: Arc<Mutex<File>>,
}

impl FileWriter {
    pub async fn create_sized(path: &Path, size: u64) -> Result<Self> {
        let file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .await?;

        file.set_len(size).await?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let mut file = self.file.lock().await;

        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(bytes).await?;

        Ok(())
    }

    pub async fn create(path: &Path) -> Result<Self> {
        let file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .await?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn append(&self, bytes: &[u8]) -> Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(bytes).await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        file.flush().await?;
        Ok(())
    }
}
