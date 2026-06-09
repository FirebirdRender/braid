use bytes::Bytes;
use tokio::fs::File;
use tokio::sync::mpsc;

use crate::buffer::pool::BufferPool;
use crate::sender::splitter::ChunkSplitter;

pub struct FileSplitter {
    inner: ChunkSplitter,
}

impl FileSplitter {
    pub fn new(buffer_size: usize) -> Self {
        let pool = BufferPool::new(2, buffer_size);
        Self {
            inner: ChunkSplitter::new(buffer_size, buffer_size, pool),
        }
    }

    pub async fn run_file(
        &self,
        input: File,
        tx: mpsc::Sender<Vec<Bytes>>,
        pause_rx: Option<mpsc::Receiver<bool>>,
    ) -> Result<(), std::io::Error> {
        self.inner.run(tx, pause_rx, input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn file_splitter_produces_fragments() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("input.bin");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .await
            .unwrap();
        let content = vec![b'x'; 200];
        file.write_all(&content).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let input = File::open(&path).await.unwrap();
        let splitter = FileSplitter::new(128);
        let (tx, mut rx) = mpsc::channel(8);

        let handle = tokio::spawn(async move { splitter.run_file(input, tx, None).await });
        let mut total_fragments = 0usize;
        while let Some(batch) = rx.recv().await {
            total_fragments += batch.len();
        }

        handle.await.unwrap().unwrap();
        assert!(total_fragments > 0);
    }
}
