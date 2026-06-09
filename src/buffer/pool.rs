use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    storage: Vec<BytesMut>,
    free_list: Mutex<Vec<usize>>,
    semaphore: Semaphore,
    buffer_size: usize,
    total_buffers: usize,
}

pub struct PoolBuffer {
    pub buffer: BytesMut,
    pub index: usize,
    pool: Arc<PoolInner>,
}

impl PoolBuffer {
    pub fn split_to(&mut self, n: usize) -> BytesMut {
        self.buffer.split_to(n)
    }
    pub fn as_bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl Drop for PoolBuffer {
    fn drop(&mut self) {
        self.buffer.resize(self.pool.buffer_size, 0);
        let mut free_list = self.pool.free_list.lock().unwrap();
        free_list.push(self.index);
        self.pool.semaphore.add_permits(1);
    }
}

impl BufferPool {
    pub fn new(num_buffers: usize, buffer_size: usize) -> Self {
        let mut storage = Vec::with_capacity(num_buffers);
        let mut free_list = Vec::with_capacity(num_buffers);
        for i in 0..num_buffers {
            storage.push(BytesMut::zeroed(buffer_size));
            free_list.push(i);
        }
        Self {
            inner: Arc::new(PoolInner {
                storage,
                free_list: Mutex::new(free_list),
                semaphore: Semaphore::new(num_buffers),
                buffer_size,
                total_buffers: num_buffers,
            }),
        }
    }

    pub async fn acquire(&self) -> PoolBuffer {
        let permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .expect("semaphore closed");
        let index = {
            let mut free_list = self.inner.free_list.lock().unwrap();
            free_list.pop().expect("free-list empty despite permit")
        };
        let buffer = self.inner.storage[index].clone();
        permit.forget();
        PoolBuffer {
            buffer,
            index,
            pool: Arc::clone(&self.inner),
        }
    }

    pub async fn acquire_many(&self, n: usize) -> Vec<PoolBuffer> {
        if n == 0 {
            return Vec::new();
        }
        let permit = self
            .inner
            .semaphore
            .acquire_many(n as u32)
            .await
            .expect("semaphore closed");
        let mut buffers = Vec::with_capacity(n);
        {
            let mut free_list = self.inner.free_list.lock().unwrap();
            for _ in 0..n {
                let index = free_list.pop().expect("free-list empty despite permit");
                let buffer = self.inner.storage[index].clone();
                buffers.push(PoolBuffer {
                    buffer,
                    index,
                    pool: Arc::clone(&self.inner),
                });
            }
        }
        permit.forget();
        buffers
    }

    pub fn used_count(&self) -> usize {
        self.inner.total_buffers - self.inner.free_list.lock().unwrap().len()
    }
    pub fn total_count(&self) -> usize {
        self.inner.total_buffers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_blocks_when_empty() {
        let pool = Arc::new(BufferPool::new(1, 32));
        let _buf1 = pool.acquire().await;
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let _buf2 = pool_clone.acquire().await;
        });
        tokio::task::yield_now().await;
        drop(_buf1);
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
        assert!(
            result.is_ok(),
            "acquire should unblock after buffer is returned"
        );
    }
    #[tokio::test]
    async fn drop_returns_to_pool() {
        let pool = Arc::new(BufferPool::new(1, 16));
        assert_eq!(pool.used_count(), 0);
        let buf = pool.acquire().await;
        assert_eq!(pool.used_count(), 1);
        drop(buf);
        assert_eq!(pool.used_count(), 0);
    }
    #[tokio::test]
    async fn used_count_tracks_in_use() {
        let pool = Arc::new(BufferPool::new(3, 32));
        assert_eq!(pool.used_count(), 0);
        let b1 = pool.acquire().await;
        assert_eq!(pool.used_count(), 1);
        let b2 = pool.acquire().await;
        assert_eq!(pool.used_count(), 2);
        drop(b1);
        assert_eq!(pool.used_count(), 1);
        drop(b2);
        assert_eq!(pool.used_count(), 0);
    }

    #[tokio::test]
    async fn split_to_returns_owned_slice() {
        let pool = BufferPool::new(1, 64);
        let mut buf = pool.acquire().await;
        buf.buffer[0] = 10;
        buf.buffer[1] = 20;
        buf.buffer[2] = 30;
        let slice = buf.split_to(3);
        assert_eq!(slice.len(), 3);
        assert_eq!(slice[0], 10);
        assert_eq!(slice[1], 20);
        assert_eq!(slice[2], 30);
        assert_eq!(buf.buffer.len(), 61);
    }
    #[tokio::test]
    async fn as_bytes_mut_returns_ref() {
        let pool = BufferPool::new(1, 32);
        let mut buf = pool.acquire().await;
        let bytes_ref = buf.as_bytes_mut();
        bytes_ref[0] = 99;
        assert_eq!(buf.buffer[0], 99);
        assert_eq!(buf.buffer.len(), 32);
    }
}
