use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct BufferPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    buffers: Mutex<Vec<Vec<u8>>>,
    buffer_size: usize,
    /// Number of buffers currently checked out (in use).
    in_use: AtomicUsize,
    /// Total number of buffers allocated (initial count).
    total_buffers: usize,
}

pub struct BufferGuard {
    buffer: Option<Vec<u8>>,
    pool: Arc<PoolInner>,
}

impl BufferPool {
    pub fn new(num_buffers: usize, buffer_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(num_buffers);
        for _ in 0..num_buffers {
            buffers.push(vec![0u8; buffer_size]);
        }

        Self {
            inner: Arc::new(PoolInner {
                buffers: Mutex::new(buffers),
                buffer_size,
                in_use: AtomicUsize::new(0),
                total_buffers: num_buffers,
            }),
        }
    }

    pub fn get_buffer(&self) -> BufferGuard {
        let mut buffers = self.inner.buffers.lock().unwrap();
        let buffer = buffers
            .pop()
            .unwrap_or_else(|| vec![0u8; self.inner.buffer_size]);
        self.inner.in_use.fetch_add(1, Ordering::Relaxed);
        BufferGuard {
            buffer: Some(buffer),
            pool: Arc::clone(&self.inner),
        }
    }

    /// Returns the number of buffers currently checked out (in use).
    pub fn used_count(&self) -> usize {
        self.inner.in_use.load(Ordering::Relaxed)
    }

    /// Returns the total number of buffers in the pool.
    pub fn total_count(&self) -> usize {
        self.inner.total_buffers
    }
}

impl Deref for BufferGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_deref().expect("buffer guard missing buffer")
    }
}

impl DerefMut for BufferGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer
            .as_deref_mut()
            .expect("buffer guard missing buffer")
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        if let Some(mut buffer) = self.buffer.take() {
            buffer.clear();
            buffer.resize(self.pool.buffer_size, 0);
            let mut buffers = self.pool.buffers.lock().unwrap();
            buffers.push(buffer);
            self.pool.in_use.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_returns_to_pool_on_drop() {
        let pool = BufferPool::new(1, 8);
        {
            let mut guard = pool.get_buffer();
            guard[0] = 7;
            guard[1] = 9;
            assert_eq!(guard.len(), 8);
        }

        let guard = pool.get_buffer();
        assert_eq!(guard.len(), 8);
    }
}
