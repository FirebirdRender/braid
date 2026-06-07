use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

pub struct RingBuffer<T> {
    inner: Mutex<Inner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
}

struct Inner<T> {
    buffer: VecDeque<T>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than zero");
        Self {
            inner: Mutex::new(Inner {
                buffer: VecDeque::with_capacity(capacity),
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            capacity,
        }
    }

    pub fn push(&self, value: T) {
        let mut inner = self.inner.lock().unwrap();
        while inner.buffer.len() == self.capacity {
            inner = self.not_full.wait(inner).unwrap();
        }
        inner.buffer.push_back(value);
        self.not_empty.notify_one();
    }

    pub fn pop(&self) -> T {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if let Some(value) = inner.buffer.pop_front() {
                self.not_full.notify_one();
                return value;
            }
            inner = self.not_empty.wait(inner).unwrap();
        }
    }

    pub fn is_near_capacity(&self, threshold: f64) -> bool {
        assert!(
            (0.0..=1.0).contains(&threshold),
            "threshold must be between 0.0 and 1.0"
        );
        let inner = self.inner.lock().unwrap();
        let occupancy = inner.buffer.len() as f64 / self.capacity as f64;
        occupancy >= threshold
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn concurrent_producer_consumer() {
        let ring = Arc::new(RingBuffer::new(4));
        let producer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                for i in 0..32 {
                    ring.push(i);
                }
            })
        };

        let consumer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                let mut out = Vec::new();
                for _ in 0..32 {
                    out.push(ring.pop());
                }
                out
            })
        };

        producer.join().unwrap();
        let out = consumer.join().unwrap();
        assert_eq!(out, (0..32).collect::<Vec<_>>());
    }

    #[test]
    fn near_capacity_signal_trips() {
        let ring = RingBuffer::new(4);
        ring.push(1);
        ring.push(2);
        ring.push(3);
        assert!(ring.is_near_capacity(0.5));
        assert!(!ring.is_near_capacity(0.9));
    }
}
