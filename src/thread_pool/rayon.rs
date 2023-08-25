use crate::{Result, ThreadPool};

/// Wrapper of rayon::ThreadPool
pub struct RayonThreadPool {
    inner: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .unwrap();
        Ok(RayonThreadPool { inner: pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.spawn(job)
    }
}
