use std::thread;

use crate::{Result, ThreadPool};

pub struct NaiveThreadPool {
    _worker_num: u32,
}

impl ThreadPool for NaiveThreadPool {
    fn new(_worker_num: u32) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool { _worker_num })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
