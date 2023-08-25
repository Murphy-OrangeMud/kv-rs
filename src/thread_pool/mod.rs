pub mod naive;
pub mod rayon;
pub mod shared_queue;

pub use crate::thread_pool::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;

use crate::Result;

pub trait ThreadPool {
    fn new(_worker_num: u32) -> Result<Self>
    where
        Self: Sized;
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
