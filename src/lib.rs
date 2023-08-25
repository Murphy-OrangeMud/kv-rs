//#![feature(test)]
#![allow(soft_unstable)]

pub mod engines;
pub mod proto;
pub mod server;
pub mod thread_pool;

//pub use engines::kv::KvStore;
pub use engines::sled::SledStore;
pub use engines::KvsEngine;
pub use engines::Result;
pub use proto::Command;
pub use proto::Record;
pub use server::KvServer;
pub use thread_pool::NaiveThreadPool;
pub use thread_pool::RayonThreadPool;
pub use thread_pool::SharedQueueThreadPool;
pub use thread_pool::ThreadPool;

// TODO: Change the result type and define a set of error type of my own
