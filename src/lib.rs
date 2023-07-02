//#![feature(test)]
#![allow(soft_unstable)]

pub mod engines;
pub mod proto;
pub mod thread_pool;

pub use engines::sled::SledStore;
pub use engines::kv::KvStore;
pub use engines::KvsEngine;
pub use engines::Result;
pub use proto::Command;
pub use proto::Record;