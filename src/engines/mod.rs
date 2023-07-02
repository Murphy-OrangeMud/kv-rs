pub mod kv;
pub mod sled;

pub type Result<T> = std::result::Result<T, std::io::Error>;

pub use kv::KvStore;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

