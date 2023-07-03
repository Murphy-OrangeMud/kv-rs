pub mod kv;
pub mod sled;

pub type Result<T> = std::result::Result<T, std::io::Error>;

pub use crate::engines::sled::SledStore;
pub use kv::KvStore;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}
