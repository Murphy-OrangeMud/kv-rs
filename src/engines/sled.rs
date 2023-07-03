use crate::engines::{KvsEngine, Result};
use log::debug;
use sled::Db;
use std::io::ErrorKind;
use std::path::PathBuf;

#[derive(Clone)]
pub struct SledStore {
    db: Db,
}

impl KvsEngine for SledStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes().to_vec())?;
        self.db.flush()?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(&key)? {
            None => Ok(None),
            Some(v) => Ok(Some(std::str::from_utf8(v.as_ref()).unwrap().to_string())),
        }
    }
    fn remove(&self, key: String) -> Result<()> {
        match self.db.get(&key)? {
            None => Err(std::io::Error::new(ErrorKind::Other, "Non existent key")),
            Some(_) => {
                self.db.remove(key)?;
                self.db.flush()?;
                Ok(())
            }
        }
    }
}

impl SledStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledStore> {
        let db = sled::open(path.into())?;
        Ok(SledStore { db })
    }
}
