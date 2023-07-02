use std::path::PathBuf;
use crate::engines::{KvsEngine, Result};
use sled::{Db};
use std::io::ErrorKind;

pub struct SledStore {
    db: Db,
}

impl KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes().to_vec())?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.db.get(&key)? {
            None => Ok(None),
            Some(v) => Ok(Some(std::str::from_utf8(v.as_ref()).unwrap().to_string())),
        }
    }
    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.get(&key)? {
            None => Err(std::io::Error::new(ErrorKind::Other, "Non existent key")),
            Some(_) => {
                self.db.remove(key)?; 
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

