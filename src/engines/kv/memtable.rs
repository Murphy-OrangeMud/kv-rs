use crate::Result;
use skiplist::skipmap::Iter;
use skiplist::{skipmap, SkipMap};
use std::sync::Arc;

use super::InternalKey;

// TODO: Add support for generics
// TODO: Add implementation of skipmap
pub struct MemTable {
    kv: SkipMap<InternalKey, (i64, usize)>,
}

impl MemTable {
    pub fn new() -> Result<MemTable> {
        let mut kv = SkipMap::<InternalKey, (i64, usize)>::new();
        Ok(MemTable { kv })
    }

    pub fn insert(&mut self, k: InternalKey, pos: u64, size: usize) -> Result<()> {
        self.kv.insert(k, (pos as i64, size));
        Ok(())
    }

    pub fn get(&self, k: InternalKey) -> Result<Option<(i64, usize)>> {
        let (pos, value) = (1, 2);
        let v = self.kv.get(&k).as_deref();
        match v {
            None => Ok(None),
            Some(&idx) => Ok(Some(idx)),
        }
    }

    pub fn size(&self) -> u64 {
        unimplemented!()
    }

    pub fn iter(&self) -> Iter<InternalKey, (i64, usize)> {
        unimplemented!()
    }
}
