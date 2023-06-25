use std::collections::HashMap;

pub struct KvStore {
    kv: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> KvStore {
        let kv = HashMap::<String, String>::new();
        KvStore { kv }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.kv.insert(key, value);
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        self.kv.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) {
        self.kv.remove(&key);
    }
}
