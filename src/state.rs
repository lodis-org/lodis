use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use lodisdb::{common::DataType, make_db, u64_to_u8x8, Map, DB};

use crate::{
    common::{LODIS_KEY_MAP, LODIS_STRING_MAP, PRIME},
    error::Result,
};

pub struct GlobalState {
    pub db: Arc<DB>,
    // Store all keys and their data type
    pub key_map: Map,
    // Global map for all string data type
    pub string_map: Map,

    // th-(PRIME + 1) lock is for LODIS_KEY_MAP map
    // th-(PRIME + 2) lock is for LODIS_STRING_MAP map
    pub locks: [Mutex<()>; 10 + PRIME as usize],
}

unsafe impl Sync for GlobalState {}
unsafe impl Send for GlobalState {}

impl GlobalState {
    pub fn new<P: AsRef<Path>>(path: P) -> GlobalState {
        let db = Arc::new(make_db(path));
        GlobalState {
            db: db.clone(),
            key_map: Map::new(LODIS_KEY_MAP.to_string(), db.clone()),
            string_map: Map::new(LODIS_STRING_MAP.to_string(), db.clone()),
            locks: unsafe {
                let mut arr: [Mutex<()>; 10 + PRIME as usize] =
                    std::mem::MaybeUninit::uninit().assume_init();
                for item in &mut arr[..] {
                    std::ptr::write(item, Mutex::new(()));
                }
                arr
            },
        }
    }

    // Record all lodisdb data keys
    //
    // Structure
    // Key: DataType + key -> timestamp
    pub fn add_key<K>(&self, key: K, data_type: DataType) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mutex = &self.locks[PRIME as usize];
        let lock = mutex.lock();
        self.key_map.setnx(
            [&data_type.flag()[..], key.as_ref()].concat(),
            &u64_to_u8x8(now)[..],
        )?;
        Ok(())
    }
}
