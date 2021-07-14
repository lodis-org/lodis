use std::sync::Arc;

use rocksdb::{WriteBatch, DB};

use crate::{
    common::{DBValue, DataType},
    crypto::siphash,
    data::LodisData,
    error::{DBError, Result},
    utils::{u32_to_u8x4, u64_to_u8x8, u8x4_to_u32},
};

const TYPE: DataType = DataType::Map;

pub struct Map {
    name: String,
    pub(crate) prefix: [u8; 9],
    db: Arc<DB>,
}

impl LodisData for Map {
    fn db(&self) -> &Arc<DB> {
        &self.db
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn prefix(&self) -> &[u8] {
        &self.prefix[..]
    }
}

impl Map {
    pub fn new(name: String, db: Arc<DB>) -> Map {
        let mut prefix: [u8; 9] = [0; 9];
        prefix[0..1].clone_from_slice(&TYPE.flag()[..]);
        prefix[1..9].clone_from_slice(&u64_to_u8x8(siphash(&name)));
        Map { name, prefix, db }
    }

    pub fn length(&self) -> Result<u32> {
        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@L");

        let raw_value = self.db.get(&dbkey)?;
        if let Some(raw_v) = raw_value {
            let mut v: [u8; 4] = [0; 4];
            v.clone_from_slice(&raw_v);
            Ok(u8x4_to_u32(&v))
        } else {
            Ok(0)
        }
    }

    fn incr_length(&self, incr: i32, batch: &mut WriteBatch) -> Result<()> {
        let length = self.length()?;
        if length == 0 && incr < 0 {
            return Ok(());
        }

        let length = (length as i32 + incr) as u32;

        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@L");

        batch.put(&dbkey, &u32_to_u8x4(length));

        Ok(())
    }

    pub fn exists<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        if self.db.get(dbkey)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get<K>(&self, key: K) -> Result<Option<DBValue>>
    where
        K: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        let value = self.db.get(&dbkey)?;

        Ok(value.map(|v| DBValue::Direct(v)))
    }

    pub fn set<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        let pre_value = self.db.get(&dbkey)?;

        let mut batch = WriteBatch::default();

        batch.put(&dbkey, &value);

        if pre_value.is_none() {
            self.incr_length(1, &mut batch)?;
        }

        self.db.write(batch)?;

        Ok(())
    }

    // Set the value of a field, only if the field does not exist.
    pub fn setnx<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        let pre_value = self.db.get(&dbkey)?;
        if pre_value.is_some() {
            return Ok(());
        }

        let mut batch = WriteBatch::default();

        batch.put(&dbkey, &value);
        self.incr_length(1, &mut batch)?;
        self.db.write(batch)?;

        Ok(())
    }

    // Increase the value only if the value is an integer string
    pub fn increase<K>(&self, key: K, incr: i64) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        let mut batch = WriteBatch::default();

        if let Some(value) = self.db.get(&dbkey)? {
            if let Ok(val_str) = std::str::from_utf8(&value) {
                if let Ok(val_int) = val_str.parse::<i64>() {
                    let new_val = (val_int + incr).to_string();
                    batch.put(&dbkey, &new_val);
                } else {
                    return Err(DBError::IsNotNumeric);
                }
            } else {
                return Err(DBError::IsNotNumeric);
            }
        } else {
            let new_val = incr.to_string();
            batch.put(&dbkey, &new_val);
            self.incr_length(1, &mut batch)?;
        }

        self.db.write(batch)?;

        Ok(())
    }

    pub fn delete<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");
        let dbkey = [&dbkey[..], key.as_ref()].concat();

        let value = self.db.get(&dbkey)?;
        if value.is_none() {
            return Ok(());
        }

        let mut batch = WriteBatch::default();

        batch.delete(&dbkey);
        self.incr_length(-1, &mut batch)?;

        self.db.write(batch)?;

        Ok(())
    }

    // Get all field names in the map
    pub fn keys(&self) -> Result<Vec<DBValue>> {
        let mut prefix: [u8; 10] = [0; 10];
        prefix[0..9].clone_from_slice(&self.prefix);
        prefix[9..10].clone_from_slice(b":");

        let length = self.length()?;

        let mut keys = Vec::new();
        let iter = self.db.prefix_iterator(&prefix);
        for (key, _) in iter.take(length as usize) {
            keys.push(DBValue::PrefixKeyB(key));
        }
        Ok(keys)
    }

    // Get all values in the map
    pub fn values(&self) -> Result<Vec<DBValue>> {
        let mut prefix: [u8; 10] = [0; 10];
        prefix[0..9].clone_from_slice(&self.prefix);
        prefix[9..10].clone_from_slice(b":");

        let length = self.length()?;

        let mut values = Vec::new();
        let iter = self.db.prefix_iterator(&prefix);
        for (_, value) in iter.take(length as usize) {
            values.push(DBValue::DirectB(value));
        }
        Ok(values)
    }

    // Get all key, value pairs in the map
    pub fn all(&self) -> Result<Vec<(DBValue, DBValue)>> {
        let mut prefix: [u8; 10] = [0; 10];
        prefix[0..9].clone_from_slice(&self.prefix);
        prefix[9..10].clone_from_slice(b":");

        let length = self.length()?;

        let mut all = Vec::new();
        let iter = self.db.prefix_iterator(&prefix);
        for (key, value) in iter.take(length as usize) {
            all.push((DBValue::PrefixKeyB(key), DBValue::DirectB(value)));
        }
        Ok(all)
    }

    // Set multiple fields to multiple values
    pub fn mset<K, V>(&self, kvs: &[(K, V)]) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");

        let mut incr = 0;
        let mut batch = WriteBatch::default();
        for (key, value) in &kvs[..] {
            let tdbkey = [&dbkey[..], key.as_ref()].concat();
            if self.db.get(&tdbkey)?.is_none() {
                incr += 1;
            }
            batch.put(tdbkey, value);
        }
        self.incr_length(incr, &mut batch)?;
        self.db.write(batch)?;

        Ok(())
    }

    // Get all values by the given fields
    pub fn mget<K>(&self, keys: &[K]) -> Result<Vec<Option<DBValue>>>
    where
        K: AsRef<[u8]>,
    {
        let mut dbkey: [u8; 10] = [0; 10];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b":");

        let mut values = Vec::new();
        for key in &keys[..] {
            let tdbkey = [&dbkey[..], key.as_ref()].concat();
            let value = self.db.get(&tdbkey)?;
            values.push(value.map(|v| DBValue::Direct(v)));
        }
        Ok(values)
    }
}

#[cfg(test)]
mod test_map {
    use rocksdb::{DBIterator, Direction, IteratorMode, Options, ReadOptions, WriteBatch, DB};

    use std::{
        sync::{Arc, Mutex},
        thread::spawn,
        u32::MAX as MAX_U32,
    };

    use crate::{data::LodisData, utils::u8x4_to_u32};

    #[test]
    fn test_map_new() {
        let path = "test-map-db1";
        {
            let db = Arc::new(DB::open_default(path).unwrap());
            let map = super::Map::new("abc".to_string(), db);

            let length = map.length().unwrap();
            assert_eq!(length, 0);
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }

    #[test]
    fn test_map_funcs() {
        let path = "test-map-db2";
        {
            let db = Arc::new(DB::open_default(path).unwrap());
            let map = super::Map::new("abc".to_string(), db);

            map.set(b"a1", b"A1").unwrap();
            map.set(b"a2", b"A2").unwrap();

            let item = map.exists(b"a1").unwrap();
            assert_eq!(item, true);

            let length = map.length().unwrap();
            assert_eq!(length, 2);

            let item = map.get(b"a1").unwrap();
            assert_eq!(&*item.unwrap(), b"A1");

            let item = map.get(b"a2").unwrap();
            assert_eq!(&*item.unwrap(), b"A2");

            map.setnx(b"b1", b"B1").unwrap();
            let item = map.get(b"b1").unwrap();
            assert_eq!(&*item.unwrap(), b"B1");

            map.setnx(b"b1", b"B3").unwrap();
            let item = map.get(b"b1").unwrap();
            assert_eq!(&*item.unwrap(), b"B1");

            map.increase(b"incr", -10).unwrap();
            let item = map.get(b"incr").unwrap();
            assert_eq!(&*item.unwrap(), b"-10");
            let length = map.length().unwrap();
            assert_eq!(length, 4);

            map.increase(b"incr", 10).unwrap();
            let item = map.get(b"incr").unwrap();
            assert_eq!(&*item.unwrap(), b"0");
            map.delete(b"incr").unwrap();

            map.delete(b"b1").unwrap();
            let item = map.get(b"b1").unwrap();
            assert_eq!(item.is_none(), true);

            let items = map.keys().unwrap();
            let mut vec: Vec<&[u8]> = Vec::new();
            for item in items.iter() {
                vec.push(item.as_ref());
            }
            assert_eq!(&vec[..], &[b"a1", b"a2"]);

            let items = map.values().unwrap();
            let mut vec: Vec<&[u8]> = Vec::new();
            for item in items.iter() {
                vec.push(item.as_ref());
            }
            assert_eq!(&vec[..], &[b"A1", b"A2"]);

            let items = map.all().unwrap();
            let mut vec: Vec<&[u8]> = Vec::new();
            for (key, value) in items.iter() {
                vec.push(key.as_ref());
                vec.push(value.as_ref());
            }
            assert_eq!(&vec[..], &[b"a1", b"A1", b"a2", b"A2"]);

            map.mset(&[(b"c1", b"C1"), (b"c2", b"C1")]).unwrap();
            let items = map.mget(&[b"a1", b"c2", b"xx"]).unwrap();
            assert_eq!(&items[0].as_ref().unwrap().as_ref(), b"A1");
            assert_eq!(&items[1].as_ref().unwrap().as_ref(), b"C1");
            assert_eq!(items[2].is_none(), true);
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }

    #[test]
    fn test_map_corrs() {
        let path = "test-map-db3";
        {
            let db = Arc::new(DB::open_default(path).unwrap());
            let db1 = db.clone();
            let db2 = db.clone();

            let lock = Arc::new(Mutex::new(()));
            let lock1 = lock.clone();
            let lock2 = lock.clone();

            let t1 = spawn(move || {
                let lock = lock1.lock();
                let map = super::Map::new("abc".to_string(), db1);
                for i in 0..1000 {
                    map.set(i.to_string().as_bytes(), i.to_string().as_bytes())
                        .unwrap();
                }
            });

            let t2 = spawn(move || {
                let lock = lock2.lock();
                let map = super::Map::new("abc".to_string(), db2);
                for i in 1000..2000 {
                    map.set(i.to_string().as_bytes(), i.to_string().as_bytes())
                        .unwrap();
                }
            });

            t1.join();
            t2.join();

            let lock = lock.lock();
            let map = super::Map::new("abc".to_string(), db.clone());
            let length = map.length().unwrap();
            assert_eq!(length, 2000);

            let mut rs = 0;
            for i in 0..2000 {
                let v = map.get(i.to_string().as_bytes()).unwrap().unwrap();
                rs += v.to_utf8().unwrap().parse::<u32>().unwrap();
            }
            assert_eq!(rs, 1999000);

            map.remove().unwrap();

            let mut readopts = ReadOptions::default();
            readopts.set_prefix_same_as_start(true);

            let mut iter = db.iterator_opt(
                IteratorMode::From(&map.prefix, Direction::Forward),
                readopts,
            );

            let item = iter.next();
            assert_eq!(item, None);
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }
}
