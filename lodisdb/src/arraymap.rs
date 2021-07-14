use std::{hash::Hash, sync::Arc};

use crate::{
    common::{DBValue, DataType, Direction},
    crypto::siphash,
    data::LodisData,
    error::{DBError, Result},
    list::List,
    map::Map,
    utils::{u32_to_u8x4, u64_to_u8x8},
};

use rocksdb::DB;

const TYPE: DataType = DataType::ArrayMap;

pub struct ArrayMap {
    name: String,
    list: List,
    map: Map,
}

impl LodisData for ArrayMap {
    fn db(&self) -> &Arc<DB> {
        self.list.db()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn prefix(&self) -> &[u8] {
        self.list.prefix()
    }

    fn remove(&self) -> Result<()> {
        self.list.remove()?;
        self.map.remove()?;
        Ok(())
    }
}

/// Array Map
///
/// This is an array, but it has a map for checking whether an element exists.
///
/// # Structure
///
/// ```
/// List Key:   TYPE + name_hash + indexi                ->   Value:           key_hash + value
///                                  |                                            |
///                                  |                                            |
///                                  |-----------------------------------|        |
///                                                                      |        |
///                                                                      |        |
///                                         ---------------------------------------
///                                         |                            |
///                                         |                            |
/// Map  Key:   TYPE + name_hash +       key_hash        ->   Value:   index + key
/// ```
impl ArrayMap {
    pub fn new(name: String, db: Arc<DB>) -> ArrayMap {
        let flag = TYPE.flag();
        let mut list = List::new(name.to_string() + "@list", db.clone());
        list.prefix[0] = flag[0];
        let mut map = Map::new(name.to_string() + "@map", db.clone());
        map.prefix[0] = flag[0];

        ArrayMap {
            name: name.to_string(),
            list,
            map,
        }
    }

    fn key_hash<K>(&self, key: K) -> [u8; 8]
    where
        K: Hash + AsRef<[u8]>,
    {
        u64_to_u8x8(siphash(&key))
    }

    pub fn length(&self) -> Result<u32> {
        self.list.length()
    }

    pub fn get<K>(&self, key: K) -> Result<Option<DBValue>>
    where
        K: Hash + AsRef<[u8]>,
    {
        let key_hash = self.key_hash(&key);
        if let Some(DBValue::Direct(v)) = self.map.get(&key_hash)? {
            let index_key = DBValue::IndexKey(v);
            if let Some(DBValue::Direct(v)) = self.list.index_with_abs(index_key.index())? {
                return Ok(Some(DBValue::KeyhashValue(v)));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.get: list value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    // Randomly returning a item
    pub fn random(&self) -> Result<Option<(DBValue, DBValue)>> {
        if let Some(DBValue::Direct(v)) = self.list.random()? {
            let keyhash_value = DBValue::KeyhashValue(v);
            if let Some(DBValue::Direct(index_key)) = self.map.get(keyhash_value.keyhash())? {
                let index_key = DBValue::IndexKey(index_key);
                return Ok(Some((index_key, keyhash_value)));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.random: map value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    pub fn exists<K>(&self, key: K) -> Result<bool>
    where
        K: Hash + AsRef<[u8]>,
    {
        let key_hash = self.key_hash(&key);
        self.map.exists(&key_hash)
    }

    fn set_new_pair<U>(&self, key_hash: U, key: U, value: U, direction: Direction) -> Result<()>
    where
        U: AsRef<[u8]>,
    {
        let indexes;
        match direction {
            Direction::Forward => {
                indexes = self
                    .list
                    .push(&[[key_hash.as_ref(), value.as_ref()].concat()])?;
            }
            Direction::Reverse => {
                indexes = self
                    .list
                    .push_left(&[[key_hash.as_ref(), value.as_ref()].concat()])?;
            }
        }
        self.map.set(
            &key_hash,
            [&u32_to_u8x4(indexes[0])[..], key.as_ref()].concat(),
        )?;
        Ok(())
    }

    fn set_list_item<U>(&self, index: u32, key_hash: U, value: U) -> Result<()>
    where
        U: AsRef<[u8]>,
    {
        self.list
            .set_by_absindex(index, [key_hash.as_ref(), value.as_ref()].concat())?;
        Ok(())
    }

    // Append an element to the list without checking whether the element exists.
    // The direction of appending:
    //     -----------+
    //                |
    pub fn push<K, V>(&self, pairs: &[(K, V)]) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        for (key, value) in pairs {
            let key_hash = self.key_hash(&key);
            if let Some(DBValue::Direct(v)) = self.map.get(&key_hash)? {
                let v = DBValue::IndexKey(v);
                self.set_list_item(v.index(), &key_hash[..], value.as_ref())?;
            } else {
                self.set_new_pair(
                    &key_hash[..],
                    key.as_ref(),
                    value.as_ref(),
                    Direction::Forward,
                )?;
            }
        }
        Ok(())
    }

    // Append an element to the list only if the element dose not exists.
    // The direction of appending:
    //     -----------+
    //                |
    pub fn pushnx<K, V>(&self, pairs: &[(K, V)]) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        for (key, value) in pairs {
            let key_hash = self.key_hash(&key);
            if self.map.exists(&key_hash)? {
                continue;
            }
            self.set_new_pair(
                &key_hash[..],
                key.as_ref(),
                value.as_ref(),
                Direction::Forward,
            )?;
        }
        Ok(())
    }

    // Append an element to the list without checking whether the element exists.
    // The direction of appending:
    //     +-----------
    //     |
    pub fn push_left<K, V>(&self, pairs: &[(K, V)]) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        for (key, value) in pairs {
            let key_hash = self.key_hash(&key);
            if let Some(DBValue::Direct(v)) = self.map.get(&key_hash)? {
                let v = DBValue::IndexKey(v);
                self.set_list_item(v.index(), &key_hash[..], value.as_ref())?;
            } else {
                self.set_new_pair(
                    &key_hash[..],
                    key.as_ref(),
                    value.as_ref(),
                    Direction::Reverse,
                )?;
            }
        }
        Ok(())
    }

    pub fn pushnx_left<K, V>(&self, pairs: &[(K, V)]) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        for (key, value) in pairs {
            let key_hash = self.key_hash(&key);
            if self.map.exists(&key_hash)? {
                continue;
            }
            self.set_new_pair(
                &key_hash[..],
                key.as_ref(),
                value.as_ref(),
                Direction::Reverse,
            )?;
        }
        Ok(())
    }

    // Increase the value only if the value is an integer string
    pub fn increase<K>(&self, key: K, incr: i64) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
    {
        let key_hash = self.key_hash(&key);

        // Change old value
        if let Some(DBValue::Direct(v)) = self.map.get(&key_hash)? {
            let index_key = DBValue::IndexKey(v);
            let index = index_key.index();
            if let Some(DBValue::Direct(v)) = self.list.index_with_abs(index)? {
                if let Some(val_str) = DBValue::KeyhashValue(v).to_utf8() {
                    if let Ok(val_int) = val_str.parse::<i64>() {
                        let new_val = (val_int + incr).to_string();
                        self.set_list_item(index, &key_hash[..], &new_val.as_bytes())?;
                    } else {
                        return Err(DBError::IsNotNumeric);
                    }
                } else {
                    return Err(DBError::IsNotNumeric);
                }
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.get: list value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        // Set a new value
        } else {
            let new_val = incr.to_string();
            self.set_new_pair(
                &key_hash[..],
                key.as_ref(),
                &new_val.as_bytes(),
                Direction::Forward,
            )?;
        }
        Ok(())
    }

    pub fn pop(&self) -> Result<Option<(DBValue, DBValue)>> {
        let value = self.list.pop()?;
        if let Some(DBValue::Direct(v)) = value {
            let keyhash_value = DBValue::KeyhashValue(v);
            if let Some(DBValue::Direct(index_key)) = self.map.get(keyhash_value.keyhash())? {
                let index_key = DBValue::IndexKey(index_key);
                self.map.delete(keyhash_value.keyhash())?;
                return Ok(Some((index_key, keyhash_value)));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.pop: map value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    pub fn pop_left(&self) -> Result<Option<(DBValue, DBValue)>> {
        let value = self.list.pop_left()?;
        if let Some(DBValue::Direct(v)) = value {
            let keyhash_value = DBValue::KeyhashValue(v);
            if let Some(DBValue::Direct(index_key)) = self.map.get(keyhash_value.keyhash())? {
                let index_key = DBValue::IndexKey(index_key);
                self.map.delete(keyhash_value.keyhash())?;
                return Ok(Some((index_key, keyhash_value)));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.pop_left: map value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    pub fn pop_random(&self) -> Result<Option<(DBValue, DBValue)>> {
        let value = self.list.pop_random()?;
        if let Some(DBValue::Direct(v)) = value {
            let keyhash_value = DBValue::KeyhashValue(v);
            if let Some(DBValue::Direct(index_key)) = self.map.get(keyhash_value.keyhash())? {
                let index_key = DBValue::IndexKey(index_key);
                self.map.delete(keyhash_value.keyhash())?;
                return Ok(Some((index_key, keyhash_value)));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.pop_left: map value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(None)
    }

    pub fn range(
        &self,
        start: u32,
        end: u32,
        direction: Direction,
    ) -> Result<Vec<(DBValue, DBValue)>> {
        let mut vec = Vec::new();
        for value in self.list.range(start, end, direction)? {
            if let DBValue::Direct(v) = value {
                let keyhash_value = DBValue::KeyhashValue(v);
                if let Some(DBValue::Direct(index_key)) = self.map.get(keyhash_value.keyhash())? {
                    let index_key = DBValue::IndexKey(index_key);
                    vec.push((index_key, keyhash_value));
                } else {
                    return Err(DBError::DBValueNotMatch(
                        "ArrayMap.range: map value is not DBValue::Direct(DBVector)".to_owned(),
                    ));
                }
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.range: list value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(vec)
    }

    pub fn keys(&self) -> Result<Vec<DBValue>> {
        let mut vec = Vec::new();
        for value in self.map.values()? {
            if let DBValue::DirectB(v) = value {
                vec.push(DBValue::IndexKeyB(v));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.keys: map value is not DBValue::DirectB(Box<[u8]>)".to_owned(),
                ));
            }
        }
        Ok(vec)
    }

    pub fn values(&self) -> Result<Vec<DBValue>> {
        let mut vec = Vec::new();
        for value in self.list.all()? {
            if let DBValue::Direct(v) = value {
                vec.push(DBValue::KeyhashValue(v));
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.values: list value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(vec)
    }

    pub fn all(&self) -> Result<Vec<(DBValue, DBValue)>> {
        let mut vec = Vec::new();
        for key in self.map.values()? {
            if let DBValue::DirectB(k) = key {
                let k = DBValue::IndexKeyB(k);
                if let Some(DBValue::Direct(v)) = self.list.index_with_abs(k.index())? {
                    let v = DBValue::KeyhashValue(v);
                    vec.push((k, v));
                } else {
                    return Err(DBError::DBValueNotMatch(
                        "ArrayMap.all: list value is not DBValue::Direct(DBVector)".to_owned(),
                    ));
                }
            } else {
                return Err(DBError::DBValueNotMatch(
                    "ArrayMap.all: map value is not DBValue::Direct(DBVector)".to_owned(),
                ));
            }
        }
        Ok(vec)
    }

    pub fn delete<K>(&self, key: K) -> Result<()>
    where
        K: Hash + AsRef<[u8]>,
    {
        let key_hash = self.key_hash(&key);
        if let Some(DBValue::Direct(v)) = self.map.get(&key_hash)? {
            let index_key = DBValue::IndexKey(v);
            // Delete the element which has the key
            self.map.delete(&key_hash)?;
            self.list.delete_with_abs_index(index_key.index())?;

            // Set moved element in list to right index
            if let Some(DBValue::Direct(v)) = self.list.index_with_abs(index_key.index())? {
                let keyhash_value = DBValue::KeyhashValue(v);
                if let Some(DBValue::Direct(v)) = self.map.get(keyhash_value.keyhash())? {
                    let old_index_key = DBValue::IndexKey(v);
                    // set new index + key
                    self.map.set(
                        keyhash_value.keyhash(),
                        [&u32_to_u8x4(index_key.index())[..], old_index_key.key()].concat(),
                    )?;
                } else {
                    return Err(DBError::DBValueNotMatch(
                        "ArrayMap.delete: map value is not DBValue::Direct(DBVector)".to_owned(),
                    ));
                }
            }
        }
        return Ok(());
    }
}

#[cfg(test)]
mod test_arraymap {
    use rocksdb::{
        DBIterator, Direction as DBDirection, IteratorMode, Options, ReadOptions, WriteBatch, DB,
    };

    use std::{
        sync::{Arc, Mutex},
        thread::spawn,
        u32::MAX as MAX_U32,
    };

    use crate::{common::Direction, utils::u8x4_to_u32};

    #[test]
    fn test_arraymap() {
        let path = "test-arraymap-db1";
        {
            let db = Arc::new(DB::open_default(path).unwrap());

            {
                let arraymap = super::ArrayMap::new("abc".to_owned(), db);

                arraymap.push(&[(b"a1", b"X1")]).unwrap();
                let v = arraymap.get(b"a1").unwrap().unwrap();
                assert_eq!(&*v, b"X1");

                arraymap.push(&[(b"a1", b"A1")]).unwrap();
                let v = arraymap.get(b"a1").unwrap().unwrap();
                assert_eq!(&*v, b"A1");

                let length = arraymap.length().unwrap();
                assert_eq!(length, 1);

                arraymap.pushnx(&[(b"a1", b"Y1")]).unwrap();
                let v = arraymap.get(b"a1").unwrap().unwrap();
                assert_eq!(&*v, b"A1");

                arraymap.push_left(&[(b"a2", b"X2")]).unwrap();
                let v = arraymap.get(b"a2").unwrap().unwrap();
                assert_eq!(&*v, b"X2");

                arraymap.push_left(&[(b"a2", b"A2")]).unwrap();
                let v = arraymap.get(b"a2").unwrap().unwrap();
                assert_eq!(&*v, b"A2");

                arraymap.pushnx_left(&[(b"a2", b"Y2")]).unwrap();
                let v = arraymap.get(b"a2").unwrap().unwrap();
                assert_eq!(&*v, b"A2");

                let keys = arraymap.keys().unwrap();
                assert_eq!(&*keys[0], b"a2");
                assert_eq!(&*keys[1], b"a1");

                let keys = arraymap.values().unwrap();
                assert_eq!(&*keys[0], b"A2");
                assert_eq!(&*keys[1], b"A1");

                let keys = arraymap.all().unwrap();
                assert_eq!(&*keys[0].0, b"a2");
                assert_eq!(&*keys[0].1, b"A2");
                assert_eq!(&*keys[1].0, b"a1");
                assert_eq!(&*keys[1].1, b"A1");

                arraymap.increase(b"incr", -10).unwrap();
                let v = arraymap.get(b"incr").unwrap().unwrap();
                assert_eq!(&*v, b"-10");
                arraymap.increase(b"incr", 10).unwrap();
                let v = arraymap.get(b"incr").unwrap().unwrap();
                assert_eq!(&*v, b"0");
                let length = arraymap.length().unwrap();
                assert_eq!(length, 3);
                arraymap.delete(b"incr").unwrap();

                let (k, v) = arraymap.pop().unwrap().unwrap();
                assert_eq!(&*k, b"a1");
                assert_eq!(&*v, b"A1");

                let (k, v) = arraymap.pop_left().unwrap().unwrap();
                assert_eq!(&*k, b"a2");
                assert_eq!(&*v, b"A2");

                for i in 0..1000 {
                    arraymap
                        .push(&[(i.to_string().as_bytes(), i.to_string().as_bytes())])
                        .unwrap();
                }
                let length = arraymap.length().unwrap();
                assert_eq!(length, 1000);

                arraymap.delete(b"100").unwrap();
                let length = arraymap.length().unwrap();
                assert_eq!(length, 1000 - 1);

                let items = arraymap.range(0, 1000, Direction::Forward).unwrap();
                let mut rs = 0;
                for (k, v) in items {
                    rs += k.to_utf8().unwrap().parse::<u32>().unwrap();
                    rs += v.to_utf8().unwrap().parse::<u32>().unwrap();
                }
                assert_eq!(rs, 499500 * 2 - 200);
            }
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }
}
