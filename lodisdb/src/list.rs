use std::sync::Arc;

use rocksdb::{WriteBatch, DB};

use rand::{self, Rng};

use crate::{
    common::{DBValue, DataType, Direction, MAX_U32},
    crypto::siphash,
    data::LodisData,
    error::{DBError, Result},
    utils::{u32_to_u8x4, u64_to_u8x8, u8x4_to_u32},
};

const TYPE: DataType = DataType::List;

/// the length of name must be 8 bytes (u64)
///
/// If _ represent empty, + as item, then
///
/// _ _ + + + + + + _ _
///   |             |
///   head          tail
///
/// The Structure of Key and Value
///
/// - key TYPE + name_hash + $ + index
///
/// - value value
#[derive(Clone)]
pub struct List {
    name: String,
    pub(crate) prefix: [u8; 9],
    db: Arc<DB>,
}

impl LodisData for List {
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

impl List {
    pub fn new(name: String, db: Arc<DB>) -> List {
        let mut prefix: [u8; 9] = [0; 9];
        prefix[0..1].clone_from_slice(&TYPE.flag()[..]);
        prefix[1..9].clone_from_slice(&u64_to_u8x8(siphash(&name)));
        List { name, prefix, db }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix[..]
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

    fn head(&self) -> Result<u32> {
        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@H");

        let raw_value = self.db.get(&dbkey)?;
        if let Some(raw_v) = raw_value {
            let mut v: [u8; 4] = [0; 4];
            v.clone_from_slice(&raw_v);
            Ok(u8x4_to_u32(&v))
        } else {
            Ok(MAX_U32)
        }
    }

    fn tail(&self) -> Result<u32> {
        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@T");

        let raw_value = self.db.get(&dbkey)?;
        if let Some(raw_v) = raw_value {
            let mut v: [u8; 4] = [0; 4];
            v.clone_from_slice(&raw_v);
            Ok(u8x4_to_u32(&v))
        } else {
            Ok(0)
        }
    }

    fn set_head(&self, value: u32, batch: &mut WriteBatch) -> Result<()> {
        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@H");

        batch.put(&dbkey, &u32_to_u8x4(value));

        Ok(())
    }

    fn set_tail(&self, value: u32, batch: &mut WriteBatch) -> Result<()> {
        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@T");

        batch.put(&dbkey, &u32_to_u8x4(value));

        Ok(())
    }

    fn incr_length(&self, incr: i64, batch: &mut WriteBatch) -> Result<()> {
        let length = self.length()?;
        if length == 0 && incr < 0 {
            return Ok(());
        }

        let mut length = length as i64 + incr;
        if length < 0 {
            length = 0;
        }

        let mut dbkey: [u8; 11] = [0; 11];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..11].clone_from_slice(b"@L");

        batch.put(&dbkey, &u32_to_u8x4(length as u32));

        Ok(())
    }

    // Get the absolute index at db
    fn abs_index(&self, index: i64) -> Result<u32> {
        if index >= 0 {
            let mut head = self.head()?;
            if head == MAX_U32 {
                head = 0;
            } else {
                head += 1;
            }
            Ok(((head as i64 + index) % (MAX_U32 as i64 + 1)) as u32)
        } else {
            let mut tail = self.tail()?;
            if tail == 0 {
                tail = MAX_U32;
            } else {
                tail -= 1;
            }
            Ok(((tail as i64 + index + 1) % (MAX_U32 as i64 + 1)) as u32)
        }
    }

    // Return a random relative index
    fn random_index(&self) -> Result<i64> {
        let mut length = self.length().unwrap() as f64;
        if length != 0f64 {
            length -= 1f64; // index starts from 0
        }
        let mut rng = rand::thread_rng();
        let y: f64 = rng.gen(); // 0 ~ 1
        let index = (length * y) as i64;
        Ok(index)
    }

    /// Get a element by its index
    ///
    /// index can be positive or negative
    /// index is an i64 which can be from -infinit to +infinit
    pub fn index(&self, index: i64) -> Result<Option<DBValue>> {
        let abs_index = self.abs_index(index)?;
        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(abs_index));

        let value = self.db.get(&dbkey)?;

        Ok(value.map(|v| DBValue::Direct(v)))
    }

    pub fn index_with_abs(&self, abs_index: u32) -> Result<Option<DBValue>> {
        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(abs_index));

        let value = self.db.get(&dbkey)?;

        Ok(value.map(|v| DBValue::Direct(v)))
    }

    // Randomly returning a item
    pub fn random(&self) -> Result<Option<DBValue>> {
        let rand_index = self.random_index()?;
        self.index(rand_index)
    }

    /// Get a range of element
    ///
    /// if direction is forward:
    ///
    ///        ------------->
    ///  ++++++++++++++++++++++++++
    ///        |             |
    ///        start         end
    ///
    /// if direction is reverse
    ///         <-------------
    ///  ++++++++++++++++++++++++++
    ///        |             |
    ///        end           start
    pub fn range(&self, start: u32, end: u32, direction: Direction) -> Result<Vec<DBValue>> {
        let mut range = Vec::new();
        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        match direction {
            Direction::Forward => {
                let head = self.head()? as u64;
                for i in start..end {
                    let index = (1 + i as u64 + head) as u32;
                    dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));
                    let value = self.db.get(&dbkey)?;
                    if let Some(value) = value {
                        range.push(DBValue::Direct(value));
                    }
                }
            }
            Direction::Reverse => {
                let tail = self.tail()? as i64;
                for i in start..end {
                    let index = (tail - i as i64 - 1) as u32;
                    dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));
                    let value = self.db.get(&dbkey)?;
                    if let Some(value) = value {
                        range.push(DBValue::Direct(value));
                    }
                }
            }
        }
        Ok(range)
    }

    pub fn all(&self) -> Result<Vec<DBValue>> {
        let head = self.head()? as u64 + 1;
        let length = self.length()?;
        let mut all = Vec::new();
        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        for i in 0..length {
            let index = (i as u64 + head) as u32;
            dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));
            let value = self.db.get(&dbkey)?;
            if let Some(value) = value {
                all.push(DBValue::Direct(value));
            }
        }
        Ok(all)
    }

    /// Store key and value to a item of list
    pub fn push<V>(&self, values: &[V]) -> Result<Vec<u32>>
    where
        V: AsRef<[u8]>,
    {
        let mut index = self.tail()?;

        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");

        let mut indexes = Vec::new();
        let mut batch = WriteBatch::default();
        for value in values {
            indexes.push(index);
            dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));
            batch.put(&dbkey, &value);
            if index == MAX_U32 {
                index = 0;
            } else {
                index += 1;
            }
        }

        self.set_tail(index, &mut batch)?;
        self.incr_length(values.len() as i64, &mut batch)?;

        self.db.write(batch)?;

        Ok(indexes)
    }

    pub fn push_left<V>(&self, values: &[V]) -> Result<Vec<u32>>
    where
        V: AsRef<[u8]>,
    {
        let mut index = self.head()?;

        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");

        let mut indexes = Vec::new();
        let mut batch = WriteBatch::default();
        for value in values {
            indexes.push(index);
            dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));
            batch.put(&dbkey, &value);
            if index == 0 {
                index = MAX_U32
            } else {
                index -= 1
            };
        }

        self.set_head(index, &mut batch)?;
        self.incr_length(values.len() as i64, &mut batch)?;

        self.db.write(batch)?;

        Ok(indexes)
    }

    pub fn set_by_absindex<V>(&self, abs_index: u32, value: V) -> Result<()>
    where
        V: AsRef<[u8]>,
    {
        let head = self.head()?;
        let tail = self.tail()?;
        //  -----+++++++++++-----
        //      |           |
        //      head        tail
        if head < tail {
            if abs_index <= head || abs_index >= tail {
                return Err(DBError::OutOfRange(abs_index));
            }
        //  ++++++-------------++++++
        //        |           |
        //        tail        head
        } else {
            if abs_index <= head && abs_index >= tail {
                return Err(DBError::OutOfRange(abs_index));
            }
        }

        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(abs_index));

        self.db.put(&dbkey, value.as_ref())?;

        Ok(())
    }

    pub fn pop(&self) -> Result<Option<DBValue>> {
        let length = self.length()?;
        if length == 0 {
            return Ok(None);
        }

        let tail = self.tail()?;
        let index = if tail == 0 { MAX_U32 } else { tail - 1 };

        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));

        let value = self.db.get(&dbkey)?;

        let mut batch = WriteBatch::default();

        batch.delete(&dbkey);
        self.set_tail(index, &mut batch)?;
        self.incr_length(-1, &mut batch)?;

        self.db.write(batch)?;

        return Ok(value.map(|v| DBValue::Direct(v)));
    }

    pub fn pop_left(&self) -> Result<Option<DBValue>> {
        let length = self.length()?;
        if length == 0 {
            return Ok(None);
        }

        let head = self.head()?;
        let index = if head == MAX_U32 { 0 } else { head + 1 };

        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(index));

        let value = self.db.get(&dbkey)?;

        let mut batch = WriteBatch::default();

        batch.delete(&dbkey);
        self.set_head(index, &mut batch)?;
        self.incr_length(-1, &mut batch)?;

        self.db.write(batch)?;

        return Ok(value.map(|v| DBValue::Direct(v)));
    }

    // Pop out a random item
    pub fn pop_random(&self) -> Result<Option<DBValue>> {
        let rand_index = self.random_index()?;
        let value = self.index(rand_index)?;
        if value.is_some() {
            self.delete(rand_index)?;
        }
        Ok(value)
    }

    pub fn delete(&self, index: i64) -> Result<()> {
        let abs_index = self.abs_index(index)?;
        self.delete_with_abs_index(abs_index)
    }

    /// Delete an element by its index
    ///
    /// If we only delete the element at the index,
    /// There will be left a gap by the index.
    ///
    /// We ignore the gap by moving the head element to the index.
    /// In the way, the order of list will be changed.
    pub fn delete_with_abs_index(&self, abs_index: u32) -> Result<()> {
        let mut dbkey: [u8; 14] = [0; 14];
        dbkey[0..9].clone_from_slice(&self.prefix);
        dbkey[9..10].clone_from_slice(b"$");
        dbkey[10..14].clone_from_slice(&u32_to_u8x4(abs_index));

        let value = self.db.get(&dbkey)?;
        if value.is_none() {
            return Ok(());
        }

        let head = self.head()?;
        let first_index = if head == MAX_U32 { 0 } else { head + 1 };
        let tail = self.tail()?;
        let last_index = if tail == 0 { MAX_U32 } else { tail - 1 };

        if abs_index == first_index {
            let mut batch = WriteBatch::default();
            batch.delete(&dbkey);
            self.incr_length(-1, &mut batch)?;
            self.set_head(abs_index, &mut batch)?;

            self.db.write(batch)?;
        } else if abs_index == last_index {
            let mut batch = WriteBatch::default();
            batch.delete(&dbkey);
            self.incr_length(-1, &mut batch)?;
            self.set_tail(abs_index, &mut batch)?;

            self.db.write(batch)?;
        } else {
            dbkey[10..14].clone_from_slice(&u32_to_u8x4(first_index));
            let first_value = self.db.get(&dbkey)?;

            let mut batch = WriteBatch::default();

            // delete first element
            batch.delete(&dbkey);
            self.incr_length(-1, &mut batch)?;
            // set head to first_index
            self.set_head(first_index, &mut batch)?;

            // set first element to abs_index
            dbkey[10..14].clone_from_slice(&u32_to_u8x4(abs_index));
            batch.put(&dbkey, &*first_value.unwrap());

            self.db.write(batch)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_list {
    use rocksdb::{DBIterator, Direction, IteratorMode, Options, ReadOptions, WriteBatch, DB};

    use std::{
        sync::{Arc, Mutex},
        thread::spawn,
        u32::MAX as MAX_U32,
    };

    use crate::{
        common::{DBValue, DataType, Direction as CDirection},
        data::LodisData,
        utils::u8x4_to_u32,
    };

    #[test]
    fn test_list_new() {
        let path = "test-list-db1";
        {
            let db = Arc::new(DB::open_default(path).unwrap());

            let list = super::List::new("abc".to_string(), db);

            let length = list.length().unwrap();
            assert_eq!(length, 0);

            let head = list.head().unwrap();
            assert_eq!(head, MAX_U32);

            let tail = list.tail().unwrap();
            assert_eq!(tail, 0);
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }

    #[test]
    fn test_list_funcs() {
        let path = "test-list-db2";
        {
            let db = Arc::new(DB::open_default(path).unwrap());
            let list = super::List::new("abc".to_string(), db);

            list.push(&[b"a1"]).unwrap();
            list.push(&[b"a2"]).unwrap();

            list.push_left(&[b"b1"]).unwrap();
            list.push_left(&[b"b2"]).unwrap();

            let length = list.length().unwrap();
            assert_eq!(length, 4);

            let head = list.head().unwrap();
            assert_eq!(head, MAX_U32 - 2);

            let tail = list.tail().unwrap();
            assert_eq!(tail, 2);

            let item = list.index(0).unwrap();
            assert_eq!(&*item.unwrap(), b"b2");

            let item = list.pop().unwrap();
            assert_eq!(&*item.unwrap(), b"a2");

            let item = list.pop_left().unwrap();
            assert_eq!(&*item.unwrap(), b"b2");

            let length = list.length().unwrap();
            assert_eq!(length, 2);

            let head = list.head().unwrap();
            assert_eq!(head, MAX_U32 - 1);

            let tail = list.tail().unwrap();
            assert_eq!(tail, 1);

            list.push(&[b"c1", b"c2", b"c3"]).unwrap();
            list.push_left(&[b"d1", b"d2", b"d3"]).unwrap();

            let item = list.index(0).unwrap();
            assert_eq!(&*item.unwrap(), b"d3");

            let item = list.index(1).unwrap();
            assert_eq!(&*item.unwrap(), b"d2");

            let item = list.index(7).unwrap();
            assert_eq!(&*item.unwrap(), b"c3");

            let item = list.index(-1).unwrap();
            assert_eq!(&*item.unwrap(), b"c3");

            let item = list.index_with_abs(0).unwrap();
            assert_eq!(&*item.unwrap(), b"a1");

            let items = list.range(0, 8, CDirection::Forward).unwrap();
            let mut vec: Vec<&[u8]> = Vec::new();
            for item in items.iter() {
                vec.push(item.as_ref());
            }
            assert_eq!(
                &vec[..],
                &[b"d3", b"d2", b"d1", b"b1", b"a1", b"c1", b"c2", b"c3"]
            );

            let items = list.all().unwrap();
            let mut vec: Vec<&[u8]> = Vec::new();
            for item in items.iter() {
                vec.push(item.as_ref());
            }
            assert_eq!(
                &vec[..],
                &[b"d3", b"d2", b"d1", b"b1", b"a1", b"c1", b"c2", b"c3"]
            );

            list.set_by_absindex(0, b"S0").unwrap();
            let item = list.index(4).unwrap();
            assert_eq!(&*item.unwrap(), b"S0");

            list.delete(0).unwrap();
            let head = list.head().unwrap();
            assert_eq!(head, MAX_U32 - 3);

            list.delete(-1).unwrap();
            let tail = list.tail().unwrap();
            assert_eq!(tail, 3);

            list.delete(3).unwrap();
            let item = list.index(0).unwrap();
            assert_eq!(&*item.unwrap(), b"d1");

            let item = list.index(2).unwrap();
            assert_eq!(&*item.unwrap(), b"d2");
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }

    #[test]
    fn test_list_corrs() {
        let path = "test-list-db3";
        {
            let db = Arc::new(DB::open_default(path).unwrap());
            let db1 = db.clone();
            let db2 = db.clone();

            let lock = Arc::new(Mutex::new(()));
            let lock1 = lock.clone();
            let lock2 = lock.clone();

            let t1 = spawn(move || {
                let lock = lock1.lock();
                let list = super::List::new("abc".to_string(), db1);
                for i in 0..1000 {
                    list.push(&[i.to_string().as_bytes()]).unwrap();
                }
            });

            let t2 = spawn(move || {
                let lock = lock2.lock();
                let list = super::List::new("abc".to_string(), db2);
                for i in 1000..2000 {
                    list.push(&[i.to_string().as_bytes()]).unwrap();
                }
            });

            t1.join();
            t2.join();

            let lock = lock.lock();
            let list = super::List::new("abc".to_string(), db.clone());
            let length = list.length().unwrap();
            assert_eq!(length, 2000);

            let head = list.head().unwrap();
            assert_eq!(head, MAX_U32);

            let tail = list.tail().unwrap();
            assert_eq!(tail, 2000);

            let mut rs = 0;
            for i in 0..2000 {
                let v = list.index(i).unwrap().unwrap();
                rs += v.to_utf8().unwrap().parse::<u32>().unwrap();
            }
            assert_eq!(rs, 1999000);

            list.remove().unwrap();

            let mut readopts = ReadOptions::default();
            readopts.set_prefix_same_as_start(true);

            let mut iter = db.iterator_opt(
                IteratorMode::From(&list.prefix, Direction::Forward),
                readopts,
            );

            let item = iter.next();
            assert_eq!(item, None);
        }

        let opts = Options::default();
        assert!(DB::destroy(&opts, path).is_ok());
    }
}
