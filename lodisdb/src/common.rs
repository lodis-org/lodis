use std::ops::Deref;

use crate::utils::u8x4_to_u32;

pub const MAX_U32: u32 = std::u32::MAX;
pub const MAX_U64: u64 = std::u64::MAX;

pub enum Direction {
    Forward,
    Reverse,
}

/// DBValue is used for lodisdb.
///
/// `Vec<u8>` is the returned value from rocksdb.
/// If `withkey` is true, the previous 8 bytes of dbvector is
/// the hash of the item's key (not dbkey).
/// `Index` is the index of the item in `list::List`
pub enum DBValue {
    Direct(Vec<u8>),
    KeyhashValue(Vec<u8>),
    IndexKey(Vec<u8>),
    DirectB(Box<[u8]>),
    KeyhashValueB(Box<[u8]>),
    IndexKeyB(Box<[u8]>),
    PrefixKeyB(Box<[u8]>),
}

use DBValue::*;

impl DBValue {
    pub fn value(&self) -> &[u8] {
        match self {
            Direct(dbvector) => &dbvector,
            KeyhashValue(dbvector) => &dbvector[8..],
            IndexKey(dbvector) => &dbvector[4..],
            DirectB(bx) => bx.as_ref(),
            KeyhashValueB(bx) => &bx.as_ref()[8..],
            IndexKeyB(bx) => &bx.as_ref()[4..],
            PrefixKeyB(bx) => &bx.as_ref()[10..],
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            IndexKey(dbvector) => &dbvector[4..],
            IndexKeyB(bx) => &bx.as_ref()[4..],
            PrefixKeyB(bx) => &bx.as_ref()[10..],
            _ => &[],
        }
    }

    pub fn keyhash(&self) -> &[u8] {
        match self {
            KeyhashValue(dbvector) => &dbvector[..8],
            KeyhashValueB(bx) => &bx.as_ref()[..8],
            _ => &[],
        }
    }

    pub fn index(&self) -> u32 {
        match self {
            IndexKey(dbvector) => {
                let mut buf: [u8; 4] = [0; 4];
                buf.clone_from_slice(&dbvector[..4]);
                u8x4_to_u32(&buf)
            }
            IndexKeyB(bx) => {
                let mut buf: [u8; 4] = [0; 4];
                buf.clone_from_slice(&bx.as_ref()[..4]);
                u8x4_to_u32(&buf)
            }
            _ => 0,
        }
    }

    pub fn to_utf8(&self) -> Option<&str> {
        ::std::str::from_utf8(self.deref()).ok()
    }
}

impl Deref for DBValue {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.value()
    }
}

impl AsRef<[u8]> for DBValue {
    fn as_ref(&self) -> &[u8] {
        // Implement this via Deref so as not to repeat ourselves
        &*self
    }
}

#[derive(Clone, Copy)]
pub enum DataType {
    List,
    Map,
    ArrayMap,
    Set,
    String,
}

impl DataType {
    pub fn flag(&self) -> [u8; 1] {
        use DataType::*;
        match self {
            List => [1],
            Map => [2],
            ArrayMap => [3],
            Set => [4],
            String => [5],
        }
    }
}
