use std::path::Path;

pub use rocksdb::DB;

mod crypto;
mod utils;

pub mod common;
pub mod error;

mod arraymap;
mod data;
mod list;
mod map;
// mod store;

pub use arraymap::ArrayMap;
pub use crypto::siphash;
pub use data::LodisData;
pub use error::DBError;
pub use list::List;
pub use map::Map;
pub use utils::{u32_to_u8x4, u64_to_u8x8, u8_to_u8x1, u8x4_to_u32, u8x8_to_i64};

pub fn make_db<P: AsRef<Path>>(path: P) -> DB {
    DB::open_default(path).unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
