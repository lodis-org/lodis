use rocksdb::Error as RocksDBError;

use thiserror::Error as ThisError;

pub type Result<T, E = DBError> = std::result::Result<T, E>;

#[derive(Debug, ThisError)]
pub enum DBError {
    #[error("RocksDBError: {0}")]
    RocksDBError(RocksDBError),

    #[error("DBValue is not matched: {0}")]
    DBValueNotMatch(String),

    #[error("Index out of range: {0}")]
    OutOfRange(u32),

    #[error("The Value is numerical")]
    IsNotNumeric,
}

impl From<RocksDBError> for DBError {
    fn from(err: RocksDBError) -> DBError {
        DBError::RocksDBError(err)
    }
}
