use std::env::VarError;

use thiserror::Error as ThisError;

use actix_web::{error::PayloadError, ResponseError};

use lodisdb::DBError;

#[derive(Debug, ThisError)]
pub enum LodisError {
    #[error("1{0}")]
    Error(String),
    #[error("2{0}")]
    LodisdbError(String),
    #[error("3Lodis Config Error")]
    ConfigError,
    #[error("4Parse body parameter error")]
    ParseParamError,
    #[error("5Parameters are not matched, {0}")]
    ParamNoMatch(String),
    #[error("6Type of parameter is not right, {0}")]
    ParamTypeError(String),
}

impl From<DBError> for LodisError {
    fn from(err: DBError) -> LodisError {
        LodisError::LodisdbError(err.to_string())
    }
}

impl From<VarError> for LodisError {
    fn from(_err: VarError) -> LodisError {
        LodisError::ConfigError
    }
}

impl From<PayloadError> for LodisError {
    fn from(err: PayloadError) -> LodisError {
        LodisError::Error(format!("{:?}", err))
    }
}

impl ResponseError for LodisError {}

pub type Result<T, E = LodisError> = std::result::Result<T, E>;
