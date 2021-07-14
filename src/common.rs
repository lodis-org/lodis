use std::ops::Deref;

use serde::Deserialize;

pub const LODIS_KEY_MAP: &'static str = "@@@LODIS_KEY_MAP@@@";
pub const LODIS_STRING_MAP: &'static str = "@@@LODIS_STRING_MAP@@@";

pub const SUCCESS: &'static [u8] = &[0];

pub const PRIME: u64 = 10007;

#[derive(Debug, Deserialize)]
pub struct KeyName {
    pub key: String,
}

impl Deref for KeyName {
    type Target = String;

    fn deref(&self) -> &String {
        &self.key
    }
}

#[derive(Debug)]
pub enum Command {
    // List
    LPUSH,
    RPUSH,
    LPOP,
    RPOP,
    RANDPOP,
    LRANGE,
    RRANGE,
    LINDEX,
    LRAND,
    LLEN,
    LDEL,
    LRM,

    // Map
    HGET,
    HSET,
    HSETNX,
    HGETALL,
    HMGET,
    HMSET,
    HINCRBY,
    HKEYS,
    HVALS,
    HEXISTS,
    HDEL,
    HLEN,
    HRM,

    // ArrayMap
    ALPUSH,
    ALPUSHNX,
    ARPUSH,
    ARPUSHNX,
    AINCRBY,
    ALPOP,
    ARPOP,
    ARANDPOP,
    AGET,
    ARAND,
    ALRANGE,
    ARRANGE,
    AKEYS,
    AVALS,
    AALL,
    AEXISTS,
    ALEN,
    ADEL,
    ARM,
}
