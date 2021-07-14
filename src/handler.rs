use lodisdb::{
    common::{DataType, Direction},
    siphash, u32_to_u8x4, u8x4_to_u32, u8x8_to_i64, ArrayMap, List, LodisData, Map,
};

use actix_web::{web, HttpResponse};

use crate::{
    common::{Command, KeyName, PRIME, SUCCESS},
    error::{LodisError, Result},
    state::GlobalState,
};

pub async fn parse_params(body: web::Bytes) -> Result<Vec<web::BytesMut>> {
    let n = body.len();

    let mut params: Vec<web::BytesMut> = Vec::new();
    let mut buf: [u8; 4] = [0; 4];
    let mut index = 0;

    loop {
        if index == n {
            break;
        }

        if index + 4 > n {
            return Err(LodisError::ParseParamError);
        }
        buf.clone_from_slice(&body[index..index + 4]);

        let end = u8x4_to_u32(&buf) as usize;
        if index + 4 + end > n {
            return Err(LodisError::ParseParamError);
        }
        params.push(web::BytesMut::from(&body[index + 4..index + 4 + end]));
        index += 4 + end;
    }
    Ok(params)
}

pub async fn handle(
    body: web::Bytes,
    key: web::Path<KeyName>,
    global_state: web::Data<GlobalState>,
    command: Command,
) -> Result<HttpResponse> {
    let params = parse_params(body).await?;
    let key: &str = &key;

    let db = global_state.db.clone();

    match command {
        // List
        Command::LPUSH => {
            if params.len() < 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::LPUSH,
                    &params
                )));
            }

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                list.push_left(&params)?;
            }
            &global_state.add_key(&key, DataType::List);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::RPUSH => {
            if params.len() < 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::RPUSH,
                    &params
                )));
            }

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                list.push(&params)?;
            }
            &global_state.add_key(&key, DataType::List);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::LPOP => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.pop_left()?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::RPOP => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.pop()?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::RANDPOP => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.pop_random()?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::LRANGE => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::LRANGE,
                    &params
                )));
            }

            let mut buf: [u8; 4] = [0; 4];
            buf.clone_from_slice(&params[0]);
            let start = u8x4_to_u32(&buf);
            buf.clone_from_slice(&params[1]);
            let end = u8x4_to_u32(&buf);

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                list.range(start, end, Direction::Forward)?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for value in values {
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::RRANGE => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::RRANGE,
                    &params
                )));
            }

            let mut buf: [u8; 4] = [0; 4];
            buf.clone_from_slice(&params[0]);
            let start = u8x4_to_u32(&buf);
            buf.clone_from_slice(&params[1]);
            let end = u8x4_to_u32(&buf);

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                list.range(start, end, Direction::Reverse)?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for value in values {
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::LINDEX => {
            if params.len() != 1 || params[0].len() != 8usize {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::LINDEX,
                    &params
                )));
            }

            let mut buf: [u8; 8] = [0; 8];
            buf.clone_from_slice(&params[0]);

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.index(u8x8_to_i64(&buf))?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::LRAND => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.random()?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::LLEN => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.length()?
            };
            return Ok(HttpResponse::Ok().body([SUCCESS, &u32_to_u8x4(value)[..]].concat()));
        }
        // Delete one element by its index
        Command::LDEL => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::LDEL,
                    &params
                )));
            }

            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let mut buf: [u8; 4] = [0; 4];
            buf.clone_from_slice(&params[0]);
            let index = u8x4_to_u32(&buf);
            {
                let lock = mutex.lock();
                list.delete(index as i64)?
            }
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::LRM => {
            let list = List::new(key.to_string(), db);
            let hash_num = siphash(&list.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                list.remove()?
            };
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }

        // Map
        Command::HGET => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HGET,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                map.get(&params[0])?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::HSET => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HSET,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                map.set(&params[0], &params[1])?;
            }
            &global_state.add_key(&key, DataType::Map);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::HSETNX => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HSETNX,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                map.setnx(&params[0], &params[1])?;
            }
            &global_state.add_key(&key, DataType::Map);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::HGETALL => {
            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                map.all()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for (key, value) in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        // Return data struct
        //
        // SUCCESS + 1u8(is Some) + key1_len + value1
        //         + 0u8(is None)
        //         + 1u8(is Some) + key3_len + value3
        //         + ...
        Command::HMGET => {
            if params.len() < 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HMGET,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                map.mget(&params)?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for value in values {
                if let Some(v) = value {
                    buf.extend_from_slice(b"\x01");
                    buf.extend_from_slice(&u32_to_u8x4(v.len() as u32)[..]);
                    buf.extend_from_slice(&v);
                } else {
                    buf.extend_from_slice(b"\x00");
                }
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::HMSET => {
            if params.len() < 2 || params.len() % 2 != 0 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HMSET,
                    &params
                )));
            }

            let pairs = {
                let mut pairs = Vec::new();
                for i in 0..params.len() / 2 {
                    pairs.push((&params[i * 2], &params[i * 2 + 1]));
                }
                pairs
            };

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                map.mset(&pairs)?
            };
            &global_state.add_key(&key, DataType::Map);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::HINCRBY => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HINCRBY,
                    &params
                )));
            }

            let incr;
            if let Ok(val_str) = ::std::str::from_utf8(&*params[1]) {
                if let Ok(val_int) = val_str.parse::<i64>() {
                    incr = val_int;
                } else {
                    return Err(LodisError::ParamTypeError(format!(
                        "command: {:?}, params: {:?}, incr is not an integer string",
                        Command::HINCRBY,
                        &params
                    )));
                }
            } else {
                return Err(LodisError::ParamTypeError(format!(
                    "command: {:?}, params: {:?}, incr is not an integer string",
                    Command::HINCRBY,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = map.prefix_hash() % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                map.increase(&params[0], incr)?;
            };
            &global_state.add_key(&key, DataType::Map);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::HKEYS => {
            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                map.keys()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for key in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::HVALS => {
            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                map.values()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for value in values {
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::HEXISTS => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HEXISTS,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                map.exists(&params[0])?
            };
            if value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &[1u8]].concat()));
            } else {
                return Ok(HttpResponse::Ok().body([SUCCESS, &[0u8]].concat()));
            }
        }
        Command::HDEL => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::HDEL,
                    &params
                )));
            }

            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                map.delete(&params[0])?
            }
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::HLEN => {
            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                map.length()?
            };
            return Ok(HttpResponse::Ok().body([SUCCESS, &u32_to_u8x4(value)[..]].concat()));
        }
        Command::HRM => {
            let map = Map::new(key.to_string(), db);
            let hash_num = siphash(&map.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                map.remove()?
            };
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }

        // ArrayMap
        Command::ALPUSH => {
            if params.len() < 2 || params.len() % 2 != 0 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ALPUSH,
                    &params
                )));
            }

            let pairs = {
                let mut pairs = Vec::new();
                for i in 0..params.len() / 2 {
                    pairs.push((&params[i * 2], &params[i * 2 + 1]));
                }
                pairs
            };

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.push_left(&pairs)?
            }
            &global_state.add_key(&key, DataType::ArrayMap);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::ALPUSHNX => {
            if params.len() < 2 || params.len() % 2 != 0 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ALPUSHNX,
                    &params
                )));
            }

            let pairs = {
                let mut pairs = Vec::new();
                for i in 0..params.len() / 2 {
                    pairs.push((&params[i * 2], &params[i * 2 + 1]));
                }
                pairs
            };

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.pushnx_left(&pairs)?;
            }
            &global_state.add_key(&key, DataType::ArrayMap);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::ARPUSH => {
            if params.len() < 2 || params.len() % 2 != 0 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ARPUSH,
                    &params
                )));
            }

            let pairs = {
                let mut pairs = Vec::new();
                for i in 0..params.len() / 2 {
                    pairs.push((&params[i * 2], &params[i * 2 + 1]));
                }
                pairs
            };

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.push(&pairs)?;
            }
            &global_state.add_key(&key, DataType::ArrayMap);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::ARPUSHNX => {
            if params.len() < 2 || params.len() % 2 != 0 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ARPUSHNX,
                    &params
                )));
            }

            let pairs = {
                let mut pairs = Vec::new();
                for i in 0..params.len() / 2 {
                    pairs.push((&params[i * 2], &params[i * 2 + 1]));
                }
                pairs
            };

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.pushnx(&pairs)?;
            }
            &global_state.add_key(&key, DataType::ArrayMap);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::AINCRBY => {
            if params.len() != 2 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::AINCRBY,
                    &params
                )));
            }

            let incr;
            if let Ok(val_str) = ::std::str::from_utf8(&*params[1]) {
                if let Ok(val_int) = val_str.parse::<i64>() {
                    incr = val_int;
                } else {
                    return Err(LodisError::ParamTypeError(format!(
                        "command: {:?}, params: {:?}, incr is not an integer string",
                        Command::AINCRBY,
                        &params
                    )));
                }
            } else {
                return Err(LodisError::ParamTypeError(format!(
                    "command: {:?}, params: {:?}, incr is not an integer string",
                    Command::AINCRBY,
                    &params
                )));
            }

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = arraymap.prefix_hash() % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.increase(&params[0], incr)?;
            }
            &global_state.add_key(&key, DataType::ArrayMap);
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }

        // Result data structure
        //
        // If there is a value
        // SUCCESS + key_len + key + value_len + value
        // else
        // SUCCESS
        Command::ALPOP => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.pop_left()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            if let Some((key, value)) = value {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::ARPOP => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.pop()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            if let Some((key, value)) = value {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::ARANDPOP => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.pop_random()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            if let Some((key, value)) = value {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::AGET => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::AGET,
                    &params
                )));
            }

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.get(&params[0])?
            };
            if let Some(value) = value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &*value].concat()));
            } else {
                return Ok(HttpResponse::Ok().body(SUCCESS));
            }
        }
        Command::ARAND => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.random()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            if let Some((key, value)) = value {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::ALRANGE => {
            if params.len() != 2 || params[0].len() != 4 || params[1].len() != 4 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ALRANGE,
                    &params
                )));
            }

            let mut buf: [u8; 4] = [0; 4];
            buf.clone_from_slice(&params[0]);
            let start = u8x4_to_u32(&buf);
            buf.clone_from_slice(&params[1]);
            let end = u8x4_to_u32(&buf);

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];

            let values = {
                let lock = mutex.lock();
                arraymap.range(start, end, Direction::Forward)?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for (key, value) in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::ARRANGE => {
            if params.len() != 2 || params[0].len() != 4 || params[1].len() != 4 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ARRANGE,
                    &params
                )));
            }

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];

            let mut buf: [u8; 4] = [0; 4];
            buf.clone_from_slice(&params[0]);
            let start = u8x4_to_u32(&buf);
            buf.clone_from_slice(&params[1]);
            let end = u8x4_to_u32(&buf);

            let values = {
                let lock = mutex.lock();
                arraymap.range(start, end, Direction::Reverse)?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for (key, value) in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::AKEYS => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                arraymap.keys()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for key in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::AVALS => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                arraymap.values()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for value in values {
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::AALL => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let values = {
                let lock = mutex.lock();
                arraymap.all()?
            };
            let mut buf = Vec::new();
            buf.extend_from_slice(SUCCESS);
            for (key, value) in values {
                buf.extend_from_slice(&u32_to_u8x4(key.len() as u32)[..]);
                buf.extend_from_slice(&key);
                buf.extend_from_slice(&u32_to_u8x4(value.len() as u32)[..]);
                buf.extend_from_slice(&value);
            }
            return Ok(HttpResponse::Ok().body(buf));
        }
        Command::AEXISTS => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::AEXISTS,
                    &params
                )));
            }

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.exists(&params[0])?
            };
            if value {
                return Ok(HttpResponse::Ok().body([SUCCESS, &[1u8]].concat()));
            } else {
                return Ok(HttpResponse::Ok().body([SUCCESS, &[0u8]].concat()));
            }
        }
        Command::ALEN => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.length()?
            };
            return Ok(HttpResponse::Ok().body([SUCCESS, &u32_to_u8x4(value)[..]].concat()));
        }
        Command::ADEL => {
            if params.len() != 1 {
                return Err(LodisError::ParamNoMatch(format!(
                    "command: {:?}, params: {:?}",
                    Command::ADEL,
                    &params
                )));
            }

            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            {
                let lock = mutex.lock();
                arraymap.delete(&params[0])?;
            }
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
        Command::ARM => {
            let arraymap = ArrayMap::new(key.to_string(), db);
            let hash_num = siphash(&arraymap.prefix()) % PRIME;
            let mutex = &global_state.locks[hash_num as usize];
            let value = {
                let lock = mutex.lock();
                arraymap.remove()?
            };
            return Ok(HttpResponse::Ok().body(SUCCESS));
        }
    }
}
