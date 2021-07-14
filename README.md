# Lodis - Local Dictionary Server

**Lodis** is a **data struction store** server storing data on-disk on local machine.
Lodis supports different kinds of absolute data structions, such as List, HashMap, ArrayMap.

All contents stored at Lodis are binary-safe strings.

Lodis uses [RocksDB](https://rocksdb.org/) as backend to handle all key-value operations
and uses http protocol as connection protocol.


## Data Struction

- List

  `List` collects string elements sorted by their indexes pushed. It is an array, not a linked list.
  Because `List` is an array, it **doesn't** support inserting. If it deletes a element which is not at
  head or tail of the array, The indexes of elements will be changed.

- HashMap

  `HashMap` composes key-value pairs. Key and value are both strings.

- ArrayMap

  `ArrayMap` is a `List`, but its' elements are key-value pairs. It can be regarded as `List` + `HashMap`.


## Commands

Following, we assume that a Lodis data struction has the name `name`.

### List

- LPUSH

  ```
  LPUSH name element1 [element2 ...]
  ```

  Append one or more elements to a `List` from left.

- RPUSH

  ```
  RPUSH name element1 [element2 ...]
  ```

  Append one or more elements to a `List` from right.

- LPOP

  ```
  LPOP name
  ```

  Pop an element from a `List` from left.

- RPOP

  ```
  RPOP name
  ```

  Pop an element from a `List` from right.

- RANDPOP

  ```
  RANDPOP name
  ```

  Pop an element from a `List` randomly.
  The command will be **changing** the indexes of elements of the `List`.

- LRANGE

  ```
  LRANGE name start end

  # start > end`
  ```

  Return a range of elements of a `List` from left start to end.

- RRANGE

  ```
  RRANGE name start end

  # start > end
  ```

  Return a range of elements of a `List` from right start to end.

- LINDEX

  ```
  LINDEX name index
  ```

  Return the element of a `List` which is at index `index`.

- LRAND

  ```
  LRAND name
  ```

  Randomly, return a element of a `List`.

- LLEN

  ```
  LLEN name
  ```

  Return the number of elements of a `List`.

- LDEL

  ```
  LDEL name index
  ```

  Delete an element of a `List` which has the index `index`.
  The command will be **changing** the indexes of elements of the `List`.

- LRM

  ```
  LDEL name
  ```

  Remove the `List` from Lodis.


### HashMap

- HGET

  ```
  HGET name field
  ```

  Return the value of the field `field` of a `HashMap`.

- HSET

  ```
  HSET name field value
  ```

  Set the value of the field `field` as `value` in a `HashMap`.

- HSETNX

  ```
  HSETNX name field value
  ```

  Set the value of the field `field` as `value` in a `HashMap`, **ONLY IF THE FIELD NO EXISTS**.

- HGETALL

  ```
  HGETALL name
  ```

  Get all field-value pairs of a `HashMap`.

- HMGET

  ```
  HMGET name field1 [field2 ...]
  ```

  Get multi-values of a list fields of a `HashMap` at once.

- HMSET

  ```
  HMSET name field1 value1 [field2 value2 ...]
  ```

  Set pairs field-value to a `HashMap`

- HINCRBY

  ```
  HINCRBY name field integer
  ```

  `integer` can be positive or negative.

  If the value of the field `field` is an integer string, the command can increase the numberic value by `num`.

- HKEYS

  ```
  HKEYS name
  ```

  Return all fields' names of a `HashMap`.

- HVALS

  ```
  HVALS name
  ```

  Return all fields' names of a `HashMap`.

- HEXISTS

  ```
  HEXISTS name field
  ```

  Check whether the field exists in a `HashMap`.

- HDEL

  ```
  HDEL name field
  ```

  Delete a pair which has field `field` from a `HashMap`.

- HLEN

  ```
  HLEN name
  ```

  Return the number of pairs in a `HashMap`.

- HRM

  ```
  HRM name
  ```

  Remove the `HashMap` from Lodis.


### ArrayMap

- ALPUSH

  ```
  ALPUSH name field1 value1 [field2 value2 ...]
  ```

  Append pairs of field-value to a `ArrayMap` from left.

- ALPUSHNX

  ```
  ALPUSHNX name field1 value1 [field2 value2 ...]
  ```

  Append pairs of field-values to a `ArrayMap` from left **ONLY IF THE FIELDS NO EXISTS**.

- ARPUSH


  ```
  ARPUSH name field1 value1 [field2 value2 ...]
  ```

  Append pairs of field-value to a `ArrayMap` from right.

- ARPUSHNX

  ```
  ARPUSHNX name field1 value1 [field2 value2 ...]
  ```

  Append pairs of field-values to a `ArrayMap` from right **ONLY IF THE FIELDS NO EXISTS**.

- AINCRBY

  ```
  AINCRBY name field integer
  ```

  `integer` can be positive or negative.

  If the value of the field `field` is an integer string, the command can increase the numberic value by `num`.

- ALPOP

  ```
  ALPOP name
  ```

  Pop a pair from a `ArrayMap` from left.

- ARPOP

  ```
  ARPOP name
  ```

  Pop a pair from a `ArrayMap` from right.

- ARANDPOP

  ```
  ARANDPOP name
  ```

  Randomly, pop a pair from a `ArrayMap`.

- AGET

  ```
  AGET name field
  ```

  Return the value of the field `field` in a `ArrayMap`.

- ARAND

  ```
  ARAND name
  ```

  Randomly, return a pair in a `ArrayMap`.

- ALRANGE

  ```
  ALRANGE name start end

  # start > end
  ```

  Return a range of pairs of a `ArrayMap` from left start to end.

- ARRANGE

  ```
  ARRANGE name start end

  # start > end
  ```

  Return a range of pairs of a `ArrayMap` from right start to end.

- AKEYS

  ```
  AKEYS name
  ```

  Return all fields of a `ArrayMap`.

- AVALS

  ```
  AVALS name
  ```

  Return all values of a `ArrayMap`.

- AALL

  ```
  AALL name
  ```

  Return all pairs of a `ArrayMap`.

- AEXISTS

  ```
  AEXISTS name field
  ```

  Check whether a field exists in a `ArrayMap`.

- ALEN

  ```
  ALEN name
  ```

  Return the number of pairs in a `ArrayMap`.

- ADEL

  ```
  ADEL name field
  ```

  Delete the pair whihc has field `field` from a `ArrayMap`.

- ARM

  ```
  ARM name
  ```

  Remove a `ArrayMap` from Lodis.


## Clients

Lodis uses http protocol as the communication protocol between servers and clients.

The Lodis server accepts the following `POST` http scheme.

```
POST /{command}/{name} HTTP/1.1
Host: ...
...

CONTENT
```

- `CONTENT` format

```
[4bytes big-endian unsign of length of arg1][bytes of arg1][4bytes big-endian unsign of length of arg2][bytes of arg2]...
```

### Reture Types

Lodis server returns a response to client with corresponding content as following:

```
[1byte status code unsign][content binaries]
```

- Status code

  `b'0'` is as successful, else as errores.

- Content Binaries

  - Bytes

    ```
    [Bytes string]
    ```

  - Bool

    ```
    [1byte big-endian unsign 0 (false) or 1 (true)]
    ```

  - Integer

    ```
    [4bytes big-endian unsign]
    ```

  - List

    ```
    [4bytes big-endian unsign of length of item1][item1 bytes][4bytes big-endian unsign of length of item2][item2 bytes]...
    ```

  - ListOption

    ```
    [1byte big-endian unsign 0 (is None) or 1 (is not None)][4bytes big-endian unsign of length of item1][item1 bytes][1byte big-endian unsign 0 (is None) or 1 (is not None)][4bytes big-endian unsign of length of item2][item2 bytes]...
    ```

  - Pair

    ```
    [4bytes big-endian unsign of length of key1][key1 bytes][4bytes big-endian unsign of length of value1][value1 bytes]
    ```

  - Pairs

    ```
    [4bytes big-endian unsign of length of key1][key1 bytes][4bytes big-endian unsign of length of value1][value1 bytes][4bytes big-endian unsign of length of key2][key2 bytes][4bytes big-endian unsign of length of value2][value2 bytes]...
    ```

  - No

    ```
    [0byte]
    ```


#### Commands Returned Content Types

|  Command | Type |
| ---- | ---- |
| LPUSH | No |
| RPUSH | No |
| LPOP | Bytes |
| RPOP | Bytes |
| RANDPOP | Bytes |
| LRANGE | List |
| RRANGE | List |
| LINDEX | Bytes |
| LRAND | Bytes |
| LLEN | Int |
| LDEL | No |
| LRM | No |
| HGET | Bytes |
| HSET | No |
| HSETNX | No |
| HGETALL | Pairs |
| HMGET | ListOption |
| HMSET | No |
| HINCRBY | No |
| HKEYS | List |
| HVALS | List |
| HEXISTS | Bool |
| HDEL | No |
| HLEN | Int |
| HRM | No |
| ALPUSH | No |
| ALPUSHNX | No |
| ARPUSH | No |
| ARPUSHNX | No |
| AINCRBY | No |
| ALPOP | Pair |
| ARPOP | Pair |
| ARANDPOP | Pair |
| AGET | Bytes |
| ARAND | Pair |
| ALRANGE | Pairs |
| ARRANGE | Pairs |
| AKEYS | List |
| AVALS | List |
| AALL | Pairs |
| AEXISTS | Bool |
| ALEN | Int |
| ADEL | No |
| ARM | No |


#### Clients

Python: lodis-py, alodis


## Run Lodis

Lodis needs following environment variables to initiate server and rocketdb.

- `LODIS_DB_PATH`

  This is the path to store persist data.

- `LODIS_IP_PORT`

  The variable points out the bind ip and port, e.g. `127.0.0.1:6666`

Use following command to start the Lodis server.

```
LODIS_DB_PATH=path/to/db \
LODIS_IP_PORT="127.0.0.1:8311" \
lodis
```
