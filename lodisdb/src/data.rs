use std::sync::Arc;

use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch, DB};

use crate::{crypto::siphash, error::Result, utils::u64_to_u8x8};

pub trait LodisData {
    fn db(&self) -> &Arc<DB>;

    fn name(&self) -> &str;

    fn prefix(&self) -> &[u8];

    fn prefix_hash(&self) -> u64 {
        siphash(&self.prefix())
    }

    fn remove(&self) -> Result<()> {
        let mut readopts = ReadOptions::default();
        readopts.set_prefix_same_as_start(true);

        let mut iter = self.db().iterator_opt(
            IteratorMode::From(&self.prefix(), Direction::Forward),
            readopts,
        );

        let item = iter.next();

        // This data does not exist
        if item.is_none() {
            return Ok(());
        }

        let (start_key, _) = item.unwrap();

        // This start_key does not belong to the data
        if &&start_key[0..9] != &self.prefix() {
            return Ok(());
        }

        let next_key_hash = u64_to_u8x8(siphash(&self.name()) + 1);
        let mut prefix: [u8; 9] = [0; 9];
        // type flag
        prefix[0] = self.prefix()[0];
        // hash
        prefix[1..9].clone_from_slice(&next_key_hash[..]);

        let mut readopts = ReadOptions::default();
        readopts.set_prefix_same_as_start(true);

        let mut iter = self.db().iterator_opt(
            IteratorMode::From(&prefix[..], Direction::Reverse),
            readopts,
        );

        let item = iter.next();

        // This end key does not exist
        if item.is_none() {
            return Ok(());
        }

        let (end_key, _) = item.unwrap();

        let mut batch = WriteBatch::default();

        // This end key does not belong to the data. We delete the start_key
        if &&end_key[0..9] != &self.prefix() {
            batch.delete(&start_key);
            self.db().write(batch)?;
            return Ok(());
        }

        // If start_key == end_key, we only need to delete end_key
        if &start_key != &end_key {
            batch.delete_range(&start_key, &end_key);
        }
        batch.delete(&end_key);
        self.db().write(batch)?;
        Ok(())
    }
}
