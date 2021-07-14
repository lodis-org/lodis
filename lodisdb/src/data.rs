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

        // This map does not exist
        if item.is_none() {
            return Ok(());
        }

        let (start_key, _) = item.unwrap();

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
        let (end_key, _) = iter.next().unwrap();

        let mut batch = WriteBatch::default();
        batch.delete_range(&start_key, &end_key);
        batch.delete(&end_key);

        self.db().write(batch)?;

        Ok(())
    }
}
