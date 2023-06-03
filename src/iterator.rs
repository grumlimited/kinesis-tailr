use crate::aws::client::KinesisClient;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use chrono::Utc;

#[async_trait]
pub trait ShardIterator {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput>;
}

pub fn latest<K: KinesisClient>(client: &K) -> LatestShardIterator<'_, K> {
    LatestShardIterator { client }
}

pub fn at_sequence<'a, K: KinesisClient>(
    client: &'a K,
    starting_sequence_number: &'a str,
) -> AtSequenceShardIterator<'a, K> {
    AtSequenceShardIterator {
        client,
        starting_sequence_number,
    }
}

pub fn at_timestamp<'a, K: KinesisClient>(
    client: &'a K,
    timestamp: &'a chrono::DateTime<Utc>,
) -> AtTimestampShardIterator<'a, K> {
    AtTimestampShardIterator { client, timestamp }
}

pub struct LatestShardIterator<'a, K: KinesisClient> {
    client: &'a K,
}

pub struct AtSequenceShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    starting_sequence_number: &'a str,
}

pub struct AtTimestampShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    timestamp: &'a chrono::DateTime<Utc>,
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for LatestShardIterator<'_, K> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_latest(stream, shard_id)
            .await
    }
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for AtSequenceShardIterator<'_, K> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_at_sequence(stream, shard_id, self.starting_sequence_number)
            .await
    }
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for AtTimestampShardIterator<'_, K> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_at_timestamp(stream, shard_id, self.timestamp)
            .await
    }
}
