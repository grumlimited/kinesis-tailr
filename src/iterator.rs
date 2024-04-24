use crate::aws::client::KinesisClient;
use crate::kinesis::models::ShardProcessorConfig;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use chrono::Utc;

#[async_trait]
pub trait ShardIterator {
    async fn iterator(&self) -> Result<GetShardIteratorOutput>;
}

pub fn latest<'a, K: KinesisClient>(
    client: &'a K,
    config: &'a ShardProcessorConfig,
) -> LatestShardIterator<'a, K> {
    LatestShardIterator { client, config }
}

pub fn at_sequence<'a, K: KinesisClient>(
    client: &'a K,
    config: &'a ShardProcessorConfig,
    starting_sequence_number: &'a str,
) -> AtSequenceShardIterator<'a, K> {
    AtSequenceShardIterator {
        client,
        config,
        starting_sequence_number,
    }
}

pub fn at_timestamp<'a, K: KinesisClient>(
    client: &'a K,
    config: &'a ShardProcessorConfig,
    timestamp: &'a chrono::DateTime<Utc>,
) -> AtTimestampShardIterator<'a, K> {
    AtTimestampShardIterator {
        client,
        config,
        timestamp,
    }
}

pub struct LatestShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    config: &'a ShardProcessorConfig,
}

pub struct AtSequenceShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    config: &'a ShardProcessorConfig,
    starting_sequence_number: &'a str,
}

pub struct AtTimestampShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    config: &'a ShardProcessorConfig,
    timestamp: &'a chrono::DateTime<Utc>,
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for LatestShardIterator<'_, K> {
    async fn iterator(&self) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_latest(&self.config.stream, &self.config.shard_id)
            .await
    }
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for AtSequenceShardIterator<'_, K> {
    async fn iterator(&self) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_at_sequence(
                &self.config.stream,
                &self.config.shard_id,
                self.starting_sequence_number,
            )
            .await
    }
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for AtTimestampShardIterator<'_, K> {
    async fn iterator(&self) -> Result<GetShardIteratorOutput> {
        self.client
            .get_shard_iterator_at_timestamp(
                &self.config.stream,
                &self.config.shard_id,
                self.timestamp,
            )
            .await
    }
}
