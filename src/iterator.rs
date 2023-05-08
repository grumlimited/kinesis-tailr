use crate::aws::client::{KinesisClient, KinesisClientOps};
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::Error;
use chrono::Utc;

#[async_trait]
pub trait ShardIterator {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput, Error>;
}

pub fn latest<'a>(client: &'a KinesisClient) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(LatestShardIterator { client })
}

pub fn at_sequence<'a>(
    client: &'a KinesisClient,
    starting_sequence_number: &'a str,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtSequenceShardIterator {
        client,
        starting_sequence_number,
    })
}

pub fn at_timestamp<'a>(
    client: &'a KinesisClient,
    timestamp: &'a chrono::DateTime<Utc>,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtTimestampShardIterator { client, timestamp })
}

struct LatestShardIterator<'a> {
    client: &'a KinesisClient,
}

struct AtSequenceShardIterator<'a> {
    client: &'a KinesisClient,
    starting_sequence_number: &'a str,
}

struct AtTimestampShardIterator<'a> {
    client: &'a KinesisClient,
    timestamp: &'a chrono::DateTime<Utc>,
}

#[async_trait]
impl ShardIterator for LatestShardIterator<'_> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput, Error> {
        self.client
            .get_shard_iterator_latest(stream, shard_id)
            .await
    }
}

#[async_trait]
impl ShardIterator for AtSequenceShardIterator<'_> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput, Error> {
        self.client
            .get_shard_iterator_at_sequence(stream, shard_id, self.starting_sequence_number)
            .await
    }
}

#[async_trait]
impl ShardIterator for AtTimestampShardIterator<'_> {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput, Error> {
        self.client
            .get_shard_iterator_at_timestamp(stream, shard_id, self.timestamp)
            .await
    }
}
