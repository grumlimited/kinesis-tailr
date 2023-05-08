use crate::aws::client::KinesisClient;
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

pub fn latest<'a, K>(client: &'a K) -> Box<dyn ShardIterator + 'a + Send + Sync>
where
    K: KinesisClient,
{
    Box::new(LatestShardIterator { client })
}

pub fn at_sequence<'a, K: KinesisClient>(
    client: &'a K,
    starting_sequence_number: &'a str,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtSequenceShardIterator {
        client,
        starting_sequence_number,
    })
}

pub fn at_timestamp<'a, K: KinesisClient>(
    client: &'a K,
    timestamp: &'a chrono::DateTime<Utc>,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtTimestampShardIterator { client, timestamp })
}

struct LatestShardIterator<'a, K: KinesisClient> {
    client: &'a K,
}

struct AtSequenceShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    starting_sequence_number: &'a str,
}

struct AtTimestampShardIterator<'a, K: KinesisClient> {
    client: &'a K,
    timestamp: &'a chrono::DateTime<Utc>,
}

#[async_trait]
impl<K: KinesisClient> ShardIterator for LatestShardIterator<'_, K> {
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
impl<K: KinesisClient> ShardIterator for AtSequenceShardIterator<'_, K> {
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
impl<K: KinesisClient> ShardIterator for AtTimestampShardIterator<'_, K> {
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
