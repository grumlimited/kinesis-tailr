use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::{Client, Error};
use chrono::Utc;

mod aws;

#[async_trait]
pub trait ShardIterator {
    async fn iterator<'a>(
        &'a self,
        stream: &'a str,
        shard_id: &'a str,
    ) -> Result<GetShardIteratorOutput, Error>;
}

fn to_aws_datetime(timestamp: &chrono::DateTime<Utc>) -> DateTime {
    DateTime::from_millis(timestamp.timestamp_millis())
}

pub fn latest<'a>(client: &'a Client) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(LatestShardIterator { client })
}

pub fn at_sequence<'a>(
    client: &'a Client,
    starting_sequence_number: &'a str,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtSequenceShardIterator {
        client,
        starting_sequence_number,
    })
}

pub fn at_timestamp<'a>(
    client: &'a Client,
    timestamp: &'a chrono::DateTime<Utc>,
) -> Box<dyn ShardIterator + 'a + Send + Sync> {
    Box::new(AtTimestampShardIterator { client, timestamp })
}

struct LatestShardIterator<'a> {
    client: &'a Client,
}

struct AtSequenceShardIterator<'a> {
    client: &'a Client,
    starting_sequence_number: &'a str,
}

struct AtTimestampShardIterator<'a> {
    client: &'a Client,
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
            .get_shard_iterator()
            .shard_iterator_type(ShardIteratorType::Latest)
            .stream_name(stream)
            .shard_id(shard_id)
            .send()
            .await
            .map_err(|e| e.into())
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
            .get_shard_iterator()
            .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
            .starting_sequence_number(self.starting_sequence_number)
            .stream_name(stream)
            .shard_id(shard_id)
            .send()
            .await
            .map_err(|e| e.into())
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
            .get_shard_iterator()
            .shard_iterator_type(ShardIteratorType::AtTimestamp)
            .timestamp(to_aws_datetime(self.timestamp))
            .stream_name(stream)
            .shard_id(shard_id)
            .send()
            .await
            .map_err(|e| e.into())
    }
}
