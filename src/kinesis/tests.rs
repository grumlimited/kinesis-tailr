use crate::aws::client::KinesisClient;
use crate::kinesis::models::{
    PanicError, ShardProcessor, ShardProcessorADT, ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::IteratorProvider;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::operation::list_shards::ListShardsOutput;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::{Client, Error};
use chrono::Utc;
use tokio::sync::mpsc;

#[tokio::test]
async fn xxx() {
    let (tx_records, _rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let client = TestKinesisClient {};

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            tx_records,
        },
    };

    processor.get_iterator().await.unwrap();
}

#[derive(Clone, Debug)]
pub struct TestKinesisClient {}

#[async_trait]
impl KinesisClient for TestKinesisClient {
    async fn list_shards(&self, stream: &str) -> Result<ListShardsOutput, Error> {
        todo!()
    }

    async fn get_records(&self, shard_iterator: &str) -> Result<GetRecordsOutput, Error> {
        todo!()
    }

    async fn get_shard_iterator_at_timestamp(
        &self,
        stream: &str,
        shard_id: &str,
        timestamp: &chrono::DateTime<Utc>,
    ) -> Result<GetShardIteratorOutput, Error> {
        todo!()
    }

    async fn get_shard_iterator_at_sequence(
        &self,
        stream: &str,
        shard_id: &str,
        starting_sequence_number: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        todo!()
    }

    async fn get_shard_iterator_latest(
        &self,
        stream: &str,
        shard_id: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        let r = GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator".to_string())
            .build();

        Ok(r)
    }

    fn get_region(&self) -> Option<&Region> {
        todo!()
    }

    fn to_aws_datetime(&self, timestamp: &chrono::DateTime<Utc>) -> DateTime {
        todo!()
    }
}
