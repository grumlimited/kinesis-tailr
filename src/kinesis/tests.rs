use crate::aws::client::KinesisClient;
use crate::kinesis::models::{
    PanicError, ShardProcessor, ShardProcessorADT, ShardProcessorConfig, ShardProcessorLatest,
};
use async_trait::async_trait;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::operation::list_shards::ListShardsOutput;
use aws_sdk_kinesis::primitives::{Blob, DateTime};
use aws_sdk_kinesis::types::{Record, Shard};
use aws_sdk_kinesis::Error;
use chrono::Utc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[tokio::test]
async fn produced_record_is_processed() {
    let (tx_records, mut rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            tx_records,
        },
    };

    // start producer
    tokio::spawn(async move { processor.run().await });

    let mut done_processing = false;
    let mut closed_resources = false;
    let mut count = 0;

    while let Some(res) = rx_records.recv().await {
        if !done_processing {
            match res {
                Ok(adt) => match adt {
                    ShardProcessorADT::Progress(res) => {
                        count += res.len();
                    }
                    _ => {}
                },
                Err(_) => {}
            }

            done_processing = true;
        } else {
            if !closed_resources {
                sleep(Duration::from_millis(100)).await;
                rx_records.close();
            }
            closed_resources = true;
        }
    }

    assert_eq!(count, 1)
}

#[derive(Clone, Debug)]
pub struct TestKinesisClient {
    region: Option<Region>,
}

#[async_trait]
impl KinesisClient for TestKinesisClient {
    async fn list_shards(&self, _stream: &str) -> Result<ListShardsOutput, Error> {
        Ok(ListShardsOutput::builder()
            .shards(Shard::builder().shard_id("000001").build())
            .build())
    }

    async fn get_records(&self, _shard_iterator: &str) -> Result<GetRecordsOutput, Error> {
        let dt = DateTime::from_secs(5000);
        let record = Record::builder()
            .approximate_arrival_timestamp(dt)
            .sequence_number("1")
            .data(Blob::new("data"))
            .build();

        Ok(GetRecordsOutput::builder()
            .records(record)
            .next_shard_iterator("shard_iterator2".to_string())
            .build())
    }

    async fn get_shard_iterator_at_timestamp(
        &self,
        _stream: &str,
        _shard_id: &str,
        _timestamp: &chrono::DateTime<Utc>,
    ) -> Result<GetShardIteratorOutput, Error> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator".to_string())
            .build())
    }

    async fn get_shard_iterator_at_sequence(
        &self,
        _stream: &str,
        _shard_id: &str,
        _starting_sequence_number: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator".to_string())
            .build())
    }

    async fn get_shard_iterator_latest(
        &self,
        _stream: &str,
        _shard_id: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator".to_string())
            .build())
    }

    fn get_region(&self) -> Option<&Region> {
        self.region.as_ref()
    }
}