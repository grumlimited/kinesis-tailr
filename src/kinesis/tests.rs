use crate::aws::client::KinesisClient;
use crate::kinesis::models::{
    PanicError, RecordResult, ShardIteratorProgress, ShardProcessor, ShardProcessorADT,
    ShardProcessorAtTimestamp, ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::ticker::TickerUpdate;
use async_trait::async_trait;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::operation::list_shards::ListShardsOutput;
use aws_sdk_kinesis::primitives::{Blob, DateTime};
use aws_sdk_kinesis::types::error::InvalidArgumentException;
use aws_sdk_kinesis::types::{Record, Shard};
use aws_sdk_kinesis::Error;
use chrono::prelude::*;
use chrono::Utc;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;

#[tokio::test]
async fn seed_shards_test() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);
    let (tx_ticker_updates, _) = mpsc::channel::<TickerUpdate>(10);

    let (tx_shard_iterator_progress, mut rx_shard_iterator_progress) =
        mpsc::channel::<ShardIteratorProgress>(1);

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(10));

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: None,
            semaphore,
            tx_records,
            tx_ticker_updates,
        },
    };

    processor.seed_shards(tx_shard_iterator_progress).await;

    let shard_iterator_progress = rx_shard_iterator_progress.recv().await.unwrap();

    assert_eq!(shard_iterator_progress.shard_id, "shardId-000000000000");
    assert_eq!(
        shard_iterator_progress.next_shard_iterator,
        Some("shard_iterator".to_string())
    );
    assert_eq!(shard_iterator_progress.last_sequence_id, None);
}

#[tokio::test]
#[should_panic]
async fn seed_shards_test_timestamp_in_future() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);
    let (tx_ticker_updates, _) = mpsc::channel::<TickerUpdate>(10);

    let (tx_shard_iterator_progress, _) = mpsc::channel::<ShardIteratorProgress>(1);

    let client = TestTimestampInFutureKinesisClient {};

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(10));

    let processor = ShardProcessorAtTimestamp {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: None,
            semaphore,
            tx_records,
            tx_ticker_updates,
        },
        from_datetime: Utc::now().add(chrono::Duration::days(1)),
    };

    processor.seed_shards(tx_shard_iterator_progress).await;
}

#[tokio::test]
async fn produced_record_is_processed() {
    let (tx_records, mut rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);
    let (tx_ticker_updates, mut rx_ticker_updates) = mpsc::channel::<TickerUpdate>(10);

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(10));

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: None,
            semaphore,
            tx_records,
            tx_ticker_updates,
        },
    };

    // start producer
    tokio::spawn(async move { processor.run().await });

    let mut done_processing = false;
    let mut closed_resources = false;
    let mut count = 0;

    let ticker_update = rx_ticker_updates.recv().await.unwrap();
    assert_eq!(
        ticker_update,
        TickerUpdate {
            shard_id: "shardId-000000000000".to_string(),
            millis_behind_latest: Some(1000)
        }
    );

    while let Some(res) = rx_records.recv().await {
        if !done_processing {
            if let Ok(ShardProcessorADT::Progress(res)) = res {
                count += res.len();
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

    assert_eq!(count, 1);
}

#[tokio::test]
async fn has_records_beyond_end_ts_when_has_end_ts() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);
    let (tx_ticker_updates, _) = mpsc::channel::<TickerUpdate>(10);

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(10));

    let to_datetime = Utc.with_ymd_and_hms(2020, 6, 1, 12, 0, 0).unwrap();
    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: Some(to_datetime),
            semaphore,
            tx_records,
            tx_ticker_updates,
        },
    };

    let records = vec![];
    assert_eq!(processor.has_records_beyond_end_ts(&records), true);

    let mut records = vec![RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_secs(1000),
        data: vec![],
    }];
    assert_eq!(processor.has_records_beyond_end_ts(&records), false);

    let r = to_datetime.add(chrono::Duration::days(1));

    records.append(&mut vec![RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_millis(r.timestamp_millis()),
        data: vec![],
    }]);
    assert_eq!(processor.has_records_beyond_end_ts(&records), true);
}

#[tokio::test]
async fn has_records_beyond_end_ts_when_no_end_ts() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(10);
    let (tx_ticker_updates, _) = mpsc::channel::<TickerUpdate>(10);

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(10));

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: None,
            semaphore,
            tx_records,
            tx_ticker_updates,
        },
    };

    let records = vec![];
    assert_eq!(processor.has_records_beyond_end_ts(&records), true);

    let records = vec![RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_secs(1000),
        data: vec![],
    }];
    assert_eq!(processor.has_records_beyond_end_ts(&records), true);
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
            .millis_behind_latest(1000)
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

#[derive(Clone, Debug)]
pub struct TestTimestampInFutureKinesisClient {}

#[async_trait]
impl KinesisClient for TestTimestampInFutureKinesisClient {
    async fn list_shards(&self, _stream: &str) -> Result<ListShardsOutput, Error> {
        unimplemented!()
    }

    async fn get_records(&self, _shard_iterator: &str) -> Result<GetRecordsOutput, Error> {
        unimplemented!()
    }

    async fn get_shard_iterator_at_timestamp(
        &self,
        _stream: &str,
        _shard_id: &str,
        _timestamp: &chrono::DateTime<Utc>,
    ) -> Result<GetShardIteratorOutput, Error> {
        Err(Error::InvalidArgumentException(
            InvalidArgumentException::builder()
                .message("Timestamp is in future")
                .build(),
        ))
    }

    async fn get_shard_iterator_at_sequence(
        &self,
        _stream: &str,
        _shard_id: &str,
        _starting_sequence_number: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        unimplemented!()
    }

    async fn get_shard_iterator_latest(
        &self,
        _stream: &str,
        _shard_id: &str,
    ) -> Result<GetShardIteratorOutput, Error> {
        unimplemented!()
    }

    fn get_region(&self) -> Option<&Region> {
        unimplemented!()
    }
}
