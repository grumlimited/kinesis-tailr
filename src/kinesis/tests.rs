use crate::aws::client::KinesisClient;
use crate::kinesis::helpers;
use crate::kinesis::helpers::wait_secs;
use crate::kinesis::models::{
    ProcessError, RecordResult, ShardIteratorProgress, ShardProcessor, ShardProcessorADT,
    ShardProcessorAtTimestamp, ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::ticker::TickerUpdate;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::operation::list_shards::ListShardsOutput;
use aws_sdk_kinesis::primitives::{Blob, DateTime};
use aws_sdk_kinesis::types::error::InvalidArgumentException;
use aws_sdk_kinesis::types::{Record, Shard};
use chrono::prelude::*;
use chrono::Utc;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;

#[tokio::test]
async fn seed_shards_test() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
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

    processor
        .seed_shards(tx_shard_iterator_progress)
        .await
        .unwrap();

    let shard_iterator_progress = rx_shard_iterator_progress.recv().await.unwrap();

    assert_eq!(shard_iterator_progress.shard_id, "shardId-000000000000");
    assert_eq!(
        shard_iterator_progress.next_shard_iterator,
        Some("shard_iterator_latest".to_string())
    );
    assert_eq!(shard_iterator_progress.last_sequence_id, None);
}

#[tokio::test]
#[should_panic]
async fn seed_shards_test_timestamp_in_future() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
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

    processor
        .seed_shards(tx_shard_iterator_progress)
        .await
        .unwrap();
}

#[tokio::test]
async fn produced_record_is_processed() {
    let (tx_records, mut rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
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
async fn beyond_to_timestamp_is_received() {
    let (tx_records, mut rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
    let (tx_ticker_updates, mut rx_ticker_updates) = mpsc::channel::<TickerUpdate>(10);

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

    // start producer
    tokio::spawn(async move { processor.run().await });

    let ticker_update = rx_ticker_updates.recv().await.unwrap();
    assert_eq!(
        ticker_update,
        TickerUpdate {
            shard_id: "shardId-000000000000".to_string(),
            millis_behind_latest: Some(1000)
        }
    );

    let result = rx_records.recv().await.unwrap().unwrap();
    assert_eq!(result, ShardProcessorADT::BeyondToTimestamp);
}

#[tokio::test]
async fn has_records_beyond_end_ts_when_has_end_ts() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
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
    assert!(processor.has_records_beyond_end_ts(&records));

    let record1 = RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_secs(1000),
        data: vec![],
    };
    let record1_clone = record1;

    let records = vec![record1_clone.clone()];

    assert_eq!(
        processor.records_before_end_ts(records),
        vec![record1_clone.clone()] as Vec<RecordResult>
    );

    let future_ts = to_datetime.add(chrono::Duration::days(1));

    let record2 = RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_millis(future_ts.timestamp_millis()),
        data: vec![],
    };

    let records = vec![record1_clone.clone(), record2];

    assert_eq!(
        processor.records_before_end_ts(records),
        vec![record1_clone]
    );
}

#[tokio::test]
async fn has_records_beyond_end_ts_when_no_end_ts() {
    let (tx_records, _) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10);
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

    let records: Vec<RecordResult> = vec![];

    assert_eq!(
        processor.records_before_end_ts(records),
        vec![] as Vec<RecordResult>
    );

    let record = RecordResult {
        shard_id: "shard_id".to_string(),
        sequence_id: "sequence_id".to_string(),
        datetime: DateTime::from_secs(1000),
        data: vec![],
    };
    let record_clone = record.clone();

    let records = vec![record];

    assert_eq!(processor.records_before_end_ts(records), vec![record_clone]);
}

#[tokio::test]
async fn handle_iterator_refresh_ok() {
    let shard_iterator_progress = ShardIteratorProgress {
        shard_id: "shardId-000000000000".to_string(),
        last_sequence_id: Some("sequence_id".to_string()),
        next_shard_iterator: Some("some_iterator".to_string()),
    };

    let client = TestKinesisClient {
        region: Some(Region::new("us-east-1")),
    };

    let provider = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            to_datetime: None,
            semaphore: Arc::new(Semaphore::new(10)),
            tx_records: mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(10).0,
            tx_ticker_updates: mpsc::channel::<TickerUpdate>(10).0,
        },
    };

    let (tx_shard_iterator_progress, mut rx_shard_iterator_progress) =
        mpsc::channel::<ShardIteratorProgress>(1);

    helpers::handle_iterator_refresh(
        shard_iterator_progress,
        provider,
        tx_shard_iterator_progress,
    )
    .await
    .unwrap();

    let progress = rx_shard_iterator_progress.recv().await.unwrap();

    assert_eq!(progress.shard_id, "shardId-000000000000".to_string());
    assert_eq!(progress.last_sequence_id, Some("sequence_id".to_string()));
    assert_eq!(
        progress.next_shard_iterator,
        Some("shard_iterator_at_sequence".to_string())
    );
}

#[test]
fn wait_secs_ok() {
    for _ in 0..1000 {
        let w = wait_secs();

        assert!(w <= 12);
        assert!(w >= 1);
    }
}

#[derive(Clone, Debug)]
pub struct TestKinesisClient {
    region: Option<Region>,
}

#[async_trait]
impl KinesisClient for TestKinesisClient {
    async fn list_shards(&self, _stream: &str) -> Result<ListShardsOutput> {
        Ok(ListShardsOutput::builder()
            .shards(Shard::builder().shard_id("000001").build())
            .build())
    }

    async fn get_records(&self, _shard_iterator: &str) -> Result<GetRecordsOutput> {
        let to_datetime = Utc.with_ymd_and_hms(2021, 6, 1, 12, 0, 0).unwrap();
        let dt = DateTime::from_secs(to_datetime.timestamp());
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
    ) -> Result<GetShardIteratorOutput> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator".to_string())
            .build())
    }

    async fn get_shard_iterator_at_sequence(
        &self,
        _stream: &str,
        _shard_id: &str,
        _starting_sequence_number: &str,
    ) -> Result<GetShardIteratorOutput> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator_at_sequence".to_string())
            .build())
    }

    async fn get_shard_iterator_latest(
        &self,
        _stream: &str,
        _shard_id: &str,
    ) -> Result<GetShardIteratorOutput> {
        Ok(GetShardIteratorOutput::builder()
            .shard_iterator("shard_iterator_latest".to_string())
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
    async fn list_shards(&self, _stream: &str) -> Result<ListShardsOutput> {
        unimplemented!()
    }

    async fn get_records(&self, _shard_iterator: &str) -> Result<GetRecordsOutput> {
        unimplemented!()
    }

    async fn get_shard_iterator_at_timestamp(
        &self,
        _stream: &str,
        _shard_id: &str,
        _timestamp: &chrono::DateTime<Utc>,
    ) -> Result<GetShardIteratorOutput> {
        Err(aws_sdk_kinesis::Error::InvalidArgumentException(
            InvalidArgumentException::builder()
                .message("Timestamp is in future")
                .build(),
        )
        .into())
    }

    async fn get_shard_iterator_at_sequence(
        &self,
        _stream: &str,
        _shard_id: &str,
        _starting_sequence_number: &str,
    ) -> Result<GetShardIteratorOutput> {
        unimplemented!()
    }

    async fn get_shard_iterator_latest(
        &self,
        _stream: &str,
        _shard_id: &str,
    ) -> Result<GetShardIteratorOutput> {
        unimplemented!()
    }

    fn get_region(&self) -> Option<&Region> {
        unimplemented!()
    }
}
