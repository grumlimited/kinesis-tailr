use crate::aws::client::KinesisClient;
use crate::iterator::at_timestamp;
use crate::kinesis::helpers::get_latest_iterator;
use crate::kinesis::ticker::TickerUpdate;
use crate::kinesis::IteratorProvider;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::primitives::DateTime;
use chrono::Utc;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct ShardIteratorProgress {
    pub(crate) shard_id: String,
    pub(crate) last_sequence_id: Option<String>,
    pub(crate) next_shard_iterator: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum ShardProcessorADT {
    Termination,
    BeyondToTimestamp,
    Progress(Vec<RecordResult>),
}

#[derive(Error, Debug, Clone)]
pub enum ProcessError {
    #[error("The stream panicked: {0}")]
    PanicError(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordResult {
    pub shard_id: String,
    pub sequence_id: String,
    pub datetime: DateTime,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct ShardProcessorConfig<K: KinesisClient> {
    pub client: K,
    pub stream: String,
    pub shard_id: String,
    pub to_datetime: Option<chrono::DateTime<Utc>>,
    pub semaphore: Arc<Semaphore>,
    pub tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
    pub tx_ticker_updates: Sender<TickerUpdate>,
}

#[derive(Clone)]
pub struct ShardProcessorLatest<K: KinesisClient> {
    pub config: ShardProcessorConfig<K>,
}

#[derive(Clone)]
pub struct ShardProcessorAtTimestamp<K: KinesisClient> {
    pub config: ShardProcessorConfig<K>,
    pub from_datetime: chrono::DateTime<Utc>,
}

#[async_trait]
impl<K: KinesisClient> IteratorProvider<K> for ShardProcessorLatest<K> {
    fn get_config(&self) -> ShardProcessorConfig<K> {
        self.config.clone()
    }

    async fn get_iterator(&self, shard_id: &str) -> Result<GetShardIteratorOutput> {
        get_latest_iterator(self.clone(), shard_id).await
    }
}

#[async_trait]
impl<K: KinesisClient> IteratorProvider<K> for ShardProcessorAtTimestamp<K> {
    fn get_config(&self) -> ShardProcessorConfig<K> {
        self.config.clone()
    }

    async fn get_iterator(&self, shard_id: &str) -> Result<GetShardIteratorOutput> {
        at_timestamp(&self.config.client, &self.from_datetime)
            .iterator(&self.config.stream, shard_id)
            .await
    }
}

#[async_trait]
pub trait ShardProcessor<K: KinesisClient>: Send + Sync {
    async fn run(&self) -> Result<()>;

    async fn seed_shards(
        &self,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()>;

    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        shard_id: String,
        tx_ticker: Sender<TickerUpdate>,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()>;

    fn has_records_beyond_end_ts(&self, records: &[RecordResult]) -> bool;

    fn records_before_end_ts(&self, records: Vec<RecordResult>) -> Vec<RecordResult>;
}
