use crate::aws::stream::StreamClient;
use crate::iterator::ShardIterator;
use crate::iterator::{at_timestamp, latest};
use crate::kinesis::ticker::TickerMessage;
use crate::kinesis::IteratorProvider;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::primitives::DateTime;
use chrono::{Duration, Utc};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct ShardIteratorProgress {
    pub(crate) last_sequence_id: Option<String>,
    pub(crate) next_shard_iterator: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum ShardProcessorADT {
    Termination,
    BeyondToTimestamp,
    Flush,
    Progress(Vec<RecordResult>),
}

#[derive(Error, Debug, Clone)]
pub enum ProcessError {
    #[error("The stream panicked: {0}")]
    PanicError(String),
    #[error("The stream timed out after {0}.")]
    Timeout(Duration),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordResult {
    pub shard_id: Arc<String>,
    pub sequence_id: String,
    pub partition_key: String,
    pub datetime: DateTime,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct ShardProcessorConfig {
    pub stream: String,
    pub shard_id: Arc<String>,
    pub to_datetime: Option<chrono::DateTime<Utc>>,
    pub semaphore: Arc<Semaphore>,
    pub tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
    pub tx_ticker_updates: Option<Sender<TickerMessage>>,
}

#[derive(Clone)]
pub struct ShardProcessorLatest<K: StreamClient> {
    pub client: K,
    pub config: ShardProcessorConfig,
}

#[derive(Clone)]
pub struct ShardProcessorAtTimestamp<K: StreamClient> {
    pub client: K,
    pub config: ShardProcessorConfig,
    pub from_datetime: chrono::DateTime<Utc>,
}

#[async_trait]
impl<K: StreamClient> IteratorProvider<K> for ShardProcessorLatest<K> {
    fn get_client(&self) -> &K {
        &self.client
    }

    fn get_config(&self) -> &ShardProcessorConfig {
        &self.config
    }

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput> {
        latest(&self.client, &self.config).iterator().await
    }
}

#[async_trait]
impl<K: StreamClient> IteratorProvider<K> for ShardProcessorAtTimestamp<K> {
    fn get_client(&self) -> &K {
        &self.client
    }

    fn get_config(&self) -> &ShardProcessorConfig {
        &self.config
    }

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput> {
        at_timestamp(&self.client, &self.config, &self.from_datetime)
            .iterator()
            .await
    }
}

#[async_trait]
pub trait ShardProcessor<K: StreamClient>: Send + Sync {
    async fn run(&self) -> Result<()>;

    async fn seed_shards(
        &self,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()>;

    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()>;

    fn records_before_end_ts(&self, records: Vec<RecordResult>) -> Vec<RecordResult>;
}
