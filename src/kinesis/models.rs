use crate::aws::client::KinesisClient;
use crate::iterator::at_timestamp;
use crate::kinesis::helpers::get_latest_iterator;
use crate::kinesis::IteratorProvider;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::Error;
use chrono::Utc;
use std::fmt::Debug;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct ShardIteratorProgress {
    pub(crate) last_sequence_id: Option<String>,
    pub(crate) next_shard_iterator: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ShardProcessorADT {
    Termination,
    Progress(Vec<RecordResult>),
}

#[derive(Debug, Clone)]
pub struct PanicError {
    pub message: String,
}
#[derive(Debug, Clone)]
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
    pub tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
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

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput, Error> {
        get_latest_iterator(self.clone()).await
    }
}

#[async_trait]
impl<K: KinesisClient> IteratorProvider<K> for ShardProcessorAtTimestamp<K> {
    fn get_config(&self) -> ShardProcessorConfig<K> {
        self.config.clone()
    }

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput, Error> {
        at_timestamp(&self.config.client, &self.from_datetime)
            .iterator(&self.config.stream, &self.config.shard_id)
            .await
    }
}

#[async_trait]
pub trait ShardProcessor<K: KinesisClient>: Send + Sync {
    async fn run(&self) -> Result<(), Error>;

    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<(), Error>;
}
