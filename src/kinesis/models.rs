use async_trait::async_trait;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::{Client, Error};
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

#[derive(Debug, Clone)]
pub struct ShardProcessorConfig {
    pub client: Client,
    pub stream: String,
    pub shard_id: String,
    pub tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
}

#[derive(Debug, Clone)]
pub struct ShardProcessorLatest {
    pub config: ShardProcessorConfig,
}

#[derive(Debug, Clone)]
pub struct ShardProcessorAtTimestamp {
    pub config: ShardProcessorConfig,
    pub from_datetime: chrono::DateTime<Utc>,
}

#[async_trait]
pub trait ShardProcessor: Send + Sync + Debug {
    async fn run(&self) -> Result<(), Error>;

    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<(), Error>;
}