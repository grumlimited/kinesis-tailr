use crate::aws::client::{AwsKinesisClient, KinesisClient};
use crate::iterator::ShardIterator;
use anyhow::Result;
use aws_sdk_kinesis::operation::get_shard_iterator::{
    GetShardIteratorError, GetShardIteratorOutput,
};
use chrono::Utc;
use log::debug;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::iterator::at_sequence;
use crate::iterator::latest;
use crate::kinesis::models::{
    ProcessError, ShardProcessor, ShardProcessorADT, ShardProcessorAtTimestamp,
    ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::ticker::TickerUpdate;
use crate::kinesis::{IteratorProvider, ShardIteratorProgress};

#[allow(clippy::too_many_arguments)]
pub fn new(
    client: AwsKinesisClient,
    stream: String,
    shard_id: String,
    from_datetime: Option<chrono::DateTime<Utc>>,
    to_datetime: Option<chrono::DateTime<Utc>>,
    semaphore: Arc<Semaphore>,
    tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
    tx_ticker_updates: Sender<TickerUpdate>,
) -> Box<dyn ShardProcessor<AwsKinesisClient> + Send + Sync> {
    debug!("Creating ShardProcessor with shard {}", shard_id);

    match from_datetime {
        Some(from_datetime) => Box::new(ShardProcessorAtTimestamp {
            config: ShardProcessorConfig {
                client,
                stream,
                shard_id,
                to_datetime,
                semaphore,
                tx_records,
                tx_ticker_updates,
            },
            from_datetime,
        }),
        None => Box::new(ShardProcessorLatest {
            config: ShardProcessorConfig {
                client,
                stream,
                shard_id,
                to_datetime,
                semaphore,
                tx_records,
                tx_ticker_updates,
            },
        }),
    }
}

pub async fn get_latest_iterator<T, K: KinesisClient>(
    iterator_provider: T,
) -> Result<GetShardIteratorOutput>
where
    T: IteratorProvider<K>,
{
    latest(&iterator_provider.get_config()).iterator().await
}

pub async fn get_iterator_since<T, K: KinesisClient>(
    iterator_provider: T,
    starting_sequence_number: &str,
) -> Result<GetShardIteratorOutput>
where
    T: IteratorProvider<K>,
{
    at_sequence(&iterator_provider.get_config(), starting_sequence_number)
        .iterator()
        .await
}

pub async fn handle_iterator_refresh<T, K: KinesisClient>(
    shard_iterator_progress: ShardIteratorProgress,
    iterator_provider: T,
    tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
) -> Result<()>
where
    T: IteratorProvider<K>,
{
    let cloned_shard_iterator_progress = shard_iterator_progress.clone();

    let result = match shard_iterator_progress.last_sequence_id {
        Some(last_sequence_id) => get_iterator_since(iterator_provider.clone(), &last_sequence_id)
            .await
            .map(|output| {
                (
                    Some(last_sequence_id.clone()),
                    output.shard_iterator().map(|v| v.to_string()),
                )
            }),
        None => get_latest_iterator(iterator_provider)
            .await
            .map(|output| (None, output.shard_iterator().map(|v| v.to_string()))),
    };

    match result {
        Ok((sequence_id, Some(iterator))) => {
            debug!(
                "Refreshing with next_shard_iterator: {:?} / last_sequence_id {:?}",
                iterator, sequence_id
            );

            tx_shard_iterator_progress
                .send(ShardIteratorProgress {
                    shard_id: shard_iterator_progress.shard_id.clone(),
                    last_sequence_id: sequence_id,
                    next_shard_iterator: Some(iterator),
                })
                .await?;
        }
        Ok((_, None)) => {
            Err(io::Error::new(io::ErrorKind::Other, "No iterator returned"))?;
        }
        Err(e) => match e.downcast_ref::<GetShardIteratorError>() {
            Some(e) => {
                if e.is_provisioned_throughput_exceeded_exception() {
                    let ws = wait_secs();
                    debug!("ProvisionedThroughputExceededException whilst refreshing iterator.  Waiting {} seconds", ws);
                    sleep(Duration::from_secs(ws)).await;
                    tx_shard_iterator_progress
                        .send(cloned_shard_iterator_progress)
                        .await?;
                }
            }
            None => {
                Err(e)?;
            }
        },
    };

    Ok(())
}

pub async fn get_shards(client: &AwsKinesisClient, stream: &str) -> io::Result<Vec<String>> {
    let resp = client
        .list_shards(stream)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        .map(|e| {
            e.shards()
                .unwrap()
                .iter()
                .map(|s| s.shard_id.as_ref().unwrap().clone())
                .collect::<Vec<String>>()
        })?;

    Ok(resp)
}

pub fn wait_secs() -> u64 {
    use rand::prelude::*;
    let mut rng = thread_rng();

    rng.gen_range(1..=12)
}
