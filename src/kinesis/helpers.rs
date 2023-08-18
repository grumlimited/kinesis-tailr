use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::error::SdkError::ServiceError;
use aws_sdk_kinesis::operation::get_shard_iterator::{
    GetShardIteratorError, GetShardIteratorOutput,
};
use aws_sdk_kinesis::operation::list_shards::{ListShardsError, ListShardsOutput};
use chrono::Utc;
use log::{debug, info};
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::aws::client::{AwsKinesisClient, KinesisClient};
use crate::iterator::at_sequence;
use crate::iterator::latest;
use crate::iterator::ShardIterator;
use crate::kinesis::models::{
    ProcessError, ShardProcessor, ShardProcessorADT, ShardProcessorAtTimestamp,
    ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::ticker::TickerMessage;
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
    tx_ticker_updates: Option<Sender<TickerMessage>>,
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
                    let ws = wait_milliseconds();
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
    let mut seed = client.list_shards(stream, None).await;

    let mut results: Vec<ListShardsOutput> = vec![];

    while let Ok(result) = &seed {
        results.push(result.clone());
        if let Some(next_token) = result.next_token() {
            let result = client.list_shards(stream, Some(next_token)).await;
            seed = result;
        } else {
            break;
        }
    }

    match seed {
        Ok(_) => {
            let shards: Vec<String> = results
                .iter()
                .flat_map(|r| {
                    r.shards()
                        .unwrap()
                        .iter()
                        .map(|s| s.shard_id().unwrap().to_string())
                        .collect::<Vec<String>>()
                })
                .collect::<Vec<String>>();

            info!("Found {} shards", shards.len());

            Ok(shards)
        }
        Err(e) => {
            let message = match e.downcast_ref::<SdkError<ListShardsError>>() {
                Some(ServiceError(inner)) => inner.err().to_string(),
                Some(other) => other.to_string(),
                _ => e.to_string(),
            };

            Err(io::Error::new(io::ErrorKind::Other, message))
        }
    }
}

pub fn wait_milliseconds() -> u64 {
    use rand::prelude::*;
    let mut rng = thread_rng();

    rng.gen_range(50..=1000)
}
