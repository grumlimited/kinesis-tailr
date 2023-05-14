use crate::aws::client::{AwsKinesisClient, KinesisClient};
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::Error;
use chrono::Utc;
use log::debug;
use tokio::sync::mpsc::{Sender, UnboundedSender};

use crate::iterator::at_sequence;
use crate::iterator::latest;
use crate::kinesis::models::{
    PanicError, ShardProcessor, ShardProcessorADT, ShardProcessorAtTimestamp, ShardProcessorConfig,
    ShardProcessorLatest,
};
use crate::kinesis::{IteratorProvider, ShardIteratorProgress};

pub fn new(
    client: AwsKinesisClient,
    stream: String,
    shard_ids: Vec<String>,
    from_datetime: Option<chrono::DateTime<Utc>>,
    tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
) -> Box<dyn ShardProcessor<AwsKinesisClient> + Send + Sync> {
    debug!("Creating ShardProcessor with {} shards", shard_ids.len());

    match from_datetime {
        Some(from_datetime) => Box::new(ShardProcessorAtTimestamp {
            config: ShardProcessorConfig {
                client,
                stream,
                shard_ids,
                tx_records,
            },
            from_datetime,
        }),
        None => Box::new(ShardProcessorLatest {
            config: ShardProcessorConfig {
                client,
                stream,
                shard_ids,
                tx_records,
            },
        }),
    }
}

pub async fn get_latest_iterator<T, K: KinesisClient>(
    iterator_provider: T,
    shard_id: &str,
) -> Result<GetShardIteratorOutput, Error>
where
    T: IteratorProvider<K>,
{
    latest(&iterator_provider.get_config().client)
        .iterator(&iterator_provider.get_config().stream, shard_id)
        .await
}

pub async fn get_iterator_since<T, K: KinesisClient>(
    iterator_provider: T,
    starting_sequence_number: &str,
    shard_id: &str,
) -> Result<GetShardIteratorOutput, Error>
where
    T: IteratorProvider<K>,
{
    at_sequence(
        &iterator_provider.get_config().client,
        starting_sequence_number,
    )
    .iterator(&iterator_provider.get_config().stream, shard_id)
    .await
}

pub async fn handle_iterator_refresh<T, K: KinesisClient>(
    shard_iterator_progress: ShardIteratorProgress,
    iterator_provider: T,
    tx_shard_iterator_progress: UnboundedSender<ShardIteratorProgress>,
) where
    T: IteratorProvider<K>,
{
    let (sequence_id, iterator) = match shard_iterator_progress.last_sequence_id {
        Some(last_sequence_id) => {
            let resp = get_iterator_since(
                iterator_provider,
                &last_sequence_id,
                &shard_iterator_progress.shard_id,
            )
            .await
            .unwrap();
            (
                Some(last_sequence_id),
                resp.shard_iterator().map(|v| v.into()),
            )
        }
        None => {
            let resp = get_latest_iterator(iterator_provider, &shard_iterator_progress.shard_id)
                .await
                .unwrap();
            (None, resp.shard_iterator().map(|v| v.into()))
        }
    };

    debug!(
        "Refreshing with next_shard_iterator: {:?} / last_sequence_id {:?}",
        iterator, sequence_id
    );

    tx_shard_iterator_progress
        .send(ShardIteratorProgress {
            shard_id: shard_iterator_progress.shard_id.clone(),
            last_sequence_id: sequence_id,
            next_shard_iterator: iterator,
        })
        .unwrap();
}

pub async fn get_shards(client: &AwsKinesisClient, stream: &str) -> Result<Vec<String>, Error> {
    let resp = client.list_shards(stream).await?;

    Ok(resp
        .shards()
        .unwrap()
        .iter()
        .map(|s| s.shard_id.as_ref().unwrap().clone())
        .collect())
}
