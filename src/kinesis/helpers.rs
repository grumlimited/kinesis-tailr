use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::{Client, Error};
use chrono::Utc;
use tokio::sync::mpsc::Sender;

use crate::iterator::latest;
use crate::iterator::{at_sequence, at_timestamp};
use crate::kinesis::{IteratorProvider, ShardIteratorProgress};

pub async fn get_latest_iterator<T>(iterator_provider: T) -> Result<GetShardIteratorOutput, Error>
where
    T: IteratorProvider,
{
    latest(&iterator_provider.get_config().client)
        .iterator(
            &iterator_provider.get_config().stream,
            &iterator_provider.get_config().shard_id,
        )
        .await
}

pub async fn get_iterator_since<T>(
    iterator_provider: T,
    starting_sequence_number: &str,
) -> Result<GetShardIteratorOutput, Error>
where
    T: IteratorProvider,
{
    at_sequence(
        &iterator_provider.get_config().client,
        starting_sequence_number,
    )
    .iterator(
        &iterator_provider.get_config().stream,
        &iterator_provider.get_config().shard_id,
    )
    .await
}

pub async fn get_iterator_at_timestamp<T>(
    iterator_provider: T,
    timestamp: chrono::DateTime<Utc>,
) -> Result<GetShardIteratorOutput, Error>
where
    T: IteratorProvider,
{
    at_timestamp(&iterator_provider.get_config().client, &timestamp)
        .iterator(
            &iterator_provider.get_config().stream,
            &iterator_provider.get_config().shard_id,
        )
        .await
}

pub async fn handle_iterator_refresh<T>(
    shard_iterator_progress: ShardIteratorProgress,
    reader: T,
    tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
) where
    T: IteratorProvider,
{
    let (sequence_id, iterator) = match shard_iterator_progress.last_sequence_id {
        Some(last_sequence_id) => {
            let resp = get_iterator_since(reader, &last_sequence_id).await.unwrap();
            (
                Some(last_sequence_id),
                resp.shard_iterator().map(|v| v.into()),
            )
        }
        None => {
            let resp = get_latest_iterator(reader).await.unwrap();
            (None, resp.shard_iterator().map(|v| v.into()))
        }
    };

    println!(
        "Refreshing with next_shard_iterator: {:?} / last_sequence_id {:?}",
        iterator, sequence_id
    );

    tx_shard_iterator_progress
        .send(ShardIteratorProgress {
            last_sequence_id: sequence_id,
            next_shard_iterator: iterator,
        })
        .await
        .unwrap();
}

pub async fn get_shards(client: &Client, stream: &str) -> Result<Vec<String>, Error> {
    let resp = client.list_shards().stream_name(stream).send().await?;

    Ok(resp
        .shards()
        .unwrap()
        .iter()
        .map(|s| s.shard_id.as_ref().unwrap().clone())
        .collect())
}
