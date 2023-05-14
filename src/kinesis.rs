use crate::aws::client::KinesisClient;
use crate::kinesis::models::*;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::Error;
use log::{debug, error};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{sleep, Duration};

pub mod helpers;
pub mod models;

#[async_trait]
pub trait IteratorProvider<K: KinesisClient>: Send + Sync + Clone + 'static {
    fn get_config(&self) -> ShardProcessorConfig<K>;

    async fn get_iterator(&self, shard_id: &str) -> Result<GetShardIteratorOutput, Error>;
}

#[async_trait]
impl<T, K> ShardProcessor<K> for T
where
    K: KinesisClient,
    T: IteratorProvider<K>,
{
    async fn run(&self) -> Result<(), Error> {
        let (tx_shard_iterator_progress, mut rx_shard_iterator_progress) =
            mpsc::unbounded_channel::<ShardIteratorProgress>();

        {
            let cloned_self = self.clone();
            let tx_shard_iterator_progress = tx_shard_iterator_progress.clone();
            tokio::spawn(async move {
                while let Some(res) = rx_shard_iterator_progress.recv().await {
                    let res_clone = res.clone();

                    match res.next_shard_iterator {
                        Some(shard_iterator) => {
                            let result = cloned_self
                                .publish_records_shard(
                                    &shard_iterator,
                                    res.shard_id.clone(),
                                    tx_shard_iterator_progress.clone(),
                                )
                                .await;

                            if let Err(e) = result {
                                match e {
                                    Error::ExpiredIteratorException(inner) => {
                                        debug!("ExpiredIteratorException: {}", inner);
                                        helpers::handle_iterator_refresh(
                                            res_clone.clone(),
                                            cloned_self.clone(),
                                            tx_shard_iterator_progress.clone(),
                                        )
                                        .await;
                                    }
                                    Error::ProvisionedThroughputExceededException(inner) => {
                                        debug!("ProvisionedThroughputExceededException: {}", inner);
                                        sleep(Duration::from_secs(10)).await;
                                        helpers::handle_iterator_refresh(
                                            res_clone.clone(),
                                            cloned_self.clone(),
                                            tx_shard_iterator_progress.clone(),
                                        )
                                        .await;
                                    }
                                    e => {
                                        error!("Error: {}", e);
                                        cloned_self
                                            .get_config()
                                            .tx_records
                                            .send(Err(PanicError {
                                                message: format!("{:?}", e),
                                            }))
                                            .await
                                            .expect("Could not send error to tx_records");
                                    }
                                }
                            }
                        }
                        None => {
                            cloned_self
                                .get_config()
                                .tx_records
                                .send(Err(PanicError {
                                    message: "ShardIterator is None".to_string(),
                                }))
                                .await
                                .expect("");
                        }
                    };
                }
            });
        }

        self.seed_shards(tx_shard_iterator_progress).await;

        Ok(())
    }

    async fn seed_shards(
        &self,
        tx_shard_iterator_progress: UnboundedSender<ShardIteratorProgress>,
    ) {
        debug!("Seeding {} shards", self.get_config().shard_ids.len());

        for shard_id in self.get_config().shard_ids {
            let tx_shard_iterator_progress = tx_shard_iterator_progress.clone();
            let resp = self.get_iterator(&shard_id).await.unwrap();
            let shard_iterator: Option<String> = resp.shard_iterator().map(|s| s.into());
            tx_shard_iterator_progress
                .clone()
                .send(ShardIteratorProgress {
                    shard_id: shard_id.to_string(),
                    last_sequence_id: None,
                    next_shard_iterator: shard_iterator,
                })
                .unwrap();
        }
    }

    /**
    * Publish records from a shard iterator.

    * Because shards are multiplexed per ShardProcessor, we need to keep
    * track of the shard_id for each shard_iterator.
    */
    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        shard_id: String,
        tx_shard_iterator_progress: UnboundedSender<ShardIteratorProgress>,
    ) -> Result<(), Error> {
        let resp = self.get_config().client.get_records(shard_iterator).await?;

        let next_shard_iterator = resp.next_shard_iterator();

        let record_results = resp
            .records()
            .unwrap()
            .iter()
            .map(|record| {
                let data = record.data().unwrap().as_ref();
                let datetime = *record.approximate_arrival_timestamp().unwrap();

                RecordResult {
                    shard_id: shard_id.clone(),
                    sequence_id: record.sequence_number().unwrap().into(),
                    datetime,
                    data: data.into(),
                }
            })
            .collect::<Vec<_>>();

        if !record_results.is_empty() {
            debug!(
                "Received {} records from {}",
                record_results.len(),
                shard_id.clone()
            );
            self.get_config()
                .tx_records
                .send(Ok(ShardProcessorADT::Progress(record_results)))
                .await
                .expect("Could not sent records to tx_records");
        }

        let last_sequence_id: Option<String> = resp
            .records()
            .and_then(|r| r.last())
            .and_then(|r| r.sequence_number())
            .map(|s| s.into());

        let results = ShardIteratorProgress {
            shard_id: shard_id.clone(),
            last_sequence_id,
            next_shard_iterator: next_shard_iterator.map(|s| s.into()),
        };

        tx_shard_iterator_progress.send(results).unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests;
