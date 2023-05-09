use crate::aws::client::KinesisClient;
use crate::kinesis::models::*;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::Error;
use log::{debug, error};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};
pub mod helpers;
pub mod models;

#[async_trait]
pub trait IteratorProvider<K: KinesisClient>: Send + Sync + Clone + 'static {
    fn get_config(&self) -> ShardProcessorConfig<K>;

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput, Error>;
}

#[async_trait]
impl<T, K> ShardProcessor<K> for T
where
    K: KinesisClient,
    T: IteratorProvider<K>,
{
    async fn run(&self) -> Result<(), Error> {
        let (tx_shard_iterator_progress, mut rx_shard_iterator_progress) =
            mpsc::channel::<ShardIteratorProgress>(100);

        {
            let cloned_self = self.clone();
            let tx_shard_iterator_progress = tx_shard_iterator_progress.clone();
            tokio::spawn(async move {
                #[allow(unused_assignments)]
                let mut current_get_records_result = ShardIteratorProgress {
                    last_sequence_id: None,
                    next_shard_iterator: None,
                };

                let current_get_records_result_ref = &mut current_get_records_result;

                while let Some(res) = rx_shard_iterator_progress.recv().await {
                    let res_clone = res.clone();

                    if res_clone.last_sequence_id.is_some() {
                        current_get_records_result_ref.last_sequence_id =
                            res_clone.last_sequence_id;
                    };

                    if res_clone.next_shard_iterator.is_some() {
                        current_get_records_result_ref.next_shard_iterator =
                            res_clone.next_shard_iterator;
                    };

                    match res.next_shard_iterator {
                        Some(shard_iterator) => {
                            let result = cloned_self
                                .publish_records_shard(
                                    &shard_iterator,
                                    tx_shard_iterator_progress.clone(),
                                )
                                .await;

                            if let Err(e) = result {
                                match e {
                                    Error::ExpiredIteratorException(inner) => {
                                        debug!("ExpiredIteratorException: {}", inner);
                                        helpers::handle_iterator_refresh(
                                            current_get_records_result_ref.clone(),
                                            cloned_self.clone(),
                                            tx_shard_iterator_progress.clone(),
                                        )
                                        .await;
                                    }
                                    Error::ProvisionedThroughputExceededException(inner) => {
                                        debug!("ProvisionedThroughputExceededException: {}", inner);
                                        sleep(Duration::from_secs(10)).await;
                                        helpers::handle_iterator_refresh(
                                            current_get_records_result_ref.clone(),
                                            cloned_self.clone(),
                                            tx_shard_iterator_progress.clone(),
                                        )
                                        .await;
                                    }
                                    e => {
                                        error!("ExpiredIteratorException: {}", e);
                                        cloned_self
                                            .get_config()
                                            .tx_records
                                            .send(Err(PanicError {
                                                message: format!("{:?}", e),
                                            }))
                                            .await
                                            .expect("TODO: panic message");
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

        let resp = self.get_iterator().await?;
        let shard_iterator = resp.shard_iterator().map(|s| s.into());
        tx_shard_iterator_progress
            .send(ShardIteratorProgress {
                last_sequence_id: None,
                next_shard_iterator: shard_iterator,
            })
            .await
            .unwrap();

        Ok(())
    }

    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
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
                    shard_id: self.get_config().shard_id,
                    sequence_id: record.sequence_number().unwrap().into(),
                    datetime,
                    data: data.into(),
                }
            })
            .collect::<Vec<RecordResult>>();

        if !record_results.is_empty() {
            self.get_config()
                .tx_records
                .send(Ok(ShardProcessorADT::Progress(record_results)))
                .await
                .expect("TODO: panic message")
        }

        let last_sequence_id: Option<String> = resp
            .records()
            .and_then(|r| r.last())
            .and_then(|r| r.sequence_number())
            .map(|s| s.into());

        let results = ShardIteratorProgress {
            last_sequence_id,
            next_shard_iterator: next_shard_iterator.map(|s| s.into()),
        };

        tx_shard_iterator_progress.send(results).await.unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests;
