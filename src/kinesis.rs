use crate::aws::client::KinesisClient;
use crate::kinesis::models::*;
use crate::kinesis::ticker::TickerUpdate;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use aws_sdk_kinesis::Error;
use chrono::prelude::*;
use chrono::{DateTime, Utc};
use log::{debug, error};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

pub mod helpers;
pub mod models;
pub mod ticker;

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
            mpsc::channel::<ShardIteratorProgress>(5);

        self.seed_shards(tx_shard_iterator_progress.clone()).await;

        tokio::spawn({
            let cloned_self = self.clone();
            let tx_shard_iterator_progress = tx_shard_iterator_progress.clone();
            let tx_ticker_updates = self.get_config().tx_ticker_updates;
            let semaphore = self.get_config().semaphore;
            async move {
                while let Some(res) = rx_shard_iterator_progress.recv().await {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    let res_clone = res.clone();

                    match res.next_shard_iterator {
                        Some(shard_iterator) => {
                            let result = cloned_self
                                .publish_records_shard(
                                    &shard_iterator,
                                    res.shard_id.clone(),
                                    tx_ticker_updates.clone(),
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
                                                message: e.to_string(),
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

                    drop(permit);
                }
            }
        });

        Ok(())
    }

    async fn seed_shards(&self, tx_shard_iterator_progress: Sender<ShardIteratorProgress>) {
        let permit = self
            .get_config()
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        debug!("Seeding shard {}", self.get_config().shard_id);

        let tx_shard_iterator_progress = tx_shard_iterator_progress.clone();

        match self.get_iterator(&self.get_config().shard_id).await {
            Ok(resp) => {
                let shard_iterator: Option<String> = resp.shard_iterator().map(|s| s.into());
                tx_shard_iterator_progress
                    .clone()
                    .send(ShardIteratorProgress {
                        shard_id: self.get_config().shard_id,
                        last_sequence_id: None,
                        next_shard_iterator: shard_iterator,
                    })
                    .await
                    .unwrap();
            }
            Err(e) => {
                self.get_config()
                    .tx_records
                    .send(Err(PanicError {
                        message: e.to_string(),
                    }))
                    .await
                    .expect("Could not send error to tx_records");
            }
        }

        drop(permit);
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
        tx_ticker_updates: Sender<TickerUpdate>,
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
                    shard_id: shard_id.clone(),
                    sequence_id: record.sequence_number().unwrap().into(),
                    datetime,
                    data: data.into(),
                }
            })
            .collect::<Vec<_>>();

        tx_ticker_updates
            .send(TickerUpdate {
                shard_id: shard_id.clone(),
                millis_behind_latest: resp.millis_behind_latest(),
            })
            .await
            .expect("Could not send TickerUpdate to tx_ticker_updates");

        if !record_results.is_empty() {
            self.get_config()
                .tx_records
                .send(Ok(ShardProcessorADT::Progress(record_results)))
                .await
                .expect("Could not send records to tx_records");
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

        tx_shard_iterator_progress.send(results).await.unwrap();

        Ok(())
    }

    fn has_records_beyond_end_ts(&self, records: &[RecordResult]) -> bool {
        match self.get_config().to_datetime {
            Some(end_ts) if !records.is_empty() => {
                let epoch = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();

                let find_most_recent_ts = |records: &[RecordResult]| -> DateTime<Utc> {
                    records.iter().fold(epoch, |current_ts, record| {
                        let record_ts = Utc.timestamp_nanos(record.datetime.as_nanos() as i64);

                        std::cmp::max(current_ts, record_ts)
                    })
                };

                let most_recent_ts = find_most_recent_ts(records);

                most_recent_ts >= end_ts
            }
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests;
