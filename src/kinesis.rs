use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
use chrono::prelude::*;
use chrono::{DateTime, Utc};
use log::{debug, warn};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};
use GetRecordsError::{ExpiredIteratorException, ProvisionedThroughputExceededException};

use crate::aws::client::KinesisClient;
use crate::kinesis::helpers::wait_milliseconds;
use crate::kinesis::models::*;
use crate::kinesis::ticker::{ShardCountUpdate, TickerMessage};

pub mod helpers;
pub mod models;
pub mod ticker;

#[async_trait]
pub trait IteratorProvider<K: KinesisClient>: Send + Sync + Clone {
    fn get_config(&self) -> ShardProcessorConfig<K>;

    async fn get_iterator(&self) -> Result<GetShardIteratorOutput>;
}

#[async_trait]
impl<T, K> ShardProcessor<K> for T
where
    K: KinesisClient,
    T: IteratorProvider<K>,
{
    async fn run(&self) -> Result<()> {
        let (tx_shard_iterator_progress, mut rx_shard_iterator_progress) =
            mpsc::channel::<ShardIteratorProgress>(5);

        self.seed_shards(tx_shard_iterator_progress.clone()).await?;

        while let Some(res) = rx_shard_iterator_progress.recv().await {
            let permit = self.get_config().semaphore.acquire_owned().await.unwrap();

            let res_clone = res.clone();

            match res.next_shard_iterator {
                Some(shard_iterator) => {
                    let result = self
                        .publish_records_shard(
                            &shard_iterator,
                            self.get_config().tx_ticker_updates.clone(),
                            tx_shard_iterator_progress.clone(),
                        )
                        .await;

                    if let Err(e) = result {
                        match e.downcast_ref::<GetRecordsError>() {
                            Some(ExpiredIteratorException(inner)) => {
                                warn!(
                                    "ExpiredIteratorException [{}]: {}",
                                    self.get_config().shard_id,
                                    inner
                                );
                                helpers::handle_iterator_refresh(
                                    res_clone.clone(),
                                    self.clone(),
                                    tx_shard_iterator_progress.clone(),
                                )
                                .await
                                .unwrap();
                            }
                            Some(ProvisionedThroughputExceededException(_)) => {
                                let milliseconds = wait_milliseconds();
                                warn!(
                                    "ProvisionedThroughputExceededException [{}]. Waiting {} milliseconds. Consider increasing --max-attempts progressively.",
                                    self.get_config().shard_id ,milliseconds
                                );
                                sleep(Duration::from_millis(milliseconds)).await;
                                helpers::handle_iterator_refresh(
                                    res_clone.clone(),
                                    self.clone(),
                                    tx_shard_iterator_progress.clone(),
                                )
                                .await
                                .unwrap();
                            }
                            e => {
                                self.get_config()
                                    .tx_records
                                    .send(Err(ProcessError::PanicError(format!("{:?}", e))))
                                    .await
                                    .expect("Could not send error to tx_records");
                            }
                        }
                    }
                }
                None => {
                    if let Some(sender) = self.get_config().tx_ticker_updates {
                        sender
                            .send(TickerMessage::RemoveShard(res.shard_id.clone()))
                            .await
                            .expect("Could not send RemoveShard to tx_ticker_updates");
                    };

                    rx_shard_iterator_progress.close();
                }
            };

            drop(permit);
        }

        debug!("ShardProcessor {} finished", self.get_config().shard_id);

        if let Some(sender) = self.get_config().tx_ticker_updates {
            sender
                .send(TickerMessage::RemoveShard(
                    self.get_config().shard_id.clone(),
                ))
                .await?;
        };

        self.get_config()
            .tx_records
            .send(Ok(ShardProcessorADT::BeyondToTimestamp))
            .await?;

        Ok(())
    }

    async fn seed_shards(
        &self,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()> {
        let permit = self.get_config().semaphore.clone().acquire_owned().await?;

        debug!("Seeding shard {}", self.get_config().shard_id);

        match self.get_iterator().await {
            Ok(resp) => {
                let shard_iterator: Option<String> = resp.shard_iterator().map(|s| s.into());
                tx_shard_iterator_progress
                    .clone()
                    .send(ShardIteratorProgress {
                        shard_id: self.get_config().shard_id,
                        last_sequence_id: None,
                        next_shard_iterator: shard_iterator,
                    })
                    .await?;
            }
            Err(e) => {
                self.get_config()
                    .tx_records
                    .send(Err(ProcessError::PanicError(e.to_string())))
                    .await?;
            }
        }

        drop(permit);
        Ok(())
    }

    /**
     * Publish records from a shard iterator.
     */
    async fn publish_records_shard(
        &self,
        shard_iterator: &str,
        tx_ticker_updates: Option<Sender<TickerMessage>>,
        tx_shard_iterator_progress: Sender<ShardIteratorProgress>,
    ) -> Result<()> {
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
                    partition_key: record.partition_key().unwrap_or("none").into(),
                    datetime,
                    data: data.into(),
                }
            })
            .collect::<Vec<_>>();

        let nb_records = record_results.len();
        let record_results = self.records_before_end_ts(record_results);
        let nb_records_before_end_ts = record_results.len();

        if let Some(millis_behind) = resp.millis_behind_latest() {
            if let Some(tx_ticker_updates) = tx_ticker_updates {
                tx_ticker_updates
                    .send(TickerMessage::CountUpdate(ShardCountUpdate {
                        shard_id: self.get_config().shard_id.clone(),
                        millis_behind,
                        nb_records,
                    }))
                    .await
                    .expect("Could not send TickerUpdate to tx_ticker_updates");
            };
        }

        if !record_results.is_empty() {
            self.get_config()
                .tx_records
                .send(Ok(ShardProcessorADT::Progress(record_results)))
                .await
                .expect("Could not send records to tx_records");
        }

        /*
         * There are 2 reasons we should keep polling a shard:
         * - nb_records == 0: we need to keep polling until we get records, ie we dont know whether we are past to_datetime
         * - nb_records_before_end_ts > 0: we still have some (at least 1) records before to_datetime (or no to_datetime set)
         */
        let should_continue = nb_records == 0 || nb_records_before_end_ts > 0;

        if should_continue {
            let last_sequence_id: Option<String> = resp
                .records()
                .and_then(|r| r.last())
                .and_then(|r| r.sequence_number())
                .map(|s| s.into());

            let shard_iterator_progress = ShardIteratorProgress {
                shard_id: self.get_config().shard_id,
                last_sequence_id,
                next_shard_iterator: next_shard_iterator.map(|s| s.into()),
            };

            tx_shard_iterator_progress
                .send(shard_iterator_progress)
                .await?;
        } else {
            debug!(
                "{} records in batch for shard-id {} and {} records before {}",
                nb_records,
                self.get_config().shard_id,
                nb_records_before_end_ts,
                self.get_config()
                    .to_datetime
                    .map(|ts| ts.to_rfc3339())
                    .unwrap_or("[No end timestamp]".to_string())
            );

            tx_shard_iterator_progress
                .send(ShardIteratorProgress {
                    shard_id: self.get_config().shard_id.clone(),
                    last_sequence_id: None,
                    next_shard_iterator: None,
                })
                .await?;
        }

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

    fn records_before_end_ts(&self, records: Vec<RecordResult>) -> Vec<RecordResult> {
        match self.get_config().to_datetime {
            Some(end_ts) if !records.is_empty() => records
                .into_iter()
                .filter(|record| {
                    let record_ts = Utc.timestamp_nanos(record.datetime.as_nanos() as i64);
                    record_ts < end_ts
                })
                .collect(),
            _ => records,
        }
    }
}

#[cfg(test)]
mod tests;
