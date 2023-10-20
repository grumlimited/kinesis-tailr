use crate::kinesis::models::{ProcessError, ShardProcessorADT};
use chrono::prelude::*;
use humantime::format_duration;
use log::info;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::kinesis::ProcessError::Timeout;

#[derive(Debug, Clone, PartialEq)]
pub enum TickerMessage {
    CountUpdate(ShardCountUpdate),
    RemoveShard(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardCountUpdate {
    pub shard_id: String,
    pub millis_behind: i64,
    pub nb_records: usize,
}

pub struct Ticker {
    counts: Arc<Mutex<HashMap<String, i64>>>,
    last_ts: Arc<Mutex<DateTime<Utc>>>,
    rx_ticker_updates: Mutex<Receiver<TickerMessage>>,
    tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
}

impl Ticker {
    pub fn new(
        rx_ticker_updates: Receiver<TickerMessage>,
        tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
    ) -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            last_ts: Arc::new(Mutex::new(Utc::now())),
            rx_ticker_updates: Mutex::new(rx_ticker_updates),
            tx_records,
        }
    }

    pub async fn run(&self) {
        let counts = self.counts.clone();
        self.print_timings(counts);
        self.check_time_out();

        {
            let counts = self.counts.clone();

            while let Some(res) = self.rx_ticker_updates.lock().await.recv().await {
                let mut counts = counts.lock().await;
                let counts = counts.deref_mut();
                match res {
                    TickerMessage::CountUpdate(res) => {
                        counts.insert(res.shard_id.clone(), res.millis_behind);

                        if res.nb_records > 0 {
                            let mut last_ts = self.last_ts.lock().await;
                            let last_ts = last_ts.deref_mut();
                            *last_ts = Utc::now();
                        }
                    }
                    TickerMessage::RemoveShard(shard_id) => {
                        counts.remove(&shard_id);
                    }
                }
            }
        }
    }

    fn check_time_out(&self) {
        let last_ts = self.last_ts.clone();
        let tx_records = self.tx_records.clone();

        tokio::spawn({
            async move {
                let delay = Duration::from_millis(100);

                loop {
                    let last_ts = last_ts.lock().await;
                    let last_ts = *last_ts;

                    let duration = Utc::now() - last_ts;

                    if duration.num_milliseconds() > 10 * 1000 {
                        tx_records.send(Err(Timeout(duration))).await.unwrap();
                    }

                    sleep(delay).await
                }
            }
        });
    }

    fn print_timings(&self, counts: Arc<Mutex<HashMap<String, i64>>>) {
        tokio::spawn({
            async move {
                let delay = Duration::from_secs(30);
                let counts = counts.clone();

                loop {
                    {
                        let counts = counts.lock().await;

                        let sorted = Self::sort_counts(&counts);

                        let mut behind_count = 0;
                        for entry in sorted.iter() {
                            let shard_id = entry.0;
                            let millis_behind = *entry.1;

                            let duration = Duration::from_millis(millis_behind as u64);
                            let behind = format_duration(duration).to_string();

                            if behind != "0s" {
                                info!("{}: {}", shard_id, behind);
                                behind_count += 1;
                            }
                        }

                        if !counts.is_empty() {
                            info!("{} shards behind", behind_count);
                            info!("------------------------------")
                        }
                    }
                    sleep(delay).await
                }
            }
        });
    }

    fn sort_counts(counts: &HashMap<String, i64>) -> Vec<(&String, &i64)> {
        let mut vec1 = Vec::from_iter(counts.iter());

        vec1.sort_by_key(|pair| pair.1);
        vec1
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::kinesis::ticker::Ticker;

    #[test]
    fn sort_counts_ok() {
        let mut counts = HashMap::new();

        counts.insert("shard-2".to_string(), 100);
        counts.insert("shard-1".to_string(), 200);

        assert_eq!(
            Ticker::sort_counts(&counts),
            vec![
                (&"shard-2".to_string(), &100_i64),
                (&"shard-1".to_string(), &200_i64),
            ]
        );
    }
}
