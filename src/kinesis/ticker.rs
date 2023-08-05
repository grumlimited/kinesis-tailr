use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;

use humantime::format_duration;
use log::info;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub enum TickerMessage {
    CountUpdate(ShardCountUpdate),
    RemoveShard(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShardCountUpdate {
    pub shard_id: String,
    pub millis_behind: i64,
}

pub struct Ticker {
    counts: Arc<Mutex<HashMap<String, i64>>>,
    rx_ticker_updates: Receiver<TickerMessage>,
}

impl Ticker {
    pub fn new(rx_ticker_updates: Receiver<TickerMessage>) -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            rx_ticker_updates,
        }
    }

    pub async fn run(&mut self) {
        let counts = self.counts.clone();
        self.print_timings(counts);

        {
            let counts = self.counts.clone();

            while let Some(res) = self.rx_ticker_updates.recv().await {
                let mut counts = counts.lock().await;
                let counts = counts.deref_mut();
                match res {
                    TickerMessage::CountUpdate(res) => {
                        counts.insert(res.shard_id.clone(), res.millis_behind);
                    }
                    TickerMessage::RemoveShard(shard_id) => {
                        counts.remove(&shard_id);
                    }
                }
            }
        }
    }

    pub fn print_timings(&mut self, counts: Arc<Mutex<HashMap<String, i64>>>) {
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
