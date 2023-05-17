use hhmmss::Hhmmss;
use log::info;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq)]
pub struct TickerUpdate {
    pub shard_id: String,
    pub millis_behind_latest: Option<i64>,
}

pub struct Ticker {
    counts: Arc<Mutex<HashMap<String, Option<i64>>>>,
    rx_ticker_updates: Receiver<TickerUpdate>,
}

impl Ticker {
    pub fn new(tx_ticker_updates: Receiver<TickerUpdate>) -> Self {
        Self {
            counts: Arc::new(Mutex::new(HashMap::new())),
            rx_ticker_updates: tx_ticker_updates,
        }
    }

    pub async fn run(&mut self) {
        let counts: Arc<Mutex<HashMap<String, Option<i64>>>> = self.counts.clone();
        self.print_timings(counts).await;

        {
            let counts = self.counts.clone();

            while let Some(res) = self.rx_ticker_updates.recv().await {
                let mut counts = counts.lock().await;
                let counts = counts.deref_mut();

                counts.insert(res.shard_id.clone(), res.millis_behind_latest);
            }
        }
    }

    pub async fn print_timings(&mut self, counts: Arc<Mutex<HashMap<String, Option<i64>>>>) {
        tokio::spawn({
            async move {
                let delay = Duration::from_secs(30);
                let counts = counts.clone();

                loop {
                    {
                        let counts = counts.lock().await;
                        let counts: &HashMap<String, Option<i64>> = counts.deref();

                        let sorted = Self::sort_counts(counts);

                        let mut behind_count = 0;
                        for entry in sorted.iter() {
                            let shard_id = entry.0;
                            let millis_behind_latest = entry.1;

                            let behind = match millis_behind_latest {
                                Some(behind) => Duration::from_millis(*behind as u64).hhmmss(),
                                None => "n/a".to_string(),
                            };

                            if behind != "00:00:00" {
                                info!("{}: {}", shard_id, behind);
                                behind_count += 1;
                            }
                        }

                        if !counts.is_empty() {
                            info!("{} shards behind", behind_count);
                            info!("------------------------------")
                        }
                    }
                    sleep(delay).await;
                }
            }
        });
    }

    fn sort_counts(counts: &HashMap<String, Option<i64>>) -> Vec<(&String, &Option<i64>)> {
        let mut vec1 = Vec::from_iter(counts.iter());

        vec1.sort_by_key(|pair| pair.1);
        vec1
    }
}

#[cfg(test)]
mod tests {
    use crate::kinesis::ticker::Ticker;
    use std::collections::HashMap;

    #[test]
    fn sort_counts_ok() {
        let mut counts = HashMap::new();

        counts.insert("shard-2".to_string(), Some(100));
        counts.insert("shard-1".to_string(), Some(200));

        assert_eq!(
            Ticker::sort_counts(&counts),
            vec![
                (&"shard-2".to_string(), &Some(100_i64)),
                (&"shard-1".to_string(), &Some(200_i64)),
            ]
        );
    }
}
