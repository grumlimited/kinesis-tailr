use hhmmss::Hhmmss;
use log::info;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
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
        tokio::spawn({
            let counts = self.counts.clone();

            async move {
                let delay = Duration::from_secs(30);
                let counts = counts.clone();

                loop {
                    {
                        let counts = counts.lock().await;
                        let counts = counts.deref();

                        for entry in counts.iter() {
                            let shard_id = entry.0;
                            let millis_behind_latest = entry.1;

                            let behind = match millis_behind_latest {
                                Some(behind) => Duration::from_millis(*behind as u64).hhmmss(),
                                None => "n/a".to_string(),
                            };

                            info!("{}: {}", shard_id, behind);
                        }

                        if !counts.is_empty() {
                            info!("---------------------------------------------------")
                        }
                    }
                    sleep(delay).await;
                }
            }
        });

        {
            let counts = self.counts.clone();

            while let Some(res) = self.rx_ticker_updates.recv().await {
                let mut counts = counts.lock().await;
                let counts = counts.deref_mut();

                counts.insert(res.shard_id.clone(), res.millis_behind_latest);
            }
        }
    }
}
