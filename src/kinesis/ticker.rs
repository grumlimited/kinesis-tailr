use log::{debug, info};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time;

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
                let delay = time::Duration::from_secs(1);
                let counts = counts.clone();

                loop {
                    let counts = counts.lock().await;
                    let counts = counts.deref();
                    info!("{:?}", counts);
                    thread::sleep(delay);
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
