use crate::kinesis::models::{ProcessError, ShardProcessorADT};
use anyhow::Result;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

pub struct BufferTicker {
    tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
}

impl BufferTicker {
    pub fn new(tx_records: Sender<Result<ShardProcessorADT, ProcessError>>) -> Self {
        Self { tx_records }
    }

    /**
     * THis method will loop in the background and send a flush message to the
     * shard Sink every 5 seconds.
     */
    pub fn start(&self) {
        let tx_records = self.tx_records.clone();

        tokio::spawn({
            async move {
                let delay = Duration::from_secs(5);

                loop {
                    sleep(delay).await;
                    tx_records
                        .send(Ok(ShardProcessorADT::Flush))
                        .await
                        .expect("Count not send ");
                }
            }
        });
    }
}
