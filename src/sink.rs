use async_trait::async_trait;
use chrono::TimeZone;
use std::io;
use std::io::{BufWriter, Error, Write};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use crate::kinesis::models::{PanicError, RecordResult, ShardProcessorADT};

pub mod console;

#[derive(Clone, Default)]
pub struct SinkConfig {
    max_messages: Option<u32>,
    no_color: bool,
    print_key: bool,
    print_shardid: bool,
    print_timestamp: bool,
    print_delimiter: bool,
    exit_after_termination: bool,
}

pub trait Configurable {
    fn get_config(&self) -> SinkConfig;
}

#[async_trait]
pub trait SinkOutput<W>
where
    W: Write + Send,
{
    fn output(&mut self) -> BufWriter<W>;

    fn write_date(&self, date: &str) -> String {
        date.to_string()
    }

    fn write_shard_id(&self, shard_id: &str) -> String {
        shard_id.to_string()
    }

    fn write_key(&self, key: &str) -> String {
        key.to_string()
    }

    fn write_delimiter(&self, delimiter: &str) -> String {
        delimiter.to_string()
    }
}

#[async_trait]
pub trait Sink<T, W>
where
    W: Write + Send,
    T: SinkOutput<W> + Configurable + Send + Sync,
{
    async fn run_inner(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
        rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
        handle: &mut BufWriter<W>,
    ) -> io::Result<()>;

    async fn run(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
        rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
    ) -> io::Result<()>;

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, PanicError>>);

    fn delimiter(&self, handle: &mut BufWriter<W>) -> Result<(), Error>;

    fn format_nb_messages(&self, messages_processed: u32) -> String {
        match messages_processed {
            0 => "0 message processed".to_string(),
            1 => "1 message processed".to_string(),
            _ => format!("{} messages processed", messages_processed),
        }
    }

    fn format_record(&self, record_result: &RecordResult) -> String;

    fn format_records(&self, record_results: &[RecordResult]) -> Vec<String> {
        record_results
            .iter()
            .map(|record_result| self.format_record(record_result))
            .collect()
    }
}

#[async_trait]
impl<T, W> Sink<T, W> for T
where
    W: Write + Send,
    T: SinkOutput<W> + Configurable + Send + Sync,
{
    async fn run_inner(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
        mut rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
        handle: &mut BufWriter<W>,
    ) -> io::Result<()> {
        self.delimiter(handle).unwrap();

        let count = Arc::new(Mutex::new(0));

        self.handle_termination(tx_records.clone());

        while let Some(res) = rx_records.recv().await {
            match res {
                Ok(adt) => match adt {
                    ShardProcessorADT::Progress(res) => {
                        let mut lock = count.lock().await;

                        match self.get_config().max_messages {
                            Some(max_messages) => {
                                if *lock >= max_messages {
                                    tx_records
                                        .send(Ok(ShardProcessorADT::Termination))
                                        .await
                                        .unwrap();
                                }

                                let remaining = if *lock < max_messages {
                                    max_messages - *lock
                                } else {
                                    0
                                };

                                if remaining > 0 && !res.is_empty() {
                                    let split_at = std::cmp::min(remaining as usize, res.len());
                                    *lock += split_at as u32;

                                    let split = res.split_at(split_at);
                                    let to_display = split.0;

                                    let data = self.format_records(to_display);

                                    data.iter().for_each(|data| {
                                        writeln!(handle, "{}", data).unwrap();
                                        self.delimiter(handle).unwrap();
                                    });
                                }
                            }
                            None => {
                                let data = self.format_records(res.as_slice());

                                *lock += data.len() as u32;
                                data.iter().for_each(|data| {
                                    writeln!(handle, "{}", data).unwrap();
                                    self.delimiter(handle).unwrap()
                                });
                            }
                        }
                    }
                    ShardProcessorADT::Termination => {
                        let messages_processed = *count.lock().await;

                        handle.flush()?;

                        writeln!(
                            io::stderr(),
                            "{}",
                            self.format_nb_messages(messages_processed)
                        )?;

                        rx_records.close();

                        if self.get_config().exit_after_termination {
                            std::process::exit(0)
                        }
                    }
                },
                Err(e) => {
                    panic!("Error: {:?}", e);
                }
            }
        }

        Ok(())
    }

    async fn run(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
        rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
    ) -> io::Result<()> {
        let r = &mut self.output();
        self.run_inner(tx_records, rx_records, r).await
    }

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, PanicError>>) {
        // Note: the exit_after_termination check is to help
        // with tests where only one handler can be registered.
        if self.get_config().exit_after_termination {
            ctrlc_async::set_async_handler(async move {
                tx_records
                    .send(Ok(ShardProcessorADT::Termination))
                    .await
                    .unwrap();
            })
            .expect("Error setting Ctrl-C handler");
        }
    }

    fn delimiter(&self, handle: &mut BufWriter<W>) -> Result<(), Error> {
        if self.get_config().print_delimiter {
            writeln!(
                handle,
                "{}",
                self.write_delimiter(
                    "------------------------------------------------------------------------"
                )
            )?
        }
        Ok(())
    }

    fn format_record(&self, record_result: &RecordResult) -> String {
        let data = std::str::from_utf8(record_result.data.as_slice())
            .unwrap()
            .to_string();

        let data = if self.get_config().print_key {
            let key = record_result.sequence_id.to_string();
            let key = self.write_key(&key);

            format!("{} {}", key, data)
        } else {
            data
        };

        let data = if self.get_config().print_shardid {
            let shard_id = record_result.shard_id.to_string();
            let shard_id = self.write_shard_id(&shard_id);

            format!("{} {}", shard_id, data)
        } else {
            data
        };

        if self.get_config().print_timestamp {
            let date = chrono::Utc
                .timestamp_opt(record_result.datetime.secs(), 0)
                .unwrap();

            let date = date.format("%+").to_string();
            let date = self.write_date(&date);

            format!("{} {}", date, data)
        } else {
            data
        }
    }
}

#[cfg(test)]
mod tests;
