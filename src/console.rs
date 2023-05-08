use async_trait::async_trait;
use chrono::TimeZone;
use std::io;
use std::io::{BufWriter, Error, Stdout, Write};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;

use crate::kinesis::models::{PanicError, RecordResult, ShardProcessorADT};

pub const CONSOLE_BUF_SIZE: usize = 8 * 1024;

pub struct ConsoleSink {
    config: SinkConfig,
}

impl ConsoleSink {
    pub fn new(
        max_messages: Option<u32>,
        print_key: bool,
        print_shardid: bool,
        print_timestamp: bool,
        print_delimiter: bool,
    ) -> Self {
        ConsoleSink {
            config: SinkConfig {
                max_messages,
                print_key,
                print_shardid,
                print_timestamp,
                print_delimiter,
                exit_after_termination: true,
            },
        }
    }
}

#[derive(Clone)]
pub struct SinkConfig {
    max_messages: Option<u32>,
    print_key: bool,
    print_shardid: bool,
    print_timestamp: bool,
    print_delimiter: bool,
    exit_after_termination: bool,
}

pub trait Configurable {
    fn get_config(&self) -> SinkConfig;
}

impl Configurable for ConsoleSink {
    fn get_config(&self) -> SinkConfig {
        self.config.clone()
    }
}

impl SinkOutput<Stdout> for ConsoleSink {
    fn offer(&mut self) -> BufWriter<Stdout> {
        let stdout = io::stdout(); // get the global stdout entity
        let handle = io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout);
        handle
    }
}

#[async_trait]
pub trait SinkOutput<W>
where
    W: Write + Send,
{
    fn offer(&mut self) -> BufWriter<W>;
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
                                    *lock += res.len() as u32;

                                    let split = res.split_at(remaining as usize);
                                    let to_display = split.0;

                                    let data = self.format_records(to_display);

                                    data.iter().for_each(|data| {
                                        writeln!(handle, "{}", data).unwrap();
                                    });
                                    self.delimiter(handle)?
                                }
                            }
                            None => {
                                let data = self.format_records(res.as_slice());

                                *lock += data.len() as u32;
                                data.iter().for_each(|data| {
                                    writeln!(handle, "{}", data).unwrap();
                                });
                                self.delimiter(handle)?
                            }
                        }
                    }
                    ShardProcessorADT::Termination => {
                        let messages_processed = match self.get_config().max_messages {
                            Some(max_messages) => max_messages,
                            _ => *count.lock().await,
                        };

                        writeln!(handle, "{}", self.format_nb_messages(messages_processed))?;
                        handle.flush()?;
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
        let r = &mut self.offer();
        self.run_inner(tx_records, rx_records, r).await
    }

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, PanicError>>) {
        ctrlc_async::set_async_handler(async move {
            tx_records
                .send(Ok(ShardProcessorADT::Termination))
                .await
                .unwrap();
        })
        .expect("Error setting Ctrl-C handler");
    }

    fn delimiter(&self, handle: &mut BufWriter<W>) -> Result<(), Error> {
        if self.get_config().print_delimiter {
            writeln!(
                handle,
                "------------------------------------------------------------------------"
            )?
        }
        Ok(())
    }

    fn format_record(&self, record_result: &RecordResult) -> String {
        let data = std::str::from_utf8(record_result.data.as_slice())
            .unwrap()
            .to_string();

        let data = if self.get_config().print_key {
            format!("{} {}", record_result.sequence_id, data)
        } else {
            data
        };

        let data = if self.get_config().print_shardid {
            format!("{} {}", record_result.shard_id, data)
        } else {
            data
        };

        if self.get_config().print_timestamp {
            let date = chrono::Utc
                .timestamp_opt(record_result.datetime.secs(), 0)
                .unwrap();

            format!("{} {}", date.format("%+"), data)
        } else {
            data
        }
    }
}

#[cfg(test)]
mod tests;
