use async_trait::async_trait;
use chrono::TimeZone;
use log::info;
use std::io;
use std::io::{BufWriter, Error, Write};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::kinesis::models::{PanicError, RecordResult, ShardProcessorADT};

pub mod console;
pub mod file;

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
    fn get_config(&self) -> &SinkConfig;
    fn shard_count(&self) -> usize;
}

#[async_trait]
pub trait SinkOutput<W>
where
    W: Write,
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
    W: Write,
    T: SinkOutput<W>,
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

    fn termination_message_and_exit(
        &self,
        handle: &mut BufWriter<W>,
        count: u32,
        rx_records: &mut Receiver<Result<ShardProcessorADT, PanicError>>,
    ) -> io::Result<()>;
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

        let mut count = 0;
        let mut sc = self.shard_count();

        self.handle_termination(tx_records.clone());

        while let Some(res) = rx_records.recv().await {
            match res {
                Ok(adt) => match adt {
                    ShardProcessorADT::BeyondToTimestamp => {
                        if sc > 0 {
                            sc = sc.saturating_sub(1);
                        }

                        if sc == 0 {
                            tx_records
                                .send(Ok(ShardProcessorADT::Termination))
                                .await
                                .expect("Boom");
                        }
                    }
                    ShardProcessorADT::Progress(res) => match self.get_config().max_messages {
                        Some(max_messages) => {
                            if count >= max_messages {
                                self.termination_message_and_exit(handle, count, &mut rx_records)?;
                            }

                            let remaining_records_to_display =
                                std::cmp::max(max_messages - count, 0);

                            if remaining_records_to_display > 0 && !res.is_empty() {
                                let split_at =
                                    std::cmp::min(remaining_records_to_display as usize, res.len());
                                count += split_at as u32;

                                let (to_display, _) = res.split_at(split_at);

                                to_display.iter().for_each(|record| {
                                    let data = self.format_record(record);
                                    writeln!(handle, "{}", data).unwrap();
                                    self.delimiter(handle).unwrap();
                                });
                            }
                        }
                        None => {
                            count += res.len() as u32;
                            res.iter().for_each(|record| {
                                let data = self.format_record(record);
                                writeln!(handle, "{}", data).unwrap();
                                self.delimiter(handle).unwrap()
                            });
                        }
                    },
                    ShardProcessorADT::Termination => {
                        info!("Termination message received");
                        let messages_processed = count;

                        self.termination_message_and_exit(
                            handle,
                            messages_processed,
                            &mut rx_records,
                        )?;
                    }
                },
                Err(e) => {
                    panic!("Error: {}", e.message);
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
        let output = &mut self.output();
        self.run_inner(tx_records, rx_records, output).await
    }

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, PanicError>>) {
        // Note: the exit_after_termination check is to help
        // with tests where only one handler can be registered.
        if self.get_config().exit_after_termination {
            ctrlc_async::set_async_handler(async move {
                info!("Setting Ctrl-C handler");
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

    fn termination_message_and_exit(
        &self,
        handle: &mut BufWriter<W>,
        count: u32,
        rx_records: &mut Receiver<Result<ShardProcessorADT, PanicError>>,
    ) -> io::Result<()> {
        handle.flush()?;

        writeln!(io::stderr(), "{}", self.format_nb_messages(count))?;

        rx_records.close();

        if self.get_config().exit_after_termination {
            std::process::exit(0)
        }

        Ok(())
    }
}

#[cfg(test)]
mod console_tests;

#[cfg(test)]
mod file_tests;
