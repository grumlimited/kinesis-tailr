use std::borrow::Cow;
use std::cmp::{max, min};
use std::io::{BufWriter, Write};
use std::{io, str};

use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use chrono::TimeZone;
use log::{debug, error, warn};
use tokio::sync::mpsc::{Receiver, Sender};

use buffer_flush::BufferTicker;

use crate::kinesis::models::{ProcessError, RecordResult, ShardProcessorADT};

pub mod console;
pub mod file;

#[derive(Clone, Default)]
pub struct SinkConfig {
    max_messages: Option<u32>,
    no_color: bool,
    print_key: bool,
    print_sequence_number: bool,
    print_shard_id: bool,
    print_timestamp: bool,
    print_delimiter: bool,
    exit_after_termination: bool,
    base64_encoding: bool,
}

pub trait Configurable {
    fn get_config(&self) -> &SinkConfig;
    fn shard_count(&self) -> usize;
}

pub trait SinkOutput<W>
where
    W: Write,
{
    fn output(&self) -> Result<BufWriter<W>>;

    fn write_date(&self, date: &str) -> String {
        date.to_string()
    }

    fn write_shard_id(&self, shard_id: &str) -> String {
        shard_id.to_string()
    }

    fn write_key(&self, key: &str) -> String {
        key.to_string()
    }

    fn write_sequence_number(&self, sq: &str) -> String {
        sq.to_string()
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
        tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
        rx_records: Receiver<Result<ShardProcessorADT, ProcessError>>,
        handle: &mut BufWriter<W>,
    ) -> Result<()>;

    async fn run(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
        rx_records: Receiver<Result<ShardProcessorADT, ProcessError>>,
    ) -> Result<()>;

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, ProcessError>>);

    fn delimiter(&self, handle: &mut BufWriter<W>) -> Result<(), Error>;

    fn format_nb_messages(&self, messages_processed: u32) -> String {
        match messages_processed {
            0 => "0 message processed".to_string(),
            1 => "1 message processed".to_string(),
            _ => format!("{} messages processed", messages_processed),
        }
    }

    fn format_record(&self, record_result: &RecordResult) -> Vec<u8>;

    fn termination_message_and_exit(
        &self,
        handle: &mut BufWriter<W>,
        count: u32,
        rx_records: &mut Receiver<Result<ShardProcessorADT, ProcessError>>,
    ) -> io::Result<()>;
}

#[async_trait]
impl<T, W> Sink<T, W> for T
where
    W: Write + Send,
    T: SinkOutput<W> + Configurable + Send,
{
    async fn run_inner(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
        mut rx_records: Receiver<Result<ShardProcessorADT, ProcessError>>,
        handle: &mut BufWriter<W>,
    ) -> Result<()> {
        self.delimiter(handle).unwrap();

        /*
         * Start the buffer ticker to flush the buffer every 5 seconds.
         * This is needed because if the buffer is not full (not enough message to trigger a nature flush),
         * then no output is displayed until ctrl^c is pressed.
         */
        BufferTicker::new(tx_records.clone()).start();

        let mut total_records_processed = 0;
        let mut active_shards_count = self.shard_count();

        self.handle_termination(tx_records.clone());

        while let Some(res) = rx_records.recv().await {
            match res {
                Ok(adt) => match adt {
                    ShardProcessorADT::BeyondToTimestamp => {
                        if active_shards_count > 0 {
                            active_shards_count = active_shards_count.saturating_sub(1);
                        }

                        if active_shards_count == 0 {
                            tx_records
                                .send(Ok(ShardProcessorADT::Termination))
                                .await
                                .expect("Could not send termination message");
                        }
                    }
                    ShardProcessorADT::Progress(records) => match self.get_config().max_messages {
                        Some(max_messages) if total_records_processed >= max_messages => self
                            .termination_message_and_exit(
                                handle,
                                total_records_processed,
                                &mut rx_records,
                            )?,
                        Some(max_messages) => {
                            let remaining_records_to_display =
                                max(max_messages - total_records_processed, 0);

                            if remaining_records_to_display > 0 && !records.is_empty() {
                                let records_to_display_count =
                                    min(remaining_records_to_display as usize, records.len());
                                total_records_processed += records_to_display_count as u32;

                                let (records_to_display, _) =
                                    records.split_at(records_to_display_count);

                                records_to_display.iter().for_each(|record| {
                                    let data = self.format_record(record);

                                    let _ = handle.write(data.as_slice()).unwrap();
                                    // writeln!(handle, "{}", data).unwrap();

                                    self.delimiter(handle).unwrap();
                                });
                            }
                        }
                        None => {
                            total_records_processed += records.len() as u32;
                            records.iter().for_each(|record| {
                                let data = self.format_record(record);
                                // writeln!(handle, "{}", data).unwrap();
                                let _ = handle.write(data.as_slice()).unwrap();
                                self.delimiter(handle).unwrap()
                            });
                        }
                    },
                    ShardProcessorADT::Flush => handle.flush().unwrap(),
                    ShardProcessorADT::Termination => {
                        debug!("Termination message received");
                        let messages_processed = total_records_processed;

                        self.termination_message_and_exit(
                            handle,
                            messages_processed,
                            &mut rx_records,
                        )?;
                    }
                },
                Err(ProcessError::PanicError(message)) => {
                    error!("Error: {}", message);
                    std::process::exit(1)
                }
                Err(ProcessError::Timeout(elapsed)) => {
                    warn!("Stream timed out after {}ms.", elapsed.num_milliseconds());
                    std::process::exit(2)
                }
            }
        }

        Ok(())
    }

    async fn run(
        &mut self,
        tx_records: Sender<Result<ShardProcessorADT, ProcessError>>,
        rx_records: Receiver<Result<ShardProcessorADT, ProcessError>>,
    ) -> Result<()> {
        let output = &mut self.output()?;
        self.run_inner(tx_records, rx_records, output).await
    }

    fn handle_termination(&self, tx_records: Sender<Result<ShardProcessorADT, ProcessError>>) {
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

    fn delimiter(&self, handle: &mut BufWriter<W>) -> Result<()> {
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

    fn format_record(&self, record_result: &RecordResult) -> Vec<u8> {
        let line_feed = vec![b'\n'];

        let payload = match &record_result.data {
            payload if self.get_config().base64_encoding => {
                use base64::{engine::general_purpose, Engine as _};
                Cow::Owned(
                    general_purpose::STANDARD
                        .encode(payload)
                        .as_bytes()
                        .to_vec(),
                )
            }
            payload => Cow::Borrowed(payload),
        };

        let partition_key = if self.get_config().print_key {
            let key = record_result.partition_key.to_string();
            let key = self.write_key(&key);

            format!("{} ", key)
        } else {
            "".to_string()
        };

        let sequence_number = if self.get_config().print_sequence_number {
            let key = record_result.sequence_id.to_string();
            let key = self.write_sequence_number(&key);

            format!("{} ", key)
        } else {
            "".to_string()
        };

        let shard_id = if self.get_config().print_shard_id {
            let shard_id = record_result.shard_id.to_string();
            let shard_id = self.write_shard_id(&shard_id);

            format!("{} ", shard_id)
        } else {
            "".to_string()
        };

        let date = if self.get_config().print_timestamp {
            let date = chrono::Utc
                .timestamp_opt(record_result.datetime.secs(), 0)
                .unwrap();

            let date = date.format("%+").to_string();
            let date = self.write_date(&date);

            format!("{} ", date)
        } else {
            "".to_string()
        };

        [
            partition_key.as_bytes(),
            sequence_number.as_bytes(),
            shard_id.as_bytes(),
            date.as_bytes(),
            payload.as_slice(),
            line_feed.as_slice(),
        ]
        .concat()
    }

    fn termination_message_and_exit(
        &self,
        handle: &mut BufWriter<W>,
        count: u32,
        rx_records: &mut Receiver<Result<ShardProcessorADT, ProcessError>>,
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

mod buffer_flush;

#[cfg(test)]
mod console_tests;

#[cfg(test)]
mod file_tests;
