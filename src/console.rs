use crate::kinesis::models::*;
use chrono::*;
use std::io::{self, BufWriter, Error, Stdout, Write};
use std::rc::Rc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
pub const CONSOLE_BUF_SIZE: usize = 8 * 1024; // 8kB

pub struct Console {
    max_messages: Option<u32>,
    print_key: bool,
    print_shardid: bool,
    print_timestamp: bool,
    print_delimiter: bool,
    rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
    tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
}

impl Console {
    pub fn new(
        max_messages: Option<u32>,
        print_key: bool,
        print_shardid: bool,
        print_timestamp: bool,
        print_delimiter: bool,
        rx_records: Receiver<Result<ShardProcessorADT, PanicError>>,
        tx_records: Sender<Result<ShardProcessorADT, PanicError>>,
    ) -> Console {
        Console {
            max_messages,
            print_key,
            print_shardid,
            print_timestamp,
            print_delimiter,
            rx_records,
            tx_records,
        }
    }

    pub async fn run(&mut self) -> io::Result<()> {
        let count = Rc::new(Mutex::new(0));

        let stdout = io::stdout(); // get the global stdout entity
        let mut handle: BufWriter<Stdout> = io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout);

        self.handle_termination();

        while let Some(res) = self.rx_records.recv().await {
            match res {
                Ok(adt) => match adt {
                    ShardProcessorADT::Progress(res) => {
                        let mut lock = count.lock().await;

                        match self.max_messages {
                            Some(max_messages) => {
                                if *lock >= max_messages {
                                    self.tx_records
                                        .send(Ok(ShardProcessorADT::Termination))
                                        .await
                                        .unwrap();
                                }

                                let remaining = if *lock < max_messages {
                                    max_messages - *lock
                                } else {
                                    0
                                };

                                if remaining > 0 && res.len() > 0 {
                                    *lock += res.len() as u32;

                                    let split = res.split_at(remaining as usize);
                                    let to_display = split.0;

                                    let data = self.format_records(&to_display);

                                    data.iter().for_each(|data| {
                                        writeln!(handle, "{}", data).unwrap();
                                    });
                                    self.delimiter(&mut handle)?
                                };
                            }
                            None => {
                                let data = self.format_records(res.as_slice());

                                *lock += data.len() as u32;
                                data.iter().for_each(|data| {
                                    writeln!(handle, "{}", data).unwrap();
                                });
                                self.delimiter(&mut handle)?
                            }
                        }
                    }
                    ShardProcessorADT::Termination => {
                        let messages_processed = match self.max_messages {
                            Some(max_messages) => max_messages,
                            _ => *count.lock().await,
                        };
                        writeln!(handle, "{} messages processed", messages_processed)?;
                        handle.flush()?;
                        self.rx_records.close();
                        std::process::exit(0);
                    }
                },
                Err(e) => {
                    panic!("Error: {:?}", e);
                }
            };
        }
        Ok(())
    }

    fn handle_termination(&self) {
        let tx_records_clone = self.tx_records.clone();
        ctrlc_async::set_async_handler(async move {
            tx_records_clone
                .send(Ok(ShardProcessorADT::Termination))
                .await
                .unwrap();
        })
        .expect("Error setting Ctrl-C handler");
    }

    fn delimiter(&self, handle: &mut BufWriter<Stdout>) -> Result<(), Error> {
        if self.print_delimiter {
            writeln!(
                handle,
                "------------------------------------------------------------------------"
            )?
        }
        Ok(())
    }

    fn format_records(&self, record_results: &[RecordResult]) -> Vec<String> {
        record_results
            .iter()
            .map(|record_result| self.format_record(record_result))
            .collect()
    }

    fn format_record(&self, record_result: &RecordResult) -> String {
        let data = std::str::from_utf8(record_result.data.as_slice())
            .unwrap()
            .to_string();

        let data = if self.print_key {
            format!("{} {}", record_result.sequence_id, data)
        } else {
            data
        };

        let data = if self.print_shardid {
            format!("{} {}", record_result.shard_id, data)
        } else {
            data
        };

        if self.print_timestamp {
            let date = chrono::Utc
                .timestamp_opt(record_result.datetime.secs(), 0)
                .unwrap();

            format!("{} {}", date.format("%+"), data)
        } else {
            data
        }
    }
}
