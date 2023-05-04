use crate::kinesis::{PanicError, ShardProcessorADT};
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

    pub async fn run(&mut self) -> io::Result<()> {
        let count = Rc::new(Mutex::new(0));

        let stdout = io::stdout(); // get the global stdout entity
        let mut handle: BufWriter<Stdout> = io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout);

        self.handle_termination();

        while let Some(res) = self.rx_records.recv().await {
            match res {
                Ok(xxx) => match xxx {
                    ShardProcessorADT::Progress(res) => {
                        let data = std::str::from_utf8(res.data.as_slice())
                            .unwrap()
                            .to_string();

                        let data = if self.print_key {
                            format!("{} {}", res.sequence_id, data)
                        } else {
                            data
                        };

                        let data = if self.print_shardid {
                            format!("{} {}", res.shard_id, data)
                        } else {
                            data
                        };

                        let data = if self.print_timestamp {
                            let date = chrono::Utc.timestamp_opt(res.datetime.secs(), 0).unwrap();

                            format!("{} {}", date.format("%+"), data)
                        } else {
                            data
                        };

                        let mut lock = count.lock().await;

                        match self.max_messages {
                            Some(max_messages) => {
                                if *lock >= max_messages {
                                    self.tx_records
                                        .send(Ok(ShardProcessorADT::Termination))
                                        .await
                                        .unwrap();
                                }

                                if *lock < max_messages {
                                    *lock += 1;
                                    writeln!(handle, "{}", data)?;
                                    self.delimiter(&mut handle)?
                                };
                            }
                            None => {
                                *lock += 1;
                                writeln!(handle, "{}", data)?;
                                self.delimiter(&mut handle)?
                            }
                        }
                    }
                    ShardProcessorADT::Termination => {
                        writeln!(handle, "{} messages processed", count.lock().await)?;
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
}
