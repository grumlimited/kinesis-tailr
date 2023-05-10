use crate::sink::{Configurable, SinkConfig, SinkOutput};
use colored::Colorize;
use std::io;
use std::io::{BufWriter, Stdout};

pub const CONSOLE_BUF_SIZE: usize = 8 * 1024;

pub struct ConsoleSink {
    pub(crate) config: SinkConfig,
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

impl Configurable for ConsoleSink {
    fn get_config(&self) -> SinkConfig {
        self.config.clone()
    }
}

impl SinkOutput<Stdout> for ConsoleSink {
    fn offer(&mut self) -> BufWriter<Stdout> {
        let stdout = io::stdout(); // get the global stdout entity
        io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout)
    }

    fn write_date(&self, date: &str) -> String {
        date.to_string().red().to_string()
    }

    fn write_shard_id(&self, shard_id: &str) -> String {
        shard_id.to_string().blue().to_string()
    }

    fn write_key(&self, key: &str) -> String {
        key.to_string().yellow().to_string()
    }

    fn write_delimiter(&self, delimiter: &str) -> String {
        // grey-ish
        delimiter.to_string().truecolor(128, 128, 128).to_string()
    }
}
