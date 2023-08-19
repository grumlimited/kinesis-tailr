use crate::sink::{Configurable, SinkConfig, SinkOutput};
use colored::Colorize;
use std::io;
use std::io::{BufWriter, Stdout};

pub const CONSOLE_BUF_SIZE: usize = 32 * 1024;

pub struct ConsoleSink {
    pub(crate) config: SinkConfig,
    pub(crate) shard_count: usize,
}

impl ConsoleSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_messages: Option<u32>,
        no_color: bool,
        print_key: bool,
        print_sequence_number: bool,
        print_shard_id: bool,
        print_timestamp: bool,
        print_delimiter: bool,
        shard_count: usize,
    ) -> Self {
        ConsoleSink {
            config: SinkConfig {
                max_messages,
                no_color,
                print_key,
                print_sequence_number,
                print_shard_id,
                print_timestamp,
                print_delimiter,
                exit_after_termination: true,
            },
            shard_count,
        }
    }
}

impl Configurable for ConsoleSink {
    fn get_config(&self) -> &SinkConfig {
        &self.config
    }
    fn shard_count(&self) -> usize {
        self.shard_count
    }
}

impl SinkOutput<Stdout> for ConsoleSink {
    fn output(&self) -> BufWriter<Stdout> {
        let stdout = io::stdout(); // get the global stdout entity
        io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout)
    }

    fn write_date(&self, date: &str) -> String {
        if self.config.no_color {
            date.to_string()
        } else {
            date.red().to_string()
        }
    }

    fn write_shard_id(&self, shard_id: &str) -> String {
        if self.config.no_color {
            shard_id.to_string()
        } else {
            shard_id.blue().to_string()
        }
    }

    fn write_key(&self, key: &str) -> String {
        if self.config.no_color {
            key.to_string()
        } else {
            key.yellow().to_string()
        }
    }

    fn write_sequence_number(&self, sq: &str) -> String {
        if self.config.no_color {
            sq.to_string()
        } else {
            sq.green().to_string()
        }
    }

    fn write_delimiter(&self, delimiter: &str) -> String {
        if self.config.no_color {
            delimiter.to_string()
        } else {
            // grey-ish
            delimiter.truecolor(128, 128, 128).to_string()
        }
    }
}
