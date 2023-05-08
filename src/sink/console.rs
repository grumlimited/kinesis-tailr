use crate::sink::{Configurable, SinkConfig, SinkOutput};
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
        let handle = io::BufWriter::with_capacity(CONSOLE_BUF_SIZE, stdout);
        handle
    }
}
