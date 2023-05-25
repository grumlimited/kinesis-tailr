use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::path::PathBuf;

use crate::sink::{Configurable, SinkConfig, SinkOutput};

pub struct FileSink {
    pub(crate) config: SinkConfig,
    pub(crate) file: PathBuf,
    pub(crate) shard_count: usize,
}

impl FileSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new<P: Into<PathBuf>>(
        max_messages: Option<u32>,
        no_color: bool,
        print_key: bool,
        print_shard_id: bool,
        print_timestamp: bool,
        print_delimiter: bool,
        shard_count: usize,
        file: P,
    ) -> Self {
        FileSink {
            config: SinkConfig {
                max_messages,
                no_color,
                print_key,
                print_shard_id,
                print_timestamp,
                print_delimiter,
                exit_after_termination: true,
            },
            file: file.into(),
            shard_count,
        }
    }
}

impl Configurable for FileSink {
    fn get_config(&self) -> &SinkConfig {
        &self.config
    }

    fn shard_count(&self) -> usize {
        self.shard_count
    }
}

impl SinkOutput<File> for FileSink {
    fn output(&self) -> BufWriter<File> {
        let file = File::create(&self.file)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Could not write to file {}", self.file.display()),
                )
            })
            .unwrap();
        BufWriter::new(file)
    }
}
