use anyhow::Result;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::path::PathBuf;

use crate::sink::{Configurable, PayloadEnc, SinkConfig, SinkOutput};

pub const FILE_BUF_SIZE: usize = 512 * 1024; // 512Ko

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
        print_sequence_number: bool,
        print_shard_id: bool,
        print_timestamp: bool,
        print_delimiter: bool,
        encoding: PayloadEnc,
        shard_count: usize,
        file: P,
    ) -> Self {
        FileSink {
            config: SinkConfig {
                max_messages,
                no_color,
                print_key,
                print_sequence_number,
                print_shard_id,
                print_timestamp,
                print_delimiter,
                encoding,
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
    fn output(&self) -> Result<BufWriter<File>> {
        let file = File::create(&self.file).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Could not write to file {}", self.file.display()),
            )
        })?;
        Ok(BufWriter::with_capacity(FILE_BUF_SIZE, file))
    }
}
