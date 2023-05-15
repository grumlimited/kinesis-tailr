use std::fs::File;
use std::io;
use std::io::{BufWriter, ErrorKind};
use std::path::{Path, PathBuf};

use crate::sink::{Configurable, SinkConfig, SinkOutput};

pub struct FileSink {
    pub(crate) config: SinkConfig,
    pub(crate) file: PathBuf,
}

impl FileSink {
    pub fn new<P: Into<PathBuf>>(
        max_messages: Option<u32>,
        no_color: bool,
        print_key: bool,
        print_shardid: bool,
        print_timestamp: bool,
        print_delimiter: bool,
        file: P,
    ) -> Self {
        FileSink {
            config: SinkConfig {
                max_messages,
                no_color,
                print_key,
                print_shardid,
                print_timestamp,
                print_delimiter,
                exit_after_termination: true,
            },
            file: file.into(),
        }
    }
}

impl Configurable for FileSink {
    fn get_config(&self) -> SinkConfig {
        self.config.clone()
    }
}

impl SinkOutput<File> for FileSink {
    fn output(&mut self) -> BufWriter<File> {
        let file = File::create(&self.file).unwrap();
        BufWriter::new(file)
    }
}

pub async fn check_path<P: AsRef<Path>>(path: P) -> io::Result<()> {
    async move {
        if path.as_ref().exists() {
            Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("{} already exists.", path.as_ref().display()),
            ))
        } else {
            Ok(())
        }
    }
    .await
}
