use aws_sdk_kinesis::meta::PKG_VERSION;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use log::info;
use std::io;
use std::io::Error;

#[derive(Debug, Parser)]
pub struct Opt {
    /// AWS Region
    #[structopt(short, long)]
    pub region: Option<String>,

    /// Name of the stream
    #[structopt(short, long)]
    pub stream_name: String,

    /// Endpoint URL to use
    #[structopt(long)]
    pub endpoint_url: Option<String>,

    /// Start datetime position to tail from. ISO 8601 format.
    #[structopt(long)]
    pub from_datetime: Option<String>,

    /// Maximum number of messages to retrieve
    #[structopt(long)]
    pub max_messages: Option<u32>,

    /// Disable color output
    #[structopt(long)]
    pub no_color: bool,

    /// Print a delimiter between each payload
    #[structopt(long)]
    pub print_delimiter: bool,

    /// Print the partition key
    #[structopt(long)]
    pub print_key: bool,

    /// Print the shard ID.
    #[structopt(long)]
    pub print_shard_id: bool,

    /// Print timestamps
    #[structopt(long)]
    pub print_timestamp: bool,

    /// Shard ID to tail from. Repeat option for each shard ID to filter on
    #[structopt(long)]
    pub shard_id: Option<Vec<String>>,

    /// Output file to write to
    #[structopt(long, short)]
    pub output_file: Option<String>,

    /// Concurrent number of shards to tail
    #[structopt(short, long)]
    #[clap(default_value_t = 10)]
    pub concurrent: usize,

    /// Display additional information
    #[structopt(short, long)]
    pub verbose: bool,
}

pub(crate) fn selected_shards<'a>(
    shards: &'a [String],
    stream_name: &str,
    shard_ids: &Option<Vec<String>>,
) -> io::Result<Vec<&'a str>> {
    let filtered = match shard_ids {
        Some(shard_ids) => shards
            .iter()
            .filter(|s| shard_ids.contains(s))
            .map(|e| e.as_str())
            .collect::<Vec<_>>(),
        None => shards.iter().map(|e| e.as_str()).collect::<Vec<_>>(),
    };

    if filtered.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("No shards found for stream {}", stream_name),
        ))
    } else {
        Ok(filtered)
    }
}

pub(crate) fn set_log_level() {
    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or("WARN,kinesis_tailr=INFO"),
    );
}

pub(crate) fn print_runtime(opt: &Opt, selected_shards: &[&str]) {
    if opt.verbose {
        info!("Kinesis client version: {}", PKG_VERSION);
        info!(
            "Region:                 {}",
            opt.region.as_ref().unwrap_or(&"us-east-1".to_owned())
        );
        info!("Stream name:            {}", &opt.stream_name);
        opt.from_datetime.iter().for_each(|f| {
            info!("From:                   {}", f);
        });

        let is_filtered = if opt.shard_id.is_some() {
            " (filtered)"
        } else {
            ""
        };
        info!(
            "Shards:                 {}{}",
            selected_shards.len(),
            is_filtered
        );
    }
}

pub fn parse_date(from: Option<&str>) -> Option<DateTime<Utc>> {
    from.map(|f| chrono::Utc.datetime_from_str(f, "%+").unwrap())
}

pub fn reset_signal_pipe_handler() -> Result<(), Error> {
    // https://github.com/rust-lang/rust/issues/46016
    // Long story short: handle SIGPIPE (ie. broken pipe) on Unix systems gracefully.
    #[cfg(target_family = "unix")]
    {
        use nix::sys::signal;

        unsafe {
            signal::signal(signal::Signal::SIGPIPE, signal::SigHandler::SigDfl)
                .map_err(Error::from)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_test_ok() {
        let date = "2023-05-04T20:57:12Z";
        let result = parse_date(Some(date)).unwrap();
        let result = result.to_rfc3339();
        assert_eq!(result, "2023-05-04T20:57:12+00:00");
    }

    #[test]
    #[should_panic]
    fn parse_date_test_fail() {
        let invalid_date = "xxx";
        parse_date(Some(invalid_date));
    }

    #[test]
    fn selected_shards_ok() {
        let shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(shards.as_slice(), "stream", &None).unwrap(),
            vec!["a", "b", "c"]
        );

        assert_eq!(
            selected_shards(shards.as_slice(), "stream", &Some(vec!["a".to_string()])).unwrap(),
            vec!["a"]
        );

        assert_eq!(
            selected_shards(shards.as_slice(), "stream", &Some(vec!["b".to_string()])).unwrap(),
            vec!["b"]
        );

        assert_eq!(
            selected_shards(shards.as_slice(), "stream", &Some(vec!["c".to_string()])).unwrap(),
            vec!["c"]
        );
    }

    #[test]
    #[should_panic]
    fn selected_shards_panic() {
        let shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(shards.as_slice(), "stream", &Some(vec!["d".to_string()])).unwrap(),
            vec![] as Vec<&str>
        );
    }
}
