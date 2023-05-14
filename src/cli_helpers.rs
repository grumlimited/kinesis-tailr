use aws_sdk_kinesis::meta::PKG_VERSION;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use log::info;
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

    /// Print the shard ID
    #[structopt(long)]
    pub print_shardid: bool,

    /// Print timestamps
    #[structopt(long)]
    pub print_timestamp: bool,

    /// Shard ID to tail from
    #[structopt(long)]
    pub shard_id: Option<String>,

    /// Display additional information
    #[structopt(short, long)]
    pub verbose: bool,
}

pub(crate) fn selected_shards<'a>(
    shards: &'a mut [String],
    stream_name: &str,
    shard_id: &Option<String>,
) -> Vec<&'a String> {
    if let Some(shard_id) = shard_id {
        if !shards.contains(shard_id) {
            panic!(
                "Shard {} does not exist in stream {}",
                shard_id, stream_name
            );
        }
    };

    shards
        .iter()
        .filter(|s| match shard_id.as_ref() {
            Some(shard_id) => shard_id == *s,
            None => true,
        })
        .collect::<Vec<_>>()
}

pub(crate) fn set_log_level() {
    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or("WARN,kinesis_tailr=INFO"),
    );
}

pub(crate) fn print_runtime(opt: &Opt, selected_shards: &Vec<String>) {
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

pub fn divide_shards<T: Clone>(source: &[T], group_size: u32) -> Vec<Vec<T>> {
    if group_size == 0 {
        return vec![];
    }

    let mut dest: Vec<Vec<T>> = Vec::new();
    let mut current_buffer: Vec<T> = Vec::new();

    let mut i = 0;
    for s in source {
        if i < group_size {
            current_buffer.push(s.clone());
            i += 1;
        } else {
            dest.push(current_buffer.clone());
            current_buffer.clear();

            current_buffer.push(s.clone());

            i = 1;
        }
    }

    if !current_buffer.is_empty() {
        dest.push(current_buffer.clone());
    }

    dest
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
        let result = result.to_rfc3339().to_string();
        assert_eq!(result, "2023-05-04T20:57:12+00:00");
    }

    #[test]
    #[should_panic]
    fn parse_date_test_fail() {
        let invalid_date = "xxx";
        parse_date(Some(invalid_date));
    }

    #[test]
    fn divide() {
        let source = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        assert_eq!(
            divide_shards::<String>(&source, 2),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
                vec!["e".to_string()],
            ]
        );

        assert_eq!(
            divide_shards::<String>(&vec!["e".to_string()], 2),
            vec![vec!["e".to_string()],]
        );

        assert_eq!(
            divide_shards::<String>(&vec![], 2),
            vec![] as Vec<Vec<String>>
        );

        assert_eq!(
            divide_shards::<String>(&source, 5),
            vec![vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string()
            ],]
        );

        assert_eq!(
            divide_shards::<String>(&source, 1),
            vec![
                vec!["a".to_string()],
                vec!["b".to_string()],
                vec!["c".to_string()],
                vec!["d".to_string()],
                vec!["e".to_string()],
            ]
        );

        assert_eq!(
            divide_shards::<String>(&source, 0),
            vec![] as Vec<Vec<String>>
        );
    }

    #[test]
    fn selected_shards_ok() {
        let mut shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(&mut shards, "stream", &None),
            vec!["a", "b", "c"]
        );

        assert_eq!(
            selected_shards(&mut shards, "stream", &Some("a".to_string())),
            vec!["a"]
        );

        assert_eq!(
            selected_shards(&mut shards, "stream", &Some("b".to_string())),
            vec!["b"]
        );

        assert_eq!(
            selected_shards(&mut shards, "stream", &Some("c".to_string())),
            vec!["c"]
        );
    }

    #[test]
    #[should_panic]
    fn selected_shards_panic() {
        let mut shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(&mut shards, "stream", &Some("d".to_string())),
            vec![] as Vec<&str>
        );
    }
}
