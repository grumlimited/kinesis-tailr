#![allow(clippy::result_large_err)]

use std::io;

use clap::Parser;
use tokio::sync::mpsc;

use kinesis::helpers::get_shards;
use kinesis::models::*;

use crate::aws::client::*;
use crate::cli_helpers::{divide_shards, parse_date, print_runtime, reset_signal_pipe_handler};
use crate::sink::console::ConsoleSink;
use crate::sink::Sink;

mod iterator;
mod kinesis;
mod sink;

mod aws;

#[derive(Debug, Parser)]
struct Opt {
    /// AWS Region
    #[structopt(short, long)]
    region: Option<String>,

    /// Name of the stream
    #[structopt(short, long)]
    stream_name: String,

    /// Endpoint URL to use
    #[structopt(long)]
    endpoint_url: Option<String>,

    /// Start datetime position to tail from. ISO 8601 format.
    #[structopt(long)]
    from_datetime: Option<String>,

    /// Maximum number of messages to retrieve
    #[structopt(long)]
    max_messages: Option<u32>,

    /// Disable color output
    #[structopt(long)]
    no_color: bool,

    /// Print a delimiter between each payload
    #[structopt(long)]
    print_delimiter: bool,

    /// Print the partition key
    #[structopt(long)]
    print_key: bool,

    /// Print the shard ID
    #[structopt(long)]
    print_shardid: bool,

    /// Print timestamps
    #[structopt(long)]
    print_timestamp: bool,

    /// Shard ID to tail from
    #[structopt(long)]
    shard_id: Option<String>,

    /// Display additional information
    #[structopt(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    reset_signal_pipe_handler().expect("TODO: panic message");

    let opt = Opt::parse();

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("warn"));

    let from_datetime = parse_date(opt.from_datetime.as_deref());
    let client = create_client(opt.region.clone(), opt.endpoint_url.clone()).await;

    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1000);

    let shards = get_shards(&client, &opt.stream_name)
        .await
        .unwrap_or_else(|_| panic!("Could not describe shards for stream {}", opt.stream_name));

    let selected_shards: Vec<String> = if let Some(shard_id) = &opt.shard_id {
        if !shards.contains(shard_id) {
            panic!(
                "Shard {} does not exist in stream {}",
                shard_id, opt.stream_name
            );
        }
        vec![shard_id.clone()]
    } else {
        shards
    };

    print_runtime(&opt, &selected_shards);

    let console = tokio::spawn({
        let tx_records = tx_records.clone();

        async move {
            ConsoleSink::new(
                opt.max_messages,
                opt.no_color,
                opt.print_key,
                opt.print_shardid,
                opt.print_timestamp,
                opt.print_delimiter,
            )
            .run(tx_records, rx_records)
            .await
            .unwrap();
        }
    });

    let shard_groups = divide_shards(&selected_shards, 500);
    for shard_id in &shard_groups {
        let shard_processor = kinesis::helpers::new(
            client.clone(),
            opt.stream_name.clone(),
            shard_id.clone(),
            from_datetime,
            tx_records.clone(),
        );

        shard_processor
            .run()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    console.await.unwrap_or(());

    Ok(())
}

mod cli_helpers {
    use aws_sdk_kinesis::meta::PKG_VERSION;
    use chrono::{DateTime, TimeZone, Utc};
    use log::info;
    use std::io::Error;

    use crate::Opt;

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
        // Long story short: handle SIGPIPE (ie broken pipe) on Unix systems gracefully.
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
}

#[cfg(test)]
mod tests {
    use super::cli_helpers::*;

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
}
