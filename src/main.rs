#![allow(clippy::result_large_err)]

use aws_sdk_kinesis::{config::Region, meta::PKG_VERSION};
use clap::Parser;
use log::info;
use std::io;
use tokio::sync::mpsc;

use crate::aws::client::*;

use crate::cli_helpers::parse_date;
use crate::sink::console::ConsoleSink;
use crate::sink::Sink;
use kinesis::helpers::get_shards;
use kinesis::models::*;

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
    let Opt {
        stream_name,
        region,
        verbose,
        max_messages,
        no_color,
        print_key,
        print_shardid: print_shard,
        print_timestamp,
        print_delimiter,
        from_datetime: from,
        shard_id,
        endpoint_url,
    } = Opt::parse();

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let from_datetime = parse_date(from.as_deref());
    let client = aws::client::create_client(region, endpoint_url).await;

    if verbose {
        info!("Kinesis client version: {}", PKG_VERSION);
        info!(
            "Region:                 {}",
            client.get_region().unwrap_or(&Region::new("us-east-1"))
        );
        info!("Stream name:            {}", &stream_name);
        from_datetime.iter().for_each(|f| {
            info!("From:                   {}", &f.format("%+"));
        });
    }

    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(50000);

    let shards = get_shards(&client, &stream_name)
        .await
        .unwrap_or_else(|_| panic!("Could not describe shards for stream {}", stream_name));

    let selected_shards = if let Some(shard_id) = &shard_id {
        if !shards.contains(shard_id) {
            panic!(
                "Shard {} does not exist in stream {}",
                shard_id, stream_name
            );
        }
        vec![shard_id.clone()]
    } else {
        shards
    };

    if verbose {
        let is_filtered = if shard_id.is_some() {
            " (filtered)"
        } else {
            ""
        };
        info!("{}{}", "Shards", is_filtered);

        for shard_id in &selected_shards {
            info!("{}{}", std::char::from_u32(0x3009).unwrap(), shard_id) // 0x3009 is '〉'
        }
    }

    let shard_processor = kinesis::helpers::new(
        client.clone(),
        stream_name.clone(),
        selected_shards,
        from_datetime,
        tx_records.clone(),
    );

    shard_processor
        .run()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    ConsoleSink::new(
        max_messages,
        no_color,
        print_key,
        print_shard,
        print_timestamp,
        print_delimiter,
    )
    .run(tx_records, rx_records)
    .await
}

mod cli_helpers {
    use chrono::{DateTime, TimeZone, Utc};

    pub fn parse_date(from: Option<&str>) -> Option<DateTime<Utc>> {
        from.map(|f| chrono::Utc.datetime_from_str(f, "%+").unwrap())
    }

    pub fn divide_shards(source: &[String]) -> Vec<Vec<String>> {
        let mut dest: Vec<Vec<String>> = Vec::new();
        let mut current_buffer: Vec<String> = Vec::new();

        let group_size = 2;
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
            divide_shards(&source),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()],
                vec!["e".to_string()],
            ]
        );

        assert_eq!(
            divide_shards(&vec!["e".to_string()]),
            vec![vec!["e".to_string()],]
        );

        assert_eq!(divide_shards(&vec![]), vec![] as Vec<Vec<String>>);
    }
}
