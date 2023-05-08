#![allow(clippy::result_large_err)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kinesis::{config::Region, meta::PKG_VERSION, Client};
use clap::Parser;
use log::info;
use std::io;
use tokio::sync::mpsc;

use crate::cli_helpers::parse_date;
use crate::console::{ConsoleSink, Sink};
use kinesis::helpers::get_shards;
use kinesis::models::*;

mod console;
mod iterator;
mod kinesis;

#[derive(Debug, Parser)]
struct Opt {
    /// AWS Region
    #[structopt(short, long)]
    region: Option<String>,

    /// Name of the stream
    #[structopt(short, long)]
    stream_name: String,

    /// Shard ID to tail from
    #[structopt(long)]
    shard_id: Option<String>,

    /// Maximum number of messages to retrieve
    #[structopt(long)]
    max_messages: Option<u32>,

    /// Start datetime position to tail from. ISO 8601 format.
    #[structopt(long)]
    from_datetime: Option<String>,

    /// Print the partition key
    #[structopt(long)]
    print_key: bool,

    /// Print the shard ID
    #[structopt(long)]
    print_shardid: bool,

    /// Print timestamps
    #[structopt(long)]
    print_timestamp: bool,

    /// Print a delimiter between each payload
    #[structopt(long)]
    print_delimiter: bool,

    /// Endpoint URL to use
    #[structopt(long)]
    endpoint_url: Option<String>,

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
        print_key,
        print_shardid: print_shard,
        print_timestamp,
        print_delimiter,
        from_datetime: from,
        shard_id,
        endpoint_url,
    } = Opt::parse();

    env_logger::init();

    let region_provider = RegionProviderChain::first_try(region.map(Region::new))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));

    let from_datetime = parse_date(from.as_deref());

    if verbose {
        info!("Kinesis client version: {}", PKG_VERSION);
        info!(
            "Region:                 {}",
            region_provider.region().await.unwrap().as_ref()
        );
        info!("Stream name:            {}", &stream_name);
        from_datetime.iter().for_each(|f| {
            info!("From:                   {}", &f.format("%+"));
        });
    }

    let shared_config = {
        let inner = aws_config::from_env().region(region_provider);

        let inner = if endpoint_url.is_some() {
            inner.endpoint_url(endpoint_url.unwrap().as_str())
        } else {
            inner
        };

        inner
    }
    .load()
    .await;

    let client = Client::new(&shared_config);

    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(500);

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

    for shard_id in &selected_shards {
        let shard_processor = kinesis::new(
            client.clone(),
            stream_name.clone(),
            shard_id.clone(),
            from_datetime,
            tx_records.clone(),
        );

        shard_processor
            .run()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    ConsoleSink::new(
        max_messages,
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
}
