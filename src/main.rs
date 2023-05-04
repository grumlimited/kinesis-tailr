#![allow(clippy::result_large_err)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kinesis::{config::Region, meta::PKG_VERSION, Client};
use chrono::TimeZone;
use clap::Parser;
use log::info;
use std::io;
use tokio::sync::mpsc;

use crate::console::Console;
use kinesis::*;

mod console;
mod iterator;
mod kinesis;

#[derive(Debug, Parser)]
struct Opt {
    /// The AWS Region.
    #[structopt(short, long)]
    region: Option<String>,

    /// The name of the stream.
    #[structopt(short, long)]
    stream_name: String,

    /// Whether to display additional information.
    #[structopt(short, long)]
    verbose: bool,

    #[structopt(long)]
    max_messages: Option<u32>,

    #[structopt(long)]
    print_key: bool,

    #[structopt(long)]
    print_shardid: bool,

    #[structopt(long)]
    print_timestamp: bool,

    #[structopt(long)]
    print_delimiter: bool,

    #[structopt(long)]
    from: Option<String>,

    #[structopt(long)]
    shard_id: Option<String>,

    #[structopt(long)]
    endpoint_url: Option<String>,
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
        from,
        shard_id,
        endpoint_url,
    } = Opt::parse();

    env_logger::init();

    let region_provider = RegionProviderChain::first_try(region.map(Region::new))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));

    let from = from.map(|f| chrono::Utc.datetime_from_str(f.as_str(), "%+").unwrap());

    if verbose {
        info!("Kinesis client version: {}", PKG_VERSION);
        info!(
            "Region:                 {}",
            region_provider.region().await.unwrap().as_ref()
        );
        info!("Stream name:            {}", &stream_name);
        from.iter().for_each(|f| {
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

    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(100);

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
            from,
            tx_records.clone(),
        );

        shard_processor
            .run()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    Console::new(
        max_messages,
        print_key,
        print_shard,
        print_timestamp,
        print_delimiter,
        rx_records,
        tx_records,
    )
    .run()
    .await
}
