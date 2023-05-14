#![allow(clippy::result_large_err)]

use std::io;

use tokio::sync::mpsc;

use crate::aws::client::*;
use crate::cli_helpers::{
    divide_shards, parse_date, print_runtime, reset_signal_pipe_handler, Opt,
};
use crate::sink::console::ConsoleSink;
use crate::sink::Sink;
use clap::Parser;
use kinesis::helpers::get_shards;
use kinesis::models::*;

mod iterator;
mod kinesis;
mod sink;

mod aws;
mod cli_helpers;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    reset_signal_pipe_handler().expect("TODO: panic message");

    let opt = Opt::parse();

    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or("WARN,kinesis_tailr=INFO"),
    );

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
