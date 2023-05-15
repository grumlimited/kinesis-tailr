#![allow(clippy::result_large_err)]

use std::io;
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};

use crate::aws::client::*;
use crate::cli_helpers::*;
use crate::sink::console::ConsoleSink;
use crate::sink::file::FileSink;
use crate::sink::{file, Sink};
use clap::Parser;
use kinesis::helpers::get_shards;
use kinesis::models::*;
use tokio::task::JoinSet;

mod iterator;
mod kinesis;
mod sink;

mod aws;
mod cli_helpers;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    reset_signal_pipe_handler().expect("TODO: panic message");
    set_log_level();

    let opt = Opt::parse();

    let from_datetime = parse_date(opt.from_datetime.as_deref());
    let client = create_client(opt.region.clone(), opt.endpoint_url.clone()).await;

    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1000);

    let shards = get_shards(&client, &opt.stream_name)
        .await
        .unwrap_or_else(|_| panic!("Could not describe shards for stream {}", opt.stream_name));

    let selected_shards = selected_shards(shards.as_slice(), &opt.stream_name, &opt.shard_id);

    print_runtime(&opt, &selected_shards);

    let handle = tokio::spawn({
        let tx_records = tx_records.clone();
        async move {
            match opt.output_file {
                Some(file) => {
                    file::check_path(&file).await?;

                    FileSink::new(
                        opt.max_messages,
                        opt.no_color,
                        opt.print_key,
                        opt.print_shardid,
                        opt.print_timestamp,
                        opt.print_delimiter,
                        file,
                    )
                    .run(tx_records, rx_records)
                    .await
                }
                None => {
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
                }
            }
        }
    });

    let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(opt.concurrent));

    let shard_processors = {
        let selected_shards = selected_shards
            .iter()
            .map(|s| (*s).clone())
            .collect::<Vec<_>>();

        selected_shards
            .iter()
            .map(|shard_id| {
                let tx_records = tx_records.clone();
                let client = client.clone();
                let stream_name = opt.stream_name.clone();
                let shard_id = shard_id.clone();
                let semaphore = semaphore.clone();

                tokio::spawn(async move {
                    let shard_processor = kinesis::helpers::new(
                        client.clone(),
                        stream_name,
                        shard_id.clone(),
                        from_datetime,
                        semaphore,
                        tx_records.clone(),
                    );

                    shard_processor
                        .run()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                        .unwrap();
                })
            })
            .collect::<Vec<_>>()
    };

    let mut shard_processors_handle = JoinSet::new();

    for shard_processor in shard_processors {
        shard_processors_handle.spawn(shard_processor);
    }

    handle.await?
}
