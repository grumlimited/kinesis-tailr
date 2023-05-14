#![allow(clippy::result_large_err)]

use std::io;

use tokio::sync::mpsc;

use crate::aws::client::*;
use crate::cli_helpers::*;
use crate::sink::console::ConsoleSink;
use crate::sink::Sink;
use clap::Parser;
use kinesis::helpers::get_shards;
use kinesis::models::*;
use log::debug;
use tokio::task::JoinSet;

mod iterator;
mod kinesis;
mod sink;

mod aws;
mod cli_helpers;

/**
 * Number of shards to process per thread.
 * This is a tradeoff between the number of threads and the number of shards to process.
 * The more shards per thread:  
 * - the less threads are needed, but the more messages are buffered in memory,
 * - the fewer concurrent AWS calls.
 *
 * 100 is chosen because the maximum number of shards, depending on the region, is between 200 and 500.
 * Therefore 100 means 5 threads, which should be a good default between concurrent connections and "responsiveness".
 */
pub const NB_SHARDS_PER_THREAD: usize = 100;

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

    let stream_name = opt.stream_name.clone();

    let shard_groups = divide_shards(&selected_shards, NB_SHARDS_PER_THREAD);
    debug!("Spawning {} threads", shard_groups.len());

    let shard_processors = shard_groups
        .iter()
        .map(|shard_ids| {
            let tx_records = tx_records.clone();
            let client = client.clone();
            let stream_name = stream_name.clone();
            let shard_ids = shard_ids
                .clone()
                .iter()
                .map(|shard_id| shard_id.to_string())
                .collect();

            async move {
                let shard_processor = kinesis::helpers::new(
                    client.clone(),
                    stream_name,
                    shard_ids,
                    from_datetime,
                    tx_records.clone(),
                );

                shard_processor
                    .run()
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                    .unwrap();
            }
        })
        .collect::<Vec<_>>();

    let mut shard_processors_handle = JoinSet::new();

    for shard_processor in shard_processors {
        shard_processors_handle.spawn(shard_processor);
    }

    console.await.unwrap();

    Ok(())
}
