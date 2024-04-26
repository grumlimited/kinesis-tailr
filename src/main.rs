#![allow(clippy::result_large_err)]

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use kinesis::helpers::get_shards;
use kinesis::models::*;
use kinesis::ticker::Ticker;

use crate::aws::client::*;
use crate::cli_helpers::*;
use crate::kinesis::ticker::TickerMessage;
use crate::sink::console::ConsoleSink;
use crate::sink::file::FileSink;
use crate::sink::Sink;

mod iterator;
mod kinesis;
mod sink;

mod aws;
mod cli_helpers;

#[tokio::main]
async fn main() -> Result<()> {
    reset_signal_pipe_handler().expect("TODO: panic message");
    set_log_level();

    let opt = Opt::parse();

    let from_datetime = parse_date(opt.from_datetime.as_deref())?;
    let to_datetime = parse_date(opt.to_datetime.as_deref())?;

    validate_time_boundaries(&from_datetime, &to_datetime)?;

    let client = create_client(
        opt.max_attempts,
        opt.region.clone(),
        opt.endpoint_url.clone(),
    )
    .await;

    let shards = get_shards(&client, &opt.stream_name).await?;

    let selected_shards = selected_shards(shards, &opt.stream_name, &opt.shard_id)?;
    let shard_count = selected_shards.len();

    let (tx_records, rx_records) =
        mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(shard_count);

    print_runtime(&opt, &selected_shards);

    let handle = tokio::spawn({
        let tx_records = tx_records.clone();
        async move {
            match opt.output_file {
                Some(file) => {
                    FileSink::new(
                        opt.max_messages,
                        opt.no_color,
                        opt.print_key,
                        opt.print_sequence_number,
                        opt.print_shard_id,
                        opt.print_timestamp,
                        opt.print_delimiter,
                        opt.base64_encoding,
                        shard_count,
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
                        opt.print_sequence_number,
                        opt.print_shard_id,
                        opt.print_timestamp,
                        opt.print_delimiter,
                        opt.base64_encoding,
                        shard_count,
                    )
                    .run(tx_records, rx_records)
                    .await
                }
            }
        }
    });

    let tx_ticker_updates = match opt.progress {
        true => {
            let (tx_ticker_updates, rx_ticker_updates) =
                mpsc::channel::<TickerMessage>(shard_count);

            let tx_records = tx_records.clone();

            tokio::spawn({
                async move {
                    let mut ticker = Ticker::new(opt.timeout, rx_ticker_updates, tx_records);
                    ticker.run().await;
                }
            });

            Some(tx_ticker_updates)
        }
        _ => None,
    };

    let shard_processors = {
        let semaphore = semaphore(shard_count, opt.concurrent);

        selected_shards
            .iter()
            .map(|shard_id| {
                let tx_ticker_updates = tx_ticker_updates.clone();
                let tx_records = tx_records.clone();
                let client = client.clone();
                let stream_name = opt.stream_name.clone();
                let shard_id = shard_id.clone();
                let semaphore = semaphore.clone();

                let shard_processor = kinesis::helpers::new(
                    client,
                    stream_name,
                    shard_id,
                    from_datetime,
                    to_datetime,
                    semaphore,
                    tx_records.clone(),
                    tx_ticker_updates.clone(),
                );

                tokio::spawn(async move {
                    shard_processor.run().await.unwrap();
                })
            })
            .collect::<Vec<_>>()
    };

    drop(tx_records);

    let mut shard_processors_handle = JoinSet::new();

    for shard_processor in shard_processors {
        shard_processors_handle.spawn(shard_processor);
    }

    let _ = handle.await?;

    Ok(())
}

fn semaphore(shard_count: usize, concurrent: Option<usize>) -> Arc<Semaphore> {
    let concurrent = match concurrent {
        Some(concurrent) => concurrent,
        None => std::cmp::min(shard_count, SEMAPHORE_DEFAULT_SIZE),
    };

    Arc::new(Semaphore::new(concurrent))
}
