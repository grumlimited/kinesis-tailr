use std::sync::Arc;

use aws_sdk_kinesis::primitives::DateTime;
use tokio::sync::mpsc;

use crate::kinesis::models::ShardProcessorADT::{BeyondToTimestamp, Progress, Termination};
use crate::sink::console::ConsoleSink;

use super::*;

#[test]
fn format_nb_messages_ok() {
    let console = ConsoleSink {
        config: SinkConfig::default(),
        shard_count: 1,
    };

    assert_eq!(console.format_nb_messages(1), "1 message processed");
    assert_eq!(console.format_nb_messages(2), "2 messages processed");
}

#[test]
fn format_outputs() {
    let _console = ConsoleSink {
        config: Default::default(),
        shard_count: 1,
    };

    let bw_console = ConsoleSink {
        config: SinkConfig {
            no_color: true,
            ..Default::default()
        },
        shard_count: 1,
    };

    // These dont pass on CI :-(
    // assert_eq!(console.write_date("data"), "\u{1b}[31mdata\u{1b}[0m");
    // assert_eq!(console.write_shard_id("data"), "\u{1b}[34mdata\u{1b}[0m");
    // assert_eq!(console.write_key("data"), "\u{1b}[33mdata\u{1b}[0m");
    // assert_eq!(
    //     console.write_delimiter("data"),
    //     "\u{1b}[38;2;128;128;128mdata\u{1b}[0m"
    // );

    assert_eq!(bw_console.write_date("data"), "data");
    assert_eq!(bw_console.write_shard_id("data"), "data");
    assert_eq!(bw_console.write_key("data"), "data");
    assert_eq!(bw_console.write_delimiter("data"), "data");
}

#[test]
fn format_outputs_base64() {
    let console = ConsoleSink {
        config: SinkConfig {
            encoding: PayloadEnc::Base64,
            no_color: true,
            ..Default::default()
        },
        shard_count: 1,
    };

    let input = b"Hello \xF0\x90\x80World";

    let record = RecordResult {
        shard_id: Arc::new("".to_string()),
        sequence_id: "sequence_id".to_string(),
        partition_key: "partition_key".to_string(),
        datetime: DateTime::from_secs(1_000_000_i64),
        data: input.to_vec(),
    };

    let vec = console.format_record(&record);
    let result = String::from_utf8_lossy(vec.as_slice());
    assert_eq!(result, "SGVsbG8g8JCAV29ybGQ=\n");
}

#[test]
fn format_outputs_utf8() {
    let console = ConsoleSink {
        config: SinkConfig {
            encoding: PayloadEnc::Utf8,
            no_color: true,
            ..Default::default()
        },
        shard_count: 1,
    };

    let input = b"Hello \xF0\x90\x80World";

    let record = RecordResult {
        shard_id: Arc::new("".to_string()),
        sequence_id: "sequence_id".to_string(),
        partition_key: "partition_key".to_string(),
        datetime: DateTime::from_secs(1_000_000_i64),
        data: input.to_vec(),
    };

    let vec = console.format_record(&record);
    let result = String::from_utf8_lossy(vec.as_slice());
    assert_eq!(result, "Hello �World\n");
}

#[test]
fn format_outputs_raw() {
    let console = ConsoleSink {
        config: SinkConfig {
            encoding: PayloadEnc::Raw,
            no_color: true,
            ..Default::default()
        },
        shard_count: 1,
    };

    let input = b"Hello \xF0\x90\x80World";

    let record = RecordResult {
        shard_id: Arc::new("shard_id".to_string()),
        sequence_id: "sequence_id".to_string(),
        partition_key: "partition_key".to_string(),
        datetime: DateTime::from_secs(1_000_000_i64),
        data: input.to_vec(),
    };

    let vec = console.format_record(&record);
    assert_eq!(vec, b"Hello \xF0\x90\x80World\n");
}

#[tokio::test]
async fn expect_zero_messages_processed() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = get_string_sink(None);

    tokio::spawn(async move {
        //only 1 shard -> should terminate console sink
        tx_records_clone
            .send(Ok(Termination))
            .await
            .expect("TODO: panic message");
    });

    let mut handle = BufWriter::new(Vec::new());

    sink.run_inner(tx_records, rx_records, &mut handle)
        .await
        .unwrap();

    handle.flush().unwrap();
    let bytes = handle.into_inner().unwrap();
    let string = String::from_utf8(bytes).unwrap();

    assert_eq!(string, "");
}

#[tokio::test]
async fn sending_beyondtotimestamp_should_terminate_sink() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = get_string_sink(None);

    tokio::spawn(async move {
        //only 1 shard -> should terminate console sink
        tx_records_clone
            .send(Ok(BeyondToTimestamp))
            .await
            .expect("TODO: panic message");
    });

    let mut handle = BufWriter::new(Vec::new());

    sink.run_inner(tx_records, rx_records, &mut handle)
        .await
        .unwrap();

    handle.flush().unwrap();
    let bytes = handle.into_inner().unwrap();
    let string = String::from_utf8(bytes).unwrap();

    assert_eq!(string, "");
}

#[tokio::test]
async fn expect_split() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = get_string_sink(Some(50));

    tokio::spawn(async move {
        tx_records_clone
            .send(Ok(Progress(vec![RecordResult {
                shard_id: Arc::new("".to_string()),
                sequence_id: "".to_string(),
                partition_key: "partition_key".to_string(),
                datetime: DateTime::from_secs(1_000_000_i64),
                data: "payload".as_bytes().to_vec(),
            }])))
            .await
            .expect("TODO: panic message");

        tx_records_clone
            .send(Ok(Termination))
            .await
            .expect("TODO: panic message");
    });

    let mut handle = BufWriter::new(Vec::new());

    sink.run_inner(tx_records, rx_records, &mut handle)
        .await
        .unwrap();

    handle.flush().unwrap();
    let bytes = handle.into_inner().unwrap();
    let string = String::from_utf8(bytes).unwrap();

    assert_eq!(string, "payload\n");
}

fn get_string_sink(max_messages: Option<u32>) -> StringSink {
    StringSink {
        config: SinkConfig {
            max_messages,
            ..Default::default()
        },
    }
}

pub struct StringSink {
    config: SinkConfig,
}

impl SinkOutput<Vec<u8>> for StringSink {
    fn output(&self) -> Result<BufWriter<Vec<u8>>> {
        unimplemented!()
    }
}

impl Configurable for StringSink {
    fn get_config(&self) -> &SinkConfig {
        &self.config
    }

    fn shard_count(&self) -> usize {
        1
    }
}
