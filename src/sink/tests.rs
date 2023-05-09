use super::*;
use crate::kinesis::models::ShardProcessorADT::{Progress, Termination};
use crate::sink::console::ConsoleSink;
use aws_sdk_kinesis::primitives::DateTime;
use tokio::sync::mpsc;

#[test]
fn format_nb_messages_ok() {
    let console = ConsoleSink {
        config: SinkConfig {
            max_messages: None,
            print_key: false,
            print_shardid: false,
            print_timestamp: false,
            print_delimiter: false,
            exit_after_termination: false,
        },
    };

    assert_eq!(console.format_nb_messages(1), "1 message processed");
    assert_eq!(console.format_nb_messages(2), "2 messages processed");
}

#[tokio::test]
async fn expect_zero_messages_processed() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = get_string_sink(None);

    tokio::spawn(async move {
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

    assert_eq!(string, "0 message processed\n");
}

#[tokio::test]
async fn expect_split() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = get_string_sink(Some(50));

    tokio::spawn(async move {
        tx_records_clone
            .send(Ok(Progress(vec![RecordResult {
                shard_id: "".to_string(),
                sequence_id: "".to_string(),
                datetime: DateTime::from_secs(1_000_000 as i64),
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

    assert_eq!(string, "payload\n1 message processed\n");
}

fn get_string_sink(max_messages: Option<u32>) -> StringSink {
    StringSink {
        config: SinkConfig {
            max_messages,
            print_key: false,
            print_shardid: false,
            print_timestamp: false,
            print_delimiter: false,
            exit_after_termination: false,
        },
    }
}

pub struct StringSink {
    config: SinkConfig,
}

impl SinkOutput<Vec<u8>> for StringSink {
    fn offer(&mut self) -> BufWriter<Vec<u8>> {
        unimplemented!()
    }
}

impl Configurable for StringSink {
    fn get_config(&self) -> SinkConfig {
        self.config.clone()
    }
}