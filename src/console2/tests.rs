use super::*;
use crate::kinesis::models::ShardProcessorADT::Termination;
use tokio::sync::mpsc;

#[test]
fn format_nb_messages_ok() {
    let console = Console2 {
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
async fn www() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let tx_records_clone = tx_records.clone();

    let mut sink = StringSink {
        config: SinkConfig {
            max_messages: None,
            print_key: false,
            print_shardid: false,
            print_timestamp: false,
            print_delimiter: false,
            exit_after_termination: false,
        },
    };

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
