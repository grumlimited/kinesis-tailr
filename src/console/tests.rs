use super::*;
use tokio::sync::mpsc;

#[test]
fn format_nb_messages_ok() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let console = Console {
        max_messages: None,
        print_key: false,
        print_shardid: false,
        print_timestamp: false,
        print_delimiter: false,
        rx_records,
        tx_records,
    };

    assert_eq!(console.format_nb_messages(1), "1 message processed");
    assert_eq!(console.format_nb_messages(2), "2 messages processed");
}
