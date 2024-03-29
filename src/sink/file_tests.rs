use crate::kinesis::models::ShardProcessorADT::{Progress, Termination};
use crate::kinesis::models::{ProcessError, RecordResult, ShardProcessorADT};
use crate::sink::file::FileSink;
use crate::sink::Sink;
use aws_sdk_kinesis::primitives::DateTime;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn file_sink_ok() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, ProcessError>>(1);

    let tx_records_clone = tx_records.clone();

    let file = PathBuf::from("test.txt");

    let mut sink = FileSink {
        config: Default::default(),
        file: file.clone(),
        shard_count: 1,
    };

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

    sink.run(tx_records, rx_records).await.unwrap();

    let path = file.clone();
    let path = path.as_path();
    let string = fs::read_to_string(path).unwrap();

    assert_eq!(string, "payload\n");
}
