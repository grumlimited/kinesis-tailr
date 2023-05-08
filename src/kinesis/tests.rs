use crate::kinesis::models::{
    PanicError, ShardProcessor, ShardProcessorADT, ShardProcessorConfig, ShardProcessorLatest,
};
use crate::kinesis::IteratorProvider;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_kinesis::config::Region;
use aws_sdk_kinesis::Client;
use tokio::sync::mpsc;

#[tokio::test]
async fn xxx() {
    let (tx_records, rx_records) = mpsc::channel::<Result<ShardProcessorADT, PanicError>>(1);

    let region = Region::new("us-east-1");
    let region_provider = RegionProviderChain::first_try(region);

    let shared_config = {
        let inner = aws_config::from_env().region(region_provider);
        inner
    }
    .load()
    .await;

    let client = Client::new(&shared_config);

    let processor = ShardProcessorLatest {
        config: ShardProcessorConfig {
            client,
            stream: "test".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            tx_records,
        },
    };

    // processor.get_iterator().await.unwrap();
}
