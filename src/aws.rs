pub mod client {

    use anyhow::Result;
    use async_trait::async_trait;
    use aws_config::meta::region::RegionProviderChain;
    use aws_sdk_kinesis::config::Region;
    use aws_sdk_kinesis::operation::get_records::GetRecordsOutput;
    use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorOutput;
    use aws_sdk_kinesis::operation::list_shards::ListShardsOutput;
    use aws_sdk_kinesis::primitives::DateTime;
    use aws_sdk_kinesis::types::ShardIteratorType;
    use aws_sdk_kinesis::Client;
    use chrono::Utc;

    #[derive(Clone, Debug)]
    pub struct AwsKinesisClient {
        client: Client,
    }

    #[async_trait]
    pub trait KinesisClient: Sync + Send + Clone {
        async fn list_shards(&self, stream: &str) -> Result<ListShardsOutput>;

        async fn get_records(&self, shard_iterator: &str) -> Result<GetRecordsOutput>;

        async fn get_shard_iterator_at_timestamp(
            &self,
            stream: &str,
            shard_id: &str,
            timestamp: &chrono::DateTime<Utc>,
        ) -> Result<GetShardIteratorOutput>;

        async fn get_shard_iterator_at_sequence(
            &self,
            stream: &str,
            shard_id: &str,
            starting_sequence_number: &str,
        ) -> Result<GetShardIteratorOutput>;

        async fn get_shard_iterator_latest(
            &self,
            stream: &str,
            shard_id: &str,
        ) -> Result<GetShardIteratorOutput>;

        fn get_region(&self) -> Option<&Region>;

        fn to_aws_datetime(timestamp: &chrono::DateTime<Utc>) -> DateTime {
            DateTime::from_millis(timestamp.timestamp_millis())
        }
    }

    #[async_trait]
    impl KinesisClient for AwsKinesisClient {
        async fn list_shards(&self, stream: &str) -> Result<ListShardsOutput> {
            self.client
                .list_shards()
                .stream_name(stream)
                .send()
                .await
                .map_err(|e| e.into())
        }

        async fn get_records(&self, shard_iterator: &str) -> Result<GetRecordsOutput> {
            self.client
                .get_records()
                .shard_iterator(shard_iterator)
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .map_err(|e| e.into())
        }

        async fn get_shard_iterator_at_timestamp(
            &self,
            stream: &str,
            shard_id: &str,
            timestamp: &chrono::DateTime<Utc>,
        ) -> Result<GetShardIteratorOutput> {
            self.client
                .get_shard_iterator()
                .shard_iterator_type(ShardIteratorType::AtTimestamp)
                .timestamp(Self::to_aws_datetime(timestamp))
                .stream_name(stream)
                .shard_id(shard_id)
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .map_err(|e| e.into())
        }

        async fn get_shard_iterator_at_sequence(
            &self,
            stream: &str,
            shard_id: &str,
            starting_sequence_number: &str,
        ) -> Result<GetShardIteratorOutput> {
            self.client
                .get_shard_iterator()
                .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
                .starting_sequence_number(starting_sequence_number)
                .stream_name(stream)
                .shard_id(shard_id)
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .map_err(|e| e.into())
        }

        async fn get_shard_iterator_latest(
            &self,
            stream: &str,
            shard_id: &str,
        ) -> Result<GetShardIteratorOutput> {
            self.client
                .get_shard_iterator()
                .shard_iterator_type(ShardIteratorType::Latest)
                .stream_name(stream)
                .shard_id(shard_id)
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .map_err(|e| e.into())
        }

        fn get_region(&self) -> Option<&Region> {
            self.client.conf().region()
        }
    }

    pub async fn create_client(
        region: Option<String>,
        endpoint_url: Option<String>,
    ) -> AwsKinesisClient {
        let region_provider = RegionProviderChain::first_try(region.map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));

        let shared_config = {
            let inner = aws_config::from_env().region(region_provider);

            let inner = match endpoint_url {
                Some(endpoint_url) => inner.endpoint_url(endpoint_url.as_str()),
                None => inner,
            };

            inner
        }
        .load()
        .await;

        let client = Client::new(&shared_config);

        AwsKinesisClient { client }
    }
}
