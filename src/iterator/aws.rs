pub mod client {
    use aws_config::meta::region::RegionProviderChain;
    use aws_sdk_kinesis::config::Region;
    use aws_sdk_kinesis::Client;

    pub struct MyClient {
        client: Client,
    }

    async fn create_client(region: Option<String>, endpoint_url: Option<String>) -> MyClient {
        let region_provider = RegionProviderChain::first_try(region.map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));

        let shared_config = {
            let inner = aws_config::from_env().region(region_provider);

            let inner = if endpoint_url.is_some() {
                inner.endpoint_url(endpoint_url.unwrap().as_str())
            } else {
                inner
            };

            inner
        }
        .load()
        .await;

        let client = Client::new(&shared_config);

        MyClient { client }
    }
}
