# kinesis-tailr

A simple tool to tail a Kinesis stream built with Rust.

# Installation

## Requirements

* `rustc`
* `make`

## From source

```bash
make install
```

Installs a single binary to `/usr/bin/kinesis-tailr`.

# Usage

    ❯ kinesis-tailr -help

    Usage: kinesis-tailr [OPTIONS] --stream-name <STREAM_NAME>
    
    Options:
        -r, --region <REGION>                AWS Region
        -s, --stream-name <STREAM_NAME>      Name of the stream
        --endpoint-url <ENDPOINT_URL>        Endpoint URL to use
        --from-datetime <FROM_DATETIME>      Start datetime position to tail from. ISO 8601 format
        --max-messages <MAX_MESSAGES>        Maximum number of messages to retrieve
        --no-color                           Disable color output
        --print-delimiter                    Print a delimiter between each payload
        --print-key                          Print the partition key
        --print-shardid                      Print the shard ID
        --print-timestamp                    Print timestamps
        --shard-id <SHARD_ID>                Shard ID to tail from
        -o, --output-file <OUTPUT_FILE>      Output file to write to
        -c, --concurrent <CONCURRENT>        Concurrent number of shards to tail [default: 10]
        -v, --verbose                        Display additional information
        -h, --help                           Print help

### Example

     kinesis-tailr \
        --region eu-west-1 \
        --stream-name=ddb-stream-dev \
        --print-timestamp \
        --from-datetime '2023-05-04T20:57:12+00:00' \
        --max-messages 2

### Logging

General logging level for debugging can be turned on with:

    export RUST_LOG="INFO"

    kinesis-tailr --stream-name mystream

    [2023-05-10T21:45:14Z INFO  aws_config::meta::region] load_region; provider=None
    [2023-05-10T21:45:14Z INFO  aws_config::meta::region] load_region; provider=EnvironmentVariableRegionProvider { env: Env(Real) }
    [2023-05-10T21:45:14Z INFO  tracing::span] lazy_load_credentials;
    [2023-05-10T21:45:14Z INFO  aws_credential_types::cache::lazy_caching] credentials cache miss occurred; added new AWS credentials (took 24.934µs)
    [...]

Specific logging for `kinesis-tailr` can be turned on with:

    export RUST_LOG="WARN,kinesis_tailr=DEBUG"

    kinesis-tailr --stream-name mystream

    [2023-05-15T22:22:28Z DEBUG kinesis_tailr::kinesis] Received 191 records from shardId-000000000048 (02:16:04 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 87 records from shardId-000000000041 (02:18:28 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 222 records from shardId-000000000037 (02:16:48 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 52 records from shardId-000000000040 (02:18:36 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 82 records from shardId-000000000045 (02:20:30 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 100 records from shardId-000000000042 (02:16:57 behind)
    [2023-05-15T22:22:29Z DEBUG kinesis_tailr::kinesis] Received 144 records from shardId-000000000047 (02:18:54 behind)
    [...]
