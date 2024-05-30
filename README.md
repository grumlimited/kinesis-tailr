# kinesis-tailr

![Continuous integration](https://github.com/grumlimited/kinesis-tailr/workflows/CI/badge.svg?branch=main)

A simple tool to tail a Kinesis stream. Built with Rust.

## Installation

### Requirements

* `rustc`
* `make`

### From source

```bash
cargo make install-local
```

Installs a single binary to `/usr/local/bin/kinesis-tailr`. Alternatively, use

```bash
cargo install --git https://github.com/grumlimited/kinesis-tailr
```

### Releases

The [release page](https://github.com/grumlimited/kinesis-tailr/releases) provides packages for Debian and CentOS and
Arch Linux.

## Usage

    ❯ kinesis-tailr -help

    Usage: kinesis-tailr [OPTIONS] --stream-name <STREAM_NAME>
    
    Options:
        -r, --region <REGION>                AWS Region
        -s, --stream-name <STREAM_NAME>      Name of the stream
        --endpoint-url <ENDPOINT_URL>        Endpoint URL to use
        --from-datetime <FROM_DATETIME>      Start datetime position to tail from. ISO 8601 format
        --to-datetime <TO_DATETIME>          End datetime position to tail up to. ISO 8601 format
        --max-messages <MAX_MESSAGES>        Maximum number of messages to retrieve
        --timeout <TIMEOUT>                  Exit if no messages received after <timeout> seconds
        --max-attempts <MAX_ATTEMPTS>        Maximum number of aws sdk retries. Increase if you are seeing throttling errors [default: 10]
        --no-color                           Disable color output
        --print-delimiter                    Print a delimiter between each payload
        --print-key                          Print the partition key
        --print-sequence-number              Print the sequence number
        --print-shard-id                     Print the shard ID
        --print-timestamp                    Print timestamps
        --progress                           Print progress status
        --shard-id <SHARD_ID>                Shard ID to tail from. Repeat option for each shard ID to filter on
        -o, --output-file <OUTPUT_FILE>      Output file to write to
        -c, --concurrent <CONCURRENT>        Concurrent number of shards to tail
        -v, --verbose                        Display additional information
            --base64                         Base64 encode payloads (eg. for binary data)
            --utf8                           Forces UTF-8 printable payloads
        -h, --help                           Print help
        -V, --version                        Print version

### Example

```bash
kinesis-tailr \
    --region eu-west-1 \
    --stream-name=ddb-stream-dev \
    --print-timestamp \
    --from-datetime '2023-05-04T20:57:12+00:00' \
    --max-messages 2
```

### UTF-8

`kinesis-tailr` expects payloads to be UTF-8 encoded. If a payload is not UTF-8 encoded, it will be base64 encoded and
printed as such.

It might be useful to print the raw payload instead though. This can be achieved with the `--no-base64` flag.

Properly UTF-8 encoded payloads will be printed as such and never base64 encoded.

### Logging

General logging level for debugging can be turned on with:

```bash
export RUST_LOG="INFO"

kinesis-tailr --stream-name mystream
```

    [2023-05-10T21:45:14Z INFO  aws_config::meta::region] load_region; provider=None
    [2023-05-10T21:45:14Z INFO  aws_config::meta::region] load_region; provider=EnvironmentVariableRegionProvider { env: Env(Real) }
    [2023-05-10T21:45:14Z INFO  tracing::span] lazy_load_credentials;
    [2023-05-10T21:45:14Z INFO  aws_credential_types::cache::lazy_caching] credentials cache miss occurred; added new AWS credentials (took 24.934µs)
    [...]

Specific logging for `kinesis-tailr` can be turned on with:

```bash
export RUST_LOG="WARN,kinesis_tailr=INFO"

kinesis-tailr --stream-name mystream --from-datetime '2023-05-17T19:00:00Z' -o output.json
```

    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001119: 00:31:23
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001144: 00:31:27
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001085: 00:31:31
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001118: 00:32:33
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001156: 00:40:21
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] shardId-000000001122: 00:41:46
    [2023-05-17T20:37:35Z INFO  kinesis_tailr::kinesis::ticker] 10 shards behind
    [...]

It is recommended to use `-o output.json` to write the output to a file, as the output can be quite verbose. This can
then be inspected with `jq` or similar.

Moreover, it also frees the console output for informational messages. Use

```bash
export RUST_LOG="WARN,kinesis_tailr=DEBUG"
```

for more debugging information.
