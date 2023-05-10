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
    -r, --region <REGION>               AWS Region
    -s, --stream-name <STREAM_NAME>     Name of the stream
    --shard-id <SHARD_ID>               ShardID to tail from
    --max-messages <MAX_MESSAGES>       Maximum number of messages to retrieve
    --from-datetime <FROM_DATETIME>     Start datetime position to tail from. ISO 8601 format
    --print-key                         Print the partition key
    --print-shardid                     Print the shard ID
    --print-timestamp                   Print timestamps
    --print-delimiter                   Print a delimiter between each payload
    --endpoint-url <ENDPOINT_URL>       Endpoint URL to use
    -v, --verbose                       Display additional information
    -h, --help                          Print help

### Example

     kinesis-tailr \
        -r eu-west-1 \
        --stream-name=ddb-stream-dev \
        --print-timestamp \
        --from-datetime '2023-05-04T20:57:12+00:00' \
        --max-messages 2
