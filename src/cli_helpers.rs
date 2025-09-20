use crate::sink::config::PayloadEnc;
use anyhow::{anyhow, Result};
use aws_sdk_kinesis::meta::PKG_VERSION;
use chrono::{DateTime, Utc};
use clap::Parser;
use log::info;

pub const SEMAPHORE_DEFAULT_SIZE: usize = 50;

#[derive(Debug, Parser)]
#[command(
    version = "{#RELEASE_VERSION} - Grum Ltd\nReport bugs to https://github.com/grumlimited/kinesis-tailr/issues"
)]
pub struct Opt {
    /// AWS Region
    #[arg(short, long)]
    pub region: Option<String>,

    /// Name of the stream
    #[arg(short, long)]
    pub stream_name: String,

    /// Endpoint URL to use
    #[arg(long)]
    pub endpoint_url: Option<String>,

    /// Start datetime position to tail from. ISO 8601 format.
    #[arg(long)]
    pub from_datetime: Option<String>,

    /// End datetime position to tail up to. ISO 8601 format.
    #[arg(long)]
    pub to_datetime: Option<String>,

    /// Maximum number of messages to retrieve
    #[arg(long)]
    pub max_messages: Option<u32>,

    /// Exit if no messages received after <timeout> seconds.
    #[arg(long)]
    pub timeout: Option<u16>,

    /// Maximum number of aws sdk retries. Increase if you are seeing throttling errors.
    #[arg(long)]
    #[clap(default_value_t = 10)]
    pub max_attempts: u32,

    /// Disable color output
    #[arg(long)]
    pub no_color: bool,

    /// Print a delimiter between each payload
    #[arg(long)]
    pub print_delimiter: bool,

    /// Print the partition key
    #[arg(long)]
    pub print_key: bool,

    /// Print the sequence number
    #[arg(long)]
    pub print_sequence_number: bool,

    /// Print the shard ID.
    #[arg(long)]
    pub print_shard_id: bool,

    /// Print timestamps
    #[arg(long)]
    pub print_timestamp: bool,

    /// Print progress status
    #[arg(long)]
    pub progress: bool,

    /// Shard ID to tail from. Repeat option for each shard ID to filter on
    #[arg(long)]
    pub shard_id: Option<Vec<String>>,

    /// Output file to write to
    #[arg(long, short)]
    pub output_file: Option<String>,

    /// Concurrent number of shards to tail
    #[arg(short, long)]
    pub concurrent: Option<usize>,

    /// Display additional information
    #[arg(short, long)]
    pub verbose: bool,

    /// Base64 encode payloads (eg. for binary data)
    #[arg(long)]
    #[arg(group = "encoding")]
    pub base64: bool,

    /// Forces UTF-8 printable payloads
    #[arg(long)]
    #[arg(group = "encoding")]
    pub utf8: bool,
}

impl Opt {
    /// Returns the payload encoding strategy based on CLI flags.
    pub fn encoding(&self) -> PayloadEnc {
        if self.base64 {
            PayloadEnc::Base64
        } else if self.utf8 {
            PayloadEnc::Utf8
        } else {
            PayloadEnc::Raw
        }
    }

    /// Validates the configuration options.
    pub fn validate(&self) -> Result<()> {
        // Validate output file path if specified
        if let Some(ref output_file) = self.output_file {
            if output_file.trim().is_empty() {
                return Err(anyhow!("Output file path cannot be empty"));
            }
            // Check if parent directory exists for the output file
            if let Some(parent) = std::path::Path::new(output_file).parent() {
                if !parent.exists() {
                    return Err(anyhow!(
                        "Parent directory does not exist for output file: {}",
                        output_file
                    ));
                }
            }
        }

        // Validate concurrent value
        if let Some(concurrent) = self.concurrent {
            if concurrent == 0 {
                return Err(anyhow!("Concurrent value must be greater than 0"));
            }
        }

        Ok(())
    }
}

pub(crate) fn selected_shards(
    shards: Vec<String>,
    stream_name: &str,
    shard_ids: &Option<Vec<String>>,
) -> Result<Vec<String>> {
    let filtered = match shard_ids {
        Some(shard_ids) => shards
            .into_iter()
            .filter(|s| shard_ids.contains(s))
            .collect::<Vec<_>>(),
        None => shards,
    };

    if filtered.is_empty() {
        Err(anyhow!(
            "No shards found for stream {} (filtered: {})",
            stream_name,
            shard_ids.is_some()
        ))
    } else {
        Ok(filtered)
    }
}

pub(crate) fn set_log_level() {
    env_logger::init_from_env(
        env_logger::Env::default().default_filter_or("WARN,kinesis_tailr=INFO"),
    );
}

pub(crate) fn print_runtime(opt: &Opt, selected_shards: &[String]) {
    if opt.verbose {
        info!("Kinesis client version: {}", PKG_VERSION);
        info!(
            "Region:                 {}",
            opt.region.as_ref().unwrap_or(&"us-east-1".to_owned())
        );
        info!("Stream name:            {}", &opt.stream_name);
        opt.from_datetime.iter().for_each(|f| {
            info!("From:                   {}", f);
        });

        let is_filtered = if opt.shard_id.is_some() {
            " (filtered)"
        } else {
            ""
        };
        info!(
            "Shards:                 {}{}",
            selected_shards.len(),
            is_filtered
        );
    }
}

pub fn validate_time_boundaries(
    from_datetime: &Option<DateTime<Utc>>,
    to_datetime: &Option<DateTime<Utc>>,
) -> Result<()> {
    from_datetime
        .zip(to_datetime.as_ref())
        .iter()
        .try_for_each(|(from, to)| {
            if std::cmp::max(from, to) == from {
                Err(anyhow!("{} must be before {}", from, to))
            } else {
                Ok(())
            }
        })
}

pub fn parse_date(datetime: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    datetime
        .map(|dt| {
            chrono::DateTime::parse_from_rfc3339(dt)
                .map_err(|_| anyhow!("Could not parse date [{}]", dt))
                .map(|d| d.with_timezone(&Utc))
        })
        .map_or(Ok(None), |r| r.map(Some))
}

pub fn reset_signal_pipe_handler() -> Result<()> {
    // https://github.com/rust-lang/rust/issues/46016
    // Long story short: handle SIGPIPE (ie. broken pipe) on Unix systems gracefully.
    #[cfg(target_family = "unix")]
    {
        use nix::sys::signal;

        unsafe {
            signal::signal(signal::Signal::SIGPIPE, signal::SigHandler::SigDfl)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn parse_date_test_ok() {
        let date = "2023-05-04T20:57:12Z";
        let result = parse_date(Some(date)).unwrap().unwrap();
        let result = result.to_rfc3339();
        assert_eq!(result, "2023-05-04T20:57:12+00:00");

        let date = "2023-05-04T20:57:12+02:00";
        let result = parse_date(Some(date)).unwrap().unwrap();
        let result = result.to_rfc3339();
        assert_eq!(result, "2023-05-04T18:57:12+00:00");
    }

    #[test]
    #[should_panic]
    fn parse_date_test_fail() {
        let invalid_date = "xxx";
        let _ = parse_date(Some(invalid_date)).unwrap();
    }

    #[test]
    fn selected_shards_ok() {
        let shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(shards.clone(), "stream", &None).unwrap(),
            vec!["a", "b", "c"]
        );

        assert_eq!(
            selected_shards(shards.clone(), "stream", &Some(vec!["a".to_string()])).unwrap(),
            vec!["a"]
        );

        assert_eq!(
            selected_shards(shards.clone(), "stream", &Some(vec!["b".to_string()])).unwrap(),
            vec!["b"]
        );

        assert_eq!(
            selected_shards(shards, "stream", &Some(vec!["c".to_string()])).unwrap(),
            vec!["c"]
        );
    }

    #[test]
    #[should_panic]
    fn selected_shards_panic() {
        let shards = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        assert_eq!(
            selected_shards(shards, "stream", &Some(vec!["d".to_string()])).unwrap(),
            vec![] as Vec<&str>
        );
    }

    #[test]
    fn validate_time_boundaries_ok() {
        let from = Some(Utc::now());
        let to = Some(from.unwrap() + Duration::days(1));
        assert!(validate_time_boundaries(&from, &to).is_ok());
    }

    #[test]
    fn validate_time_boundaries_from_is_after_to() {
        let from = Some(Utc::now());
        let to = Some(from.unwrap() - Duration::days(1));
        assert!(validate_time_boundaries(&from, &to).is_err());
    }

    #[test]
    fn validate_time_boundaries_nones() {
        let from = Some(Utc::now());
        let to = Some(from.unwrap() + Duration::days(1));

        assert!(validate_time_boundaries(&from, &None).is_ok());
        assert!(validate_time_boundaries(&None, &to).is_ok());
        assert!(validate_time_boundaries(&None, &None).is_ok());
    }

    #[test]
    fn opt_validate_empty_output_file() {
        let mut opt = Opt {
            region: None,
            stream_name: "test-stream".to_string(),
            endpoint_url: None,
            from_datetime: None,
            to_datetime: None,
            max_messages: None,
            timeout: None,
            max_attempts: 10,
            no_color: false,
            print_delimiter: false,
            print_key: false,
            print_sequence_number: false,
            print_shard_id: false,
            print_timestamp: false,
            progress: false,
            shard_id: None,
            output_file: Some("".to_string()),
            concurrent: None,
            verbose: false,
            base64: false,
            utf8: false,
        };

        assert!(opt.validate().is_err());

        opt.output_file = Some("   ".to_string());
        assert!(opt.validate().is_err());
    }

    #[test]
    fn opt_validate_zero_concurrent() {
        let opt = Opt {
            region: None,
            stream_name: "test-stream".to_string(),
            endpoint_url: None,
            from_datetime: None,
            to_datetime: None,
            max_messages: None,
            timeout: None,
            max_attempts: 10,
            no_color: false,
            print_delimiter: false,
            print_key: false,
            print_sequence_number: false,
            print_shard_id: false,
            print_timestamp: false,
            progress: false,
            shard_id: None,
            output_file: None,
            concurrent: Some(0),
            verbose: false,
            base64: false,
            utf8: false,
        };

        assert!(opt.validate().is_err());
    }

    #[test]
    fn opt_validate_success() {
        let opt = Opt {
            region: None,
            stream_name: "test-stream".to_string(),
            endpoint_url: None,
            from_datetime: None,
            to_datetime: None,
            max_messages: None,
            timeout: None,
            max_attempts: 10,
            no_color: false,
            print_delimiter: false,
            print_key: false,
            print_sequence_number: false,
            print_shard_id: false,
            print_timestamp: false,
            progress: false,
            shard_id: None,
            output_file: None,
            concurrent: Some(5),
            verbose: false,
            base64: false,
            utf8: false,
        };

        assert!(opt.validate().is_ok());
    }
}
