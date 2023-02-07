mod extract_from_cmd;
mod output;

use buska_core::search::bounds::SearchEnd;
use buska_core::search::notifications::SearchNotification;
use chrono::Duration as ChronoDuration;
use clap::Parser;

use crate::extract_from_cmd::{extract_search_bounds, perform_search};
use crate::output::{cli_notifications_loop, Out};

/// Welcome to Buska, a CLI for looking for messages within an Apache Kafka cluster
/// The CLI is composed of 3 parts:
///   - The Kafka configuration:
///     - either a toml file describing cluster options, like SSL config. etc. see ... (TODO: add link for an example config file)
///     - or by passing --bootstrap-servers as in other Kafka CLIs
///   - Specifying the search bounds:
///      - where should we start at? (--from-beginning, --from-epoch-millis=1636308199000, --from-iso_8601=2021-11-07T19:03:55+0100)
///      - when should we stop searching? (--to-current-last, --to-epoch-millis=1636308199000, --to-iso_8601=2021-11-07T19:03:55+0100)
///   - What are we looking for
///     - How do we extract the information from a Kafka record (--extract-header=the_header, --extract-value-json-path=$.somefield.someother)
///     - What do we match against? (--matches-exactly=some_value  --matches-one-of=foo,bar,baz  --matches-regex=[0-9]+)
///     - The topic we should search in: --topic
///
/// A full valid example could be:
///   buska --bootstrap-servers localhost:9092 --from-beginning --to-current-last --extract-header=someheader --matches-exactly=something --out=stdout
///
/// Another one:
///   buska --cluster-config-file=/opt/secrets/cluster.toml --from-iso-8601=2021-11-07T00:00:00+0100 --to-iso-8601=2021-11-08T00:00:00+0100 --extract-value-json-path=$.somefield.nested --matches-exactly=valueWereLookingFor --out=stdout
///
/// Another one:
///   buska --bootstrap-servers localhost:9092 --from-beginning --to-current-last --extract-key=true --matches-regex=prefix_.*  --out=/tmp/matched.txt
///
#[derive(Clone, Parser, Debug)]
#[clap(version = "0.0.1", author = "Arnaud Esteve <arnaud.esteve@gmail.com>")]
struct BuskaCli {
    /// Kafka cluster configuration
    #[clap(short, long)]
    cluster_config_file: Option<String>,
    #[clap(short, long)]
    bootstrap_servers: Option<String>,

    /// Search bounds
    /// Start searching from the beginning of the topic ('earliest')
    #[clap(short, long)]
    from_beginning: Option<bool>,
    /// Start searching from a specific timestamp, in epoch milliseconds
    #[clap(short, long)]
    from_epoch_millis: Option<i64>,
    /// Start searching from a date time, specified in ISO 8601 format
    #[clap(short, long)]
    from_iso_8601: Option<String>,

    /// Stop searching when the current end of topic is reached
    #[clap(short, long)]
    to_current_last: Option<bool>,
    /*
    /// Keep searching for records -- disabled for now
    to_infinite: Option<bool>,
     */
    /// Stop searching when reached a specific timestamp, in epoch milliseconds
    #[clap(short, long)]
    to_epoch_millis: Option<i64>,
    /// Stop searching when reached a specific date time, specified in ISO 9601 format
    #[clap(short, long)]
    to_iso_8601: Option<String>,

    /// Extract a header by its name for matching
    #[clap(short, long)]
    extract_header: Option<String>,
    /// Extract a header by its name for matching
    #[clap(short, long)]
    extract_key: Option<bool>,
    /// Extract a single part of the record's value by using a JSON path definition for matching (if JSON path extracts an array, returns the first one: use value_json_path_array)
    #[clap(short, long)]
    extract_value_json_path: Option<String>,

    /// Matches the extracted value from a record against a specific value
    #[clap(short, long)]
    matches_exactly: Option<String>,

    /// Matches the extracted value (must be a String) from a record against a regular expression
    #[clap(short, long)]
    matches_regex: Option<String>,

    /// Matches one of the values from this comma-separated list of string
    #[clap(short, long)]
    matches_one_of: Option<String>,

    /// The name of the topic we should search records in
    #[clap(short, long)]
    topic: String,

    /// Where to output matching records, can be `stdout` or any file path
    #[clap(short, long)]
    out: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli: BuskaCli = BuskaCli::parse();
    let display_every = ChronoDuration::seconds(1);
    let bounds = extract_search_bounds(&cli);
    let unbounded = bounds.end == SearchEnd::Unbounded;
    let (sender, receiver) = tokio::sync::mpsc::channel::<SearchNotification>(1024);

    let out = if cli.out.to_lowercase() == "stdout" {
        Out::Stdout
    } else {
        Out::File(cli.out.clone())
    };
    let mut tasks = vec![];
    // Launch the loop that will listen to search notifications and print the result to stdout
    tasks.push(tokio::task::spawn(async move {
        cli_notifications_loop(receiver, unbounded, out).await;
    }));
    // Launch the Search process with the appropriate options extracted from user-inputs
    tasks.push(perform_search(cli, bounds, display_every, sender));
    // Wait for both tasks (searching / notifying) to complete
    futures::future::join_all(tasks).await;
}
