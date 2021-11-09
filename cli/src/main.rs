use std::path::Path;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use rdkafka::Message;
use buska_core::config::KafkaClusterConfig;
use buska_core::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use buska_core::search::headers::match_header;
use buska_core::search::json::JsonPathSingleMatcher;
use buska_core::search::SearchNotification;
use buska_core::search_topic;


/// Welcome to Buska, a CLI for looking for messages within an Apache Kafka cluster
/// The CLI is composed of 3 parts:
///   - The Kafka configuration:
///     - either a toml file describing cluster options, like SSL config. etc. see ... (TODO: add link for an example config file)
///     - or by passing --bootstrap-servers as in other Kafka CLIs
///   - Specifying the search bounds:
///      - where should we start at? (--from-beginning, --from-epoch-millis=1636308199000, --from-iso_8601=2021-11-07T19:03:55+0100)
///      - when should we stop searching? (--to-current-last, --to-epoch-millis=1636308199000, --to-iso_8601=2021-11-07T19:03:55+0100)
///   - What are we looking for
///     - How do we extract the information from a Kafka record (--header-name=the_header, --value-json-path=$.somefield.someother)
///     - What do we match against? (--exactly-matches=some_value)
///     - The topic we should search in: --topic
///
/// A full valid example could be:
///   buska --bootstrap-servers localhost:9092 --from-beginning --to-current-last --header-name=someheader --exactly-matches=something
///
/// Another one:
///   buska --cluster-config-file=/opt/secrets/cluster.toml --from-iso-8601=2021-11-07T00:00:00+0100 --to-iso-8601=2021-11-08T00:00:00+0100 --value-json-path=$.somefield.nested --exactly-matches=valueWereLookingFor
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


    /// Extract part of the record's value by using a JSON path definition for matching
    #[clap(short, long)]
    value_json_path: Option<String>,

    /// Extract a header by its name for matching
    #[clap(short, long)]
    header_name: Option<String>,

    /// Matches the extracted value from a record against a specific value
    #[clap(short, long)]
    exactly_matches: String,

    /// The name of the topic we should search records in
    #[clap(short, long)]
    topic: String,

}


#[tokio::main]
async fn main() {
    let cli: BuskaCli = BuskaCli::parse();
    let bounds = extract_search_bounds(&cli);
    let cluster_config = extract_cluster_config(&cli);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<SearchNotification>(1024);


    let mut tasks = vec![];
    tasks.push(tokio::task::spawn(async move {
        let mut stop = false;
        while !stop {
            if let Some(received) = receiver.recv().await {
                match received {
                    SearchNotification::Start => println!("Started searching"),
                    SearchNotification::Finish(notif) => {
                        stop = true;
                        println!("Finished searching. Summary:");
                        println!("{}", notif);
                    }
                    SearchNotification::Progress(progress) => {
                        println!("Query made progress:");
                        println!("{}", progress);
                    },
                    SearchNotification::Match(matched) => {
                        println!("Match found! {}",
                            matched.payload_view::<str>()
                                .expect("Could not display matched record as string")
                                .expect("Could not display matched record as string")
                        )
                    }
                }
            }
        }

    }));

    if let Some(header_name) = cli.header_name.as_ref() {
        let mut predicate = match_header(header_name, &cli.exactly_matches);
        tasks.push(tokio::task::spawn(async move {
            search_topic(cluster_config, cli.topic, sender, bounds, &mut predicate).await;
        }));
    } else if let Some(path) = cli.value_json_path {
        tasks.push(tokio::task::spawn(async move {
            let expected_value = serde_json::json!(cli.exactly_matches);
            let mut predicate = JsonPathSingleMatcher::new(&path, expected_value).expect("JSON path specified through --value-json-path may be an invalid");
            search_topic(cluster_config, cli.topic, sender, bounds, &mut predicate).await;
        }));
    } else {
        panic!("No way to extract from a Kafka record specified. Please use --value-json-path=$.something for extracting from the record's value by using a JSON path, or --header-name=something for searching against a specific header")
    }

    futures::future::join_all(tasks).await;
}


fn extract_cluster_config(cli: &BuskaCli) -> KafkaClusterConfig {
    if let Some(bootstrap_servers) = cli.bootstrap_servers.as_ref() {
        KafkaClusterConfig {
            bootstrap_servers: bootstrap_servers.clone(),
            security: None
        }
    } else if let Some(config_file_path) = cli.cluster_config_file.as_ref() {
        let mut conf = config::Config::new();
        conf.merge(config::File::from(Path::new(config_file_path.as_str()))).expect(&*format!("Could not read the specified config file: {:?}", config_file_path));
        conf.try_into::<KafkaClusterConfig>().expect("Could not create Kafka cluster configuration from file")
    } else {
        panic!("Kafka cluster configuration not found, try using either --bootstrap-servers or --cluster-config-file")
    }
}

fn extract_search_bounds(cli: &BuskaCli) -> SearchBounds {
    let start = if cli.from_beginning.is_some() {
        SearchStart::Earliest
    } else if let Some(time) = cli.from_epoch_millis {
        SearchStart::Time(Utc.timestamp_millis(time))
    } else if let Some(repr) = &cli.from_iso_8601 {
        SearchStart::Time(DateTime::parse_from_rfc3339(&*repr).expect(&*format!("Could not parse input date: {}. Is this a valid ISO-8601 (RFC-3339) formatted date?", repr)).with_timezone(&Utc))
    } else {
        panic!("Please specify the search start by using: --from-beginning, --from-epoch-millis=1636308199000 or --from-iso_8601=2021-11-07T19:03:55+0100")
    };
    let end = if cli.to_current_last.is_some() {
        SearchEnd::CurrentLast
    } else if let Some(_time) = cli.to_epoch_millis {
        panic!("--to-epoch-millis isn't supported at the moment, please use --to-current-last, sorry about that")
    } else if let Some(_repr) = cli.to_iso_8601.as_ref() {
        panic!("--to-iso-8601 isn't supported at the moment, please use --to-current-last, sorry about that")
    } else {
        panic!("Please specify the search start by using: --to-current-last, --to-epoch-millis=1636308199000 or --to-iso_8601=2021-11-07T19:03:55+0100")
    };
    SearchBounds { start, end }
}
