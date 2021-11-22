use std::path::Path;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use buska_core::config::KafkaClusterConfig;
use buska_core::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use buska_core::search::extractors::header::HeaderStringExtractor;
use buska_core::search::matchers::{OneOf, PerfectMatch, RegexMatch};
use buska_core::search::notifications::SearchNotification;
use buska_core::search::{Predicate, SearchDefinition};
use buska_core::search::extractors::json::json_single_extract;
use buska_core::search::extractors::key::KeyExtractor;
use buska_core::search_topic;
use crate::BuskaCli;

type SizedPredicate<T> = dyn Predicate<T> + Send;


pub(crate) fn perform_search(
    cli: BuskaCli,
    bounds: SearchBounds,
    display_every: ChronoDuration,
    sender: Sender<SearchNotification>
) -> tokio::task::JoinHandle<()> {
    let cluster_config = extract_cluster_config(&cli);
    let matcher: Box<SizedPredicate<String>> = match (
        cli.matches_exactly.clone(),
        cli.matches_one_of.clone(),
        cli.matches_regex.clone()
    ) {
        (Some(perfect_match), _, _) =>
            Box::new(PerfectMatch::new(perfect_match)),
        (_, Some(one_of), _) =>
            Box::new(
                OneOf::new(one_of.split(',')
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>())
            ),
        (_, _, Some(regexp)) =>
            Box::new(RegexMatch::new(&regexp).expect("Could not create regular expression")),
        _ => panic!("No matcher specified. Expecting: --matches-exactly, or --matches-one-of")
    };
    if let Some(header_name) = cli.extract_header.as_ref() {
        let extractor = HeaderStringExtractor { name: header_name.clone() };
        tokio::task::spawn(async move {
            search_topic(
                cluster_config,
                cli.topic,
                sender,
                bounds,
                &mut SearchDefinition::new(extractor, matcher),
                display_every
            ).await
        })
    } else if cli.extract_key.is_some() {
        tokio::task::spawn(async move {
            let extractor = KeyExtractor::default();
            search_topic(
                cluster_config,
                cli.topic,
                sender,
                bounds,
                &mut SearchDefinition::new(extractor, matcher),
                display_every
            ).await;
        })
    } else if let Some(path) = cli.extract_value_json_path {
        tokio::task::spawn(async move {
            let extractor = json_single_extract(&path).expect("JSON path specified through --value-json-path may be an invalid");
            let matcher: Box<SizedPredicate<Value>> = match (
                cli.matches_exactly,
                cli.matches_one_of,
                cli.matches_regex
            ) {
                (Some(perfect_match), _, _) =>
                    Box::new(PerfectMatch::new(serde_json::json!(perfect_match))),
                (_, Some(one_of), _) => {
                    let jsons: Vec<Value> = one_of.split(",").into_iter().map(|s| serde_json::json!(s.to_string())).collect();
                    Box::new(OneOf::new(jsons))
                },
                (_, _, Some(regexp)) =>
                    Box::new(RegexMatch::new(&regexp).expect("Could not create regular expression")),
                _ => panic!("No matcher specified. Expecting: --matches-exactly, or --matches-one-of")
            };
            search_topic(
                cluster_config,
                cli.topic,
                sender,
                bounds,
                &mut SearchDefinition::new(extractor, matcher),
                display_every
            ).await;
        })
    } else {
        panic!("No way to extract from a Kafka record specified. Please use --value-json-path=$.something for extracting from the record's value by using a JSON path, or --header-name=something for searching against a specific header")
    }
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

pub (crate) fn extract_search_bounds(cli: &BuskaCli) -> SearchBounds {
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
