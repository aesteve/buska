pub mod config;
pub mod search;
pub mod utils;

use crate::search::Predicate;
use crate::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use crate::search::notifications::{FinishNotification, ProgressNotification, SearchNotification};
use crate::utils::MeanExt;

use chrono::{Utc, Duration as ChronoDuration};
use config::KafkaClusterConfig;
use rdkafka::{ClientConfig, TopicPartitionList, Offset as RdOffset, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::topic_partition_list::Offset::Offset;
use rdkafka::util::Timeout;

use std::collections::HashMap;
use std::time::Duration;
use rdkafka::message::OwnedMessage;
use tokio::sync::mpsc::Sender;

/// The main entry point of this library
/// Searches a whole topic on a Kafka cluster (describe by its KafkaClusterConfig)
///   - within some search bounds (start / end)
///   - matching some kind of record's predicate
/// And sends notifications (including the search results) over a mpsc channel
pub async fn search_topic<T: Predicate<OwnedMessage> + Send>(
    conf: KafkaClusterConfig,
    topic: String,
    sender: Sender<SearchNotification>,
    bounds: SearchBounds,
    predicate: &mut T
) {
    let search_start = Utc::now();
    let consumer = create_client(&conf);
    log::debug!("Kafka consumer created in {} millis", (Utc::now() - search_start).num_milliseconds());
    let loop_infinitely = bounds.end == SearchEnd::Unbounded;
    let mut offset_range: HashMap<i32, (i64, i64)> = seek_partitions(conf.clone(), &consumer, &topic, &bounds).await
        .expect("Could not seek partitions to desired offset")
        .into_iter()
        .filter(|(_, (_, max))| *max > 0)
        .collect();
    log::debug!("Offsets ranges to search = {:?}", offset_range);
    let mut last_displayed = 0.0;
    let mut progress_per_partition = HashMap::with_capacity(offset_range.len());
    let mut matches = 0;
    let mut read = 0;
    let display_step = 0.1;
    sender.send(SearchNotification::Start).await.expect("Could not send start notification. Crashing");
    while loop_infinitely || !offset_range.is_empty() {
        match &consumer.recv().await {
            Err(e) =>
                log::error!("Kafka error while reading topic: {}", e),
            Ok(m) => {
                read += 1;
                let msg = m.detach();
                if predicate.matches(&msg) {
                    matches += 1;
                    let to_send = SearchNotification::Match(msg.clone());
                    if let Err(e) = sender.send(to_send).await {
                        log::error!("Could not forward matching msg: {:?}. {}", msg, e)
                    }
                }
                if let Some((min, max)) = offset_range.get(&m.partition()) {
                    if m.offset() >= *max - 1 {
                        log::info!("Reached end for partition {}", m.partition());
                        offset_range.remove(&m.partition());
                    } else {
                        let curr_progress = (m.offset() - *min) as f64 / (*max - *min) as f64;
                        progress_per_partition.insert(m.partition(), curr_progress);
                        let overall_progress: f64 = progress_per_partition.values().into_iter().mean();
                        if curr_progress - last_displayed > display_step {
                            last_displayed = curr_progress;
                            log::info!("Partition {} progress: {:.2}%", m.partition(), curr_progress * 100.0);
                            let elapsed = Utc::now() - search_start;
                            let eta = search_start + ChronoDuration::milliseconds((elapsed.num_milliseconds() as f64 / overall_progress) as i64);
                            let to_send = ProgressNotification {
                                topic: topic.to_string(),
                                overall_progress,
                                per_partition_progress: Default::default(),
                                matches,
                                read,
                                elapsed: Utc::now() - search_start,
                                eta
                            };
                            if let Err(e) = sender.send(SearchNotification::Progress(to_send)).await {
                                log::error!("Could not send progress notification: {:?}", e)
                            }
                        }
                    }
                }
            }
        }
    }
    log::info!("Reached end of consumption. Sending end marker");
    let elapsed = Utc::now() - search_start;
    let notification = FinishNotification {
        topic,
        matches,
        read,
        elapsed,
        read_rate_msg_sec: (read as f64 / elapsed.num_milliseconds() as f64) * 1000.0 // using num_milliseconds *1000 avoids dividing by 0 if < 0.5seconds
    };
    sender.send(SearchNotification::Finish(notification))
        .await
        .expect("Could not notify end of topic search, crashing");
}

/// Seek partitions for the given consumer / topic to the given bounds (start bounds)
/// Returns a map of <partition, (min_offset, max_offset)> while:
///     assigning the consumer to every partition,
///     seeking the consumer to min_offset for every partition
async fn seek_partitions(config: KafkaClusterConfig, consumer: &StreamConsumer, topic: &str, bounds: &SearchBounds) -> KafkaResult<HashMap<i32, (i64, i64)>> {
    log::info!("Seeking partitions");
    let loop_infinitely = bounds.end == SearchEnd::Unbounded;
    // if let SearchStart::Time(beginning) = bounds.start {
    //     seek_all_partitions_to_time(config, consumer, topic, beginning, !loop_infinitely).await
    // } else {
    //     seek_all_partitions_to_start(config, consumer, topic, !loop_infinitely).await
    // }
    let req_timeout = Timeout::After(Duration::from_secs(2));
    let metadata = consumer.fetch_metadata(Some(topic), req_timeout)?;
    let partitions: Vec<i32> = metadata.topics()[0].partitions().iter().map(|part| part.id()).collect();
    let nb_partitions = partitions.len();
    let mut topic_partition_list = TopicPartitionList::with_capacity(nb_partitions);
    for partition in partitions {
        let mut part = topic_partition_list.add_partition(topic, partition);
        if let SearchStart::Time(beginning) = bounds.start {
            part.set_offset(Offset(beginning.timestamp_millis()))?;
        }
    }
    // 1. Assign partitions to consumer
    consumer.assign(&topic_partition_list)?;

    // 2. Fetch watermarks (min/max offset for partition)
    let mut tasks = vec![];
    let before_loop = Utc::now();
    let partition_map: Vec<(String, i32)> = topic_partition_list.to_topic_map().iter().map(|((topic, partition), _)| (topic.to_string(), *partition)).collect();
    for (topic, partition) in partition_map.clone() {
        let conf = config.clone();
        tasks.push(tokio::task::spawn(async move {
            let consumer = create_client(&conf);
            let (_, max_offset) = if !loop_infinitely {
                log::info!("Fetching watermarks for partition");
                consumer.fetch_watermarks(&topic, partition, req_timeout).expect("Could not fetch watermarks for partition") // FIXME: fail the task?
            } else {
                (-1, -1)
            };
            (partition, max_offset)
        }));
    }


    let mut partitions_min_max = HashMap::<i32, (i64, i64)>::with_capacity(nb_partitions);
    let start_offsets: HashMap<i32, RdOffset> =
        if let SearchStart::Time(_) = bounds.start {
            let offsets = consumer.offsets_for_times(topic_partition_list, req_timeout)?;
            offsets.to_topic_map().into_iter().map(|((_, partition), offset)| (partition, offset)).collect()
        } else {
            partition_map.clone().iter().map(|(_, partition_id)| (*partition_id, RdOffset::Beginning)).collect()
        };
    for (partition, max) in (futures::future::join_all(tasks).await).into_iter().flatten() {
        let min = start_offsets.get(&partition).expect("Could not find offset for partition");
        partitions_min_max.insert(partition, (min.to_raw().unwrap(), max));
    }

    // 3. Seek every partition to min
    for (topic, partition) in &partition_map {
        let offset_to_seek = start_offsets[partition];
        let mut nb_retries = 0;
        let mut seeked = false;
        let max_retries = 5;
        let mut error = KafkaError::Seek("Could not seek partition".to_string());
        while !seeked && nb_retries < max_retries {
            if let Err(err) = consumer.seek(topic, *partition, offset_to_seek, req_timeout) {
                nb_retries += 1;
                log::warn!("Could not seek partition to desired offset, retrying");
                tokio::time::sleep(Duration::from_millis(100)).await; // Subscription must be effective before seeking
                error = err;
            } else {
                seeked = true
            }
        }
        if !seeked {
            return KafkaResult::Err(error);
        }
    }
    println!("All partitions watermarks fetched + synced in {}", (Utc::now() - before_loop).num_milliseconds());

    Ok(partitions_min_max)
}

fn create_client(conf: &KafkaClusterConfig) -> StreamConsumer {
    let mut builder = ClientConfig::new();
    builder
        .set("bootstrap.servers", conf.bootstrap_servers.clone())
        .set_log_level(RDKafkaLogLevel::Debug)
        .set("group.id", "buska-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    if let Some(security_conf) = &conf.security {
        builder
            .set("security.protocol", "SSL")
            .set("ssl.key.location", security_conf.service_key_location.clone())
            .set("ssl.certificate.location", security_conf.service_cert_location.clone())
            .set("ssl.ca.location", security_conf.ca_pem_location.clone());
    }
    builder
        .create()
        .unwrap()
}


#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use serde::{Serialize, Deserialize};
    use serde_json::to_string;
    use tokio::sync::mpsc::Receiver;

    use crate::config::KafkaClusterConfig;
    use crate::search::notifications::SearchNotification;

    /// The root module contains test utilities, tests themselves are placed in every submodule
    /// Most tests rely on having a Kafka cluster running
    /// This can be achieved by running `docker compose up` from the project's root
    /// As an alternative, one could use `docker run -p 9092:9092 vectorized/redpanda:latest`

    /// The port the test cluster is running on, to avoid repetition
    /// README: it could be really cool
    pub(crate) const KAFKA_PORT: u16 = 9092;

    /// Returns the test cluster's bootstrap servers address configuration
    /// (i.e. localhost, KAFKA_PORT)
    pub(crate) fn test_bootstrap_servers() -> String {
        format!("localhost:{}", KAFKA_PORT)
    }


    /// Returns a KafkaClusterConfig matching the test environment
    /// (i.e. localhost, KAFKA_PORT, and no SSL config)
    pub(crate) fn test_cluster_config() -> KafkaClusterConfig {
        KafkaClusterConfig {
            bootstrap_servers: test_bootstrap_servers(),
            security: None
        }
    }

    /// Returns a Kafka (librdkafka-rust) client configuration matching the test environment
    /// Can be used to create an AdminClient to create, cleanup test topics for instance
    pub(crate) fn test_client_config() -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", test_bootstrap_servers());
        config.set("message.timeout.ms", "5000");
        config
    }


    /// Returns a Kafka (librdkafka-rust) producer configured to match the test environment
    /// Useful for creating mock data
    pub(crate) fn test_producer() -> FutureProducer {
        test_client_config()
            .create::<FutureProducer>()
            .expect("Failed to create Kafka producer")
    }

    /// A record we will be using in some tests for writing JSON data in some topics
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub(crate) struct TestRecord {
        pub(crate) key: String,
        pub(crate) nested: NestedTestRecord
    }

    /// In order to allow writing & testing complex JSON matchers, the TestRecord contains nested JSON
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub(crate) struct NestedTestRecord {
        pub(crate) int: u32,
        pub(crate) ints: Vec<u32>,
        pub(crate) string: String,
    }


    /// Utility method to write JSON TestRecords in a topic
    pub(crate) async fn produce_json_records(topic: &str, recs: &[TestRecord]) {
        let producer = test_producer();
        for rec in recs {
            producer
                .send(
                    FutureRecord::to(topic)
                        .key(&rec.key)
                        .payload(&to_string(rec).expect("Could not serialize test record as JSON"))
                    ,
                    Duration::from_millis(100)
                ).await
                .expect("Could not produce test record");
        }
    }

    /// Given a topic name, sets a topic up in the test cluster
    ///  - Cleans the topic if it exists
    ///  - Creates the test topic with a replication factor of 1, and 12 partitions
    pub(crate) async fn prepare(topic: &str) {
        clean(topic).await;
        test_client_config()
            .create::<AdminClient<_>>()
            .expect("Could not create admin client")
            .create_topics(vec![&NewTopic::new(topic, 12, TopicReplication::Fixed(1))], &AdminOptions::default())
            .await
            .expect("Could not prepare test topics");
    }

    /// Given its name, deletes a topic from the test Kafka cluster
    pub(crate) async fn clean(topic: &str) {
        test_client_config()
            .create::<AdminClient<_>>()
            .expect("Could not create admin client")
            .delete_topics(&[topic], &AdminOptions::default())
            .await
            .expect("Could not cleanup test topics");
    }

    /// Given a receiver, collects every received notification until the end marker has been received
    /// Then returns the list of every received notification
    pub(crate) async fn collect_search_notifications(receiver: &mut Receiver<SearchNotification>) -> Vec<SearchNotification> {
        let mut notifications = Vec::new();
        let mut stop = false;
        while !stop {
            if let Some(received) = receiver.recv().await {
                notifications.push(received.clone());
                if let SearchNotification::Finish(_) = received {
                    stop = true;
                }
            }
        }
        notifications
    }

}
