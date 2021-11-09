pub mod config;
pub mod search;

pub(crate) mod utils;

use crate::search::{FinishNotification, ProgressNotification, SearchNotification, SearchPredicate};
use crate::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use crate::utils::MeanExt;

use chrono::{DateTime, Utc, Duration as ChronoDuration};
use config::KafkaClusterConfig;
use log::{error, info};
use rdkafka::{ClientConfig, TopicPartitionList, Offset as RdOffset, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::Offset::Offset;
use rdkafka::util::Timeout;

use std::collections::HashMap;
use std::time::Duration;
use rdkafka::message::OwnedMessage;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

pub async fn search_topic<T: SearchPredicate<OwnedMessage> + Send>(
    conf: KafkaClusterConfig,
    topic: String,
    sender: Sender<SearchNotification>,
    bounds: SearchBounds,
    predicate: &mut T
) {
    let start = Utc::now();
    let consumer = create_client(&conf);
    println!("Client created in {} millis", (Utc::now() - start).num_milliseconds());
    let loop_inifinitely = bounds.end != SearchEnd::CurrentLast;
    let mut offset_range: HashMap<i32, (i64, i64)> = seek_partitions(conf.clone(), &consumer, &topic, &bounds).await
        .expect("Could not seek partitions to desired offset")
        .into_iter()
        .filter(|(_, (_, max))| *max > 0)
        .collect();
    println!("Offsets ranges to search = {:?}", offset_range);
    let mut last_displayed = 0.0;
    let mut progress_per_partition = HashMap::with_capacity(offset_range.len());
    let mut matches = 0;
    let mut read = 0;
    let display_step = 0.1;
    sender.send(SearchNotification::Start).await.expect("Could not send start notification. Crashing");
    while loop_inifinitely || !offset_range.is_empty() {
        match &consumer.recv().await {
            Err(e) =>
                error!("Kafka error while reading topic: {}", e),
            Ok(m) => {
                read += 1;
                let msg = m.detach();
                if predicate.matches(&msg) {
                    matches += 1;
                    let to_send = SearchNotification::Match(msg.clone());
                    if let Err(e) = sender.send(to_send).await {
                        error!("Could not forward matching msg: {:?}. {}", msg, e)
                    }
                }
                if let Some((min, max)) = offset_range.get(&m.partition()) {
                    if m.offset() >= *max - 1 {
                        info!("Reached end for partition {}", m.partition());
                        offset_range.remove(&m.partition());
                    } else {
                        let curr_progress = (m.offset() - *min) as f64 / (*max - *min) as f64;
                        progress_per_partition.insert(m.partition(), curr_progress);
                        let overall_progress: f64 = progress_per_partition.values().into_iter().mean();
                        if curr_progress - last_displayed > display_step {
                            last_displayed = curr_progress;
                            info!("Partition {} progress: {:.2}%", m.partition(), curr_progress * 100.0);
                            let elapsed = Utc::now() - start;
                            let eta = start + ChronoDuration::milliseconds((elapsed.num_milliseconds() as f64 / overall_progress) as i64);
                            let to_send = ProgressNotification {
                                topic: topic.to_string(),
                                overall_progress,
                                per_partition_progress: Default::default(),
                                matches,
                                read,
                                elapsed: Utc::now() - start,
                                eta
                            };
                            if let Err(e) = sender.send(SearchNotification::Progress(to_send)).await {
                                error!("Could not send progress notification: {:?}", e)
                            }
                        }
                    }
                }
            }
        }
    }
    info!("Reached end of consumption. Sending end marker");
    let elapsed = Utc::now() - start;
    let notification = FinishNotification {
        topic,
        matches,
        read,
        elapsed,
        read_rate_msg_sec: (read as f64 / elapsed.num_milliseconds() as f64) * 1000.0
    };
    sender.send(SearchNotification::Finish(notification))
        .await
        .expect("Could not notify end of topic search, crashing");
}

async fn seek_partitions(config: KafkaClusterConfig, consumer: &StreamConsumer, topic: &str, bounds: &SearchBounds) -> KafkaResult<HashMap<i32, (i64, i64)>> {
    tokio::time::sleep(Duration::from_millis(500)).await; // Subscription must be effective before seeking
    println!("Seeking partitions");
    let loop_inifinitely = bounds.end == SearchEnd::Unbounded;
    if let SearchStart::Time(beginning) = bounds.start {
        seek_all_partitions_to_time(config, consumer, topic, beginning, !loop_inifinitely).await
    } else {
        seek_all_partitions_to_start(config, consumer, topic, !loop_inifinitely).await
    }
}

async fn seek_all_partitions_to_time(config: KafkaClusterConfig, consumer: &StreamConsumer, topic: &str, beginning: DateTime<Utc>, stop_at_end: bool) -> KafkaResult<HashMap<i32, (i64, i64)>> {
    let req_timeout = Timeout::After(Duration::from_secs(1));
    let metadata = consumer.fetch_metadata(Some(topic), req_timeout)?;
    let partitions: Vec<i32> = metadata.topics()[0].partitions().iter().map(|part| part.id()).collect();
    let nb_partitions = partitions.len();
    let mut topic_partition_list = TopicPartitionList::with_capacity(nb_partitions);
    for partition in partitions {
        let mut part = topic_partition_list.add_partition(topic, partition);
        part.set_offset(Offset(beginning.timestamp_millis()))?;
    }
    consumer.assign(&topic_partition_list)?;


    let mut tasks = vec![];
    let before_loop = Utc::now();
    // FIXME remove duplicated code
    let parts: Vec<(String, i32)> = topic_partition_list.to_topic_map().iter().map(|((topic, partition), _)| (topic.to_string(), *partition)).collect();
    for (topic, partition) in parts.clone() {
        let conf = config.clone();
        tasks.push(tokio::task::spawn(async move {
            let consumer = create_client(&conf);
            let (_, max_offset) = if stop_at_end {
                println!("Fetching watermarks for partition");
                consumer.fetch_watermarks(&topic, partition, req_timeout).expect("Could not fetch watermarks for partition") // FIXME: fail the task?
            } else {
                (-1, -1)
            };
            (partition, max_offset)
        }));
    }

    // let offsets = consumer.offsets_for_times(topic_partition_list, req_timeout)?;
    // let mut partitions_max = HashMap::<i32, (i64, i64)>::with_capacity(nb_partitions);
    // for ((topic, partition), start_offset) in offsets.to_topic_map() {
    //     let max_offset = if stop_at_end {
    //         let (_, high) = consumer.fetch_watermarks(&topic, partition, req_timeout)?;
    //         high
    //     } else {
    //         -1
    //     };
    //     partitions_max.insert(partition, (start_offset.to_raw().unwrap_or(-1), max_offset));
    //     // println!("{} -> {}", partition, start_offset.to_raw().unwrap());
    //     consumer.seek(&topic, partition, start_offset, req_timeout)?;
    // }

    let offsets = consumer.offsets_for_times(topic_partition_list, req_timeout)?;
    let mut partitions_max = HashMap::<i32, (i64, i64)>::with_capacity(nb_partitions);
    let min_offsets: HashMap<i32, RdOffset> = offsets.to_topic_map().into_iter().map(|((_, partition), offset)| (partition, offset)).collect();
    for (partition, max) in (futures::future::join_all(tasks).await).into_iter().flatten() {
        let min = min_offsets.get(&partition).expect("Could not find offset for partition");
        partitions_max.insert(partition, (min.to_raw().unwrap(), max));
    }
    for (topic, partition) in &parts {
        consumer.seek(topic, *partition, RdOffset::Beginning, req_timeout)?
    }
    println!("All partitions watermarks fetched + synced in {}", (Utc::now() - before_loop).num_milliseconds());

    Ok(partitions_max)
}

async fn seek_all_partitions_to_start(config: KafkaClusterConfig, consumer: &StreamConsumer, topic: &str, stop_at_end: bool) -> KafkaResult<HashMap<i32, (i64, i64)>> {
    let req_timeout = Timeout::After(Duration::from_secs(1));
    let metadata = consumer.fetch_metadata(Some(topic), req_timeout)?;
    let partitions: Vec<i32> = metadata.topics()[0].partitions().iter().map(|part| part.id()).collect();
    let nb_partitions = partitions.len();
    let mut topic_partition_list = TopicPartitionList::with_capacity(nb_partitions);
    for partition in partitions {
        topic_partition_list.add_partition(topic, partition);
    }
    consumer.assign(&topic_partition_list)?;
    let mut partitions_max = HashMap::<i32, (i64, i64)>::with_capacity(nb_partitions);
    let mut tasks: Vec<JoinHandle<(i32, (i64, i64))>> = vec![];
    let parts: Vec<(String, i32)> = topic_partition_list.to_topic_map().iter().map(|((topic, partition), _)| (topic.to_string(), *partition)).collect();
    let before_loop = Utc::now();
    for (topic, partition) in parts.clone() {
        let conf = config.clone();
        tasks.push(tokio::task::spawn(async move {
            let consumer = create_client(&conf);
            let (_, max_offset) = if stop_at_end {
                println!("Fetching watermarks for partition");
                consumer.fetch_watermarks(&topic, partition, req_timeout).expect("Could not fetch watermarks for partition") // FIXME: fail the task?
            } else {
                (-1, -1)
            };
            (partition, (0, max_offset))
        }));
        // Unparallel version for ref.
        // let start = Utc::now();
        // let (_, max_offset) = if stop_at_end {
        //     println!("Fetching watermarks for partition");
        //     consumer.fetch_watermarks(&topic, partition, req_timeout).expect("Could not fetch watermarks for partition") // FIXME: fail the task?
        //
        // } else {
        //     (-1, -1)
        // };
        // println!("Watermarks fetched in {}", (Utc::now() - start).num_milliseconds());
        // partitions_max.insert(partition, (0, max_offset));
        // // println!("{} -> {}", partition, max_offset);
        // println!("Seeking partition");
        // let start = Utc::now();
        // consumer.seek(&topic, partition, RdOffset::Beginning, req_timeout).expect("Could not seek partition to desired offset"); // FIXME: fail the task?
        // println!("Partition seeked in {}", (Utc::now() - start).num_milliseconds());
    }
    for (partition, min_max) in (futures::future::join_all(tasks).await).into_iter().flatten() {
        partitions_max.insert(partition, min_max);
    }
    for (topic, partition) in &parts {
        consumer.seek(topic, *partition, RdOffset::Beginning, req_timeout)?
    }
    println!("All partitions watermarks fetched + synced in {}", (Utc::now() - before_loop).num_milliseconds());
    Ok(partitions_max)
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


pub fn all_matches<I: IntoIterator<Item = SearchNotification>>(notifications: I) -> Vec<OwnedMessage> {
    notifications
        .into_iter()
        .filter_map(|notification| {
            if let SearchNotification::Match(msg) = notification {
                Some(msg)
            } else {
                None
            }
        })
        .collect()
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
    use crate::search::SearchNotification;

    pub(crate) const KAFKA_PORT: u16 = 9092;

    pub(crate) fn test_cluster_config() -> KafkaClusterConfig {
        KafkaClusterConfig {
            bootstrap_servers: test_bootstrap_servers(),
            security: None
        }
    }

    pub(crate) fn test_bootstrap_servers() -> String {
        format!("localhost:{}", KAFKA_PORT)
    }

    pub(crate) fn test_client_config() -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", test_bootstrap_servers());
        config.set("message.timeout.ms", "5000");
        config
    }


    pub(crate) fn test_producer() -> FutureProducer {
        test_client_config()
            .create::<FutureProducer>()
            .expect("Failed to create Kafka producer")
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub(crate) struct NestedTestRecord {
        pub(crate) int: u32,
        pub(crate) string: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub(crate) struct TestRecord {
        pub(crate) key: String,
        pub(crate) nested: NestedTestRecord
    }

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

    pub(crate) async fn prepare(topic: &str) {
        clean(topic).await;
        test_client_config()
            .create::<AdminClient<_>>()
            .expect("Could not create admin client")
            .create_topics(vec![&NewTopic::new(topic, 12, TopicReplication::Fixed(1))], &AdminOptions::default())
            .await
            .expect("Could not prepare test topics");
    }

    pub(crate) async fn clean(topic: &str) {
        test_client_config()
            .create::<AdminClient<_>>()
            .expect("Could not create admin client")
            .delete_topics(&[topic], &AdminOptions::default())
            .await
            .expect("Could not cleanup test topics");
    }

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
