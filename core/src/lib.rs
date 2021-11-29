pub mod config;
pub mod search;
pub mod utils;

use crate::search::Predicate;
use crate::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use crate::search::notifications::{FinishNotification, FinishPartitionNotification, PartitionMsg, PartitionProgress, PreparationStep, Progress, ProgressNotification, SearchNotification};

use chrono::{Utc, Duration as ChronoDuration, TimeZone};
use config::KafkaClusterConfig;
use rdkafka::{ClientConfig, TopicPartitionList, Offset as RdOffset, Message};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::topic_partition_list::Offset::Offset;
use rdkafka::util::Timeout;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;
use rdkafka::message::OwnedMessage;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

/// The main entry point of this library
/// Searches a whole topic on a Kafka cluster (describe by its KafkaClusterConfig)
///   - within some search bounds (start / end)
///   - matching some kind of record's predicate
/// And sends notifications (including the search results) over a mpsc channel
pub async fn search_topic<T: Predicate<OwnedMessage> + Send + ?Sized>(
    conf: KafkaClusterConfig,
    topic: String,
    sender: Sender<SearchNotification>,
    bounds: SearchBounds,
    predicate: &mut T,
    notify_every: ChronoDuration,
) {
    let search_start = Utc::now();
    if let Err(e) = sender.send(SearchNotification::Prepare(PreparationStep::CreateClient)).await {
        log::error!("Could not send notification {}", e);
    }
    let consumer = create_client(&conf);
    log::debug!("Kafka consumer created in {} millis", (Utc::now() - search_start).num_milliseconds());
    let loop_infinitely = bounds.end == SearchEnd::Unbounded;
    let metadata = fetch_topic_metadata(&consumer, &sender, &topic).await.expect("Could not fetch topic metadata");
    let nb_partitions = metadata.len();
    let mut seek_tasks = Vec::with_capacity(nb_partitions);
    let mut to_finish: HashSet<i32> = HashSet::with_capacity(nb_partitions);
    if let Err(e) = sender.send(SearchNotification::Prepare(PreparationStep::FetchWatermarks)).await {
        log::error!("Could not send notification {}", e);
    }
    for partition in metadata {
        let conf = conf.clone();
        let topic = topic.clone();
        let sender = sender.clone();
        let bounds = bounds.clone();
        seek_tasks.push(tokio::task::spawn(async move {
            prepare_partition(&conf, partition, &topic, &sender, &bounds)
        }));
    }
    let mut progress_per_partition = BTreeMap::new();
    let mut search_tasks: Vec<JoinHandle<KafkaResult<()>>> = Vec::with_capacity(nb_partitions);
    let mut offset_range = HashMap::with_capacity(nb_partitions);
    let (msg_sender, mut msg_receiver) = tokio::sync::mpsc::channel::<PartitionProgress>(1024);
    for res in futures::future::join_all(seek_tasks).await {
        match res {
            Ok(Ok((consumer, part, (min_offset, max)))) => {
                if let Err(e) = sender.send(SearchNotification::Prepare(PreparationStep::SeekPartitions)).await {
                    log::error!("Could not send notification {}", e);
                }
                let min = min_offset.to_raw().unwrap_or_else(|| panic!("Could not represent offset {:?}", min_offset));
                log::debug!("offset for partition: {} is {:?}", part, (min, max));
                offset_range.insert(part, (min, max));
                progress_per_partition.insert(part, Progress::new(0, max - min));
                let msg_sender = msg_sender.clone();
                let topic = topic.clone();
                search_tasks.push(tokio::task::spawn(async move {
                    seek_partition(&consumer, &topic, part, min_offset).await?;
                    consume_partition(&consumer, part, min, max, &msg_sender, loop_infinitely).await;
                    Ok(())
                }));
                to_finish.insert(part);
            },
            Ok(Err(err)) =>
                log::error!("Could not seek partition to proper offset. Kafka error: {:?}", err),
            _ =>
                log::error!("Could not seek partition to proper offset")
        }
    }
    let mut matches = 0;
    let initial_notif = ProgressNotification {
        topic: topic.clone(),
        per_partition_progress: progress_per_partition.clone(),
        overall_progress: Progress::new(0, offset_range.clone().values().map(|(min, max)| max - min).sum()),
        matches,
        elapsed: ChronoDuration::seconds(0),
        eta: Utc::now() // dummy
    };
    sender.send(SearchNotification::Start).await.expect("Could not send start notification. Crashing");
    sender.send(SearchNotification::Progress(initial_notif)).await.expect("Could not send first progress notification. Crashing");
    let mut read = 0;
    let mut last_displayed = Utc::now();
    while !to_finish.is_empty() {
        if let Some(consumed) = msg_receiver.recv().await {
            match consumed {
                PartitionProgress::Start => {},
                PartitionProgress::Msg(msg) => {
                    read += 1;
                    progress_per_partition.insert(msg.partition, msg.progress);
                    if predicate.matches(&msg.msg) {
                        matches += 1;
                        let to_send = SearchNotification::Match(msg.msg.clone());
                        if let Err(e) = sender.send(to_send).await {
                            log::error!("Could not forward matching msg: {:?}. {}", msg.msg, e)
                        }
                    }
                    if Utc::now() > last_displayed + notify_every {
                        last_displayed = Utc::now();
                        let total_to_read: i64 = progress_per_partition.values().map(|p| p.total).sum();
                        let total_read: i64 = progress_per_partition.values().map(|p| p.done).sum();
                        let overall_progress = Progress::new(total_read, total_to_read);
                        let elapsed = Utc::now() - search_start;
                        let eta = if overall_progress.rate > 0.1 {
                            search_start + ChronoDuration::milliseconds((elapsed.num_milliseconds() as f64 / overall_progress.rate) as i64)
                        } else {
                            Utc.from_utc_datetime(&chrono::naive::MAX_DATETIME)
                        };
                        let to_send = ProgressNotification {
                            topic: topic.to_string(),
                            overall_progress,
                            per_partition_progress: progress_per_partition.clone(),
                            matches,
                            elapsed: Utc::now() - search_start,
                            eta
                        };
                        if let Err(e) = sender.send(SearchNotification::Progress(to_send)).await {
                            log::error!("Could not send progress notification: {:?}", e)
                        }
                    }
                },
                PartitionProgress::Finish(notif) => {
                    log::trace!("Finished consuming partition {}", notif.partition);
                    progress_per_partition.insert(notif.partition, notif.progress.clone());
                    to_finish.remove(&notif.partition);
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

async fn fetch_topic_metadata(consumer: &StreamConsumer, sender: &Sender<SearchNotification>, topic: &str) -> KafkaResult<Vec<i32>> {
    let req_timeout = Timeout::After(Duration::from_secs(60));
    if let Err(e) = sender.send(SearchNotification::Prepare(PreparationStep::FetchMetadata)).await {
        log::error!("Could not send notification {}", e);
    }

    let metadata = consumer.fetch_metadata(Some(topic), req_timeout)?;
    let partitions: Vec<i32> = metadata.topics()[0].partitions().iter().map(|part| part.id()).collect();
    Ok(partitions)
}

async fn consume_partition(
    consumer: &StreamConsumer,
    partition: i32,
    min: i64,
    max: i64,
    sender: &Sender<PartitionProgress>,
    loop_infinitely: bool,
) {
    let mut curr_offset = min;
    if let Err(err) = sender.send(PartitionProgress::Start).await {
        log::error!("Could not send start notification for partition {}", err);
    }
    while loop_infinitely || (max > 0 && curr_offset < max - 1) {
        match &consumer.recv().await {
            Err(e) =>
                log::error!("Kafka error while reading topic: {}", e),
            Ok(m) => {
                curr_offset = m.offset();
                let msg = m.detach();
                let done = msg.offset() - min;
                let total = max - min;
                let notif = PartitionProgress::Msg(PartitionMsg {
                    partition,
                    msg: msg.clone(),
                    progress: Progress::new(done, total)
                });
                if let Err(err) = sender.send(notif).await {
                    log::error!("Could not send partition progress notification {}", err);
                }
            }
        }
    }
    log::debug!("Reached end for partition {}", partition);
    let notif = PartitionProgress::Finish(FinishPartitionNotification {
        partition,
        progress: Progress::new(max, max)
    });
    sender.send(notif)
        .await
        .expect("Could not send end notification for partition");
}

fn prepare_partition(
    config: &KafkaClusterConfig,
    partition: i32,
    topic: &str,
    _sender: &Sender<SearchNotification>,
    bounds: &SearchBounds
) -> KafkaResult<(StreamConsumer, i32, (RdOffset, i64))> {
    let loop_infinitely = SearchEnd::Unbounded == bounds.end;
    let req_timeout = Timeout::After(Duration::from_secs(60));
    let consumer = create_client(config);
    let mut topic_partition_list = TopicPartitionList::default();
    let mut part = topic_partition_list.add_partition(topic, partition);
    // 2. Assign partitions to consumer
    if let SearchStart::Time(beginning) = bounds.start {
        part.set_offset(Offset(beginning.timestamp_millis())).expect("Could not set offset");
    }
    consumer.assign(&topic_partition_list)?;
    // 3. Fetch offsets for time
    let start_offset: RdOffset =
        if let SearchStart::Time(_) = bounds.start {
            let offsets = consumer.offsets_for_times(topic_partition_list, req_timeout)?;
            let offsets = offsets.to_topic_map();
            let res = offsets.get(&(topic.to_string(), partition));
            *res.expect("Could not find offset for desired time")
        } else {
            RdOffset::Beginning
        };
    // 4. Fetch watermarks (min/max offset for partition)
    let (_, max_offset) = if !loop_infinitely {
        log::info!("Fetching watermarks for partition {}", partition);
        consumer.fetch_watermarks(topic, partition, req_timeout)?
    } else {
        (-1, -1)
    };
    Ok((consumer, partition, (start_offset, max_offset)))
}

async fn seek_partition(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    start_offset: RdOffset) -> KafkaResult<()> {
    // 5. Seek consumer to proper offset
    let req_timeout = Timeout::After(Duration::from_secs(60));
    let mut nb_retries = 0;
    let mut seeked = false;
    let max_retries = 20;
    let mut error = KafkaError::Seek("Could not seek partition".to_string());
    while !seeked && nb_retries < max_retries {
        if let Err(err) = consumer.seek(topic, partition, start_offset, req_timeout) {
            nb_retries += 1;
            log::warn!("Could not seek partition to desired offset {:?}, retrying", err);
            tokio::time::sleep(Duration::from_millis(100)).await; // Subscription must be effective before seeking
            error = err;
        } else {
            seeked = true
        }
    }
    if !seeked {
        KafkaResult::Err(error)
    } else {
        Ok(())
    }

}

fn create_client(conf: &KafkaClusterConfig) -> StreamConsumer {
    let mut builder = ClientConfig::new();
    builder
        .set("bootstrap.servers", conf.bootstrap_servers.clone())
        .set_log_level(RDKafkaLogLevel::Debug)
        .set("group.id", "buska-consumer")
        .set("enable.auto.commit", "false")
        .set("fetch.min.bytes", "1000000")
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
    use chrono::Utc;
    use quickcheck::{Arbitrary, Gen};

    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::{ClientConfig, Message, Timestamp};
    use rdkafka::message::{Headers, OwnedHeaders, OwnedMessage};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use serde::{Serialize, Deserialize};
    use serde_json::to_string;
    use tokio::sync::mpsc::Receiver;
    use crate::{ChronoDuration, search_topic, SearchBounds, SearchEnd, SearchStart};

    use crate::config::KafkaClusterConfig;
    use crate::search::extractors::json::json_single_extract;
    use crate::search::matchers::PerfectMatch;
    use crate::search::notifications::SearchNotification;
    use crate::search::SearchDefinition;

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
        config.set("acks", "0");
        config.set("batch.size", "100000");
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
        let mut tasks = vec![];
        for chunk in recs.chunks(1000) {
            let t: String = topic.into();
            let c = chunk.to_vec();
            tasks.push(tokio::task::spawn(async move {
                let producer = test_producer();
                for rec in c {
                    producer
                        .send(
                            FutureRecord::to(&t)
                                .key(&rec.clone().key)
                                .payload(&to_string(&rec.clone()).expect("Could not serialize test record as JSON"))
                            ,
                            Duration::from_millis(100)
                        ).await
                        .expect("Could not produce test record");
                }
            }));
        }
        futures::future::join_all(tasks).await;
    }

    /// Given a topic name, sets a topic up in the test cluster
    ///  - Cleans the topic if it exists
    ///  - Creates the test topic with a replication factor of 1, and 12 partitions
    pub(crate) async fn prepare(topic: &str, num_partitions: i32) {
        clean(topic).await;
        test_client_config()
            .create::<AdminClient<_>>()
            .expect("Could not create admin client")
            .create_topics(vec![&NewTopic::new(topic, num_partitions, TopicReplication::Fixed(1))], &AdminOptions::default())
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
    pub(crate) async fn collect_search_notifications(receiver: &mut Receiver<SearchNotification>, timeout: ChronoDuration) -> Vec<SearchNotification> {
        let mut notifications = Vec::new();
        let mut stop = false;
        let start = Utc::now();
        while !stop && Utc::now() < (start + timeout) {
            if let Some(received) = receiver.recv().await {
                notifications.push(received.clone());
                if let SearchNotification::Finish(_) = received {
                    stop = true;
                }
            }
        }
        notifications
    }


    // Property-Based Testing
    // Some of our tests are not "example-based", some are in the form of: "no matter what input, [...] should happen"
    // For this kind of tests we are using quickcheck
    // This part of the module contains generators / shrinkers for rust-rdkafka

    /// We can't `impl Arbitrary for OwnedMessage` since these are 2 types we don't own, thus the use of a custom test type
    #[derive(Debug, Clone)]
    pub(crate) struct TestMessage(pub(crate) OwnedMessage);

    /// A few utility methods
    impl TestMessage {
        pub(crate) fn without_key(&self) -> OwnedMessage {
            OwnedMessage::new(
                self.0.payload().map(|v| v.to_vec()),
                None,
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                self.0.headers().cloned()
            )
        }

        pub(crate) fn without_headers(&self) -> OwnedMessage {
            OwnedMessage::new(
                self.0.payload().map(|v| v.to_vec()),
                self.0.key().map(|v| v.to_vec()),
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                None
            )
        }

        pub(crate) fn without_payload(&self) -> OwnedMessage {
            OwnedMessage::new(
                None,
                self.0.key().map(|v| v.to_vec()),
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                None
            )
        }

        pub(crate) fn withbytes_payload(&self, payload: &[u8]) -> OwnedMessage {
            OwnedMessage::new(
                Some(payload.to_vec()),
                self.0.key().map(|v| v.to_vec()),
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                None
            )
        }



        pub(crate) fn add_header(&self, name: &str, value: &str) -> OwnedMessage {
            let headers =  match self.0.headers().cloned().as_mut() {
                Some(h) =>
                    copy_headers(h)
                        .add(name, value),
                _ =>
                    OwnedHeaders::new_with_capacity(1)
                        .add(name, value)
            };
            OwnedMessage::new(
                self.0.payload().map(|v| v.to_vec()),
                self.0.key().map(|v| v.to_vec()),
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                Some(headers)
            )
        }
    }


    fn rand_timestamp(g: &mut Gen) -> Timestamp {
        let timestamps = &[
            Timestamp::NotAvailable,
            Timestamp::CreateTime(i64::arbitrary(g)),
            Timestamp::LogAppendTime(i64::arbitrary(g))
        ];
        *g.choose(timestamps).unwrap()
    }

    /// In our tests using the `#[quickcheck]` macro we will be using `TestMessage` as method parameter
    /// From there we will be able to destructure it and get the message using:
    /// ```rust
    /// #[quickcheck]
    /// fn some_test(msg: TestMessage) {
    ///   let kafka_msg = msg.0;
    ///   // => test kafka_msg
    /// }
    /// ```
    impl Arbitrary for TestMessage {
        fn arbitrary(g: &mut Gen) -> Self {
            TestMessage(OwnedMessage::new(
                Option::arbitrary(g),
                Option::arbitrary(g),
                String::arbitrary(g),
                rand_timestamp(g),
                i32::arbitrary(g),
                i64::arbitrary(g),
                None
            ))
        }
    }

    fn copy_headers(h: &OwnedHeaders) -> OwnedHeaders {
        let c = h.count();
        let mut res = OwnedHeaders::new_with_capacity(c);
        for i in 0..c {
            let header = h.get(i).expect("Could not extract header by index");
            res = res.add(header.0, header.1);
        }
        res
    }


    #[tokio::test]
    #[ignore]
    async fn test_data_integration() {
        //clean("test").await;
        //prepare("test", 60).await;
        let before = Utc::now();
        // let range = 1..120;
        // let range = 1..1_000_000;
        // let range = 1_000_001..2_000_000;
        // let range = 2_000_001..3_000_000;
        // let range = 3_000_001..4_000_000;
        // let range = 4_000_001..5_000_000;
        // let range = 5_000_001..6_000_000;
        // let range = 5_000_001..6_000_000;
        // let range = 6_000_001..7_000_000;
        // let range = 7_000_001..8_000_000;
        // let range = 8_000_001..9_000_000;
        let range = 9_000_001..10_000_000;
        let recs = range.into_iter().map(|i| TestRecord { key: format!("key-{}",i), nested: NestedTestRecord {
            int: i,
            ints: vec![i-1, i, i+1],
            string: format!("some-{}", i)
        }}).collect::<Vec<TestRecord>>();
        produce_json_records("test", &recs).await;
        println!("Elapsed: {:?}", Utc::now() - before);
    }


    #[tokio::test]
    #[ignore]
    async fn local_search() {
        let conf = test_cluster_config();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<SearchNotification>(1024);
        let bounds = SearchBounds {
            start: SearchStart::Earliest,
            end: SearchEnd::CurrentLast
        };
        let mut tasks = vec![];
        tasks.push(
            tokio::task::spawn(async move {
                collect_search_notifications(&mut receiver, ChronoDuration::seconds(90)).await
            })
        );
        // std::thread::sleep(core::time::Duration::from_secs(2));
        tasks.push(
            tokio::task::spawn(async move {
                let extractor = json_single_extract("$.nested.int").expect("Could not create JSON path");
                let matcher = PerfectMatch::new(serde_json::json!(4));
                let mut search_definition = SearchDefinition::new(extractor, Box::new(matcher));
                search_topic(conf, "test2".to_string(), sender, bounds, &mut search_definition, chrono::Duration::seconds(1)).await;
                vec![]
            })
        );
        futures::future::join_all(tasks).await;
    }

}
