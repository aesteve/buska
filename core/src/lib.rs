pub mod search;
pub mod utils;

mod partition;

use crate::search::bounds::{SearchBounds, SearchEnd, SearchStart};
use crate::search::notifications::{
    FinishNotification, PartitionMsg, PartitionProgress, PreparationStep, Progress,
    ProgressNotification, SearchNotification,
};
use crate::search::Predicate;

use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, TimeZone, Utc};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Offset as RdOffset, Offset};

use crate::partition::{consume_partition, prepare_partition, seek_partition};
use rdkafka::message::OwnedMessage;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinError;

type PreparedPartition = (StreamConsumer, i32, (Offset, i64));
type PartitionPreparationResult = KafkaResult<PreparedPartition>;

struct PerPartitionStatus {
    progress: BTreeMap<i32, Progress>,
    to_finish: HashSet<i32>,
    start: DateTime<Utc>,
}

impl PerPartitionStatus {
    fn new(nb_partition: usize) -> Self {
        PerPartitionStatus {
            progress: BTreeMap::new(),
            to_finish: HashSet::with_capacity(nb_partition),
            start: Utc::now(),
        }
    }

    fn start(&mut self, part: i32, min: i64, max: i64) {
        self.progress.insert(part, Progress::new(0, max - min));
        self.to_finish.insert(part);
    }

    fn make_progress(&mut self, part: i32, progress: Progress) {
        if progress.is_finished() {
            self.to_finish.remove(&part);
        }
        self.progress.insert(part, progress);
    }

    fn progress(&self) -> Progress {
        Progress::new(self.done(), self.total())
    }

    fn progress_notification(&self, topic: String, matches: i64) -> SearchNotification {
        let overall_progress = self.progress();
        let elapsed = Utc::now() - self.start;
        let eta = if overall_progress.rate > 0.1 {
            self.start
                + ChronoDuration::milliseconds(
                    (elapsed.num_milliseconds() as f64 / overall_progress.rate) as i64,
                )
        } else {
            Utc.from_utc_datetime(&NaiveDateTime::MAX)
        };
        SearchNotification::Progress(ProgressNotification {
            topic,
            overall_progress: self.progress(),
            per_partition_progress: self.progress_per_partition(),
            matches,
            elapsed,
            eta,
        })
    }

    fn total(&self) -> i64 {
        self.progress.values().map(|p| p.total).sum()
    }

    fn done(&self) -> i64 {
        self.progress.values().map(|p| p.done).sum()
    }

    fn progress_per_partition(&self) -> BTreeMap<i32, Progress> {
        self.progress.clone()
    }

    fn is_empty(&self) -> bool {
        self.to_finish.is_empty()
    }
}

/// The main entry point of this library
/// Searches a whole topic on a Kafka cluster (describe by its KafkaClusterConfig)
///   - within some search bounds (start / end)
///   - matching some kind of record's predicate
/// And sends notifications (including the search results) over a mpsc channel
pub async fn search_topic<T: Predicate<OwnedMessage> + Send + ?Sized>(
    conf: &HashMap<String, String>,
    topic: String,
    sender: Sender<SearchNotification>,
    bounds: SearchBounds,
    predicate: &mut T,
    notify_every: ChronoDuration,
) {
    let search_start = Utc::now();
    let loop_infinitely = bounds.end == SearchEnd::Unbounded;
    if let Err(e) = sender
        .send(SearchNotification::Prepare(PreparationStep::CreateClient))
        .await
    {
        log::error!("Could not send notification {}", e);
    }
    let consumer = create_client(conf);
    log::debug!(
        "Kafka consumer created in {} millis",
        (Utc::now() - search_start).num_milliseconds()
    );
    let topic_metadata = fetch_topic_metadata(&consumer, &sender, &topic)
        .await
        .expect("Could not fetch topic metadata");
    let nb_partitions = topic_metadata.len();
    let mut partition_status = PerPartitionStatus::new(nb_partitions);
    if let Err(e) = sender
        .send(SearchNotification::Prepare(
            PreparationStep::FetchWatermarks,
        ))
        .await
    {
        log::error!("Could not send notification {}", e);
    }
    let prepared = prepare_all_partitions(topic_metadata, conf, topic.clone(), &bounds).await;
    if let Err(e) = sender
        .send(SearchNotification::Prepare(PreparationStep::SeekPartitions))
        .await
    {
        log::error!("Could not send notification {}", e);
    }
    let (msg_sender, mut msg_receiver) = tokio::sync::mpsc::channel::<PartitionProgress>(1024);
    for res in prepared {
        seek_and_consume_partition(
            topic.clone(),
            &msg_sender,
            res,
            &mut partition_status,
            loop_infinitely,
        );
    }
    sender
        .send(SearchNotification::Start)
        .await
        .expect("Could not send start notification. Crashing");
    sender
        .send(partition_status.progress_notification(topic.clone(), 0))
        .await
        .expect("Could not send first progress notification. Crashing");
    let (nb_read, nb_match) = wait_for_partitions(
        &topic,
        &mut msg_receiver,
        &sender,
        &mut partition_status,
        predicate,
        notify_every,
    )
    .await;
    log::debug!("Reached end of consumption. Sending end marker");
    let search_duration = Utc::now() - search_start;
    sender
        .send(SearchNotification::Finish(FinishNotification::new(
            topic,
            nb_match,
            nb_read,
            search_duration,
        )))
        .await
        .expect("Could not notify end of topic search, crashing");
}

pub(crate) fn create_client(conf: &HashMap<String, String>) -> StreamConsumer {
    let mut builder = ClientConfig::new();
    builder.set_log_level(RDKafkaLogLevel::Debug);
    for (key, val) in conf {
        builder.set(key, val);
    }
    builder.set("group.id", "buska-consumer")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    builder.create().unwrap()
}

async fn wait_for_partitions<T: Predicate<OwnedMessage> + Send + ?Sized>(
    topic: &str,
    msg_receiver: &mut Receiver<PartitionProgress>,
    sender: &Sender<SearchNotification>,
    partition_status: &mut PerPartitionStatus,
    predicate: &mut T,
    notify_every: ChronoDuration,
) -> (i64, i64) {
    let mut read = 0;
    let mut matches = 0;
    let mut last_displayed = Utc::now();
    while !partition_status.is_empty() {
        if let Some(consumed) = msg_receiver.recv().await {
            match consumed {
                PartitionProgress::Msg(msg) => {
                    read += 1;
                    partition_status.make_progress(msg.partition, msg.progress);
                    if predicate.matches(&msg.msg) {
                        matches += 1;
                        let to_send = SearchNotification::Match(msg.msg.clone());
                        if let Err(e) = sender.send(to_send).await {
                            log::error!("Could not forward matching msg: {:?}. {}", msg.msg, e)
                        }
                    }
                    if Utc::now() > last_displayed + notify_every {
                        last_displayed = Utc::now();
                        let notification =
                            partition_status.progress_notification(topic.to_string(), matches);
                        if let Err(e) = sender.send(notification).await {
                            log::error!("Could not send progress notification: {:?}", e)
                        }
                    }
                }
                PartitionProgress::Finish(notif) => {
                    log::debug!("Finished consuming partition {}", notif.partition);
                    partition_status.make_progress(notif.partition, notif.progress);
                }
                PartitionProgress::Start => {}
            }
        }
    }
    (read, matches)
}

async fn prepare_all_partitions(
    topic_metadata: Vec<i32>,
    conf: &HashMap<String, String>,
    topic: String,
    bounds: &SearchBounds,
) -> Vec<Result<PartitionPreparationResult, JoinError>> {
    let mut preparation_tasks = Vec::with_capacity(topic_metadata.len());
    for partition in topic_metadata {
        let conf = conf.clone();
        let topic = topic.clone();
        let bounds = bounds.clone();
        preparation_tasks.push(tokio::task::spawn(async move {
            prepare_partition(&conf, partition, &topic, &bounds)
        }));
    }
    futures::future::join_all(preparation_tasks).await
}

fn seek_and_consume_partition(
    topic: String,
    msg_sender: &Sender<PartitionProgress>,
    res: Result<PartitionPreparationResult, JoinError>,
    partition_status: &mut PerPartitionStatus,
    loop_infinitely: bool,
) {
    match res {
        Ok(Ok((consumer, part, (min_offset, max)))) => {
            let min = min_offset
                .to_raw()
                .unwrap_or_else(|| panic!("Could not represent offset {min_offset:?}"));
            log::debug!("offset for partition: {} is {:?}", part, (min, max));
            partition_status.start(part, min, max);
            let msg_sender = msg_sender.clone();
            tokio::task::spawn(async move {
                seek_partition(&consumer, &topic, part, min_offset)
                    .await
                    .expect("Could not seek partition");
                consume_partition(&consumer, part, min, max, &msg_sender, loop_infinitely).await;
            });
        }
        Ok(Err(err)) => log::error!(
            "Could not seek partition to proper offset. Kafka error: {:?}",
            err
        ),
        _ => log::error!("Could not seek partition to proper offset"),
    }
}

async fn fetch_topic_metadata(
    consumer: &StreamConsumer,
    sender: &Sender<SearchNotification>,
    topic: &str,
) -> KafkaResult<Vec<i32>> {
    let req_timeout = Timeout::After(Duration::from_secs(60));
    if let Err(e) = sender
        .send(SearchNotification::Prepare(PreparationStep::FetchMetadata))
        .await
    {
        log::error!("Could not send notification {}", e);
    }

    let metadata = consumer.fetch_metadata(Some(topic), req_timeout)?;
    let partitions: Vec<i32> = metadata.topics()[0]
        .partitions()
        .iter()
        .map(|part| part.id())
        .collect();
    Ok(partitions)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use chrono::Utc;
    use quickcheck::{Arbitrary, Gen};
    use std::time::Duration;

    use crate::{search_topic, ChronoDuration, SearchBounds, SearchEnd, SearchStart};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::message::{Header, OwnedHeaders, OwnedMessage};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Message, Timestamp};
    use serde::{Deserialize, Serialize};
    use serde_json::to_string;
    use tokio::sync::mpsc::Receiver;

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
        format!("localhost:{KAFKA_PORT}")
    }

    /// Returns a KafkaClusterConfig matching the test environment
    /// (i.e. localhost, KAFKA_PORT, and no SSL config)
    pub(crate) fn test_cluster_config() -> HashMap<String, String> {
        HashMap::from([
            ("bootstrap.servers".to_string(), test_bootstrap_servers())
        ])
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
        pub(crate) nested: NestedTestRecord,
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
                            FutureRecord::to(&t).key(&rec.clone().key).payload(
                                &to_string(&rec.clone())
                                    .expect("Could not serialize test record as JSON"),
                            ),
                            Duration::from_millis(100),
                        )
                        .await
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
            .create_topics(
                vec![&NewTopic::new(
                    topic,
                    num_partitions,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::default(),
            )
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
    pub(crate) async fn collect_search_notifications(
        receiver: &mut Receiver<SearchNotification>,
        timeout: ChronoDuration,
    ) -> Vec<SearchNotification> {
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
                self.0.headers().cloned(),
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
                None,
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
                None,
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
                None,
            )
        }

        pub(crate) fn add_header(&self, name: &str, value: &str) -> OwnedMessage {
            let headers = match self.0.headers().cloned().as_mut() {
                Some(h) => copy_headers(h).insert(Header {
                    key: name,
                    value: Some(value),
                }),
                _ => OwnedHeaders::new_with_capacity(1).insert(Header {
                    key: name,
                    value: Some(value),
                }),
            };
            OwnedMessage::new(
                self.0.payload().map(|v| v.to_vec()),
                self.0.key().map(|v| v.to_vec()),
                self.0.topic().to_string(),
                self.0.timestamp(),
                self.0.partition(),
                self.0.offset(),
                Some(headers),
            )
        }
    }

    fn rand_timestamp(g: &mut Gen) -> Timestamp {
        let timestamps = &[
            Timestamp::NotAvailable,
            Timestamp::CreateTime(i64::arbitrary(g)),
            Timestamp::LogAppendTime(i64::arbitrary(g)),
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
                None,
            ))
        }
    }

    fn copy_headers(h: &OwnedHeaders) -> OwnedHeaders {
        h.clone()
    }

    #[tokio::test]
    #[ignore]
    async fn test_data_integration() {
        //clean("test").await;
        //prepare("test", 60).await;
        let before = Utc::now();
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
        let recs = range
            .into_iter()
            .map(|i| TestRecord {
                key: format!("key-{i}"),
                nested: NestedTestRecord {
                    int: i,
                    ints: vec![i - 1, i, i + 1],
                    string: format!("some-{i}"),
                },
            })
            .collect::<Vec<TestRecord>>();
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
            end: SearchEnd::CurrentLast,
        };
        let mut tasks = vec![];
        tasks.push(tokio::task::spawn(async move {
            collect_search_notifications(&mut receiver, ChronoDuration::seconds(90)).await
        }));
        // std::thread::sleep(core::time::Duration::from_secs(2));
        tasks.push(tokio::task::spawn(async move {
            let extractor =
                json_single_extract("$.nested.int").expect("Could not create JSON path");
            let matcher = PerfectMatch::new(serde_json::json!(4));
            let mut search_definition = SearchDefinition::new(extractor, Box::new(matcher));
            search_topic(
                &conf,
                "test2".to_string(),
                sender,
                bounds,
                &mut search_definition,
                chrono::Duration::seconds(1),
            )
            .await;
            vec![]
        }));
        futures::future::join_all(tasks).await;
    }
}
