use std::time::Duration;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::{Message, TopicPartitionList};
use rdkafka::Offset::Offset;
use rdkafka::util::Timeout;
use tokio::sync::mpsc::Sender;
use crate::{create_client, KafkaClusterConfig, PartitionMsg, PartitionProgress, Progress, RdOffset, SearchBounds, SearchEnd, SearchStart};

pub(crate) async fn consume_partition(
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
    let total = max - min;
    while loop_infinitely || (max > 0 && curr_offset < max - 1) {
        match &consumer.recv().await {
            Err(e) =>
                log::error!("Kafka error while reading topic: {}", e),
            Ok(m) => {
                curr_offset = m.offset();
                let msg = m.detach();
                let done = msg.offset() - min;
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
    sender.send(PartitionProgress::finished(partition, max))
        .await
        .expect("Could not send end notification for partition");
}

pub(crate) fn prepare_partition(
    config: &KafkaClusterConfig,
    partition: i32,
    topic: &str,
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
            *consumer
                .offsets_for_times(topic_partition_list, req_timeout)?
                .to_topic_map()
                .get(&(topic.to_string(), partition))
                .expect("Could not find offset for desired time")
        } else {
            RdOffset::Beginning
        };
    // 4. Fetch watermarks (min/max offset for partition)
    let max_offset = if !loop_infinitely {
        log::debug!("Fetching watermarks for partition {}", partition);
        consumer.fetch_watermarks(topic, partition, req_timeout)?.1
    } else {
        -1
    };
    Ok((consumer, partition, (start_offset, max_offset)))
}

pub(crate) async fn seek_partition(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    start_offset: RdOffset
) -> KafkaResult<()> {
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
