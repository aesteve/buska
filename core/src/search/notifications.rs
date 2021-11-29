use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use rdkafka::message::OwnedMessage;

#[derive(Debug, Clone)]
pub enum SearchNotification {
    Start,
    Prepare(PreparationStep),
    Finish(FinishNotification),
    Progress(ProgressNotification),
    Match(OwnedMessage)
}

#[derive(Debug, Clone, PartialEq)]
pub enum PreparationStep {
    CreateClient,
    FetchMetadata,
    FetchWatermarks,
    SeekPartitions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    pub done: i64,
    pub total: i64,
    pub rate: f64,
}

impl Progress {
    pub fn new(done: i64, total: i64) -> Progress {
        Progress {
            done,
            total,
            rate: done as f64 / total as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProgressNotification {
    pub topic: String,
    pub overall_progress: Progress,
    pub per_partition_progress: BTreeMap<i32, Progress>,
    pub matches: i64,
    pub elapsed: Duration,
    pub eta: DateTime<Utc>
}

#[derive(Debug, Clone)]
pub struct FinishNotification {
    pub topic: String,
    pub matches: i64,
    pub read: i64,
    pub elapsed: Duration,
    pub read_rate_msg_sec: f64
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionMsg {
    pub(crate) partition: i32,
    pub(crate) msg: OwnedMessage,
    pub(crate) progress: Progress
}

#[derive(Debug, Clone)]
pub(crate) struct FinishPartitionNotification {
    pub(crate) partition: i32,
    pub(crate) progress: Progress,
}

#[derive(Debug, Clone)]
pub(crate) enum PartitionProgress {
    Start,
    Msg(PartitionMsg),
    Finish(FinishPartitionNotification)
}

impl Display for ProgressNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\t topic = {}", self.topic)?;
        writeln!(f, "\t progress = {:.2}%", self.overall_progress.rate * 100.0)?;
        writeln!(f, "\t total read = {}", self.overall_progress.done)?;
        writeln!(f, "\t matches = {}", self.matches)?;
        writeln!(f, "\t elapsed = {} seconds", self.elapsed.num_seconds())?;
        writeln!(f, "\t eta = {}", self.eta)
    }
}

impl Display for FinishNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\t topic = {}", self.topic)?;
        writeln!(f, "\t matches = {}", self.matches)?;
        writeln!(f, "\t total read = {}", self.read)?;
        writeln!(f, "\t elapsed = {} seconds", self.elapsed.num_seconds())?;
        writeln!(f, "\t messages read per second = {:.2}", self.read_rate_msg_sec)
    }
}

impl Display for PreparationStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PreparationStep::CreateClient => write!(f, "Creating client"),
            PreparationStep::FetchMetadata => write!(f, "Fetching topic metadata (nb. of partitions)"),
            PreparationStep::FetchWatermarks => write!(f, "Fetching topic watermarks (min/max offset per partition)"),
            PreparationStep::SeekPartitions => write!(f, "Seeking partitions to desired offsets"),
        }
    }
}

