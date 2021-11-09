use rdkafka::message::OwnedMessage;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use chrono::{DateTime, Duration, Utc};

pub mod bounds;
pub mod headers;
pub mod payload;
pub mod json;

#[derive(Debug, Clone)]
pub enum SearchNotification {
    Start,
    Finish(FinishNotification),
    Progress(ProgressNotification),
    Match(OwnedMessage)
}

#[derive(Debug, Clone)]
pub struct ProgressNotification {
    pub topic: String,
    pub overall_progress: f64,
    pub per_partition_progress: HashMap<i32, f64>,
    pub matches: u32,
    pub read: u32,
    pub elapsed: Duration,
    pub eta: DateTime<Utc>
}

#[derive(Debug, Clone)]
pub struct FinishNotification {
    pub topic: String,
    pub matches: u32,
    pub read: u32,
    pub elapsed: Duration,
    pub read_rate_msg_sec: f64
}


impl Display for ProgressNotification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\t topic = {}", self.topic)?;
        writeln!(f, "\t progress = {:.2}%", self.overall_progress * 100.0)?;
        writeln!(f, "\t matches = {}", self.matches)?;
        writeln!(f, "\t total read = {}", self.read)?;
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


pub trait SearchPredicate<T> {
    fn matches(&mut self, t: &T) -> bool;
}

pub struct PerfectMatchPredicate<T: PartialEq> {
    expected: T
}

impl<T: PartialEq> PerfectMatchPredicate<T> {
    pub fn new(t: T) -> Self {
        PerfectMatchPredicate { expected: t }
    }
}

impl<T: PartialEq> SearchPredicate<T> for PerfectMatchPredicate<T> {
    fn matches(&mut self, t: &T) -> bool {
        *t == self.expected
    }
}

pub trait MsgExtractor<T> {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<T>;
}

pub struct SearchDefinition<T, E: MsgExtractor<T>, S: SearchPredicate<T>> {
    pub extractor: E,
    pub matcher: S,
    phantom: PhantomData<T>
}

impl<T, E: MsgExtractor<T>, S: SearchPredicate<T>> SearchPredicate<OwnedMessage> for SearchDefinition<T, E, S> {
    fn matches(&mut self, msg: &OwnedMessage) -> bool {
        match self.extractor.extract(msg) {
            None => false,
            Some(extracted) => self.matcher.matches(&extracted)
        }
    }
}