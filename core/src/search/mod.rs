use rdkafka::message::OwnedMessage;

use std::marker::PhantomData;
use crate::search::extractors::ExtractResult;

pub mod bounds;
pub mod extractors;
pub mod matchers;
pub mod notifications;

pub trait Predicate<T> {
    fn matches(&mut self, t: &T) -> bool;
}

pub trait MsgExtractor<T> {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<T>;
}

pub struct SearchDefinition<V, E, P>
where E: MsgExtractor<V>,
      P: Predicate<V> {
    extractor: E,
    matcher: P,
    phantom: PhantomData<V>
}

impl <V, E, P> SearchDefinition<V, E, P>
where E: MsgExtractor<V>,
      P: Predicate<V> {
    pub fn new(extractor: E, matcher: P) -> SearchDefinition<V, E, P> {
        SearchDefinition {
            extractor,
            matcher,
            phantom: PhantomData::default()
        }
    }
}

impl<V, E, P> Predicate<OwnedMessage> for SearchDefinition<V, E, P>
where E: MsgExtractor<V>,
      P: Predicate<V> {
    fn matches(&mut self, msg: &OwnedMessage) -> bool {
        match self.extractor.extract(msg) {
            Ok(Some(extracted)) =>
                self.matcher.matches(&extracted),
            _ =>
                false
        }
    }
}