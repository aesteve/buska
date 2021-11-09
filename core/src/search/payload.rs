use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::{MsgExtractor, SearchDefinition, SearchPredicate};

pub struct PayloadStringExtractor {}

impl MsgExtractor<String> for PayloadStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<String> {
        match msg.payload_view::<str>() {
            Some(Ok(payload)) => Some(payload.to_string()),
            _ => None
        }
    }
}

pub fn payload_matches<P: SearchPredicate<String>>(matcher: P) -> SearchDefinition<String, PayloadStringExtractor, P> {
    SearchDefinition {
        extractor: PayloadStringExtractor {},
        matcher,
        phantom: Default::default()
    }
}