use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::MsgExtractor;

pub struct PayloadStringExtractor {}

impl MsgExtractor<String> for PayloadStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<String> {
        match msg.payload_view::<str>() {
            Some(Ok(payload)) => Some(payload.to_string()),
            _ => None
        }
    }
}
