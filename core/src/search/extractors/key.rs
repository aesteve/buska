use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::MsgExtractor;

pub struct KeyExtractor {
    pub key: String
}

impl MsgExtractor<Vec<u8>> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<Vec<u8>> {
        msg.key().map(|k| k.to_vec())
    }
}

impl MsgExtractor<String> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<String> {
        msg.key()
            .map(|bytes|
                String::from_utf8(bytes.to_vec())
                    .expect("Could not ")
            )
    }
}