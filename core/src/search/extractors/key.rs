use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::MsgExtractor;

pub struct KeyExtractor {
    pub key: String
}

impl MsgExtractor<Vec<u8>> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Result<Option<Vec<u8>>, String> {
        match msg.key() {
            Some(k) => Ok(Some(k.to_vec())),
            None => Ok(None)
        }
    }
}

impl MsgExtractor<String> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Result<Option<String>, String> {
        match msg.key() {
            Some(bytes) =>
                String::from_utf8(bytes.to_vec())
                    .map(|str| Some(str))
                    .map_err(|e| e.to_string()),
            None =>
                Ok(None)
        }
    }
}