use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::MsgExtractor;

pub struct PayloadStringExtractor {}

impl MsgExtractor<String> for PayloadStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Result<Option<String>, String> {
        match msg.payload_view::<str>() {
            Some(Err(e)) => Err(format!("Could not extract payload as string. {}", e)),
            Some(Ok(payload)) => Ok(Some(payload.to_string())),
            None => Ok(None),
        }
    }
}
