use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::extractors::ExtractResult;
use crate::search::MsgExtractor;

#[derive(Clone, Debug)]
pub struct PayloadExtractor {}

impl Default for PayloadExtractor {
    fn default() -> Self {
        PayloadExtractor {}
    }
}

impl MsgExtractor<Vec<u8>> for PayloadExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<Vec<u8>> {
        match msg.payload_view::<[u8]>() {
            Some(Err(_)) => Err("Could not extract message payload as desired type".to_string()),
            Some(Ok(payload)) => Ok(Some(payload.to_vec())),
            None => Ok(None),
        }
    }
}

impl MsgExtractor<String> for PayloadExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<String> {
        match msg.payload_view::<str>() {
            Some(Err(_)) => Err("Could not extract message payload as desired type".to_string()),
            Some(Ok(payload)) => Ok(Some(payload.to_string())),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::search::extractors::payload::PayloadExtractor;
    use crate::tests::TestMessage;
    use quickcheck_macros::quickcheck;
    use crate::search::extractors::ExtractResult;
    use crate::search::MsgExtractor;

    #[quickcheck]
    fn extracting_from_a_message_with_no_payload_should_never_fail_and_return_none(msg: TestMessage) {
        let res: ExtractResult<Vec<u8>> = PayloadExtractor::default().extract(&msg.without_payload());
        assert!(res.is_ok(), "Extracting payload as bytes from a message with no payload should never fail");
        assert!(res.unwrap().is_none(), "Extracting payload as bytes from a message with no payload should return none");
    }

    #[quickcheck]
    fn extracting_as_string_from_a_message_with_no_payload_should_never_fail_and_return_none(msg: TestMessage) {
        let res: ExtractResult<String> = PayloadExtractor::default().extract(&msg.without_payload());
        assert!(res.is_ok(), "Extracting payload as string from a message with no payload should never fail");
        assert!(res.unwrap().is_none(), "Extracting payload as string from a message with no payload should return none");
    }

    #[quickcheck]
    fn extracting_as_bytes_should_return_the_payload(msg: TestMessage, payload: String) {
        let res: ExtractResult<Vec<u8>> = PayloadExtractor::default().extract(&msg.withbytes_payload(payload.as_bytes()));
        assert!(res.is_ok(), "Extracting payload as bytes from a message with a string payload should not fail");
        assert_eq!(res.unwrap(), Some(payload.as_bytes().to_vec()), "Extracting payload as bytes from a message with a string payload should return the payload");
    }

    #[quickcheck]
    fn extracting_as_string_should_return_the_payload(msg: TestMessage, payload: String) {
        let res: ExtractResult<String> = PayloadExtractor::default().extract(&msg.withbytes_payload(payload.as_bytes()));
        assert!(res.is_ok(), "Extracting payload as string from a message with a string payload should not fail");
        assert_eq!(res.unwrap(), Some(payload), "Extracting payload as string from a message with a string payload should return the payload");
    }
}