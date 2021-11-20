use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use crate::search::extractors::ExtractResult;
use crate::search::MsgExtractor;

pub struct KeyExtractor {
    pub key: String
}

impl MsgExtractor<Vec<u8>> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<Vec<u8>> {
        match msg.key() {
            Some(k) => Ok(Some(k.to_vec())),
            None => Ok(None)
        }
    }
}

impl MsgExtractor<String> for KeyExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<String> {
        match msg.key() {
            Some(bytes) =>
                String::from_utf8(bytes.to_vec())
                    .map(Some)
                    .map_err(|e| e.to_string()),
            None =>
                Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::TestMessage;
    use quickcheck_macros::quickcheck;
    use crate::search::extractors::ExtractResult;
    use crate::search::extractors::key::KeyExtractor;
    use crate::search::MsgExtractor;

    #[quickcheck]
    fn extracting_from_a_keyless_message_should_never_fail_and_return_none(msg: TestMessage, key: String) {
        let mut extractor = KeyExtractor { key };
        let res: ExtractResult<String> = extractor.extract(&msg.without_key());
        assert!(res.is_ok(), "Extracting the key from a keyless message should not fail");
        assert!(res.unwrap().is_none(), "Extracting the key from a keyless message should return None");
    }

    #[quickcheck]
    fn extracting_as_vec_from_a_keyless_message_should_never_fail_and_return_none(msg: TestMessage, key: String) {
        let mut extractor = KeyExtractor { key };
        let res: ExtractResult<Vec<u8>> = extractor.extract(&msg.without_key());
        assert!(res.is_ok(), "Extracting the key from a keyless message should not fail");
        assert!(res.unwrap().is_none(), "Extracting the key from a keyless message should return None");
    }

}