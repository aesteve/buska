use rdkafka::Message;
use rdkafka::message::{Headers, OwnedMessage};
use crate::search::extractors::ExtractResult;
use crate::search::MsgExtractor;


/// TODO: NON-STRING HEADER NAMES


#[derive(Debug, Clone)]
pub struct HeaderStringExtractor {
    pub name: String
}

impl MsgExtractor<String> for HeaderStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<String> {
        match msg.headers() {
            None => Ok(None),
            Some(headers) => {
                for idx in 0..headers.count() {
                    if let Some((name, Ok(value))) = headers.get_as::<str>(idx) {
                        if name == self.name {
                            return Ok(Some(value.to_string()))
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}

impl MsgExtractor<Vec<u8>> for HeaderStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> ExtractResult<Vec<u8>> {
        match msg.headers() {
            None => Ok(None),
            Some(headers) => {
                for idx in 0..headers.count() {
                    if let Some((name, value)) = headers.get(idx) {
                        if name == self.name {
                            return Ok(Some(value.to_vec()))
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use crate::search::extractors::ExtractResult;
    use crate::search::extractors::header::HeaderStringExtractor;
    use crate::search::MsgExtractor;
    use crate::tests::TestMessage;

    impl Arbitrary for HeaderStringExtractor {
        fn arbitrary(g: &mut Gen) -> Self {
            HeaderStringExtractor { name: String::arbitrary(g) }
        }
    }

    #[quickcheck]
    fn extracting_from_a_message_with_no_headers_should_return_none_and_never_fail(msg: TestMessage, mut extractor: HeaderStringExtractor) {
        let res: ExtractResult<String> = extractor.extract(&msg.without_headers());
        assert!(res.is_ok(), "Extracting header from a message without headers should never fail");
        assert!(res.unwrap().is_none(), "header Extracting from a message without headers should return None");
    }

    #[quickcheck]
    fn extracting_as_bytes_from_a_message_with_no_headers_should_return_none_and_never_fail(msg: TestMessage, mut extractor: HeaderStringExtractor) {
        let res: ExtractResult<String> = extractor.extract(&msg.without_headers());
        assert!(res.is_ok(), "Extracting header from a message without headers should never fail");
        assert!(res.unwrap().is_none(), "Extracting header from the message without headers should return None");
    }

    #[quickcheck]
    fn the_right_header_is_extracted(msg: TestMessage) {
        let expected_value = "something";
        let name = "the_name";
        let mut extractor = HeaderStringExtractor { name: name.to_string() };
        let res: ExtractResult<String> = extractor.extract(&msg.add_header(name, expected_value));
        assert_eq!(res, Ok(Some(expected_value.to_string())));
    }


}