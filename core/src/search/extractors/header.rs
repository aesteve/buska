use rdkafka::Message;
use rdkafka::message::{Headers, OwnedMessage};
use crate::search::MsgExtractor;


/// TODO: NON-STRING HEADERS


#[derive(Debug, Clone)]
pub struct HeaderStringExtractor {
    pub name: String
}

impl MsgExtractor<String> for HeaderStringExtractor {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<String> {
        msg.headers().and_then(|headers| {
            for idx in 0..headers.count() {
                if let Some((name, Ok(value))) = headers.get_as::<str>(idx) {
                    if name == self.name {
                        return Some(value.to_string())
                    }
                }
            }
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use rdkafka::message::{Headers, OwnedHeaders, OwnedMessage};
    use rdkafka::Message;
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
        let msg = msg.0;
        assert!(extractor.extract(&msg).is_none());
    }

    #[quickcheck]
    fn the_right_header_is_extracted(msg: TestMessage) {
        let expected_value = "something";
        let mut extractor = HeaderStringExtractor { name: "the_name".to_string() };
        let headers =  match msg.0.headers().cloned().as_mut() {
            Some(h) => {
                copy_headers(h)
                    .add(extractor.name.as_str(), expected_value)
            },
            _ =>
                OwnedHeaders::new_with_capacity(1)
                    .add(extractor.name.as_str(), expected_value)
        };
        let msg = OwnedMessage::new(
            msg.0.payload().map(|v| v.to_vec()),
            msg.0.key().map(|v| v.to_vec()),
            msg.0.topic().to_string(),
            msg.0.timestamp(),
            msg.0.partition(),
            msg.0.offset(),
            Some(headers)
        ) ;
        assert_eq!(extractor.extract(&msg), Some(expected_value.to_string()));
    }


    fn copy_headers(h: &OwnedHeaders) -> OwnedHeaders {
        let c = h.count();
        let mut res = OwnedHeaders::new_with_capacity(c);
        for i in 0..c {
            let header = h.get(i).expect("Could not extract header by index");
            res = res.add(header.0, header.1);
        }
        res
    }
}