use rdkafka::Message;
use rdkafka::message::{Headers, OwnedMessage};
use crate::search::{MsgExtractor, PerfectMatchPredicate, SearchDefinition};

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

pub fn match_header(name: &str, expected_value: &str) -> SearchDefinition<String, HeaderStringExtractor, PerfectMatchPredicate<String>> {
    SearchDefinition {
        extractor: HeaderStringExtractor { name: name.to_string() },
        matcher: PerfectMatchPredicate::new(expected_value.to_string()),
        phantom: Default::default()
    }
}