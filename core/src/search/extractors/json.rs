// README:
// Unfortunately, the JsonPath struct doesn't seem to be accessible (crate private), that's a real shame since the JSON path will be parsed for every msg resulting in very poor performances
// Best thing would be to parse the path when creating the struct, and storing it
// In order to workaround this restriction, we have used `set_json` and therefore made `MsgExtractor::extract` accept &mut self...

use jsonpath_rust::JsonPathFinder;
use rdkafka::Message;
use rdkafka::message::OwnedMessage;
use serde_json::Value;
use crate::search::MsgExtractor;

pub fn json_single_extract(path: &str) -> Result<JsonPathSingleExtract, String> {
    Ok(JsonPathSingleExtract { path: JsonPathFinder::from_str(r"{}", path)? })
}

pub fn json_multi_extract(path: &str) -> Result<JsonPathMultiExtract, String> {
    Ok(JsonPathMultiExtract { path: JsonPathFinder::from_str(r"{}", path)? })
}

/// Extracts a single JSON value from a record, by using a JSON path definition
/// Example: Extracts `"foo"` from `r"{"field": "foo"}` by using path: `"$.field"`
pub struct JsonPathSingleExtract {
    pub(crate) path: JsonPathFinder
}


/// Extracts multiple JSON values from a record, by using a JSON path definition
/// Example: Extracts "["foo", "bar"]" from `r"{"field": ["foo", "bar"]}"` by using path: `"$.field"`
pub struct JsonPathMultiExtract {
    pub(crate) path: JsonPathFinder
}

impl MsgExtractor<Value> for JsonPathSingleExtract {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<Value> {
        let finder = &mut self.path;
        match msg.payload_view::<str>() {
            Some(Ok(json)) => {
                if let Ok(value) = serde_json::from_str::<Value>(json) {
                    finder.set_json(value);
                    let found = finder.find(); // returns a JSON array
                    found.as_array()
                        .and_then(|values| values.first().cloned())
                } else {
                    None
                }
            },
            _ => None,
        }
    }
}

impl MsgExtractor<Vec<Value>> for JsonPathMultiExtract {
    fn extract(&mut self, msg: &OwnedMessage) -> Option<Vec<Value>> {
        let finder = &mut self.path;
        match msg.payload_view::<str>() {
            Some(Ok(json)) => {
                if let Ok(value) = serde_json::from_str::<Value>(json) {
                    finder.set_json(value);
                    Some(finder.find_slice().into_iter().cloned().collect())
                } else {
                    None
                }
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rdkafka::message::OwnedMessage;
    use rdkafka::{Message, Timestamp};
    use tokio::sync::mpsc;
    use crate::search::bounds::{SearchBounds, SearchEnd, SearchStart};
    use crate::search::extractors::json::json_single_extract;
    use crate::search::matchers::PerfectMatch;
    use crate::search::{MsgExtractor, SearchDefinition};
    use crate::search::notifications::{ProgressNotification, SearchNotification};
    use crate::search_topic;
    use crate::tests::{clean, collect_search_notifications, NestedTestRecord, prepare, produce_json_records, test_cluster_config, TestRecord};
    use crate::utils::all_matches;

    #[test]
    fn test_single_extract() {
        let mut extractor = json_single_extract("$.nested.int").expect("Could not create JSON path");
        let key = "some";
        let i = 4;
        let payload = TestRecord {
            key: key.to_string(),
            nested: NestedTestRecord {
                int: i,
                ints: vec![i],
                string: "some-value".to_string()
            }

        };
        let json_payload = Some(serde_json::to_string(&payload).expect("Could not serialize test record").into_bytes());
        let record = OwnedMessage::new(json_payload, Some(key.as_bytes().to_vec()), "topic".to_string(), Timestamp::CreateTime(Utc::now().timestamp_millis()), 0, 0, None);
        assert_eq!(extractor.extract(&record), Some(serde_json::json!(i)));
    }


    #[test]
    fn test_multi_extract() {
        let mut extractor = json_single_extract("$.nested.ints").expect("Could not create JSON path");
        let key = "some";
        let i = 4;
        let payload = TestRecord {
            key: key.to_string(),
            nested: NestedTestRecord {
                int: i,
                ints: vec![i, i+1],
                string: "some-value".to_string()
            }

        };
        let json_payload = Some(serde_json::to_string(&payload).expect("Could not serialize test record").into_bytes());
        let record = OwnedMessage::new(json_payload, Some(key.as_bytes().to_vec()), "topic".to_string(), Timestamp::CreateTime(Utc::now().timestamp_millis()), 0, 0, None);
        assert_eq!(extractor.extract(&record), Some(serde_json::json!(vec![i, i+1])));
    }


    #[tokio::test]
    async fn test_single_match_from_kafka() {
        env_logger::builder().is_test(true).init();
        let topic = "some_topic";
        prepare(topic).await;
        let records = (1..1000)
            .into_iter()
            .map(|i|TestRecord { key: i.to_string(), nested: NestedTestRecord { int: i, ints: vec![i], string: format!("{}_nested", i) } })
            .collect::<Vec<TestRecord>>();
        produce_json_records(topic, &records).await;
        let conf = test_cluster_config();
        let (sender, mut receiver) = mpsc::channel::<SearchNotification>(1024);
        let bounds = SearchBounds {
            start: SearchStart::Earliest,
            end: SearchEnd::CurrentLast
        };
        let mut tasks = vec![];
        tasks.push(
            tokio::task::spawn(async move {
                collect_search_notifications(&mut receiver).await
            })
        );
        tasks.push(
            tokio::task::spawn(async move {
                let extractor = json_single_extract("$.nested.int").expect("Could not create JSON path");
                let matcher = PerfectMatch::new(serde_json::json!(4));
                let mut search_definition = SearchDefinition::new(extractor, matcher);
                search_topic(conf, topic.to_string(), sender, bounds, &mut search_definition).await;
                vec![]
            })
        );
        let all_notifs: Vec<SearchNotification> = futures::future::join_all(tasks).await
            .into_iter()
            .flat_map(|task_res| task_res.unwrap().to_vec())
            .collect();
        // println!("{:?}", all_notifs);
        let first = &all_notifs[0];
        assert!(matches!(first, SearchNotification::Start), "The first notification received must be the Start notification");

        let finished = all_notifs.last().expect("Could access the last received notification");
        assert!(matches!(finished, SearchNotification::Finish(_)), "The last notification received must be the Finished notification");

        let progresses_received: Vec<ProgressNotification> = all_notifs
            .iter()
            .filter_map(|notification| {
                if let SearchNotification::Progress(p) = notification {
                    Some(p.clone())
                } else {
                    None
                }
            })
            .collect();

        assert!(progresses_received.len() > 1, "Multiple progress notifications must have been received");
        let mut last_nb_read = 0;
        let mut last_nb_matches = 0;
        for progress in progresses_received {
            assert_eq!(topic, progress.topic.as_str());
            assert!(progress.read > last_nb_read, "Progress notifications must have been received in order");
            last_nb_read = progress.read;
            assert!(progress.matches >= last_nb_matches, "Progress notifications must have been received in order");
            last_nb_matches = progress.matches;
        }

        let matches = all_matches(all_notifs);
        assert_eq!(1, matches.len());
        let matched_payload = matches[0].payload_view::<str>()
            .expect("Could not read payload from matched msg")
            .expect("Could not read payload from matched msg");
        let matched: TestRecord = serde_json::from_str(matched_payload).expect("Could not parse JSON matched");
        assert_eq!(4, matched.nested.int);

        clean(topic).await;
    }

}