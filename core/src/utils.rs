use rdkafka::message::OwnedMessage;
use crate::search::notifications::SearchNotification;

/// Returns the matched messages out of a list of search notifications
pub fn all_matches<I: IntoIterator<Item = SearchNotification>>(notifications: I) -> Vec<OwnedMessage> {
    notifications
        .into_iter()
        .filter_map(|notification| {
            if let SearchNotification::Match(msg) = notification {
                Some(msg)
            } else {
                None
            }
        })
        .collect()
}