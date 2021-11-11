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


pub(crate) trait MeanExt: Iterator {

    fn mean<M>(self) -> M
        where
            M: Mean<Self::Item>,
            Self: Sized
    {
        M::mean(self)
    }
}

impl<I: Iterator> MeanExt for I {}

pub(crate) trait Mean<A = Self> {
    fn mean<I: Iterator<Item = A>>(iter: I) -> Self;
}

impl Mean for f64 {
    fn mean<I>(iter: I) -> Self
        where
            I: Iterator<Item = f64>,
    {
        let mut sum = 0.0;
        let mut count: usize = 0;
        for v in iter {
            sum += v;
            count += 1;
        }
        if count > 0 {
            sum / (count as f64)
        } else {
            0.0
        }
    }
}

impl<'a> Mean<&'a f64> for f64 {
    fn mean<I>(iter: I) -> Self
        where
            I: Iterator<Item = &'a f64>,
    {
        iter.copied().mean()
    }
}
