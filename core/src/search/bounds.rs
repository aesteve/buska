use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchStart {
    Earliest,
    Time(DateTime<Utc>)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchEnd {
    CurrentLast,
    Time(DateTime<Utc>),
    Unbounded
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchBounds {
    pub start: SearchStart,
    pub end: SearchEnd
}

impl SearchBounds {
    pub fn from_datetime_to_now(time: DateTime<Utc>) -> SearchBounds {
        SearchBounds {
            start: SearchStart::Time(time),
            end: SearchEnd::CurrentLast
        }
    }

    pub fn from_origin_to_now() -> SearchBounds {
        SearchBounds {
            start: SearchStart::Earliest,
            end: SearchEnd::CurrentLast
        }
    }
}

