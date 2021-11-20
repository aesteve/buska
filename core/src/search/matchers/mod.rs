use regex::{Error, Regex};
use serde_json::Value;
use crate::search::Predicate;

pub struct PerfectMatch<T: PartialEq> {
    expected: T
}

impl<T: PartialEq> PerfectMatch<T> {
    pub fn new(t: T) -> Self {
        PerfectMatch { expected: t }
    }
}

impl<T: PartialEq> Predicate<T> for PerfectMatch<T> {
    fn matches(&mut self, t: &T) -> bool {
        *t == self.expected
    }
}

pub struct OneOf<T: Eq> {
    candidates: Vec<T>
}

impl<T: Eq> OneOf<T> {
    pub fn new<I: IntoIterator<Item = T>>(candidates: I) -> Self {
        OneOf { candidates: candidates.into_iter().collect() }
    }
}

impl<T: Eq> Predicate<T> for OneOf<T> {
    fn matches(&mut self, t: &T) -> bool {
        self.candidates.contains(t)
    }
}

pub struct RegexMatch {
    regex: Regex
}

impl RegexMatch {
    pub fn new(str: &str) -> Result<Self, Error> {
        Ok(RegexMatch { regex: Regex::new(str)? })
    }
}

impl Predicate<String> for RegexMatch {
    fn matches(&mut self, t: &String) -> bool {
        self.regex.is_match(t)
    }
}

impl Predicate<Value> for RegexMatch {
    fn matches(&mut self, t: &Value) -> bool {
        if !t.is_string() {
            return false;
        }
        self.regex.is_match(t.as_str().unwrap())
    }
}

impl Predicate<str> for RegexMatch {
    fn matches(&mut self, t: &str) -> bool {
        self.regex.is_match(t)
    }
}

#[cfg(test)]
mod tests {

    use quickcheck_macros::quickcheck;
    use crate::Predicate;
    use crate::search::matchers::{OneOf, PerfectMatch, RegexMatch};

    #[quickcheck]
    fn perfectly_matches_itself(itself: String) {
        assert!(PerfectMatch::new(itself.clone()).matches(&itself));
    }

    #[quickcheck]
    fn empty_vec_never_matches(any_string: String) {
        assert!(!OneOf::new(vec![]).matches(&any_string), "An empty vec should match nothing");
    }

    #[quickcheck]
    fn matches_itself_only(itself: Vec<String>) {
        assert!(OneOf::new(vec![itself.clone()]).matches(&itself), "itself must be one_of vec![itself]");
    }

    #[quickcheck]
    fn matches_itself_with_others(others: Vec<String>, itself: String) {
        let mut candidates = others;
        candidates.push(itself.clone());
        assert!(OneOf::new(candidates).matches(&itself), "itself must be one_of vec![itself]");
    }

    #[quickcheck]
    fn any_string_matches_wildcard_regex(str: String) {
        let mut any_match = RegexMatch::new(".*").expect("Could not create any regex matcher");
        assert!(any_match.matches(&str));
    }

    #[test]
    fn regex_tests() {
        let mut matcher = RegexMatch::new("[0-9]+").expect("Could not create regex matcher");
        assert!(matcher.matches("0"));
        assert!(!matcher.matches("a"));
        let mut matcher = RegexMatch::new("^34(?:6[0-9]|7[1-9])[0-9]{7}$").expect("Could not create regex matcher for phone number");
        assert!(matcher.matches("34751237721"));
        assert!(!matcher.matches("33621297452"));
    }
}
