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

#[cfg(test)]
mod tests {

    use quickcheck_macros::quickcheck;
    use crate::Predicate;
    use crate::search::matchers::{OneOf, PerfectMatch};

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

}
