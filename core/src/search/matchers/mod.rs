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
