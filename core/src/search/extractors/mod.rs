pub mod header;
pub mod json;
pub mod key;
pub mod payload;

/// A type alias for types returned by extractors
pub type ExtractResult<T> = Result<Option<T>, String>;