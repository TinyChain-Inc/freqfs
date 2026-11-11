//! Local storage provenance. This marker conveys recovery needs, not host policy.

use std::{fmt, io};

/// A detected inconsistency in stored data or invalidated storage ownership.
/// Ordinary input validation and I/O errors must not acquire this marker.
#[derive(Debug)]
pub struct IntegrityError(io::Error);

impl IntegrityError {
    /// Preserve an error's kind and cause while identifying a local integrity failure.
    pub fn mark(cause: io::Error) -> io::Error {
        if Self::is_marked(&cause) {
            cause
        } else {
            io::Error::new(cause.kind(), Self(cause))
        }
    }

    /// Classify malformed or truncated contents at a required native typed-read boundary.
    /// Missing paths and other I/O errors retain their ordinary classification.
    pub fn from_read(error: io::Error) -> io::Error {
        match error.kind() {
            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => Self::mark(error),
            _ => error,
        }
    }

    /// Inspect native provenance; never infer this property from diagnostic text.
    pub fn is_marked(error: &io::Error) -> bool {
        error.get_ref().is_some_and(|cause| cause.is::<Self>())
    }

    /// Report malformed stored data, retaining the existing InvalidData kind.
    pub fn invalid_data(cause: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
        Self::mark(io::Error::new(io::ErrorKind::InvalidData, cause))
    }
}

impl fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for IntegrityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
