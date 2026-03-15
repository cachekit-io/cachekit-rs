use serde::{de::DeserializeOwned, Serialize};

use crate::error::CachekitError;

/// Serialize `value` to MessagePack bytes using named fields (map format).
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, CachekitError> {
    rmp_serde::to_vec_named(value).map_err(|e| CachekitError::Serialization(e.to_string()))
}

/// Deserialize `bytes` from MessagePack into `T`.
pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CachekitError> {
    rmp_serde::from_slice(bytes).map_err(|e| CachekitError::Serialization(e.to_string()))
}
