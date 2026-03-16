use blake2::{digest::consts::U32, Blake2b, Digest};

use crate::error::CachekitError;

type Blake2b256 = Blake2b<U32>;

pub fn generate_cache_key(
    namespace: &str,
    function_name: &str,
    serialized_args: &[u8],
) -> Result<String, CachekitError> {
    let key_material = rmp_serde::to_vec(&(function_name, serialized_args))
        .map_err(|e| CachekitError::Serialization(e.to_string()))?;

    let mut hasher = Blake2b256::new();
    hasher.update(&key_material);
    let hash = hasher.finalize();
    let hex_hash = hex::encode(hash);

    Ok(if namespace.is_empty() {
        hex_hash
    } else {
        format!("{namespace}:{hex_hash}")
    })
}
