use proc_macro::TokenStream;

/// Proc-macro decorator for caching function results.
/// Stub — full implementation in a future chunk.
#[proc_macro_attribute]
pub fn cache(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Return the item unchanged for now.
    item
}
