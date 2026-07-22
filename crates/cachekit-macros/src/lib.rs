use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Ident, ItemFn, LitInt, LitStr, ReturnType, Token, Type,
};

// ── Attribute argument parsing ───────────────────────────────────────────────

struct MacroArgs {
    client: Ident,
    ttl: u64,
    interop: String,
    namespace: String,
    secure: bool,
}

/// Compile-time mirror of the interop/v1 segment grammar
/// (`^[a-z0-9][a-z0-9._-]{0,63}$` — see `cachekit::interop`'s
/// `validate_segment`, the canonical implementation). Duplicated because a
/// proc-macro crate cannot depend on the runtime crate; `interop_key`
/// re-validates at runtime, so drift fails loudly, never silently.
fn segment_is_valid(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    matches!(bytes.first(), Some(b) if b.is_ascii_lowercase() || b.is_ascii_digit())
        && bytes.len() <= 64
        && bytes[1..].iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

/// Extract and validate an interop segment from a string literal, spanning
/// the error to the literal.
fn parse_segment(kind: &str, lit: &LitStr) -> syn::Result<String> {
    let value = lit.value();
    if segment_is_valid(&value) {
        Ok(value)
    } else {
        Err(syn::Error::new(
            lit.span(),
            format!(
                "`{kind}` {value:?} is not a valid interop/v1 key segment: must match \
                 ^[a-z0-9][a-z0-9._-]{{0,63}}$ (lowercase ASCII letters, digits, '.', '_', \
                 '-'; 1-64 chars)"
            ),
        ))
    }
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut client: Option<Ident> = None;
        let mut ttl: Option<u64> = None;
        let mut interop: Option<String> = None;
        let mut namespace: Option<String> = None;
        let mut secure = false;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "client" => {
                    input.parse::<Token![=]>()?;
                    client = Some(input.parse()?);
                }
                "ttl" => {
                    input.parse::<Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    ttl = Some(lit.base10_parse()?);
                }
                "interop" => {
                    input.parse::<Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    interop = Some(parse_segment("interop", &lit)?);
                }
                "namespace" => {
                    input.parse::<Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    namespace = Some(parse_segment("namespace", &lit)?);
                }
                "secure" => {
                    secure = true;
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown attribute `{other}`"),
                    ));
                }
            }

            // Consume trailing comma if present
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        let client = client
            .ok_or_else(|| input.error("`client` is required: #[cachekit(client = name, ...)]"))?;
        let ttl = ttl.ok_or_else(|| {
            input.error("`ttl` is required: #[cachekit(client = name, ttl = 60)]")
        })?;
        let interop = interop.ok_or_else(|| {
            input.error(
                "`interop` is required: #[cachekit(client = name, ttl = 60, interop = \
                 \"get_user\", namespace = \"users\")] — it names the operation segment of the \
                 interop/v1 cache key `{namespace}:{operation}:{args_hash}`, and the spec \
                 requires it to be explicit (protocol spec/interop-mode.md, SDK requirement 1)",
            )
        })?;
        let namespace = namespace.ok_or_else(|| {
            input.error(
                "`namespace` is required: #[cachekit(client = name, ttl = 60, interop = \
                 \"get_user\", namespace = \"users\")] — cache keys are interop/v1 \
                 `{namespace}:{operation}:{args_hash}` and the namespace segment cannot be empty",
            )
        })?;

        Ok(MacroArgs {
            client,
            ttl,
            interop,
            namespace,
            secure,
        })
    }
}

// ── Return-type extraction ───────────────────────────────────────────────────

/// Extract `T` from `Result<T, E>` in a function's return type.
fn extract_ok_type(ret: &ReturnType) -> syn::Result<Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                ret,
                "#[cachekit] function must return Result<T, CachekitError>",
            ));
        }
    };

    // Walk through the type to find Result<T, E> and extract T.
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            if seg.ident != "Result" {
                return Err(syn::Error::new_spanned(
                    ty,
                    "#[cachekit] function must return Result<T, CachekitError>",
                ));
            }
            if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                if let Some(syn::GenericArgument::Type(first)) = args.args.first() {
                    return Ok(first.clone());
                }
            }
        }
    }

    Err(syn::Error::new_spanned(
        ty,
        "#[cachekit] function must return Result<T, CachekitError>",
    ))
}

// ── Proc-macro entry point ──────────────────────────────────────────────────

/// Cache the result of an async function under an interop/v1 cache key.
///
/// Keys are `{namespace}:{operation}:{args_hash}` per the protocol's
/// interop-mode spec, so the same entry is addressable from the Python and
/// TypeScript SDKs — Python's `@cache(interop="get_user",
/// namespace="users")` computes the identical key for the same arguments.
///
/// # Attributes
///
/// - `client = <ident>` (required): Name of the `&CacheKit` parameter.
/// - `ttl = <integer>` (required): TTL in seconds.
/// - `interop = "<string>"` (required): explicit, language-neutral operation
///   segment (`^[a-z0-9][a-z0-9._-]{0,63}$`) — same meaning as Python's
///   `interop=` / TypeScript's `interop:`.
/// - `namespace = "<string>"` (required): interop/v1 namespace segment,
///   same grammar.
/// - `secure` (optional flag): Use encrypted cache via `cache.secure()`.
///
/// # Requirements
///
/// - Every non-client argument must convert into
///   `cachekit::interop::InteropValue` (integers, floats, `bool`, strings,
///   `Uuid`, …). Out-of-model argument types fail at compile time.
/// - The client must be built **without** `.namespace()` /
///   `CACHEKIT_NAMESPACE` — interop keys carry their own namespace segment,
///   and reads fail closed on a namespaced client.
///
/// # Example
///
/// ```ignore
/// #[cachekit(client = cache, ttl = 60, interop = "get_user", namespace = "users")]
/// async fn get_user(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
///     Ok(User { name: format!("User {id}") })
/// }
/// ```
#[proc_macro_attribute]
pub fn cachekit(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as MacroArgs);
    let input_fn = parse_macro_input!(item as ItemFn);

    match expand(&args, input_fn) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(args: &MacroArgs, mut func: ItemFn) -> syn::Result<TokenStream2> {
    let client_ident = &args.client;
    let ttl_secs = args.ttl;
    let namespace = args.namespace.as_str();
    let operation = args.interop.as_str();

    // Extract the Ok type from Result<T, E>
    let ok_type = extract_ok_type(&func.sig.output)?;

    // Collect non-client parameter idents for cache key serialization.
    let non_client_idents: Vec<&Ident> = func
        .sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(pi) = pat.pat.as_ref() {
                    if pi.ident != *client_ident {
                        return Some(&pi.ident);
                    }
                }
            }
            None
        })
        .collect();

    // Convert each non-client argument to an InteropValue. `.clone()` keeps
    // the original binding usable by the function body; for Copy types it
    // compiles to a copy. Types outside the interop data model fail here at
    // COMPILE time with a missing `From<T> for InteropValue` — that is the
    // contract, not a bug: interop/v1 keys only hash modelable values.
    let interop_args = non_client_idents
        .iter()
        .map(|id| quote! { cachekit::interop::InteropValue::from(#id.clone()) });

    // Generate cache get/set calls depending on `secure` flag.
    let (get_expr, set_expr) = if args.secure {
        (
            quote! {
                {
                    let __ck_sec = #client_ident.secure()?;
                    __ck_sec.interop_get::<#ok_type>(&__ck_key).await?
                }
            },
            quote! {
                let __ck_sec = #client_ident.secure()?;
                let _ = __ck_sec.set_with_ttl(&__ck_key, __ck_val, std::time::Duration::from_secs(#ttl_secs)).await;
            },
        )
    } else {
        (
            quote! { #client_ident.interop_get::<#ok_type>(&__ck_key).await? },
            quote! {
                let _ = #client_ident.set_with_ttl(&__ck_key, __ck_val, std::time::Duration::from_secs(#ttl_secs)).await;
            },
        )
    };

    // Capture the original function body.
    let original_body = &func.block;

    // Extract the raw return type (without `->` arrow) for type annotation.
    let ret_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "#[cachekit] function must return Result<T, CachekitError>",
            ));
        }
    };

    // Build the new function body.
    let new_body: syn::Block = syn::parse_quote! {
        {
            // interop/v1 key: {namespace}:{operation}:{args_hash} — the same
            // key is computable from any SDK (protocol spec/interop-mode.md).
            let __ck_key = cachekit::interop::interop_key(
                #namespace,
                #operation,
                &[#(#interop_args),*],
            )?;

            // Try cache hit
            if let Some(__ck_cached) = #get_expr {
                return Ok(__ck_cached);
            }

            // Execute original function body
            let __ck_result: #ret_ty = (async #original_body).await;

            // Cache on success
            if let Ok(ref __ck_val) = __ck_result {
                #set_expr
            }

            __ck_result
        }
    };

    *func.block = new_body;
    Ok(quote! { #func })
}
