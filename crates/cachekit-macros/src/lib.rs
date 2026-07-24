use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    spanned::Spanned,
    Ident, ItemFn, LitInt, LitStr, ReturnType, Token, Type,
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
/// - Every non-client argument must be a plain identifier (no destructuring
///   patterns, no `self`) and must convert into
///   `cachekit::interop::InteropValue` via `From` (`bool`, `i32`/`i64`/
///   `u32`/`u64`/`i128`, `f64`, `&str`/`String`, `Uuid`). Unsupported
///   argument types fail at compile time.
/// - In-model *values* outside the interop ranges fail at CALL time with
///   `CachekitError::Serialization` before the body runs: non-finite floats
///   (`NAN`, `±INFINITY`) and `i128` outside `[-2^63, 2^64-1]`. This
///   fail-loud contract matches the Python and TypeScript SDKs — silently
///   running uncached would mask cross-SDK key divergence.
/// - The client must be built **without** `.namespace()` /
///   `CACHEKIT_NAMESPACE` — interop keys carry their own namespace segment,
///   and reads fail closed on a namespaced client.
/// - A stored entry that cannot be decoded as the return type is treated as
///   a miss and overwritten (self-healing), never an error loop.
///
/// # Requirements (continued)
///
/// - The function must be `async` — the generated body awaits the cache
///   read, the cold-miss single-flight, and SWR refresh scheduling. A sync
///   function is a clear compile-time error, never a silent no-op.
///
/// # Reliability behaviour
///
/// - **Stale-while-revalidate** (client SWR enabled — the default with the
///   `l1` feature on native targets): an L1 hit past the client's freshness
///   threshold (`swr_threshold_ratio` × entry TTL, ±10% jitter) but before
///   hard expiry is returned **immediately** — the caller never blocks on a
///   merely-stale entry — while one background task re-executes the function
///   and rewrites both cache layers. Refresh dedup rides
///   [`CacheKit::single_flight`]: N concurrent stale readers trigger exactly
///   one re-execution per process (and, on lock-capable backends, per
///   fleet). Hard-expired entries always take the normal blocking miss path.
///   The refresh task needs a tokio runtime (skipped otherwise — the stale
///   value was already served) and captures arguments by owned copy
///   (`Clone`/`ToOwned` — already required for key derivation); the future
///   must be `Send`. Refresh failures are absorbed: the stale value keeps
///   serving until hard expiry, where the blocking path surfaces errors.
/// - **Graceful degradation**: on an outage-class backend failure —
///   transient, timeout, or an open circuit breaker — the plain path fails
///   *open*: the function executes uncached and its result is returned.
///   Permanent and authentication errors propagate even on the plain path
///   (a wrong API key must fail loudly, not silently disable caching
///   forever). With `secure`, *every* backend and decryption error fails
///   *closed* and propagates: an encrypted workload never silently
///   degrades.
/// - **Cold-miss single-flight**: concurrent calls that miss on the same
///   key are collapsed to one execution per process (and per fleet, when
///   the backend supports distributed fill locks — CachekitIO and Redis do,
///   with the `reliability` feature). Waiters re-read the filled entry
///   instead of recomputing. See `cachekit::flight`.
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

    // The generated body awaits (cache reads, single-flight, SWR refresh
    // scheduling); a sync function cannot drive any of it. Fail with a clear
    // decoration-time error instead of a cascade of "await is only allowed
    // inside async functions" — no silent no-op (the sibling SDK posture).
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            func.sig.fn_token,
            "#[cachekit] requires an `async fn`: the cache read, cold-miss single-flight, \
             and stale-while-revalidate background refresh all await — annotate the \
             function `async` (a sync function cannot be cached by this macro)",
        ));
    }

    // Extract the Ok type from Result<T, E>
    let ok_type = extract_ok_type(&func.sig.output)?;

    // Collect non-client parameters (ident + type) for cache key derivation
    // and SWR refresh capture. Every parameter MUST contribute to the key — a
    // silently skipped parameter means two different calls share one cache
    // entry (wrong-data collision), so anything we can't name is a compile
    // error, never a skip.
    let mut non_client_params: Vec<(&Ident, &Type)> = Vec::new();
    for arg in &func.sig.inputs {
        match arg {
            syn::FnArg::Receiver(recv) => {
                return Err(syn::Error::new_spanned(
                    recv,
                    "#[cachekit] does not support methods: a `self` receiver cannot \
                     contribute to the interop/v1 cache key — wrap a free function instead",
                ));
            }
            syn::FnArg::Typed(pat) => match pat.pat.as_ref() {
                syn::Pat::Ident(pi) => {
                    if pi.ident != *client_ident {
                        non_client_params.push((&pi.ident, pat.ty.as_ref()));
                    }
                }
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "#[cachekit] parameters must be plain identifiers: destructuring \
                         patterns cannot contribute to the cache key, and skipping them \
                         would make different calls share one cache entry",
                    ));
                }
            },
        }
    }
    let non_client_idents: Vec<&Ident> = non_client_params.iter().map(|(id, _)| *id).collect();

    // Convert each non-client argument to an InteropValue. `.clone()` keeps
    // the original binding usable by the function body; for Copy types it
    // compiles to a copy. Types outside the interop data model fail here at
    // COMPILE time with a missing `From<T> for InteropValue` — that is the
    // contract, not a bug: interop/v1 keys only hash modelable values.
    // quote_spanned pins that error to the offending parameter, not the
    // attribute.
    let interop_args = non_client_idents.iter().map(|id| {
        quote_spanned! {id.span()=> cachekit::interop::InteropValue::from(#id.clone()) }
    });

    // Generate cache get/set calls depending on `secure` flag.
    let (get_expr, get_swr_expr, set_expr) = if args.secure {
        (
            quote! {
                {
                    let __ck_sec = #client_ident.secure()?;
                    __ck_sec.interop_get::<#ok_type>(&__ck_key).await
                }
            },
            quote! {
                {
                    let __ck_sec = #client_ident.secure()?;
                    __ck_sec.interop_get_swr::<#ok_type>(&__ck_key).await
                }
            },
            quote! {
                let __ck_sec = #client_ident.secure()?;
                let _ = __ck_sec.set_with_ttl(&__ck_key, __ck_val, std::time::Duration::from_secs(#ttl_secs)).await;
            },
        )
    } else {
        (
            quote! { #client_ident.interop_get::<#ok_type>(&__ck_key).await },
            quote! { #client_ident.interop_get_swr::<#ok_type>(&__ck_key).await },
            quote! {
                let _ = #client_ident.set_with_ttl(&__ck_key, __ck_val, std::time::Duration::from_secs(#ttl_secs)).await;
            },
        )
    };

    // SWR refresh capture: the background task is `'static`, so every
    // argument is re-materialised as an owned value before the spawn and
    // rebound under its original name (and, for references, its original
    // shape) inside the task — the function body runs unchanged:
    //   - `&str`     → captured as `String`, rebound via `.as_str()` (exact)
    //   - other `&T` → captured as `T` via `ToOwned`, rebound as `&owned`
    //   - owned `T`  → captured via `Clone`, rebound by value
    let mut swr_captures = TokenStream2::new();
    let mut swr_rebinds = TokenStream2::new();
    for (i, (id, ty)) in non_client_params.iter().enumerate() {
        let holder = quote::format_ident!("__ck_swr_arg_{i}");
        match ty {
            Type::Reference(r) => {
                // The refresh task re-executes the body from an owned
                // snapshot rebound as an immutable borrow — mutation through
                // a `&mut` argument cannot be preserved, so reject it up
                // front instead of surfacing a confusing downstream error.
                if r.mutability.is_some() {
                    return Err(syn::Error::new_spanned(
                        ty,
                        "#[cachekit] does not support `&mut` parameters: the SWR background \
                         refresh re-executes the function from an owned snapshot, so mutation \
                         through the reference cannot be preserved — take the argument by \
                         owned value instead",
                    ));
                }
                swr_captures.extend(quote_spanned! {id.span()=>
                    let #holder = ::std::borrow::ToOwned::to_owned(#id);
                });
                let is_str = matches!(r.elem.as_ref(),
                    Type::Path(p) if p.path.is_ident("str"));
                if is_str {
                    swr_rebinds.extend(quote! { let #id = #holder.as_str(); });
                } else {
                    swr_rebinds.extend(quote! { let #id = &#holder; });
                }
            }
            _ => {
                swr_captures.extend(quote_spanned! {id.span()=>
                    #[allow(clippy::clone_on_copy)]
                    let #holder = #id.clone();
                });
                swr_rebinds.extend(quote! { let #id = #holder; });
            }
        }
    }

    // Graceful degradation (LAB-518): on an OUTAGE-class backend failure —
    // retryable (transient/timeout) or a fast-failing open circuit breaker —
    // the plain path fails OPEN: the wrapped function runs uncached, so a
    // cache outage costs performance, not availability. Permanent and
    // authentication errors PROPAGATE even on the plain path: a wrong API
    // key that silently fell open would run uncached forever with zero
    // signal while looking healthy (expert-panel finding). The `secure`
    // path stays fail-CLOSED on everything: backend and decryption errors
    // reach the caller, so an encrypted workload never silently degrades.
    let fail_open_arm = if args.secure {
        quote! {}
    } else {
        quote! {
            Err(cachekit::error::CachekitError::Backend(__ck_be))
                if __ck_be.kind.is_retryable()
                    || matches!(
                        __ck_be.kind,
                        cachekit::error::BackendErrorKind::CircuitOpen
                    ) => {}
        }
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
            // The generated `.clone()` per argument keeps the binding usable
            // by the body; for Copy types it is a copy and for `&str` it is
            // a deliberate reference copy — hence the allows.
            #[allow(clippy::clone_on_copy, noop_method_call)]
            let __ck_key = cachekit::interop::interop_key(
                #namespace,
                #operation,
                &[#(#interop_args),*],
            )?;

            // Try cache hit, classified against the stale-while-revalidate
            // freshness window. A Serialization error means the stored entry
            // cannot be decoded as #ok_type (poisoned, foreign-shaped, or a
            // Python-internal CK frame) — treat it as a miss so the fresh
            // result OVERWRITES it, instead of hard-failing every call until
            // TTL expiry. Backend errors fail open on the plain path (run
            // the function uncached) and fail closed on the secure path.
            // Other errors (namespaced-client Config, decryption) propagate.
            match #get_swr_expr {
                Ok(cachekit::SwrRead::Fresh(__ck_cached)) => return Ok(__ck_cached),
                // Stale (past the freshness threshold, before hard expiry):
                // serve the cached value immediately and schedule ONE
                // background refresh. Dedup rides the same single-flight as
                // the cold-miss path: the first refresh task leads and
                // re-executes the function; concurrent tasks queue, see the
                // (still-present) entry, and exit without computing. Refresh
                // failures are deliberately absorbed — the stale value keeps
                // being served, a later stale read retries, and hard expiry
                // falls through to the blocking path where errors surface.
                Ok(cachekit::SwrRead::Stale(__ck_cached)) => {
                    let __ck_swr_client = ::std::clone::Clone::clone(#client_ident);
                    let __ck_swr_key = __ck_key.clone();
                    #swr_captures
                    cachekit::__swr_spawn(async move {
                        let #client_ident = &__ck_swr_client;
                        let __ck_key = __ck_swr_key;
                        #swr_rebinds
                        let _: ::std::result::Result<(), cachekit::error::CachekitError> = async {
                            let mut __ck_flight = #client_ident.single_flight(&__ck_key).await;
                            while __ck_flight.wait_for_fill().await {
                                if matches!(#get_expr, Ok(Some(_))) {
                                    // Another worker is already on it (the
                                    // stale entry is still present, or the
                                    // leader has refreshed) — stand down.
                                    __ck_flight.release().await;
                                    return Ok(());
                                }
                            }
                            let __ck_result: #ret_ty = (async #original_body).await;
                            if let Ok(ref __ck_val) = __ck_result {
                                #set_expr
                            }
                            __ck_flight.release().await;
                            __ck_result.map(|_| ())
                        }
                        .await;
                    });
                    return Ok(__ck_cached);
                }
                Ok(cachekit::SwrRead::Miss) => {}
                Err(cachekit::error::CachekitError::Serialization(_)) => {}
                #fail_open_arm
                Err(__ck_err) => return Err(__ck_err.into()),
            }

            // Cold-miss single-flight: collapse concurrent fills of this key
            // to one execution (misses are billable). While another worker is
            // filling, re-check the cache instead of recomputing.
            let mut __ck_flight = #client_ident.single_flight(&__ck_key).await;
            while __ck_flight.wait_for_fill().await {
                match #get_expr {
                    Ok(Some(__ck_cached)) => {
                        __ck_flight.release().await;
                        return Ok(__ck_cached);
                    }
                    Ok(None) => {}
                    Err(cachekit::error::CachekitError::Serialization(_)) => {}
                    #fail_open_arm
                    Err(__ck_err) => {
                        __ck_flight.release().await;
                        return Err(__ck_err.into());
                    }
                }
            }

            // Execute original function body
            let __ck_result: #ret_ty = (async #original_body).await;

            // Cache on success
            if let Ok(ref __ck_val) = __ck_result {
                #set_expr
            }

            __ck_flight.release().await;
            __ck_result
        }
    };

    *func.block = new_body;
    Ok(quote! { #func })
}

#[cfg(test)]
mod tests {
    use super::{expand, segment_is_valid, MacroArgs};

    /// A sync fn must fail at decoration time with a clear, actionable error
    /// — never a cascade of "await is only allowed inside async" diagnostics,
    /// and never a silent no-op (LAB-728 acceptance criterion).
    #[test]
    fn sync_fn_is_a_clear_decoration_time_error() {
        let args: MacroArgs = syn::parse_quote!(
            client = cache,
            ttl = 60,
            interop = "get_user",
            namespace = "users"
        );
        let func: syn::ItemFn = syn::parse_quote! {
            fn get_user(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
                Ok(format!("User {id}"))
            }
        };
        let err = expand(&args, func).expect_err("sync fn must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("requires an `async fn`"),
            "error must name the fix, got: {msg}"
        );
    }

    /// `&mut` parameters are rejected at decoration time: the SWR refresh
    /// task rebinds references as immutable borrows of an owned snapshot,
    /// so mutation through the argument cannot be modelled.
    #[test]
    fn mut_ref_param_is_a_clear_decoration_time_error() {
        let args: MacroArgs = syn::parse_quote!(
            client = cache,
            ttl = 60,
            interop = "get_user",
            namespace = "users"
        );
        let func: syn::ItemFn = syn::parse_quote! {
            async fn get_user(cache: &CacheKit, id: &mut u64) -> Result<String, CachekitError> {
                Ok(format!("User {id}"))
            }
        };
        let err = expand(&args, func).expect_err("&mut param must be rejected");
        assert!(
            err.to_string().contains("`&mut` parameters"),
            "error must name the constraint, got: {err}"
        );
    }

    /// The same function with `async` added expands cleanly — proving the
    /// rejection above is precisely about asyncness.
    #[test]
    fn async_fn_expands() {
        let args: MacroArgs = syn::parse_quote!(
            client = cache,
            ttl = 60,
            interop = "get_user",
            namespace = "users"
        );
        let func: syn::ItemFn = syn::parse_quote! {
            async fn get_user(cache: &CacheKit, id: u64) -> Result<String, CachekitError> {
                Ok(format!("User {id}"))
            }
        };
        expand(&args, func).expect("async fn must expand");
    }

    /// Mirror of interop.rs's validate_segment acceptance table — the two
    /// implementations must not drift (see the NOTE on validate_segment).
    #[test]
    fn segment_grammar_table() {
        for ok in [
            "a",
            "0",
            "users",
            "users.fetch_by_id",
            "a-b_c.d",
            &"a".repeat(64),
        ] {
            assert!(segment_is_valid(ok), "{ok:?} should be accepted");
        }
        for bad in [
            "",
            ".users",
            "-users",
            "_users",
            "Users",
            "users\n",
            "users:x",
            "users/x",
            "usérs",
            &"a".repeat(65),
        ] {
            assert!(!segment_is_valid(bad), "{bad:?} should be rejected");
        }
    }
}
