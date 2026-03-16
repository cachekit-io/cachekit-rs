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
    namespace: Option<String>,
    secure: bool,
}

impl Parse for MacroArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut client: Option<Ident> = None;
        let mut ttl: Option<u64> = None;
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
                "namespace" => {
                    input.parse::<Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    namespace = Some(lit.value());
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

        Ok(MacroArgs {
            client,
            ttl,
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

/// Cache the result of an async function.
///
/// # Attributes
///
/// - `client = <ident>` (required): Name of the `&CacheKit` parameter.
/// - `ttl = <integer>` (required): TTL in seconds.
/// - `namespace = "<string>"` (optional): Namespace prefix for the cache key.
/// - `secure` (optional flag): Use encrypted cache via `cache.secure()`.
///
/// # Example
///
/// ```ignore
/// #[cachekit(client = cache, ttl = 60)]
/// async fn get_user(cache: &CacheKit, id: u64) -> Result<User, CachekitError> {
///     Ok(User { name: format!("User {id}") })
/// }
/// ```
#[proc_macro_attribute]
pub fn cachekit(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as MacroArgs);
    let input_fn = parse_macro_input!(item as ItemFn);

    match expand(args, input_fn) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(args: MacroArgs, mut func: ItemFn) -> syn::Result<TokenStream2> {
    let client_ident = &args.client;
    let ttl_secs = args.ttl;
    let namespace = args.namespace.as_deref().unwrap_or("");
    let fn_name = func.sig.ident.to_string();

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

    // Build the tuple expression for serialization: (&arg1, &arg2, ...)
    let args_tuple = if non_client_idents.is_empty() {
        quote! { () }
    } else {
        let refs = non_client_idents.iter().map(|id| quote! { &#id });
        quote! { (#(#refs,)*) }
    };

    // Generate cache get/set calls depending on `secure` flag.
    let (get_expr, set_expr) = if args.secure {
        (
            quote! {
                {
                    let __ck_sec = #client_ident.secure()?;
                    __ck_sec.get::<#ok_type>(&__ck_key).await?
                }
            },
            quote! {
                let __ck_sec = #client_ident.secure()?;
                let _ = __ck_sec.set_with_ttl(&__ck_key, __ck_val, std::time::Duration::from_secs(#ttl_secs)).await;
            },
        )
    } else {
        (
            quote! { #client_ident.get::<#ok_type>(&__ck_key).await? },
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
            // Serialize non-client args for cache key generation
            let __ck_args = cachekit::__private::rmp_serde::to_vec(&#args_tuple)
                .map_err(|e| cachekit::error::CachekitError::Serialization(e.to_string()))?;
            let __ck_key = cachekit::key::generate_cache_key(#namespace, #fn_name, &__ck_args)?;

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
