//! Shared scope-tracking plumbing for the AST visitors.
//!
//! Every visitor that needs name resolution embeds a `ScopeStack` and uses the
//! `scoped_visits!` macro to generate the `Visit` overrides that push and pop
//! scopes in a fixed pre-order. The *collector* creates scopes in that order;
//! the analyzer and rewriter walk the same order and refer to scopes by
//! ordinal, so the traversal order must never diverge between them.

use syn::Type;

/// Name of the self type of an impl block (`impl Foo<T>` -> `Foo`).
pub fn self_type_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(tp) => tp.path.segments.last().map(|s| s.ident.to_string()),
        Type::Reference(r) => self_type_name(&r.elem),
        Type::Paren(p) => self_type_name(&p.elem),
        Type::Group(g) => self_type_name(&g.elem),
        _ => None,
    }
}

/// Generates the scope push/pop overrides. The visitor must provide:
///
/// * `fn enter(&mut self, kind: ScopeKind, name: Option<String>, generics: &Generics)`
/// * `fn leave(&mut self)`
/// * `fn push_impl_self(&mut self, name: Option<String>)` / `fn pop_impl_self(&mut self)`
/// * inner visit fns named `inner_<node>` for each node type.
#[macro_export]
macro_rules! scoped_visits {
    () => {
        fn visit_item_struct(&mut self, n: &'ast syn::ItemStruct) {
            self.enter($crate::db::ScopeKind::Type, Some(n.ident.to_string()), &n.generics);
            self.inner_item_struct(n);
            self.leave();
        }
        fn visit_item_enum(&mut self, n: &'ast syn::ItemEnum) {
            self.enter($crate::db::ScopeKind::Type, Some(n.ident.to_string()), &n.generics);
            self.inner_item_enum(n);
            self.leave();
        }
        fn visit_item_union(&mut self, n: &'ast syn::ItemUnion) {
            self.enter($crate::db::ScopeKind::Type, Some(n.ident.to_string()), &n.generics);
            self.inner_item_union(n);
            self.leave();
        }
        fn visit_item_type(&mut self, n: &'ast syn::ItemType) {
            self.enter($crate::db::ScopeKind::Type, Some(n.ident.to_string()), &n.generics);
            self.inner_item_type(n);
            self.leave();
        }
        fn visit_item_trait(&mut self, n: &'ast syn::ItemTrait) {
            self.enter($crate::db::ScopeKind::Trait, Some(n.ident.to_string()), &n.generics);
            self.inner_item_trait(n);
            self.leave();
        }
        fn visit_item_trait_alias(&mut self, n: &'ast syn::ItemTraitAlias) {
            self.enter($crate::db::ScopeKind::Type, Some(n.ident.to_string()), &n.generics);
            self.inner_item_trait_alias(n);
            self.leave();
        }
        fn visit_item_fn(&mut self, n: &'ast syn::ItemFn) {
            self.enter($crate::db::ScopeKind::Fn, Some(n.sig.ident.to_string()), &n.sig.generics);
            self.inner_item_fn(n);
            self.leave();
        }
        fn visit_item_impl(&mut self, n: &'ast syn::ItemImpl) {
            self.push_impl_self($crate::walk::self_type_name(&n.self_ty));
            self.enter($crate::db::ScopeKind::Impl, None, &n.generics);
            self.inner_item_impl(n);
            self.leave();
            self.pop_impl_self();
        }
        fn visit_impl_item_fn(&mut self, n: &'ast syn::ImplItemFn) {
            self.enter($crate::db::ScopeKind::Fn, Some(n.sig.ident.to_string()), &n.sig.generics);
            self.inner_impl_item_fn(n);
            self.leave();
        }
        fn visit_trait_item_fn(&mut self, n: &'ast syn::TraitItemFn) {
            self.enter($crate::db::ScopeKind::Fn, Some(n.sig.ident.to_string()), &n.sig.generics);
            self.inner_trait_item_fn(n);
            self.leave();
        }
        fn visit_impl_item_type(&mut self, n: &'ast syn::ImplItemType) {
            self.enter($crate::db::ScopeKind::AssocType, Some(n.ident.to_string()), &n.generics);
            self.inner_impl_item_type(n);
            self.leave();
        }
        fn visit_trait_item_type(&mut self, n: &'ast syn::TraitItemType) {
            self.enter($crate::db::ScopeKind::AssocType, Some(n.ident.to_string()), &n.generics);
            self.inner_trait_item_type(n);
            self.leave();
        }
        fn visit_item_macro(&mut self, n: &'ast syn::ItemMacro) {
            if n.mac.path.is_ident("macro_rules") {
                return;
            }
            self.visit_macro(&n.mac);
        }
        fn visit_macro(&mut self, mac: &'ast syn::Macro) {
            let key = $crate::spans::range_of(mac).start;
            if let Some(exprs) = self.macro_exprs().get(&key) {
                for e in exprs {
                    self.visit_expr(e);
                }
            } else {
                self.macro_fallback(mac);
            }
        }
    };
}

/// Pre-parse every macro invocation body as a comma-separated expression list
/// (works for `assert!`, `assert_eq!`, `vec!`, `format!`-style macros and
/// most others). Keyed by the macro's start byte offset.
pub fn preparse_macros(file: &syn::File) -> std::collections::HashMap<usize, Vec<syn::Expr>> {
    use syn::visit::Visit;
    struct V(std::collections::HashMap<usize, Vec<syn::Expr>>);
    impl<'ast> Visit<'ast> for V {
        fn visit_item_macro(&mut self, n: &'ast syn::ItemMacro) {
            if n.mac.path.is_ident("macro_rules") {
                return;
            }
            syn::visit::visit_item_macro(self, n);
        }
        fn visit_macro(&mut self, mac: &'ast syn::Macro) {
            use syn::punctuated::Punctuated;
            if let Ok(p) = mac.parse_body_with(Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated) {
                let exprs: Vec<syn::Expr> = p.into_iter().collect();
                self.0.insert(crate::spans::range_of(mac).start, exprs);
            }
        }
    }
    let mut v = V(Default::default());
    v.visit_file(file);
    v.0
}

/// Generates default `inner_*` delegations for the nodes listed.
#[macro_export]
macro_rules! default_inner {
    ($($inner:ident => $visit:ident : $node:ty),* $(,)?) => {
        $(
            fn $inner(&mut self, n: &'ast $node) {
                syn::visit::$visit(self, n);
            }
        )*
    };
}
