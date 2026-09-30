//! Global analysis: collect scopes and run the classification fixpoint.

use std::collections::{HashMap, HashSet};
use quote::ToTokens;
use syn::visit::Visit;
use syn::{Expr, Generics, Path, QSelf, Type};

use crate::db::{
    bounds_have_ethspec, scope_from_generics, Db, FileScopes, LookupKind, ScopeId, ScopeKind,
    ScopeStack, StructInfo,
};
use crate::specmap;
use crate::{default_inner, scoped_visits};

// ---------------------------------------------------------------------------
// Pass 0: collect scopes
// ---------------------------------------------------------------------------

struct Collector<'ast> {
    scopes: FileScopes,
    impl_depth: usize,
    macros: &'ast MacroExprs,
    source: &'ast str,
    spec_traits: &'ast HashSet<String>,
}

pub type MacroExprs = HashMap<usize, Vec<Expr>>;

impl<'ast> Collector<'ast> {
    fn inner_item_struct(&mut self, n: &'ast syn::ItemStruct) {
        let base = n.ident.to_string();
        let mut aliases: Vec<String> = superstruct_variants(&n.attrs)
            .into_iter()
            .map(|v| format!("{base}{v}"))
            .collect();
        if !aliases.is_empty() {
            aliases.push(format!("{base}Ref"));
            aliases.push(format!("{base}RefMut"));
        }
        let phantom = phantom_params(n.fields.to_token_stream());
        let spec_traits = self.spec_traits;
        if let Some(scope) = self.scopes.scopes.last_mut() {
            scope.aliases = aliases;
            for p in &mut scope.params {
                p.flags.phantom_any = phantom.contains(&p.name);
                p.flags.spec_bound = is_spec_bound(p, spec_traits);
            }
        }
        syn::visit::visit_item_struct(self, n);
    }
    fn inner_item_enum(&mut self, n: &'ast syn::ItemEnum) {
        let fields: proc_macro2::TokenStream = n.variants.iter().map(|v| v.fields.to_token_stream()).collect();
        let phantom = phantom_params(fields);
        let spec_traits = self.spec_traits;
        if let Some(scope) = self.scopes.scopes.last_mut() {
            for p in &mut scope.params {
                p.flags.phantom_any = phantom.contains(&p.name);
                p.flags.spec_bound = is_spec_bound(p, spec_traits);
            }
        }
        syn::visit::visit_item_enum(self, n);
    }
    default_inner!(
        inner_item_union => visit_item_union: syn::ItemUnion,
        inner_item_type => visit_item_type: syn::ItemType,
        inner_item_trait => visit_item_trait: syn::ItemTrait,
        inner_item_trait_alias => visit_item_trait_alias: syn::ItemTraitAlias,
        inner_item_fn => visit_item_fn: syn::ItemFn,
        inner_item_impl => visit_item_impl: syn::ItemImpl,
        inner_impl_item_fn => visit_impl_item_fn: syn::ImplItemFn,
        inner_trait_item_fn => visit_trait_item_fn: syn::TraitItemFn,
        inner_impl_item_type => visit_impl_item_type: syn::ImplItemType,
        inner_trait_item_type => visit_trait_item_type: syn::TraitItemType,
    );
    fn macro_exprs(&self) -> &'ast MacroExprs {
        self.macros
    }
    fn macro_fallback(&mut self, _mac: &syn::Macro) {}
    fn enter(&mut self, kind: ScopeKind, name: Option<String>, generics: &Generics) {
        self.scopes
            .scopes
            .push(scope_from_generics(kind, name, generics, self.source));
    }
    fn leave(&mut self) {}
    fn push_impl_self(&mut self, _: Option<String>) {
        self.impl_depth += 1;
    }
    fn pop_impl_self(&mut self) {
        self.impl_depth -= 1;
    }
}

impl<'ast> Collector<'ast> {

}

impl<'ast> Visit<'ast> for Collector<'ast> {
    scoped_visits!();
}

pub fn collect_file<'ast>(
    file: &'ast syn::File,
    macros: &'ast MacroExprs,
    source: &'ast str,
    spec_traits: &'ast HashSet<String>,
) -> FileScopes {
    let mut c = Collector {
        scopes: FileScopes::default(),
        impl_depth: 0,
        macros,
        source,
        spec_traits,
    };
    c.visit_file(file);
    c.scopes
}

/// Trait associated types bounded by `EthSpec` (e.g. `type EthSpec: types::EthSpec;`)
/// and the traits declaring them.
pub fn collect_spec_assoc_names(file: &syn::File, out: &mut HashSet<String>, traits: &mut HashSet<String>) {
    struct V<'a>(&'a mut HashSet<String>, &'a mut HashSet<String>);
    impl<'ast> Visit<'ast> for V<'_> {
        fn visit_item_trait(&mut self, t: &'ast syn::ItemTrait) {
            for item in &t.items {
                if let syn::TraitItem::Type(n) = item {
                    if bounds_have_ethspec(n.bounds.iter()) {
                        self.0.insert(n.ident.to_string());
                        self.1.insert(t.ident.to_string());
                    }
                }
            }
        }
    }
    V(out, traits).visit_file(file);
}

/// `type E = MainnetEthSpec;` aliases anywhere in the file.
pub fn collect_file_aliases(file: &syn::File) -> HashSet<String> {
    struct V(HashSet<String>);
    impl<'ast> Visit<'ast> for V {
        fn visit_item_type(&mut self, n: &'ast syn::ItemType) {
            if n.generics.params.is_empty() {
                if let Type::Path(tp) = &*n.ty {
                    if tp.qself.is_none() {
                        if let Some(last) = tp.path.segments.last() {
                            if specmap::is_concrete_spec(&last.ident.to_string()) {
                                self.0.insert(n.ident.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    let mut v = V(HashSet::new());
    v.visit_file(file);
    v.0
}

// ---------------------------------------------------------------------------
// Pass 1: fixpoint analyzer
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Evidence {
    spec: bool,
    /// Used in the item header (anything outside a nested method).
    header_used: bool,
    /// Methods (ordinals) using the parameter.
    method_uses: Vec<usize>,
    /// Had at least one occurrence consumed by a spec rule.
    spec_use: bool,
    /// Appears in a `PhantomData` field (a header use we may drop).
    phantom_used: bool,
}

pub struct Analyzer<'ast, 'db> {
    stack: ScopeStack<'db>,
    next_ordinal: usize,
    evidence: HashMap<(usize, usize), Evidence>,
    macros: &'ast MacroExprs,
    in_phantom: bool,
    /// Definition slots (any file) that received a spec-like argument.
    cross: HashSet<(ScopeId, usize)>,
}

impl<'ast, 'db> Analyzer<'ast, 'db> {
    fn macro_exprs(&self) -> &'ast MacroExprs {
        self.macros
    }
    fn macro_fallback(&mut self, mac: &syn::Macro) {
        self.token_scan(mac.tokens.clone());
    }
    fn enter(&mut self, kind: ScopeKind, name: Option<String>, _generics: &Generics) {
        check_scope(self.stack.db, self.stack.file, self.next_ordinal, kind, &name);
        self.stack.stack.push(self.next_ordinal);
        self.next_ordinal += 1;
    }
    fn leave(&mut self) {
        self.stack.stack.pop();
    }
    fn push_impl_self(&mut self, name: Option<String>) {
        self.stack.impl_self.push(name);
    }
    fn pop_impl_self(&mut self) {
        self.stack.impl_self.pop();
    }

    fn mark(&mut self, name: &str, spec: bool, used: bool) {
        if let Some(r) = self.stack.resolve(name) {
            let decl_kind = self.stack.db.scope(r.id).kind;
            // Is the use inside a method nested in an impl/trait scope?
            let mut method: Option<usize> = None;
            if matches!(decl_kind, ScopeKind::Impl | ScopeKind::Trait) {
                if let Some(pos) = self.stack.stack.iter().position(|&o| o == r.id.ordinal) {
                    for &o in &self.stack.stack[pos + 1..] {
                        if self.stack.db.files[self.stack.file].scopes[o].kind == ScopeKind::Fn {
                            method = Some(o);
                            break;
                        }
                    }
                }
            }
            let e = self.evidence.entry((r.id.ordinal, r.index)).or_default();
            e.spec |= spec;
            if used {
                match method {
                    Some(m) => {
                        if !e.method_uses.contains(&m) {
                            e.method_uses.push(m);
                        }
                    }
                    None => e.header_used = true,
                }
            }
        }
    }

    fn mark_spec_use(&mut self, name: &str) {
        if let Some(r) = self.stack.resolve(name) {
            let e = self.evidence.entry((r.id.ordinal, r.index)).or_default();
            e.spec_use = true;
        }
    }

    fn mark_phantom_use(&mut self, name: &str) {
        if let Some(r) = self.stack.resolve(name) {
            let e = self.evidence.entry((r.id.ordinal, r.index)).or_default();
            e.phantom_used = true;
        }
    }

    /// Central path handling. Returns true if the path was a spec reference
    /// (and therefore fully consumed).
    fn handle_path(&mut self, qself: Option<&'ast QSelf>, path: &'ast Path) -> bool {
        if let Some(n) = self.stack.spec_prefix_len(qself, path) {
            // Evidence: `P::Assoc` / `P::method` marks an unbounded P as spec.
            if qself.is_none() {
                if let Some(first) = path.segments.first() {
                    let first_name = first.ident.to_string();
                    if n == 1 {
                        if let Some(second) = path.segments.iter().nth(1) {
                            let s = second.ident.to_string();
                            if specmap::is_assoc_type(&s) || specmap::is_old_method(&s) {
                                self.mark(&first_name, true, false);
                            }
                        }
                    } else if n == 2 {
                        // `T::EthSpec`: a spec-related use of T.
                        self.mark_spec_use(&first_name);
                    }
                }
            } else if let Some(q) = qself {
                // `<T as BeaconChainTypes>::EthSpec`
                if let Some(b) = bare_ident(&q.ty) {
                    self.mark_spec_use(&b);
                }
            }
            // Anything after the spec prefix is not a use of a parameter.
            if let Some(q) = qself {
                // `<T as BeaconChainTypes>::EthSpec` — T is not "used".
                if !self.stack.is_spec_type(&q.ty) {
                    // nothing
                } else {
                    self.visit_type(&q.ty);
                }
            }
            return true;
        }
        if let Some(q) = qself {
            // `<E as EthSpec>::Assoc` on a not-yet-classified parameter: spec evidence.
            let trait_is_ethspec = q.position > 0
                && path.segments.iter().nth(q.position - 1).map(|s| s.ident == "EthSpec").unwrap_or(false);
            if trait_is_ethspec {
                if let Some(b) = bare_ident(&q.ty) {
                    self.mark(&b, true, false);
                    return true;
                }
            }
            self.visit_type(&q.ty);
        } else if let Some(first) = path.segments.first() {
            let name = first.ident.to_string();
            if self.stack.is_plain_param(&name) && !self.in_phantom {
                self.mark(&name, false, true);
            }
        }
        let n = path.segments.len();
        for (i, seg) in path.segments.iter().enumerate() {
            match &seg.arguments {
                syn::PathArguments::AngleBracketed(ab) => {
                    let ident = seg.ident.to_string();
                    let kind = if i + 1 == n && ident.chars().next().map(|c| c.is_lowercase()).unwrap_or(false) {
                        LookupKind::Fn
                    } else {
                        LookupKind::Type
                    };
                    self.process_args(&ident, kind, ab);
                }
                syn::PathArguments::Parenthesized(p) => {
                    for t in &p.inputs {
                        self.visit_type(t);
                    }
                    self.visit_return_type(&p.output);
                }
                syn::PathArguments::None => {}
            }
        }
        false
    }

    fn process_args(&mut self, name: &str, kind: LookupKind, ab: &'ast syn::AngleBracketedGenericArguments) {
        let type_args: Vec<&Type> = ab
            .args
            .iter()
            .filter_map(|a| match a {
                syn::GenericArgument::Type(t) => Some(t),
                _ => None,
            })
            .collect();
        let positions = self.stack.db.positions(name, kind, type_args.len());
        let mut ti = 0;
        for arg in &ab.args {
            match arg {
                syn::GenericArgument::Type(t) => {
                    let idx = ti;
                    ti += 1;
                    if self.stack.is_spec_type(t) {
                        // Spec-like argument: consumed. `T::EthSpec` is a
                        // spec-related use of T.
                        if let Some(b) = self.stack.spec_base_param(t) {
                            self.mark_spec_use(&b);
                        }
                        // A definition slot that receives the spec *is* a spec
                        // slot, provided the name is unambiguous (several
                        // crates define e.g. `Error<..>`).
                        let defs = self.stack.db.compatible_defs(name, kind, type_args.len());
                        if let [id] = defs.as_slice() {
                            self.cross.insert((*id, idx));
                        }
                        continue;
                    }
                    let bare = bare_ident(t);
                    if let Some(p) = &positions {
                        if p.spec_like[idx] {
                            if let Some(b) = &bare {
                                self.mark(b, true, false);
                                continue;
                            }
                        }
                        if p.removed[idx] {
                            if let Some(b) = &bare {
                                self.mark_spec_use(b);
                                continue; // not a use
                            }
                        }
                    }
                    self.visit_type(t);
                }
                syn::GenericArgument::AssocType(a) => {
                    if self.stack.db.spec_assoc_names.contains(&a.ident.to_string()) {
                        continue;
                    }
                    self.visit_generic_argument(arg);
                }
                _ => self.visit_generic_argument(arg),
            }
        }
    }

    fn visit_call_args(&mut self, args: &'ast syn::punctuated::Punctuated<Expr, syn::Token![,]>) {
        for a in args {
            if is_spec_value(&self.stack, a) {
                if let Expr::Path(ep) = a {
                    if ep.qself.is_none() && ep.path.segments.len() == 2 {
                        let first = ep.path.segments[0].ident.to_string();
                        self.mark_spec_use(&first);
                    }
                }
                continue;
            }
            self.visit_expr(a);
        }
    }

    fn token_scan(&mut self, tokens: proc_macro2::TokenStream) {
        let toks: Vec<proc_macro2::TokenTree> = tokens.into_iter().collect();
        let mut i = 0;
        while i < toks.len() {
            match &toks[i] {
                proc_macro2::TokenTree::Group(g) => {
                    self.token_scan(g.stream());
                    i += 1;
                }
                proc_macro2::TokenTree::Ident(id) => {
                    let name = id.to_string();
                    // Path run?
                    let mut idents = vec![name.clone()];
                    let mut j = i;
                    while j + 3 <= toks.len() + 0 && is_colon2(&toks, j + 1) {
                        if let Some(proc_macro2::TokenTree::Ident(next)) = toks.get(j + 3) {
                            idents.push(next.to_string());
                            j += 3;
                        } else {
                            break;
                        }
                    }
                    if self.stack.is_spec_ident(&name) {
                        // spec itself; nothing to mark
                    } else if self.stack.is_plain_param(&name) {
                        if idents.len() >= 2 && self.stack.db.spec_assoc_names.contains(&idents[1]) {
                            // T::EthSpec — not a use
                        } else {
                            self.mark(&name, false, true);
                        }
                    } else if idents.len() >= 2
                        && (specmap::is_assoc_type(&idents[1]) || specmap::is_old_method(&idents[1]))
                    {
                        self.mark(&name, true, false);
                    }
                    i = j + 1;
                }
                _ => i += 1,
            }
        }
    }
}

/// Panic loudly if a visitor's traversal order diverges from the collector's.
pub fn check_scope(db: &Db, file: usize, ordinal: usize, kind: ScopeKind, name: &Option<String>) {
    let scope = db.files[file]
        .scopes
        .get(ordinal)
        .unwrap_or_else(|| panic!("scope ordinal {ordinal} out of range in file {file}"));
    if scope.kind != kind || &scope.name != name {
        panic!(
            "scope order mismatch in file {file} at ordinal {ordinal}: expected {:?} {:?}, got {:?} {:?}",
            scope.kind, scope.name, kind, name
        );
    }
}

pub fn is_colon2(toks: &[proc_macro2::TokenTree], at: usize) -> bool {
    matches!(
        (toks.get(at), toks.get(at + 1)),
        (Some(proc_macro2::TokenTree::Punct(a)), Some(proc_macro2::TokenTree::Punct(b)))
            if a.as_char() == ':' && b.as_char() == ':'
    )
}

pub fn bare_ident(t: &Type) -> Option<String> {
    match t {
        Type::Path(tp) if tp.qself.is_none() && tp.path.segments.len() == 1 => {
            let s = &tp.path.segments[0];
            if s.arguments.is_empty() {
                Some(s.ident.to_string())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// `MinimalEthSpec`, `E::default()`, `MainnetEthSpec::default()` as a value.
pub fn is_spec_value(stack: &ScopeStack<'_>, e: &Expr) -> bool {
    match e {
        Expr::Path(ep) => {
            matches!(stack.spec_prefix_len(ep.qself.as_ref(), &ep.path), Some(n) if n == ep.path.segments.len())
        }
        Expr::Call(c) if c.args.is_empty() => match &*c.func {
            Expr::Path(ep) => {
                let n = stack.spec_prefix_len(ep.qself.as_ref(), &ep.path);
                match n {
                    Some(n) if n + 1 == ep.path.segments.len() => {
                        ep.path.segments.last().map(|s| s.ident == "default").unwrap_or(false)
                    }
                    _ => false,
                }
            }
            _ => false,
        },
        _ => false,
    }
}

impl<'ast, 'db> Analyzer<'ast, 'db> {
    default_inner!(
        inner_item_struct => visit_item_struct: syn::ItemStruct,
        inner_item_enum => visit_item_enum: syn::ItemEnum,
        inner_item_union => visit_item_union: syn::ItemUnion,
        inner_item_type => visit_item_type: syn::ItemType,
        inner_item_trait => visit_item_trait: syn::ItemTrait,
        inner_item_trait_alias => visit_item_trait_alias: syn::ItemTraitAlias,
        inner_item_fn => visit_item_fn: syn::ItemFn,
        inner_item_impl => visit_item_impl: syn::ItemImpl,
        inner_impl_item_fn => visit_impl_item_fn: syn::ImplItemFn,
        inner_trait_item_fn => visit_trait_item_fn: syn::TraitItemFn,
        inner_impl_item_type => visit_impl_item_type: syn::ImplItemType,
        inner_trait_item_type => visit_trait_item_type: syn::TraitItemType,
    );

}

impl<'ast, 'db> Visit<'ast> for Analyzer<'ast, 'db> {
    scoped_visits!();

    fn visit_item_use(&mut self, _: &'ast syn::ItemUse) {}
    fn visit_attribute(&mut self, _: &'ast syn::Attribute) {}

    fn visit_path(&mut self, path: &'ast Path) {
        self.handle_path(None, path);
    }

    fn visit_type_path(&mut self, tp: &'ast syn::TypePath) {
        if tp.qself.is_some() {
            self.handle_path(tp.qself.as_ref(), &tp.path);
        } else {
            self.handle_path(None, &tp.path);
        }
    }

    fn visit_expr_path(&mut self, ep: &'ast syn::ExprPath) {
        self.handle_path(ep.qself.as_ref(), &ep.path);
    }

    fn visit_expr_struct(&mut self, es: &'ast syn::ExprStruct) {
        self.handle_path(es.qself.as_ref(), &es.path);
        for f in &es.fields {
            self.visit_expr(&f.expr);
        }
        if let Some(rest) = &es.rest {
            self.visit_expr(rest);
        }
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Expr::Path(ep) = &*call.func {
            if self.handle_path(ep.qself.as_ref(), &ep.path) {
                self.visit_call_args(&call.args);
                return;
            }
        } else {
            self.visit_expr(&call.func);
        }
        self.visit_call_args(&call.args);
    }

    fn visit_expr_method_call(&mut self, mc: &'ast syn::ExprMethodCall) {
        if let Some(tf) = &mc.turbofish {
            self.process_args(&mc.method.to_string(), LookupKind::Fn, tf);
        }
        self.visit_expr(&mc.receiver);
        self.visit_call_args(&mc.args);
    }

    fn visit_where_predicate(&mut self, wp: &'ast syn::WherePredicate) {
        if let syn::WherePredicate::Type(pt) = wp {
            if bare_ident(&pt.bounded_ty).is_some() {
                for b in &pt.bounds {
                    self.visit_type_param_bound(b);
                }
                return;
            }
        }
        syn::visit::visit_where_predicate(self, wp);
    }

    fn visit_field(&mut self, f: &'ast syn::Field) {
        if self.stack.is_spec_type(&f.ty) || self.stack.is_spec_phantom(&f.ty) {
            if let Some(b) = self.stack.spec_base_param(&f.ty) {
                self.mark_spec_use(&b);
            }
            return;
        }
        // `PhantomData<T>` / `PhantomData<(T, E)>`: a phantom use of T, not a
        // real one.
        if let Some(inner) = phantom_inner(&f.ty) {
            let mut names = Vec::new();
            phantom_param_names(inner, &mut names);
            for n in names {
                if self.stack.is_plain_param(&n) {
                    self.mark_phantom_use(&n);
                }
            }
            // Still visit for spec-like parts (handled by the tuple rule),
            // without counting the parameters as used.
            let prev = self.in_phantom;
            self.in_phantom = true;
            syn::visit::visit_field(self, f);
            self.in_phantom = prev;
            return;
        }
        syn::visit::visit_field(self, f);
    }

    fn visit_fn_arg(&mut self, a: &'ast syn::FnArg) {
        if let syn::FnArg::Typed(pt) = a {
            if self.stack.is_spec_type(&pt.ty) {
                if let Some(b) = self.stack.spec_base_param(&pt.ty) {
                    self.mark_spec_use(&b);
                }
                return;
            }
        }
        syn::visit::visit_fn_arg(self, a);
    }
}

pub struct FileInput {
    pub path: std::path::PathBuf,
    pub source: String,
    pub ast: syn::File,
    pub aliases: HashSet<String>,
    pub macros: MacroExprs,
}

/// Build the database and run the fixpoint.
pub fn analyze(files: &[FileInput]) -> Db {
    let mut db = Db::default();
    for f in files {
        collect_spec_assoc_names(&f.ast, &mut db.spec_assoc_names, &mut db.spec_traits);
    }
    let spec_traits_snapshot = db.spec_traits.clone();
    for (fi, f) in files.iter().enumerate() {
        let scopes = collect_file(&f.ast, &f.macros, &f.source, &spec_traits_snapshot);
        for (oi, scope) in scopes.scopes.iter().enumerate() {
            if let Some(name) = &scope.name {
                let id = ScopeId {
                    file: fi,
                    ordinal: oi,
                };
                match scope.kind {
                    ScopeKind::Type | ScopeKind::Trait => {
                        db.type_defs.entry(name.clone()).or_default().push(id);
                        for a in &scope.aliases {
                            db.type_defs.entry(a.clone()).or_default().push(id);
                        }
                    }
                    ScopeKind::Fn => db.fn_defs.entry(name.clone()).or_default().push(id),
                    _ => {}
                }
            }
        }
        db.files.push(scopes);
    }

    // Impls that prevent dropping a phantom-pinned parameter.
    let mut vetoes: HashSet<(String, usize)> = HashSet::new();
    for f in files {
        let mut v = VetoScan {
            db: &db,
            vetoes: &mut vetoes,
            spec_assoc: &db.spec_assoc_names,
        };
        v.visit_file(&f.ast);
    }
    for f in &mut db.files {
        for scope in &mut f.scopes {
            let names: Vec<String> = scope.name.iter().cloned().chain(scope.aliases.iter().cloned()).collect();
            for (i, p) in scope.params.iter_mut().enumerate() {
                if names.iter().any(|n| vetoes.contains(&(n.clone(), i))) {
                    p.flags.vetoed = true;
                }
            }
        }
    }

    let mut iterations = 0;
    loop {
        iterations += 1;
        let mut changed = false;
        let mut updates: Vec<(ScopeId, usize, bool, bool, std::collections::BTreeSet<usize>)> = Vec::new();
        let mut cross_updates: HashSet<(ScopeId, usize)> = HashSet::new();
        for (fi, f) in files.iter().enumerate() {
            let mut a = Analyzer {
                stack: ScopeStack::new(&db, fi, f.aliases.clone()),
                next_ordinal: 0,
                evidence: HashMap::new(),
                macros: &f.macros,
                in_phantom: false,
                cross: HashSet::new(),
            };
            a.visit_file(&f.ast);
            for (id, idx) in a.cross.drain() {
                cross_updates.insert((id, idx));
            }
            debug_assert_eq!(a.next_ordinal, db.files[fi].scopes.len(), "scope count mismatch in {}", f.path.display());
            for (oi, scope) in db.files[fi].scopes.iter().enumerate() {
                for (pi, p) in scope.params.iter().enumerate() {
                    let ev = a.evidence.get(&(oi, pi)).cloned().unwrap_or_default();
                    let spec = p.flags.spec_like || ev.spec;
                    let phantom_drop = ev.phantom_used && p.flags.spec_bound && !p.flags.vetoed;
                    let orphan = !spec && !ev.header_used && (ev.spec_use || phantom_drop);
                    let method_uses: std::collections::BTreeSet<usize> = ev.method_uses.iter().copied().collect();
                    if spec != p.flags.spec_like || orphan != p.flags.orphan || (orphan && method_uses != p.flags.method_uses) {
                        updates.push((ScopeId { file: fi, ordinal: oi }, pi, spec, orphan, method_uses));
                    }
                }
            }
        }
        for (id, pi) in cross_updates {
            if let Some(p) = db.scope_mut(id).params.get_mut(pi) {
                if !p.flags.spec_like {
                    p.flags.spec_like = true;
                    changed = true;
                }
            }
        }
        for (id, pi, spec, orphan, method_uses) in updates {
            let p = &mut db.scope_mut(id).params[pi];
            // Monotone: flags only ever turn on.
            if spec && !p.flags.spec_like {
                p.flags.spec_like = true;
                changed = true;
            }
            if orphan && !p.flags.orphan {
                p.flags.orphan = true;
                changed = true;
            }
            if orphan && p.flags.method_uses != method_uses {
                p.flags.method_uses = method_uses;
                changed = true;
            }
        }
        if !changed || iterations > 50 {
            eprintln!("analysis: fixpoint after {iterations} iteration(s)");
            break;
        }
    }

    // Rewrite parameter defaults (`FullPayload<E>` -> `FullPayload`).
    for fi in 0..db.files.len() {
        for oi in 0..db.files[fi].scopes.len() {
            for pi in 0..db.files[fi].scopes[oi].params.len() {
                let Some(text) = db.files[fi].scopes[oi].params[pi].default_text.clone() else { continue };
                let Ok(ty) = syn::parse_str::<Type>(&text) else { continue };
                let mut stack = ScopeStack::new(&db, fi, files[fi].aliases.clone());
                stack.stack.push(oi);
                let empty = MacroExprs::default();
                let mut rw = crate::rewrite::Rewriter::new(stack, &empty, &text);
                rw.visit_type(&ty);
                let edits = std::mem::take(&mut rw.edits);
                drop(rw);
                let out = crate::edit::apply_edits(&text, edits).text;
                db.files[fi].scopes[oi].params[pi].default_rewritten = Some(out);
            }
        }
    }

    // Trait parameters moved onto methods.
    let mut moved: HashMap<(String, String), Vec<(String, String, Vec<String>)>> = HashMap::new();
    for f in &db.files {
        for scope in &f.scopes {
            if scope.kind != ScopeKind::Trait {
                continue;
            }
            let Some(trait_name) = &scope.name else { continue };
            for p in &scope.params {
                if !p.flags.orphan {
                    continue;
                }
                for &m in &p.flags.method_uses {
                    if let Some(method_name) = &f.scopes[m].name {
                        moved
                            .entry((trait_name.clone(), method_name.clone()))
                            .or_default()
                            .push((p.name.clone(), p.decl_text.clone(), p.where_texts.clone()));
                    }
                }
            }
        }
    }
    db.trait_method_moved = moved;

    // Struct scan: which fields disappear.
    for (fi, f) in files.iter().enumerate() {
        let mut s = StructScan {
            stack: ScopeStack::new(&db, fi, f.aliases.clone()),
            next_ordinal: 0,
            out: HashMap::new(),
            macros: &f.macros,
        };
        s.visit_file(&f.ast);
        for (name, info) in s.out {
            let e = db.structs.entry(name).or_default();
            e.removed_fields.extend(info.removed_fields);
            e.removed_positions.extend(info.removed_positions);
            e.becomes_unit |= info.becomes_unit;
        }
    }
    db
}

// ---------------------------------------------------------------------------
// Pass 2: struct field scan
// ---------------------------------------------------------------------------

struct StructScan<'ast, 'db> {
    stack: ScopeStack<'db>,
    next_ordinal: usize,
    out: HashMap<String, StructInfo>,
    macros: &'ast MacroExprs,
}

impl<'ast, 'db> StructScan<'ast, 'db> {
    fn macro_exprs(&self) -> &'ast MacroExprs {
        self.macros
    }
    fn macro_fallback(&mut self, _mac: &syn::Macro) {}
    fn enter(&mut self, kind: ScopeKind, name: Option<String>, _generics: &Generics) {
        check_scope(self.stack.db, self.stack.file, self.next_ordinal, kind, &name);
        self.stack.stack.push(self.next_ordinal);
        self.next_ordinal += 1;
    }
    fn leave(&mut self) {
        self.stack.stack.pop();
    }
    fn push_impl_self(&mut self, name: Option<String>) {
        self.stack.impl_self.push(name);
    }
    fn pop_impl_self(&mut self) {
        self.stack.impl_self.pop();
    }
    fn field_removed(&self, f: &syn::Field) -> bool {
        self.stack.is_spec_type(&f.ty) || self.stack.is_spec_phantom(&f.ty)
    }

    /// Enum variants: recorded as `Enum::Variant` (and looked up via `Self::Variant`).
    fn inner_item_enum(&mut self, n: &'ast syn::ItemEnum) {
        let enum_name = n.ident.to_string();
        for v in &n.variants {
            let mut info = StructInfo::default();
            match &v.fields {
                syn::Fields::Named(named) => {
                    for f in &named.named {
                        if self.field_removed(f) {
                            if let Some(id) = &f.ident {
                                info.removed_fields.push(id.to_string());
                            }
                        }
                    }
                }
                syn::Fields::Unnamed(un) => {
                    for (i, f) in un.unnamed.iter().enumerate() {
                        if self.field_removed(f) {
                            info.removed_positions.push(i);
                        }
                    }
                }
                syn::Fields::Unit => {}
            }
            if !info.removed_fields.is_empty() || !info.removed_positions.is_empty() {
                self.out.insert(format!("{enum_name}::{}", v.ident), info);
            }
        }
        syn::visit::visit_item_enum(self, n);
    }
}

impl<'ast, 'db> StructScan<'ast, 'db> {
    default_inner!(
        inner_item_union => visit_item_union: syn::ItemUnion,
        inner_item_type => visit_item_type: syn::ItemType,
        inner_item_trait => visit_item_trait: syn::ItemTrait,
        inner_item_trait_alias => visit_item_trait_alias: syn::ItemTraitAlias,
        inner_item_fn => visit_item_fn: syn::ItemFn,
        inner_item_impl => visit_item_impl: syn::ItemImpl,
        inner_impl_item_fn => visit_impl_item_fn: syn::ImplItemFn,
        inner_trait_item_fn => visit_trait_item_fn: syn::TraitItemFn,
        inner_impl_item_type => visit_impl_item_type: syn::ImplItemType,
        inner_trait_item_type => visit_trait_item_type: syn::TraitItemType,
    );
    fn inner_item_struct(&mut self, n: &'ast syn::ItemStruct) {
        let mut info = StructInfo::default();
        match &n.fields {
            syn::Fields::Named(named) => {
                for f in &named.named {
                    if self.field_removed(f) {
                        if let Some(id) = &f.ident {
                            info.removed_fields.push(id.to_string());
                        }
                    }
                }
            }
            syn::Fields::Unnamed(un) => {
                if !un.unnamed.is_empty() && un.unnamed.iter().all(|f| self.field_removed(f)) {
                    info.becomes_unit = true;
                } else {
                    for (i, f) in un.unnamed.iter().enumerate() {
                        if self.field_removed(f) {
                            info.removed_positions.push(i);
                        }
                    }
                }
            }
            syn::Fields::Unit => {}
        }
        if !info.removed_fields.is_empty() || info.becomes_unit || !info.removed_positions.is_empty() {
            let base = n.ident.to_string();
            // superstruct generates `NameVariant` structs with the same fields.
            for v in superstruct_variants(&n.attrs) {
                self.out.insert(format!("{base}{v}"), info.clone());
            }
            self.out.insert(base, info);
        }
        syn::visit::visit_item_struct(self, n);
    }

}

impl<'ast, 'db> Visit<'ast> for StructScan<'ast, 'db> {
    scoped_visits!();
}


/// Variant names from `#[superstruct(variants(A, B, ...), ...)]`.
pub fn superstruct_variants(attrs: &[syn::Attribute]) -> Vec<String> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("superstruct") {
            continue;
        }
        let syn::Meta::List(list) = &attr.meta else { continue };
        let toks: Vec<proc_macro2::TokenTree> = list.tokens.clone().into_iter().collect();
        for (i, tt) in toks.iter().enumerate() {
            if let proc_macro2::TokenTree::Ident(id) = tt {
                if id == "variants" {
                    if let Some(proc_macro2::TokenTree::Group(g)) = toks.get(i + 1) {
                        for t in g.stream() {
                            if let proc_macro2::TokenTree::Ident(v) = t {
                                out.push(v.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    out
}


/// Type parameter names that appear inside `PhantomData<..>` in the fields.
fn phantom_params(fields: proc_macro2::TokenStream) -> HashSet<String> {
    let mut plain: HashSet<String> = HashSet::new();
    let mut phantom: HashSet<String> = HashSet::new();
    collect_idents_split(fields, &mut plain, &mut phantom, false);
    phantom
}

fn is_spec_bound(p: &crate::db::Param, spec_traits: &HashSet<String>) -> bool {
    spec_traits
        .iter()
        .any(|t| p.decl_text.contains(t.as_str()) || p.where_texts.iter().any(|w| w.contains(t.as_str())))
}

/// `PhantomData<X>` -> `X`.
pub fn phantom_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else { return None };
    let last = tp.path.segments.last()?;
    if last.ident != "PhantomData" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(ab) = &last.arguments else { return None };
    let mut types = ab.args.iter().filter_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    });
    match (types.next(), types.next()) {
        (Some(t), None) => Some(t),
        _ => None,
    }
}

/// Bare parameter names in `T` or `(T, E, ..)`.
pub fn phantom_param_names(ty: &Type, out: &mut Vec<String>) {
    match ty {
        Type::Tuple(t) => {
            for e in &t.elems {
                phantom_param_names(e, out);
            }
        }
        other => {
            if let Some(b) = bare_ident(other) {
                out.push(b);
            }
        }
    }
}

fn collect_idents_split(
    ts: proc_macro2::TokenStream,
    plain: &mut HashSet<String>,
    phantom: &mut HashSet<String>,
    in_phantom: bool,
) {
    use proc_macro2::TokenTree as TT;
    let toks: Vec<TT> = ts.into_iter().collect();
    let mut i = 0;
    while i < toks.len() {
        match &toks[i] {
            TT::Group(g) => {
                collect_idents_split(g.stream(), plain, phantom, in_phantom);
                i += 1;
            }
            TT::Ident(id) if id == "PhantomData" && !in_phantom => {
                // Skip to matching `>` collecting into the phantom set.
                if let Some(TT::Punct(p)) = toks.get(i + 1) {
                    if p.as_char() == '<' {
                        let mut depth = 0i32;
                        let mut j = i + 1;
                        let mut inner = proc_macro2::TokenStream::new();
                        while j < toks.len() {
                            match &toks[j] {
                                TT::Punct(p) if p.as_char() == '<' => depth += 1,
                                TT::Punct(p) if p.as_char() == '>' => {
                                    depth -= 1;
                                    if depth == 0 {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                            if j > i + 1 {
                                inner.extend(std::iter::once(toks[j].clone()));
                            }
                            j += 1;
                        }
                        collect_idents_split(inner, plain, phantom, true);
                        i = j + 1;
                        continue;
                    }
                }
                i += 1;
            }
            TT::Ident(id) => {
                if in_phantom {
                    phantom.insert(id.to_string());
                } else {
                    plain.insert(id.to_string());
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
}

/// Finds impls that prevent dropping a phantom-only parameter: trait impls
/// that use it at all, and inherent impls that use it only in a method body
/// (where it could not be inferred once moved onto the method).
struct VetoScan<'a> {
    db: &'a Db,
    vetoes: &'a mut HashSet<(String, usize)>,
    spec_assoc: &'a HashSet<String>,
}

impl<'ast> Visit<'ast> for VetoScan<'_> {
    fn visit_item_impl(&mut self, n: &'ast syn::ItemImpl) {
        // Which impl params sit at phantom-only positions of the self type?
        let Type::Path(tp) = &*n.self_ty else {
            syn::visit::visit_item_impl(self, n);
            return;
        };
        let Some(last) = tp.path.segments.last() else { return };
        let self_name = last.ident.to_string();
        let mut candidates: Vec<(String, usize)> = Vec::new();
        if let syn::PathArguments::AngleBracketed(ab) = &last.arguments {
            let mut ti = 0;
            for a in &ab.args {
                if let syn::GenericArgument::Type(t) = a {
                    let idx = ti;
                    ti += 1;
                    let Some(b) = bare_ident(t) else { continue };
                    let is_impl_param = n.generics.params.iter().any(|gp| matches!(gp, syn::GenericParam::Type(x) if x.ident == b));
                    if !is_impl_param {
                        continue;
                    }
                    let phantom_pos = self
                        .db
                        .type_defs
                        .get(&self_name)
                        .map(|ids| {
                            ids.iter().any(|id| {
                                self.db.scope(*id).params.get(idx).map(|p| p.flags.phantom_any).unwrap_or(false)
                            })
                        })
                        .unwrap_or(false);
                    if phantom_pos {
                        candidates.push((b, idx));
                    }
                }
            }
        }
        if candidates.is_empty() {
            syn::visit::visit_item_impl(self, n);
            return;
        }
        let is_trait_impl = n.trait_.is_some();
        // Uses in the impl header (other params' bounds, where clauses on
        // other types, the trait path) cannot be moved: veto.
        {
            let mut u = UseScan {
                names: candidates.iter().map(|(b, _)| b.clone()).collect(),
                in_sig: true,
                sig_used: HashSet::new(),
                body_used: HashSet::new(),
                spec_assoc: self.spec_assoc,
            };
            for gp in &n.generics.params {
                if let syn::GenericParam::Type(tp) = gp {
                    if candidates.iter().any(|(b, _)| b == &tp.ident.to_string()) {
                        continue; // its own declaration
                    }
                    for b in &tp.bounds {
                        u.visit_type_param_bound(b);
                    }
                }
            }
            if let Some(wc) = &n.generics.where_clause {
                for pred in &wc.predicates {
                    if let syn::WherePredicate::Type(pt) = pred {
                        if let Some(b) = bare_ident(&pt.bounded_ty) {
                            if candidates.iter().any(|(c, _)| c == &b) {
                                continue; // predicate on the candidate itself moves with it
                            }
                        }
                    }
                    u.visit_where_predicate(pred);
                }
            }
            if let Some((_, path, _)) = &n.trait_ {
                u.visit_path(path);
            }
            for (b, idx) in &candidates {
                if u.sig_used.contains(b) {
                    self.vetoes.insert((self_name.clone(), *idx));
                }
            }
        }
        for item in &n.items {
            let mut u = UseScan {
                names: candidates.iter().map(|(b, _)| b.clone()).collect(),
                in_sig: false,
                sig_used: HashSet::new(),
                body_used: HashSet::new(),
                spec_assoc: self.spec_assoc,
            };
            match item {
                syn::ImplItem::Fn(f) => {
                    u.in_sig = true;
                    u.visit_signature(&f.sig);
                    u.in_sig = false;
                    u.visit_block(&f.block);
                }
                other => u.visit_impl_item(other),
            }
            for (b, idx) in &candidates {
                let used_sig = u.sig_used.contains(b);
                let used_body = u.body_used.contains(b);
                let veto = if is_trait_impl {
                    used_sig || used_body
                } else {
                    used_body && !used_sig
                };
                if veto {
                    self.vetoes.insert((self_name.clone(), *idx));
                }
            }
        }
        syn::visit::visit_item_impl(self, n);
    }
}

struct UseScan<'a> {
    names: HashSet<String>,
    in_sig: bool,
    sig_used: HashSet<String>,
    body_used: HashSet<String>,
    spec_assoc: &'a HashSet<String>,
}

impl<'ast> Visit<'ast> for UseScan<'_> {
    fn visit_path(&mut self, p: &'ast Path) {
        let segs: Vec<String> = p.segments.iter().map(|s| s.ident.to_string()).collect();
        if let Some(first) = segs.first() {
            let spec_ref = segs.len() >= 2 && self.spec_assoc.contains(&segs[1]);
            if self.names.contains(first) && !spec_ref {
                if self.in_sig {
                    self.sig_used.insert(first.clone());
                } else {
                    self.body_used.insert(first.clone());
                }
            }
        }
        syn::visit::visit_path(self, p);
    }
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        for tt in mac.tokens.clone() {
            if let proc_macro2::TokenTree::Ident(id) = tt {
                let s = id.to_string();
                if self.names.contains(&s) {
                    self.body_used.insert(s);
                }
            }
        }
    }
}
