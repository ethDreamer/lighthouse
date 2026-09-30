//! Per-file edit generation.
//!
//! Walks the AST with the scope stack (same traversal order as the analysis)
//! and emits tagged span edits. Nothing is re-printed: every change is a byte
//! range replacement against the original text.

use std::ops::Range;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Expr, Generics, Path, QSelf, Type};

use crate::analysis::{bare_ident, check_scope, is_colon2, is_spec_value, MacroExprs};
use crate::attrs;
use crate::db::{bounds_have_ethspec, LookupKind, ScopeKind, ScopeStack};
use crate::edit::{extend_to_full_lines, remove_list_elems, Edit, ListElem};
use crate::spans::{join, range_of, span_range};
use crate::specmap::{self, MethodTarget};
use crate::{default_inner, scoped_visits};

pub const R_IMPORTS: &str = "imports";
pub const R_TEST: &str = "test-spec";
pub const R_CONST: &str = "const-access";
pub const R_TYPEPARAMS: &str = "type-params";
pub const R_PROJ: &str = "projections";
pub const R_TYPENUM: &str = "typenum";
pub const R_BOUNDS: &str = "trait-bounds";
pub const R_CLEANUP: &str = "cleanup";

pub const ALL_RULES: &[&str] = &[
    R_IMPORTS, R_TEST, R_CONST, R_TYPEPARAMS, R_PROJ, R_TYPENUM, R_BOUNDS, R_CLEANUP,
];

/// Std-ish containers whose spec-typed arguments are *replaced* (by `Spec`)
/// rather than removed.
const KEEP_CONTAINERS: &[&str] = &[
    "Vec", "Option", "Box", "Arc", "Rc", "Weak", "Result", "HashMap", "HashSet", "BTreeMap",
    "BTreeSet", "VecDeque", "Mutex", "RwLock", "Cow", "Pin", "PhantomData", "RefCell", "Cell",
    "Sender", "Receiver", "UnboundedSender", "UnboundedReceiver", "LinkedList", "BinaryHeap",
    "Once", "OnceLock", "LazyLock", "Cell",
];

pub fn list_elems<T: quote::ToTokens, P: quote::ToTokens>(p: &Punctuated<T, P>) -> Vec<ListElem> {
    p.pairs()
        .map(|pair| ListElem {
            range: range_of(pair.value()),
            comma: pair.punct().map(|c| range_of(*c)),
        })
        .collect()
}

pub fn delim_inner(span: &proc_macro2::extra::DelimSpan) -> Range<usize> {
    span_range(span.open()).end..span_range(span.close()).start
}

pub fn delim_whole(span: &proc_macro2::extra::DelimSpan) -> Range<usize> {
    span_range(span.open()).start..span_range(span.close()).end
}

/// Should a where-predicate be dropped entirely?
pub fn predicate_removed(stack: &ScopeStack<'_>, p: &syn::WherePredicate) -> bool {
    match p {
        syn::WherePredicate::Type(pt) => {
            if stack.is_spec_type(&pt.bounded_ty) {
                return true;
            }
            if let Some(name) = bare_ident(&pt.bounded_ty) {
                if let Some(r) = stack.resolve(&name) {
                    if stack.flags(&r).removed() {
                        return true;
                    }
                }
            }
            // A predicate whose only bound is `EthSpec` on something we do not
            // recognise: drop it too.
            !pt.bounds.is_empty() && pt.bounds.iter().all(|b| match b {
                syn::TypeParamBound::Trait(t) => {
                    t.path.segments.last().map(|s| s.ident == "EthSpec").unwrap_or(false)
                }
                _ => false,
            })
        }
        _ => false,
    }
}

fn idents_after(path: &Path, n: usize) -> Vec<String> {
    path.segments.iter().skip(n).map(|s| s.ident.to_string()).collect()
}

fn first_is_concrete(stack: &ScopeStack<'_>, qself: Option<&QSelf>, path: &Path) -> bool {
    if qself.is_some() {
        return false;
    }
    let Some(first) = path.segments.first() else { return false };
    let name = first.ident.to_string();
    specmap::is_concrete_spec(&name) || stack.file_aliases.contains(&name) || {
        path.segments.len() >= 2
            && specmap::is_concrete_spec(&path.segments[1].ident.to_string())
    }
}

fn is_lower(s: &str) -> bool {
    s.chars().next().map(|c| c.is_lowercase()).unwrap_or(false)
}

pub struct Rewriter<'ast, 'db> {
    pub stack: ScopeStack<'db>,
    next_ordinal: usize,
    macros: &'ast MacroExprs,
    source: &'ast str,
    pub edits: Vec<Edit>,
    in_type: bool,
    pub warnings: Vec<String>,
    /// Attribute start offsets already handled by an item-level merge.
    skip_attrs: std::collections::HashSet<usize>,
    /// Trait name of the innermost trait impl, if any.
    impl_trait: Vec<Option<String>>,
}

impl<'ast, 'db> Rewriter<'ast, 'db> {
    pub fn new(stack: ScopeStack<'db>, macros: &'ast MacroExprs, source: &'ast str) -> Self {
        Rewriter {
            stack,
            next_ordinal: 0,
            macros,
            source,
            edits: Vec::new(),
            in_type: false,
            warnings: Vec::new(),
            skip_attrs: Default::default(),
            impl_trait: Vec::new(),
        }
    }

    pub fn ordinal_count(&self) -> usize {
        self.next_ordinal
    }

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

    /// Parameters that must be (re-)declared on the method whose scope is
    /// currently on top of the stack: orphaned parameters of the enclosing
    /// impl/trait that the method still uses, plus parameters the trait
    /// definition moved onto a method of the same name.
    fn moved_params_for_method(&self, sig: &syn::Signature) -> Vec<(String, String, Vec<String>)> {
        let n = self.stack.stack.len();
        if n < 2 {
            return Vec::new();
        }
        let file = &self.stack.db.files[self.stack.file];
        let method_ord = self.stack.stack[n - 1];
        let enclosing = &file.scopes[self.stack.stack[n - 2]];
        if !matches!(enclosing.kind, ScopeKind::Impl | ScopeKind::Trait) {
            return Vec::new();
        }
        let mut out: Vec<(String, String, Vec<String>)> = Vec::new();
        for p in &enclosing.params {
            if p.flags.orphan && p.flags.method_uses.contains(&method_ord) {
                out.push((p.name.clone(), p.decl_text.clone(), p.where_texts.clone()));
            }
        }
        if enclosing.kind == ScopeKind::Impl {
            if let Some(Some(trait_name)) = self.impl_trait.last() {
                let key = (trait_name.clone(), sig.ident.to_string());
                if let Some(req) = self.stack.db.trait_method_moved.get(&key) {
                    for (name, decl, wheres) in req {
                        if !out.iter().any(|(n, _, _)| n == name) {
                            // Prefer the impl's own declaration text if it
                            // declares a parameter of the same name.
                            let own = enclosing.params.iter().find(|p| &p.name == name);
                            match own {
                                Some(p) => out.push((p.name.clone(), p.decl_text.clone(), p.where_texts.clone())),
                                None => out.push((name.clone(), decl.clone(), wheres.clone())),
                            }
                        }
                    }
                }
            }
        }
        // Skip any the method already declares.
        out.retain(|(name, _, _)| {
            !sig.generics.params.iter().any(|gp| matches!(gp, syn::GenericParam::Type(tp) if tp.ident == name))
        });
        out
    }

    /// Emit the edits that add `moved` parameters to a method signature.
    /// `body_start` is where a `where` clause would be inserted if none exists.
    fn add_params_to_sig(&mut self, sig: &syn::Signature, moved: &[(String, String, Vec<String>)], body_start: usize) {
        if moved.is_empty() {
            return;
        }
        let decls: Vec<&str> = moved.iter().map(|(_, d, _)| d.as_str()).collect();
        match &sig.generics.lt_token {
            Some(lt) => {
                let at = range_of(lt).end;
                self.edits.push(Edit::insert(R_TYPEPARAMS, at, format!("{}, ", decls.join(", "))));
            }
            None => {
                let at = range_of(&sig.ident).end;
                self.edits.push(Edit::insert(R_TYPEPARAMS, at, format!("<{}>", decls.join(", "))));
            }
        }
        let preds: Vec<&str> = moved.iter().flat_map(|(_, _, w)| w.iter().map(|s| s.as_str())).collect();
        if !preds.is_empty() {
            match &sig.generics.where_clause {
                Some(wc) => {
                    let at = range_of(&wc.where_token).end;
                    self.edits.push(Edit::insert(R_BOUNDS, at, format!(" {},", preds.join(", "))));
                }
                None => {
                    self.edits.push(Edit::insert(R_BOUNDS, body_start, format!("where {} ", preds.join(", "))));
                }
            }
        }
    }

    fn warn(&mut self, msg: String) {
        self.warnings.push(msg);
    }

    fn src(&self, r: &Range<usize>) -> &str {
        &self.source[r.clone()]
    }

    /// `Spec::CONST` possibly cast, parenthesised if followed by `<`.
    fn const_expr(&self, konst: &str, conv: &str, after: usize) -> Option<String> {
        let cast = |t: &str| {
            let base = format!("Spec::{konst} as {t}");
            let rest = self.source[after..].trim_start();
            if rest.starts_with('<') {
                format!("({base})")
            } else {
                base
            }
        };
        Some(match conv {
            "to_usize" | "USIZE" => format!("Spec::{konst}"),
            "to_u64" | "U64" => match specmap::u64_method_for_const(konst) {
                Some(m) => format!("Spec::{m}()"),
                None => cast("u64"),
            },
            "to_u32" | "U32" => cast("u32"),
            "to_u16" | "U16" => cast("u16"),
            "to_u8" | "U8" => cast("u8"),
            "to_i32" | "I32" => cast("i32"),
            "to_i64" | "I64" => cast("i64"),
            _ => return None,
        })
    }

    /// Returns true if the path was a spec reference and has been handled.
    fn handle_path(&mut self, qself: Option<&'ast QSelf>, path: &'ast Path, full: Range<usize>) -> bool {
        if let Some(n) = self.stack.spec_prefix_len(qself, path) {
            let rest = idents_after(path, n);
            let rule = if first_is_concrete(&self.stack, qself, path) { R_TEST } else { R_PROJ };
            if self.in_type {
                match rest.as_slice() {
                    [] => self.edits.push(Edit::replace(rule, full, "Spec")),
                    [assoc] => match specmap::assoc_to_const(assoc) {
                        Some(c) => self
                            .edits
                            .push(Edit::replace(R_TYPENUM, full, format!("U<{{ Spec::{c} }}>"))),
                        None => self.warn(format!("unknown assoc type in type position: {}", self.src(&full))),
                    },
                    _ => self.warn(format!("unhandled spec path (type): {}", self.src(&full))),
                }
            } else {
                match rest.as_slice() {
                    [] => {} // bare value; parent decides
                    [assoc, conv] if specmap::assoc_to_const(assoc).is_some() => {
                        let c = specmap::assoc_to_const(assoc).unwrap();
                        match self.const_expr(&c, conv, full.end) {
                            Some(t) => self.edits.push(Edit::replace(R_CONST, full, t)),
                            None => self.warn(format!("unhandled const conv: {}", self.src(&full))),
                        }
                    }
                    [assoc] if specmap::assoc_to_const(assoc).is_some() => {
                        let c = specmap::assoc_to_const(assoc).unwrap();
                        self.edits.push(Edit::replace(R_CONST, full, format!("Spec::{c}")));
                    }
                    [m] if is_lower(m) => match specmap::method_target(m) {
                        MethodTarget::Method(name) | MethodTarget::Const(name) => {
                            self.edits.push(Edit::replace(R_CONST, full, format!("Spec::{name}")))
                        }
                        MethodTarget::GenesisEpoch => {
                            self.edits.push(Edit::replace(R_CONST, full, "Spec::genesis_epoch"))
                        }
                    },
                    _ => self.warn(format!("unhandled spec path (expr): {}", self.src(&full))),
                }
            }
            return true;
        }
        for seg in &path.segments {
            if seg.ident == "EthSpecId" {
                self.edits.push(Edit::replace(R_CLEANUP, range_of(&seg.ident), "SpecId"));
            }
        }
        if let Some(q) = qself {
            self.visit_type(&q.ty);
        }
        let n = path.segments.len();
        for (i, seg) in path.segments.iter().enumerate() {
            match &seg.arguments {
                syn::PathArguments::AngleBracketed(ab) => {
                    let ident = seg.ident.to_string();
                    let kind = if i + 1 == n && is_lower(&ident) {
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
        let is_container = KEEP_CONTAINERS.contains(&name);
        let arity = ab
            .args
            .iter()
            .filter(|a| matches!(a, syn::GenericArgument::Type(_)))
            .count();
        let positions = self.stack.db.positions(name, kind, arity);
        let elems = list_elems(&ab.args);
        let mut remove = Vec::with_capacity(elems.len());
        let mut ti = 0;
        for arg in &ab.args {
            let r = match arg {
                syn::GenericArgument::Type(t) => {
                    let idx = ti;
                    ti += 1;
                    if !is_container && self.stack.is_spec_type(t) {
                        true
                    } else if let Some(p) = &positions {
                        p.removed[idx]
                    } else {
                        false
                    }
                }
                syn::GenericArgument::AssocType(a) => {
                    self.stack.db.spec_assoc_names.contains(&a.ident.to_string())
                }
                _ => false,
            };
            remove.push(r);
        }
        let whole = match &ab.colon2_token {
            Some(c) => join(c, &ab.gt_token),
            None => join(&ab.lt_token, &ab.gt_token),
        };
        // A turbofish left with only `_` arguments is pointless: drop it.
        if ab.colon2_token.is_some()
            && remove.iter().any(|r| *r)
            && ab.args.iter().zip(remove.iter()).all(|(a, r)| *r || matches!(a, syn::GenericArgument::Type(Type::Infer(_))))
        {
            // Unless the definition has defaulted parameters that were being
            // relied on for inference: name them explicitly.
            if remove.iter().all(|r| *r) {
                if let Some(defaults) = self.stack.db.remaining_defaults(name, kind, arity) {
                    self.edits.push(Edit::replace(R_TYPEPARAMS, whole, format!("::<{}>", defaults.join(", "))));
                    return;
                }
            }
            self.edits.push(Edit::delete(R_TYPEPARAMS, whole));
            return;
        }
        self.edits
            .extend(remove_list_elems(R_TYPEPARAMS, &elems, &remove, whole));
        for (arg, r) in ab.args.iter().zip(remove.iter()) {
            if !r {
                self.visit_generic_argument(arg);
            }
        }
    }

    fn visit_call_args(&mut self, args: &'ast Punctuated<Expr, syn::Token![,]>, paren: &syn::token::Paren) {
        let elems = list_elems(args);
        let remove: Vec<bool> = args.iter().map(|a| is_spec_value(&self.stack, a)).collect();
        self.edits
            .extend(remove_list_elems(R_TEST, &elems, &remove, delim_inner(&paren.span)));
        for (a, r) in args.iter().zip(remove.iter()) {
            if !r {
                self.visit_expr(a);
            }
        }
    }

    /// Would every type argument of this segment be removed?
    fn all_args_removed(&self, seg: &syn::PathSegment, kind: LookupKind) -> bool {
        let syn::PathArguments::AngleBracketed(ab) = &seg.arguments else {
            return false;
        };
        let name = seg.ident.to_string();
        let type_args: Vec<&Type> = ab
            .args
            .iter()
            .filter_map(|a| match a {
                syn::GenericArgument::Type(t) => Some(t),
                _ => None,
            })
            .collect();
        if type_args.is_empty() || ab.args.len() != type_args.len() {
            return false;
        }
        let positions = self.stack.db.positions(&name, kind, type_args.len());
        type_args.iter().enumerate().all(|(i, t)| {
            self.stack.is_spec_type(t) || positions.as_ref().map(|p| p.removed[i]).unwrap_or(false)
        })
    }

    /// `<Foo<E>>::bar` -> `Foo::bar` when `Foo` ends up with no arguments.
    fn maybe_strip_qself(&mut self, qself: Option<&QSelf>) {
        let Some(q) = qself else { return };
        if q.position != 0 || q.as_token.is_some() {
            return;
        }
        let Type::Path(tp) = &*q.ty else { return };
        if tp.qself.is_some() || tp.path.segments.len() != 1 {
            return;
        }
        if self.all_args_removed(&tp.path.segments[0], LookupKind::Type) {
            self.edits.push(Edit::delete(R_TYPEPARAMS, range_of(&q.lt_token)));
            self.edits.push(Edit::delete(R_TYPEPARAMS, range_of(&q.gt_token)));
        }
    }

    /// Replacement text for `E::method() as T` / `E::Assoc::to_x() as T`.
    fn cast_replacement(&self, c: &syn::ExprCast) -> Option<String> {
        let cast_ty = bare_ident(&c.ty)?;
        let Expr::Call(call) = &*c.expr else { return None };
        let Expr::Path(ep) = &*call.func else { return None };
        let n = self.stack.spec_prefix_len(ep.qself.as_ref(), &ep.path)?;
        let rest = idents_after(&ep.path, n);
        match rest.as_slice() {
            [m] if is_lower(m) && call.args.is_empty() => match specmap::method_target_cast(m, Some(&cast_ty)) {
                MethodTarget::Method(name) if cast_ty == "u64" && specmap::is_new_method(&name) => {
                    Some(format!("Spec::{name}()"))
                }
                MethodTarget::Const(k) if cast_ty == "usize" => Some(format!("Spec::{k}")),
                MethodTarget::Const(k) => Some(format!("Spec::{k} as {cast_ty}")),
                _ => None,
            },
            [assoc, conv] => {
                let k = specmap::assoc_to_const(assoc)?;
                if !conv.starts_with("to_") {
                    return None;
                }
                match cast_ty.as_str() {
                    "usize" => Some(format!("Spec::{k}")),
                    "u64" => Some(match specmap::u64_method_for_const(&k) {
                        Some(m) => format!("Spec::{m}()"),
                        None => format!("Spec::{k} as u64"),
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// `|x| f::<E>(x)` where the turbofish vanishes entirely.
    fn redundant_closure<'b>(&self, cl: &'b syn::ExprClosure) -> Option<(Range<usize>, &'b syn::ExprCall)> {
        if cl.inputs.len() != 1 || cl.capture.is_some() || cl.asyncness.is_some() || cl.movability.is_some() {
            return None;
        }
        let syn::Pat::Ident(pi) = &cl.inputs[0] else { return None };
        if pi.subpat.is_some() || pi.by_ref.is_some() || pi.mutability.is_some() {
            return None;
        }
        let Expr::Call(call) = &*cl.body else { return None };
        if call.args.len() != 1 {
            return None;
        }
        let Expr::Path(arg) = &call.args[0] else { return None };
        if !arg.path.is_ident(&pi.ident) {
            return None;
        }
        let Expr::Path(func) = &*call.func else { return None };
        if func.qself.is_some() {
            return None;
        }
        let n = func.path.segments.len();
        let any_removed = func.path.segments.iter().enumerate().any(|(i, seg)| {
            let kind = if i + 1 == n && is_lower(&seg.ident.to_string()) { LookupKind::Fn } else { LookupKind::Type };
            self.all_args_removed(seg, kind)
        });
        let any_kept_args = func.path.segments.iter().enumerate().any(|(i, seg)| {
            let kind = if i + 1 == n && is_lower(&seg.ident.to_string()) { LookupKind::Fn } else { LookupKind::Type };
            !seg.arguments.is_empty() && !self.all_args_removed(seg, kind)
        });
        if any_removed && !any_kept_args {
            Some((range_of(func), call))
        } else {
            None
        }
    }

    /// Positional fields removed from the tuple struct / enum variant named
    /// by `path` (`Foo`, `Enum::Variant`, `Self::Variant`).
    fn removed_positions_for(&self, path: &Path) -> Vec<usize> {
        let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
        let n = segs.len();
        let mut keys: Vec<String> = Vec::new();
        if n >= 2 {
            let head = if segs[n - 2] == "Self" {
                self.stack.current_impl_self().map(|s| s.to_string())
            } else {
                Some(segs[n - 2].clone())
            };
            if let Some(h) = head {
                keys.push(format!("{h}::{}", segs[n - 1]));
            }
        }
        keys.push(segs[n - 1].clone());
        for k in keys {
            if let Some(info) = self.stack.db.structs.get(&k) {
                if !info.removed_positions.is_empty() {
                    return info.removed_positions.clone();
                }
            }
        }
        Vec::new()
    }

    fn field_removed(&self, f: &syn::Field) -> bool {
        self.stack.is_spec_type(&f.ty) || self.stack.is_spec_phantom(&f.ty)
    }

    // ---- macro token fallback -------------------------------------------------

    fn token_scan(&mut self, tokens: proc_macro2::TokenStream) {
        use proc_macro2::TokenTree as TT;
        let toks: Vec<TT> = tokens.into_iter().collect();
        let mut i = 0;
        while i < toks.len() {
            match &toks[i] {
                TT::Group(g) => {
                    self.token_scan(g.stream());
                    i += 1;
                }
                TT::Ident(id) => {
                    let mut run_end = i;
                    while is_colon2(&toks, run_end + 1) && matches!(toks.get(run_end + 3), Some(TT::Ident(_))) {
                        run_end += 3;
                    }
                    if run_end > i {
                        let ts: proc_macro2::TokenStream = toks[i..=run_end].iter().cloned().collect();
                        if let Ok(path) = syn::parse2::<Path>(ts) {
                            let run_range = span_range(toks[i].span()).start..span_range(toks[run_end].span()).end;
                            if let Some(n) = self.stack.spec_prefix_len(None, &path) {
                                let rest = idents_after(&path, n);
                                let next_group = match toks.get(run_end + 1) {
                                    Some(TT::Group(g)) if g.delimiter() == proc_macro2::Delimiter::Parenthesis => Some(g),
                                    _ => None,
                                };
                                match rest.as_slice() {
                                    [] => {
                                        if !self.try_remove_in_generic_list(&toks, i, run_end) {
                                            self.edits.push(Edit::replace(R_PROJ, run_range, "Spec"));
                                        }
                                    }
                                    [m] if is_lower(m) => match specmap::method_target(m) {
                                        MethodTarget::Method(name) => self
                                            .edits
                                            .push(Edit::replace(R_CONST, run_range, format!("Spec::{name}"))),
                                        MethodTarget::Const(c) => {
                                            let end = next_group
                                                .map(|g| span_range(g.span()).end)
                                                .unwrap_or(run_range.end);
                                            self.edits.push(Edit::replace(
                                                R_CONST,
                                                run_range.start..end,
                                                format!("Spec::{c}"),
                                            ))
                                        }
                                        MethodTarget::GenesisEpoch => {
                                            let end = next_group
                                                .map(|g| span_range(g.span()).end)
                                                .unwrap_or(run_range.end);
                                            self.edits.push(Edit::replace(
                                                R_CONST,
                                                run_range.start..end,
                                                "Epoch::new(Spec::genesis_epoch())",
                                            ))
                                        }
                                    },
                                    [assoc, conv] if specmap::assoc_to_const(assoc).is_some() => {
                                        let c = specmap::assoc_to_const(assoc).unwrap();
                                        let end = next_group
                                            .map(|g| span_range(g.span()).end)
                                            .unwrap_or(run_range.end);
                                        if let Some(t) = self.const_expr(&c, conv, end) {
                                            self.edits.push(Edit::replace(R_CONST, run_range.start..end, t));
                                        }
                                    }
                                    [assoc] if specmap::assoc_to_const(assoc).is_some() => {
                                        let c = specmap::assoc_to_const(assoc).unwrap();
                                        self.edits.push(Edit::replace(
                                            R_TYPENUM,
                                            run_range,
                                            format!("U<{{ Spec::{c} }}>"),
                                        ));
                                    }
                                    _ => {}
                                }
                                if let Some(g) = next_group {
                                    self.token_scan(g.stream());
                                    i = run_end + 2;
                                } else {
                                    i = run_end + 1;
                                }
                                continue;
                            }
                        }
                        for k in (i..=run_end).step_by(3) {
                            if let TT::Ident(id) = &toks[k] {
                                if id == "EthSpecId" {
                                    self.edits.push(Edit::replace(R_CLEANUP, span_range(id.span()), "SpecId"));
                                }
                            }
                        }
                        i = run_end + 1;
                        continue;
                    }
                    let name = id.to_string();
                    if name == "EthSpecId" {
                        self.edits.push(Edit::replace(R_CLEANUP, span_range(id.span()), "SpecId"));
                    } else if self.stack.is_spec_ident(&name) && !(i >= 2 && is_colon2(&toks, i - 2)) {
                        // `E: EthSpec` inside `<...>` in a macro invocation.
                        let mut end = i;
                        if let (Some(TT::Punct(c)), Some(TT::Ident(b))) = (toks.get(i + 1), toks.get(i + 2)) {
                            if c.as_char() == ':' && b == "EthSpec" {
                                end = i + 2;
                            }
                        }
                        self.try_remove_in_generic_list(&toks, i, end);
                    }
                    i += 1;
                }
                _ => i += 1,
            }
        }
    }

    /// Remove tokens `start..=end` when they sit in a `<...>` argument list,
    /// taking an adjacent comma with them. Returns false if not in such a list.
    fn try_remove_in_generic_list(&mut self, toks: &[proc_macro2::TokenTree], start: usize, end: usize) -> bool {
        use proc_macro2::TokenTree as TT;
        let punct = |t: Option<&TT>, c: char| matches!(t, Some(TT::Punct(p)) if p.as_char() == c);
        let prev = if start > 0 { toks.get(start - 1) } else { None };
        let next = toks.get(end + 1);
        let s = span_range(toks[start].span()).start;
        let e = span_range(toks[end].span()).end;
        if punct(next, ',') {
            let after = toks
                .get(end + 2)
                .map(|t| span_range(t.span()).start)
                .unwrap_or(span_range(toks[end + 1].span()).end);
            self.edits.push(Edit::delete(R_TYPEPARAMS, s..after));
            true
        } else if punct(prev, ',') && (punct(next, '>') || next.is_none()) {
            let ps = span_range(prev.unwrap().span()).start;
            self.edits.push(Edit::delete(R_TYPEPARAMS, ps..e));
            true
        } else if punct(prev, '<') && punct(next, '>') {
            // Include a preceding `::` (turbofish) if present.
            let mut ps = span_range(prev.unwrap().span()).start;
            if start >= 3 && is_colon2(toks, start - 3) {
                ps = span_range(toks[start - 3].span()).start;
            }
            let ne = span_range(next.unwrap().span()).end;
            self.edits.push(Edit::delete(R_TYPEPARAMS, ps..ne));
            true
        } else {
            false
        }
    }
}

impl<'ast, 'db> Rewriter<'ast, 'db> {
    default_inner!(
        inner_item_union => visit_item_union: syn::ItemUnion,
        inner_item_trait => visit_item_trait: syn::ItemTrait,
        inner_item_trait_alias => visit_item_trait_alias: syn::ItemTraitAlias,
        inner_item_fn => visit_item_fn: syn::ItemFn,
    );

    fn inner_item_struct(&mut self, n: &'ast syn::ItemStruct) {
        if let Some(k) = attrs::merge_item_educe(&n.attrs, &self.stack, self.source, &mut self.edits) {
            self.skip_attrs.insert(k);
        }
        syn::visit::visit_item_struct(self, n);
    }

    fn inner_item_enum(&mut self, n: &'ast syn::ItemEnum) {
        if let Some(k) = attrs::merge_item_educe(&n.attrs, &self.stack, self.source, &mut self.edits) {
            self.skip_attrs.insert(k);
        }
        syn::visit::visit_item_enum(self, n);
    }

    fn inner_item_impl(&mut self, n: &'ast syn::ItemImpl) {
        let trait_name = n
            .trait_
            .as_ref()
            .and_then(|(_, p, _)| p.segments.last().map(|s| s.ident.to_string()));
        self.impl_trait.push(trait_name);
        syn::visit::visit_item_impl(self, n);
        self.impl_trait.pop();
    }

    fn inner_impl_item_fn(&mut self, n: &'ast syn::ImplItemFn) {
        let moved = self.moved_params_for_method(&n.sig);
        let body_start = span_range(n.block.brace_token.span.open()).start;
        self.add_params_to_sig(&n.sig, &moved, body_start);
        syn::visit::visit_impl_item_fn(self, n);
    }

    fn inner_trait_item_fn(&mut self, n: &'ast syn::TraitItemFn) {
        let moved = self.moved_params_for_method(&n.sig);
        let body_start = match (&n.default, &n.semi_token) {
            (Some(b), _) => span_range(b.brace_token.span.open()).start,
            (None, Some(semi)) => range_of(semi).start,
            (None, None) => range_of(&n.sig).end,
        };
        self.add_params_to_sig(&n.sig, &moved, body_start);
        syn::visit::visit_trait_item_fn(self, n);
    }

    fn inner_item_type(&mut self, n: &'ast syn::ItemType) {
        if n.generics.params.is_empty() && self.stack.is_spec_type(&n.ty) {
            let r = extend_to_full_lines(self.source, range_of(n));
            self.edits.push(Edit::delete(R_TEST, r));
            return;
        }
        syn::visit::visit_item_type(self, n);
    }

    fn inner_impl_item_type(&mut self, n: &'ast syn::ImplItemType) {
        if self.stack.db.spec_assoc_names.contains(&n.ident.to_string()) || self.stack.is_spec_type(&n.ty) {
            let r = extend_to_full_lines(self.source, range_of(n));
            self.edits.push(Edit::delete(R_CLEANUP, r));
            return;
        }
        syn::visit::visit_impl_item_type(self, n);
    }

    fn inner_trait_item_type(&mut self, n: &'ast syn::TraitItemType) {
        if bounds_have_ethspec(n.bounds.iter()) {
            let r = extend_to_full_lines(self.source, range_of(n));
            self.edits.push(Edit::delete(R_CLEANUP, r));
            return;
        }
        syn::visit::visit_trait_item_type(self, n);
    }

}

impl<'ast, 'db> Visit<'ast> for Rewriter<'ast, 'db> {
    scoped_visits!();

    fn visit_item_use(&mut self, _: &'ast syn::ItemUse) {}

    fn visit_attribute(&mut self, attr: &'ast syn::Attribute) {
        if self.skip_attrs.contains(&range_of(attr).start) {
            return;
        }
        attrs::process_attribute(attr, &self.stack, self.source, &mut self.edits, &mut self.warnings);
    }

    fn visit_generics(&mut self, g: &'ast Generics) {
        let Some(&ordinal) = self.stack.stack.last() else {
            syn::visit::visit_generics(self, g);
            return;
        };
        let scope = &self.stack.db.files[self.stack.file].scopes[ordinal];
        let elems = list_elems(&g.params);
        let mut remove = Vec::with_capacity(elems.len());
        let mut ti = 0;
        for gp in &g.params {
            match gp {
                syn::GenericParam::Type(_) => {
                    let r = scope.params.get(ti).map(|p| p.flags.removed()).unwrap_or(false);
                    ti += 1;
                    remove.push(r);
                }
                _ => remove.push(false),
            }
        }
        if let (Some(lt), Some(gt)) = (&g.lt_token, &g.gt_token) {
            self.edits
                .extend(remove_list_elems(R_TYPEPARAMS, &elems, &remove, join(lt, gt)));
        }
        for (gp, r) in g.params.iter().zip(remove.iter()) {
            if !r {
                self.visit_generic_param(gp);
            }
        }
        if let Some(wc) = &g.where_clause {
            let elems = list_elems(&wc.predicates);
            let remove: Vec<bool> = wc
                .predicates
                .iter()
                .map(|p| predicate_removed(&self.stack, p))
                .collect();
            let whole = range_of(wc);
            self.edits
                .extend(remove_list_elems(R_BOUNDS, &elems, &remove, whole));
            for (p, r) in wc.predicates.iter().zip(remove.iter()) {
                if !r {
                    self.visit_where_predicate(p);
                }
            }
        }
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

    fn visit_type(&mut self, ty: &'ast Type) {
        if self.stack.is_spec_type(ty) {
            let rule = match ty {
                Type::Path(tp) if first_is_concrete(&self.stack, tp.qself.as_ref(), &tp.path) => R_TEST,
                _ => R_PROJ,
            };
            self.edits.push(Edit::replace(rule, range_of(ty), "Spec"));
            return;
        }
        syn::visit::visit_type(self, ty);
    }

    fn visit_type_path(&mut self, tp: &'ast syn::TypePath) {
        let prev = self.in_type;
        self.in_type = true;
        self.maybe_strip_qself(tp.qself.as_ref());
        self.handle_path(tp.qself.as_ref(), &tp.path, range_of(tp));
        self.in_type = prev;
    }

    fn visit_expr_path(&mut self, ep: &'ast syn::ExprPath) {
        let prev = self.in_type;
        self.in_type = false;
        self.maybe_strip_qself(ep.qself.as_ref());
        self.handle_path(ep.qself.as_ref(), &ep.path, range_of(ep));
        self.in_type = prev;
    }

    fn visit_expr_cast(&mut self, c: &'ast syn::ExprCast) {
        if let Some(text) = self.cast_replacement(c) {
            self.edits.push(Edit::replace(R_CONST, range_of(c), text));
            return;
        }
        syn::visit::visit_expr_cast(self, c);
    }

    fn visit_expr_closure(&mut self, cl: &'ast syn::ExprClosure) {
        // `|x| Path::<E>(x)` -> `Path` once the turbofish goes (clippy would
        // flag the redundant closure, and the PR removed them).
        if let Some((func_range, call)) = self.redundant_closure(cl) {
            let r = range_of(cl);
            self.edits.push(Edit::delete(R_CLEANUP, r.start..func_range.start));
            self.edits.push(Edit::delete(R_CLEANUP, func_range.end..r.end));
            self.visit_expr(&call.func);
            return;
        }
        syn::visit::visit_expr_closure(self, cl);
    }

    fn visit_path(&mut self, path: &'ast Path) {
        self.handle_path(None, path, range_of(path));
    }

    fn visit_type_tuple(&mut self, t: &'ast syn::TypeTuple) {
        let remove: Vec<bool> = t.elems.iter().map(|e| self.stack.is_spec_type(e)).collect();
        if !remove.iter().any(|r| *r) {
            syn::visit::visit_type_tuple(self, t);
            return;
        }
        let kept = remove.iter().filter(|r| !**r).count();
        if kept == 0 {
            self.edits.push(Edit::replace(R_TYPEPARAMS, range_of(t), "()"));
            return;
        }
        let elems = list_elems(&t.elems);
        self.edits
            .extend(remove_list_elems(R_TYPEPARAMS, &elems, &remove, delim_inner(&t.paren_token.span)));
        if kept == 1 {
            self.edits.push(Edit::delete(R_TYPEPARAMS, span_range(t.paren_token.span.open())));
            self.edits.push(Edit::delete(R_TYPEPARAMS, span_range(t.paren_token.span.close())));
            // A lone kept element that is last keeps its trailing comma: drop it.
            if let Some(last_kept) = remove.iter().rposition(|r| !*r) {
                if last_kept + 1 == elems.len() {
                    if let Some(c) = &elems[last_kept].comma {
                        self.edits.push(Edit::delete(R_TYPEPARAMS, c.clone()));
                    }
                }
            }
        }
        for (e, r) in t.elems.iter().zip(remove.iter()) {
            if !r {
                self.visit_type(e);
            }
        }
    }

    fn visit_fields_named(&mut self, fs: &'ast syn::FieldsNamed) {
        let elems = list_elems(&fs.named);
        let remove: Vec<bool> = fs.named.iter().map(|f| self.field_removed(f)).collect();
        self.edits
            .extend(remove_list_elems(R_CLEANUP, &elems, &remove, delim_inner(&fs.brace_token.span)));
        for (f, r) in fs.named.iter().zip(remove.iter()) {
            if !r {
                self.visit_field(f);
            }
        }
    }

    fn visit_fields_unnamed(&mut self, fs: &'ast syn::FieldsUnnamed) {
        let elems = list_elems(&fs.unnamed);
        let remove: Vec<bool> = fs.unnamed.iter().map(|f| self.field_removed(f)).collect();
        if !elems.is_empty() && remove.iter().all(|r| *r) {
            self.edits.push(Edit::delete(R_CLEANUP, delim_whole(&fs.paren_token.span)));
            return;
        }
        self.edits
            .extend(remove_list_elems(R_CLEANUP, &elems, &remove, delim_inner(&fs.paren_token.span)));
        for (f, r) in fs.unnamed.iter().zip(remove.iter()) {
            if !r {
                self.visit_field(f);
            }
        }
    }

    fn visit_signature(&mut self, sig: &'ast syn::Signature) {
        self.visit_generics(&sig.generics);
        let elems = list_elems(&sig.inputs);
        let remove: Vec<bool> = sig
            .inputs
            .iter()
            .map(|a| matches!(a, syn::FnArg::Typed(pt) if self.stack.is_spec_type(&pt.ty)))
            .collect();
        self.edits
            .extend(remove_list_elems(R_TYPEPARAMS, &elems, &remove, delim_inner(&sig.paren_token.span)));
        for (a, r) in sig.inputs.iter().zip(remove.iter()) {
            if !r {
                self.visit_fn_arg(a);
            }
        }
        self.visit_return_type(&sig.output);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Expr::Path(ep) = &*call.func {
            if let Some(n) = self.stack.spec_prefix_len(ep.qself.as_ref(), &ep.path) {
                let rest = idents_after(&ep.path, n);
                let call_range = range_of(call);
                let func_range = range_of(ep);
                match rest.as_slice() {
                    // `E::get_committee_count_per_slot(count, spec)` lost its
                    // `ChainSpec` convenience form: pass the two fields.
                    [m] if m == "get_committee_count_per_slot" && call.args.len() == 2 => {
                        let spec_arg = self.src(&range_of(&call.args[1])).to_string();
                        self.edits.push(Edit::replace(R_CONST, func_range, "Spec::get_committee_count_per_slot"));
                        self.edits.push(Edit::replace(
                            R_CONST,
                            range_of(&call.args[1]),
                            format!("{spec_arg}.max_committees_per_slot, {spec_arg}.target_committee_size"),
                        ));
                    }
                    [m] if is_lower(m) => match specmap::method_target(m) {
                        MethodTarget::Method(name) => self
                            .edits
                            .push(Edit::replace(R_CONST, func_range, format!("Spec::{name}"))),
                        MethodTarget::Const(c) => {
                            if call.args.is_empty() {
                                self.edits.push(Edit::replace(R_CONST, call_range, format!("Spec::{c}")));
                            } else {
                                self.edits.push(Edit::replace(R_CONST, func_range, format!("Spec::{c}")));
                            }
                        }
                        MethodTarget::GenesisEpoch => {
                            self.edits.push(Edit::replace(R_CONST, call_range, "Epoch::new(Spec::genesis_epoch())"));
                        }
                    },
                    [assoc, conv] if specmap::assoc_to_const(assoc).is_some() => {
                        let c = specmap::assoc_to_const(assoc).unwrap();
                        match self.const_expr(&c, conv, call_range.end) {
                            Some(t) => self.edits.push(Edit::replace(R_CONST, call_range, t)),
                            None => self.warn(format!("unhandled const conv call: {}", self.src(&call_range))),
                        }
                    }
                    _ => self.warn(format!("unhandled spec call: {}", self.src(&func_range))),
                }
                self.visit_call_args(&call.args, &call.paren_token);
                return;
            }
            if ep.qself.is_none() {
                if let Some(last) = ep.path.segments.last() {
                    if let Some(info) = self.stack.db.structs.get(&last.ident.to_string()) {
                        if info.becomes_unit && call.args.iter().all(is_phantom_expr) {
                            let text = self.src(&range_of(ep)).to_string();
                            self.edits.push(Edit::replace(R_CLEANUP, range_of(call), text));
                            return;
                        }
                    }
                }
                // Tuple struct / variant constructor that lost positional fields.
                let positions = self.removed_positions_for(&ep.path);
                if !positions.is_empty() && call.args.len() > *positions.iter().max().unwrap() {
                    let elems = list_elems(&call.args);
                    let remove: Vec<bool> = (0..call.args.len()).map(|i| positions.contains(&i)).collect();
                    self.edits.extend(remove_list_elems(R_CLEANUP, &elems, &remove, delim_inner(&call.paren_token.span)));
                    self.visit_expr(&call.func);
                    for (a, r) in call.args.iter().zip(remove.iter()) {
                        if !r {
                            self.visit_expr(a);
                        }
                    }
                    return;
                }
            }
        }
        self.visit_expr(&call.func);
        self.visit_call_args(&call.args, &call.paren_token);
    }

    fn visit_pat_tuple_struct(&mut self, p: &'ast syn::PatTupleStruct) {
        let positions = if p.qself.is_none() { self.removed_positions_for(&p.path) } else { Vec::new() };
        if !positions.is_empty() && p.elems.len() > *positions.iter().max().unwrap() {
            let elems = list_elems(&p.elems);
            let remove: Vec<bool> = (0..p.elems.len()).map(|i| positions.contains(&i)).collect();
            self.edits.extend(remove_list_elems(R_CLEANUP, &elems, &remove, delim_inner(&p.paren_token.span)));
            self.handle_path(None, &p.path, range_of(&p.path));
            for (e, r) in p.elems.iter().zip(remove.iter()) {
                if !r {
                    self.visit_pat(e);
                }
            }
            return;
        }
        syn::visit::visit_pat_tuple_struct(self, p);
    }

    fn visit_expr_method_call(&mut self, mc: &'ast syn::ExprMethodCall) {
        if let Some(tf) = &mc.turbofish {
            self.process_args(&mc.method.to_string(), LookupKind::Fn, tf);
        }
        self.visit_expr(&mc.receiver);
        self.visit_call_args(&mc.args, &mc.paren_token);
    }

    fn visit_expr_struct(&mut self, es: &'ast syn::ExprStruct) {
        let name = es
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .and_then(|n| {
                if n == "Self" {
                    self.stack.current_impl_self().map(|s| s.to_string())
                } else {
                    Some(n)
                }
            });
        let removed_fields: Vec<String> = name
            .and_then(|n| self.stack.db.structs.get(&n))
            .map(|i| i.removed_fields.clone())
            .unwrap_or_default();
        let elems = list_elems(&es.fields);
        let remove: Vec<bool> = es
            .fields
            .iter()
            .map(|f| match &f.member {
                syn::Member::Named(id) => removed_fields.contains(&id.to_string()),
                _ => false,
            })
            .collect();
        let whole = match &es.dot2_token {
            Some(d) => span_range(es.brace_token.span.open()).end..range_of(d).start,
            None => delim_inner(&es.brace_token.span),
        };
        self.edits
            .extend(remove_list_elems(R_CLEANUP, &elems, &remove, whole));
        let prev = self.in_type;
        self.in_type = true;
        self.handle_path(es.qself.as_ref(), &es.path, range_of(&es.path));
        self.in_type = prev;
        for (f, r) in es.fields.iter().zip(remove.iter()) {
            if !r {
                self.visit_expr(&f.expr);
            }
        }
        if let Some(rest) = &es.rest {
            self.visit_expr(rest);
        }
    }
}

fn is_phantom_expr(e: &Expr) -> bool {
    match e {
        Expr::Path(ep) => ep.path.segments.last().map(|s| s.ident == "PhantomData").unwrap_or(false),
        Expr::Call(c) => match &*c.func {
            Expr::Path(ep) => {
                let segs: Vec<String> = ep.path.segments.iter().map(|s| s.ident.to_string()).collect();
                segs.len() >= 2 && segs[segs.len() - 2] == "PhantomData" && segs[segs.len() - 1] == "default"
            }
            _ => false,
        },
        _ => false,
    }
}
