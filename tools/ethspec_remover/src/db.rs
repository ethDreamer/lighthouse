//! Global definition database and generic-parameter classification.
//!
//! Every generic-bearing item (struct, enum, trait, type alias, fn, impl, ...)
//! is a *scope* with an ordered list of type parameters. Each parameter carries
//! two flags:
//!
//! * `spec_like` — the parameter *is* the spec (`E: EthSpec`, or an unbounded
//!   `E`, or a parameter that is passed into a spec-like position elsewhere).
//! * `orphan`    — the parameter is not the spec but becomes unused once all
//!   spec-like references are rewritten (typically `T: BeaconChainTypes` used
//!   only as `T::EthSpec`).
//!
//! Both kinds are removed from the declaration and from every use site. The
//! flags are computed by a global fixpoint (see `analysis.rs`).

use std::collections::{HashMap, HashSet};
use syn::{GenericParam, Generics, Path, QSelf, Type, TypeParamBound};

use crate::specmap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeKind {
    /// struct / enum / union / type alias / trait alias
    Type,
    /// trait definition
    Trait,
    /// free fn / method / trait method
    Fn,
    /// impl block (anonymous)
    Impl,
    /// associated type with generics (rare)
    AssocType,
}

#[derive(Debug, Clone, Default)]
pub struct ParamFlags {
    pub spec_like: bool,
    /// Not the spec, but no longer used in the item header once spec-like
    /// references are rewritten. Removed from the declaration; for impl/trait
    /// scopes it is re-declared on the methods listed in `method_uses`.
    pub orphan: bool,
    /// Methods (scope ordinals within the same file) that still use the
    /// parameter after it has been orphaned from an impl/trait.
    pub method_uses: std::collections::BTreeSet<usize>,
    /// (structs/enums) the parameter appears inside a `PhantomData` field.
    pub phantom_any: bool,
    /// The parameter is bounded by a spec-bearing trait (`T: BeaconChainTypes`).
    pub spec_bound: bool,
    /// An impl needs the parameter in a way that cannot move to a method.
    pub vetoed: bool,
}

impl ParamFlags {
    pub fn removed(&self) -> bool {
        self.spec_like || self.orphan
    }
}

#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub has_default: bool,
    pub flags: ParamFlags,
    /// Source text of the inline declaration, e.g. `T: BeaconChainTypes`.
    pub decl_text: String,
    /// Source text of where-predicates bounding this parameter.
    pub where_texts: Vec<String>,
    /// Source text of the default type, if any (`= FullPayload<E>`).
    pub default_text: Option<String>,
    /// The default type after the spec rewrite (`FullPayload`).
    pub default_rewritten: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Scope {
    pub kind: ScopeKind,
    pub name: Option<String>,
    pub params: Vec<Param>,
    /// Other names sharing this definition's generics (superstruct variants).
    pub aliases: Vec<String>,
}

impl Scope {
    pub fn param_index(&self, name: &str) -> Option<usize> {
        self.params.iter().position(|p| p.name == name)
    }
}

#[derive(Debug, Default, Clone)]
pub struct FileScopes {
    pub scopes: Vec<Scope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScopeId {
    pub file: usize,
    pub ordinal: usize,
}

/// Per-struct information about fields removed entirely.
#[derive(Debug, Default, Clone)]
pub struct StructInfo {
    /// Named fields removed (phantom or bare spec-typed).
    pub removed_fields: Vec<String>,
    /// Positional (tuple) fields removed, by index.
    pub removed_positions: Vec<usize>,
    /// The tuple struct lost all its fields and becomes a unit struct.
    pub becomes_unit: bool,
}

#[derive(Debug, Default)]
pub struct Db {
    pub files: Vec<FileScopes>,
    pub type_defs: HashMap<String, Vec<ScopeId>>,
    pub fn_defs: HashMap<String, Vec<ScopeId>>,
    /// Associated type names in traits that are bounded by `EthSpec`
    /// (e.g. `BeaconChainTypes::EthSpec`). `T::<name>` is then the spec.
    pub spec_assoc_names: HashSet<String>,
    /// Traits declaring such an associated type (e.g. `BeaconChainTypes`).
    pub spec_traits: HashSet<String>,
    /// Struct name -> removed fields.
    pub structs: HashMap<String, StructInfo>,
    /// (trait name, method name) -> parameters the trait moved onto that
    /// method: (name, declaration text, where predicate texts).
    pub trait_method_moved: HashMap<(String, String), Vec<(String, String, Vec<String>)>>,
    /// Conflicts observed when consulting positional information.
    pub conflicts: HashSet<String>,
}

/// Position information for a use site `Name<A0, A1, ...>`.
pub struct Positions {
    pub removed: Vec<bool>,
    pub spec_like: Vec<bool>,
}

impl Db {
    pub fn scope(&self, id: ScopeId) -> &Scope {
        &self.files[id.file].scopes[id.ordinal]
    }

    pub fn scope_mut(&mut self, id: ScopeId) -> &mut Scope {
        &mut self.files[id.file].scopes[id.ordinal]
    }

    /// Look up which type-argument positions of `name` (used with `arity` type
    /// arguments) are removed / spec-like, consulting all definitions with a
    /// compatible arity. Positions are only marked removed when *all*
    /// compatible definitions agree.
    pub fn positions(&self, name: &str, kind: LookupKind, arity: usize) -> Option<Positions> {
        let defs = match kind {
            LookupKind::Type => self.type_defs.get(name)?,
            LookupKind::Fn => self.fn_defs.get(name)?,
        };
        let mut removed = vec![true; arity];
        let mut spec_like = vec![true; arity];
        let mut any = false;
        let mut any_removed = vec![false; arity];
        for id in defs {
            let scope = self.scope(*id);
            let n = scope.params.len();
            let n_defaults = scope.params.iter().filter(|p| p.has_default).count();
            if arity > n || arity + n_defaults < n {
                continue;
            }
            any = true;
            for i in 0..arity {
                let f = &scope.params[i].flags;
                removed[i] &= f.removed();
                spec_like[i] &= f.spec_like;
                any_removed[i] |= f.removed();
            }
        }
        if !any {
            return None;
        }
        Some(Positions {
            removed,
            spec_like,
        })
    }

    /// Definitions of `name` compatible with a use site supplying `arity`
    /// type arguments.
    pub fn compatible_defs(&self, name: &str, kind: LookupKind, arity: usize) -> Vec<ScopeId> {
        let defs = match kind {
            LookupKind::Type => self.type_defs.get(name),
            LookupKind::Fn => self.fn_defs.get(name),
        };
        let Some(defs) = defs else { return Vec::new() };
        defs.iter()
            .copied()
            .filter(|id| {
                let scope = self.scope(*id);
                let n = scope.params.len();
                let n_defaults = scope.params.iter().filter(|p| p.has_default).count();
                arity <= n && arity + n_defaults >= n
            })
            .collect()
    }

    /// For a turbofish `Name::<A0..An-1>` whose supplied arguments all vanish:
    /// the rewritten defaults of the kept, unsupplied parameters (so that the
    /// turbofish can be kept for type inference, e.g. `BeaconBlock::<FullPayload>`).
    pub fn remaining_defaults(&self, name: &str, kind: LookupKind, arity: usize) -> Option<Vec<String>> {
        let defs = match kind {
            LookupKind::Type => self.type_defs.get(name)?,
            LookupKind::Fn => self.fn_defs.get(name)?,
        };
        let mut result: Option<Vec<String>> = None;
        for id in defs {
            let scope = self.scope(*id);
            let n = scope.params.len();
            let n_defaults = scope.params.iter().filter(|p| p.has_default).count();
            if arity > n || arity + n_defaults < n {
                continue;
            }
            let mut out = Vec::new();
            for p in scope.params.iter().skip(arity) {
                if p.flags.removed() {
                    continue;
                }
                out.push(p.default_rewritten.clone()?);
            }
            match &result {
                None => result = Some(out),
                Some(r) if *r == out => {}
                Some(_) => return None,
            }
        }
        result.filter(|r| !r.is_empty())
    }

    pub fn note_conflict(&self, _msg: String) {
        // Conflicts are collected via interior mutability elsewhere; keep the
        // API minimal for now.
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LookupKind {
    Type,
    Fn,
}

/// A stack of scopes active at a point in the AST, used for name resolution.
#[derive(Clone)]
pub struct ScopeStack<'db> {
    pub db: &'db Db,
    pub file: usize,
    pub stack: Vec<usize>,
    /// Aliases declared at file level: `type E = MainnetEthSpec;`.
    pub file_aliases: HashSet<String>,
    /// Name of the self type of the innermost impl block, if any.
    pub impl_self: Vec<Option<String>>,
}

pub struct Resolved {
    pub id: ScopeId,
    pub index: usize,
}

impl<'db> ScopeStack<'db> {
    pub fn new(db: &'db Db, file: usize, file_aliases: HashSet<String>) -> Self {
        ScopeStack {
            db,
            file,
            stack: Vec::new(),
            file_aliases,
            impl_self: Vec::new(),
        }
    }

    pub fn resolve(&self, name: &str) -> Option<Resolved> {
        for &ordinal in self.stack.iter().rev() {
            let scope = &self.db.files[self.file].scopes[ordinal];
            if let Some(index) = scope.param_index(name) {
                return Some(Resolved {
                    id: ScopeId {
                        file: self.file,
                        ordinal,
                    },
                    index,
                });
            }
        }
        None
    }

    pub fn flags(&self, r: &Resolved) -> &ParamFlags {
        &self.db.scope(r.id).params[r.index].flags
    }

    /// Is a single identifier (with no path prefix) a spec type?
    pub fn is_spec_ident(&self, name: &str) -> bool {
        if let Some(r) = self.resolve(name) {
            return self.flags(&r).spec_like;
        }
        specmap::is_concrete_spec(name)
            || name == specmap::CONVENTIONAL_PARAM
            || self.file_aliases.contains(name)
    }

    /// Is `name` a declared, non-spec generic parameter (e.g. `T: BeaconChainTypes`)?
    pub fn is_plain_param(&self, name: &str) -> bool {
        match self.resolve(name) {
            Some(r) => !self.flags(&r).spec_like,
            None => false,
        }
    }

    /// Number of leading path segments (in `path.segments`) that denote the
    /// spec itself. `None` if the path does not start with the spec.
    ///
    /// Handles: `E`, `T::EthSpec`, `MainnetEthSpec`, `types::MainnetEthSpec`,
    /// `<E as EthSpec>::X`, `<T as BeaconChainTypes>::EthSpec`,
    /// `<T::EthSpec as EthSpec>::X`, `Self::EthSpec`.
    pub fn spec_prefix_len(&self, qself: Option<&QSelf>, path: &Path) -> Option<usize> {
        let segs: Vec<&syn::PathSegment> = path.segments.iter().collect();
        if let Some(q) = qself {
            let pos = q.position;
            // `<X as Trait>::rest`
            if pos == 0 {
                // `<X>::rest` — no trait.
                return if self.is_spec_type(&q.ty) {
                    Some(0)
                } else {
                    None
                };
            }
            let trait_last = segs.get(pos - 1)?.ident.to_string();
            if trait_last == "EthSpec" && self.is_spec_type(&q.ty) {
                return Some(pos);
            }
            // `<T as BeaconChainTypes>::EthSpec`
            if let Some(next) = segs.get(pos) {
                if self.db.spec_assoc_names.contains(&next.ident.to_string()) && !self.is_spec_type(&q.ty) {
                    return Some(pos + 1);
                }
            }
            return None;
        }
        let first = segs.first()?;
        let first_name = first.ident.to_string();
        if first.arguments.is_empty() && self.is_spec_ident(&first_name) {
            return Some(1);
        }
        if let Some(second) = segs.get(1) {
            let second_name = second.ident.to_string();
            // `T::EthSpec` where `T` is a generic parameter (or `Self`).
            if self.db.spec_assoc_names.contains(&second_name)
                && first.arguments.is_empty()
                && (self.is_plain_param(&first_name)
                    || first_name == "Self"
                    || looks_like_type_param(&first_name))
            {
                return Some(2);
            }
            // `types::MainnetEthSpec`, `crate::MinimalEthSpec`
            if second.arguments.is_empty()
                && specmap::is_concrete_spec(&second_name)
                && (first_name == "types" || first_name == "crate")
            {
                return Some(2);
            }
        }
        None
    }

    /// For a spec-like type of the form `T::EthSpec` /
    /// `<T as BeaconChainTypes>::EthSpec`, the parameter `T`.
    pub fn spec_base_param(&self, ty: &Type) -> Option<String> {
        let Type::Path(tp) = ty else { return None };
        if let Some(q) = &tp.qself {
            if let Type::Path(inner) = &*q.ty {
                if inner.qself.is_none() && inner.path.segments.len() == 1 {
                    let name = inner.path.segments[0].ident.to_string();
                    if self.is_plain_param(&name) {
                        return Some(name);
                    }
                }
            }
            return None;
        }
        if tp.path.segments.len() == 2 {
            let first = tp.path.segments[0].ident.to_string();
            let second = tp.path.segments[1].ident.to_string();
            if self.db.spec_assoc_names.contains(&second) && self.is_plain_param(&first) {
                return Some(first);
            }
        }
        None
    }

    /// Is this type *exactly* the spec (so it can be removed as an argument or
    /// replaced with `Spec`)?
    pub fn is_spec_type(&self, ty: &Type) -> bool {
        match ty {
            Type::Path(tp) => {
                let n = self.spec_prefix_len(tp.qself.as_ref(), &tp.path);
                matches!(n, Some(n) if n == tp.path.segments.len())
            }
            Type::Paren(p) => self.is_spec_type(&p.elem),
            Type::Group(g) => self.is_spec_type(&g.elem),
            _ => false,
        }
    }

    /// Is this type `PhantomData<X>` where X is entirely spec-like (a spec
    /// type, or a tuple of spec types)?
    pub fn is_spec_phantom(&self, ty: &Type) -> bool {
        let Type::Path(tp) = ty else { return false };
        let Some(last) = tp.path.segments.last() else { return false };
        if last.ident != "PhantomData" {
            return false;
        }
        let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
            return false;
        };
        let mut types = args.args.iter().filter_map(|a| match a {
            syn::GenericArgument::Type(t) => Some(t),
            _ => None,
        });
        let (Some(inner), None) = (types.next(), types.next()) else {
            return false;
        };
        self.is_all_spec(inner)
    }

    fn is_all_spec(&self, ty: &Type) -> bool {
        match ty {
            Type::Tuple(t) => !t.elems.is_empty() && t.elems.iter().all(|e| self.is_all_spec(e)),
            other => self.is_spec_type(other) || self.is_removed_param_type(other),
        }
    }

    /// A bare generic parameter that is being removed (spec-like or orphan).
    pub fn is_removed_param_type(&self, ty: &Type) -> bool {
        let Type::Path(tp) = ty else { return false };
        if tp.qself.is_some() || tp.path.segments.len() != 1 || !tp.path.segments[0].arguments.is_empty() {
            return false;
        }
        match self.resolve(&tp.path.segments[0].ident.to_string()) {
            Some(r) => self.flags(&r).removed(),
            None => false,
        }
    }

    pub fn current_impl_self(&self) -> Option<&str> {
        self.impl_self.last().and_then(|s| s.as_deref())
    }
}

/// `T`, `S`, `TSpec` ... single upper-case-led short identifiers used as
/// generic parameters inside macros where we cannot resolve declarations.
pub fn looks_like_type_param(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => name.len() <= 2 || name == "TSpec",
        _ => false,
    }
}

/// Does a bound list include `EthSpec`?
pub fn bounds_have_ethspec<'a>(mut bounds: impl Iterator<Item = &'a TypeParamBound>) -> bool {
    bounds.any(|b| match b {
        TypeParamBound::Trait(t) => t
            .path
            .segments
            .last()
            .map(|s| s.ident == "EthSpec")
            .unwrap_or(false),
        _ => false,
    })
}

/// Build the initial `Scope` for a generics declaration.
pub fn scope_from_generics(kind: ScopeKind, name: Option<String>, generics: &Generics, source: &str) -> Scope {
    let mut params = Vec::new();
    for gp in &generics.params {
        if let GenericParam::Type(tp) = gp {
            let name = tp.ident.to_string();
            let decl_range = crate::spans::range_of(tp);
            // Declaration text without any default (`T: Bound = Default`).
            let decl_text = match (&tp.eq_token, &tp.default) {
                (Some(eq), Some(_)) => source[decl_range.start..crate::spans::range_of(eq).start].trim_end().to_string(),
                _ => source[decl_range].to_string(),
            };
            let default_text = tp.default.as_ref().map(|d| source[crate::spans::range_of(d)].to_string());
            let mut where_texts = Vec::new();
            if let Some(wc) = &generics.where_clause {
                for pred in &wc.predicates {
                    if let syn::WherePredicate::Type(pt) = pred {
                        if let Type::Path(tp2) = &pt.bounded_ty {
                            if tp2.qself.is_none() && tp2.path.is_ident(&name) {
                                where_texts.push(source[crate::spans::range_of(pred)].to_string());
                            }
                        }
                    }
                }
            }
            let mut spec_like = bounds_have_ethspec(tp.bounds.iter());
            if let Some(wc) = &generics.where_clause {
                for pred in &wc.predicates {
                    if let syn::WherePredicate::Type(pt) = pred {
                        if let Type::Path(tp2) = &pt.bounded_ty {
                            if tp2.qself.is_none() && tp2.path.is_ident(&name) {
                                spec_like |= bounds_have_ethspec(pt.bounds.iter());
                            }
                        }
                    }
                }
            }
            params.push(Param {
                name,
                has_default: tp.default.is_some(),
                flags: ParamFlags {
                    spec_like,
                    orphan: false,
                    method_uses: Default::default(),
                    phantom_any: false,
                    spec_bound: false,
                    vetoed: false,
                },
                decl_text,
                where_texts,
                default_text,
                default_rewritten: None,
            });
        }
    }
    Scope {
        kind,
        name,
        params,
        aliases: Vec::new(),
    }
}
