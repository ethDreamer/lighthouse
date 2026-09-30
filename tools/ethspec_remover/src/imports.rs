//! Post-pass over the rewritten file: fix `use` items.
//!
//! * Rename `EthSpecId` -> `SpecId` leaves.
//! * Turn a `use ...::EthSpec` leaf into `Spec` when the file now needs `Spec`.
//! * Add `use types::Spec;` / `use typenum::U;` where needed and not in scope.
//! * Remove leaves that are no longer referenced (`EthSpec`, `Unsigned`,
//!   `PhantomData`, `MainnetEthSpec`, ...).
//!
//! Scoping is per module (file root and inline `mod {}` blocks); a child
//! module with `use super::*` inherits its parent's imports.

use std::collections::HashSet;
use std::ops::Range;

use quote::ToTokens;
use syn::{Item, ItemUse, UseTree};

use crate::edit::{extend_to_full_lines, remove_list_elems, Edit, ListElem};
use crate::rewrite::{list_elems, R_IMPORTS};
use crate::spans::range_of;

const CLEANUP_NAMES: &[&str] = &[
    "EthSpec",
    "SpecId",
    "Unsigned",
    "PhantomData",
    "MainnetEthSpec",
    "MinimalEthSpec",
    "GnosisEthSpec",
    "U",
    "Educe",
];

struct Leaf {
    name: String,
    ident_range: Range<usize>,
    removed: bool,
}

enum Node {
    Leaf(Leaf),
    Path(Box<Node>),
    Group {
        elems: Vec<(Node, ListElem)>,
        whole: Range<usize>,
    },
    Other,
}

fn build_node(tree: &UseTree) -> Node {
    match tree {
        UseTree::Path(p) => Node::Path(Box::new(build_node(&p.tree))),
        UseTree::Name(n) => Node::Leaf(Leaf {
            name: n.ident.to_string(),
            ident_range: range_of(&n.ident),
            removed: false,
        }),
        UseTree::Group(g) => {
            let elems = list_elems(&g.items);
            Node::Group {
                elems: g.items.iter().zip(elems).map(|(t, e)| (build_node(t), e)).collect(),
                whole: range_of(g),
            }
        }
        _ => Node::Other,
    }
}

fn leaves_mut<'a>(node: &'a mut Node, out: &mut Vec<&'a mut Leaf>) {
    match node {
        Node::Leaf(l) => out.push(l),
        Node::Path(c) => leaves_mut(c, out),
        Node::Group { elems, .. } => {
            for (n, _) in elems {
                leaves_mut(n, out);
            }
        }
        Node::Other => {}
    }
}

/// Emit removal edits; returns true if the whole node vanished.
fn emit(node: &Node, edits: &mut Vec<Edit>) -> bool {
    match node {
        Node::Leaf(l) => l.removed,
        Node::Path(c) => emit(c, edits),
        Node::Group { elems, whole } => {
            let flags: Vec<bool> = elems.iter().map(|(n, _)| emit(n, edits)).collect();
            if !flags.is_empty() && flags.iter().all(|f| *f) {
                return true;
            }
            let le: Vec<ListElem> = elems.iter().map(|(_, e)| e.clone()).collect();
            edits.extend(remove_list_elems(R_IMPORTS, &le, &flags, whole.clone()));
            false
        }
        Node::Other => false,
    }
}

fn glob_prefix(tree: &UseTree, prefix: &mut Vec<String>, out: &mut Vec<String>) {
    match tree {
        UseTree::Path(p) => {
            prefix.push(p.ident.to_string());
            glob_prefix(&p.tree, prefix, out);
            prefix.pop();
        }
        UseTree::Glob(_) => out.push(prefix.join("::")),
        UseTree::Group(g) => {
            for t in &g.items {
                glob_prefix(t, prefix, out);
            }
        }
        _ => {}
    }
}

struct Use {
    range: Range<usize>,
    node: Node,
    globs: Vec<String>,
}

struct Scope {
    uses: Vec<Use>,
    own_idents: HashSet<String>,
    needs_u: bool,
    children: Vec<Scope>,
    /// Where to insert new `use` lines.
    insert_at: usize,
}

fn collect_idents(ts: proc_macro2::TokenStream, out: &mut HashSet<String>, needs_u: &mut bool) {
    let toks: Vec<proc_macro2::TokenTree> = ts.into_iter().collect();
    for (i, tt) in toks.iter().enumerate() {
        match tt {
            proc_macro2::TokenTree::Group(g) => collect_idents(g.stream(), out, needs_u),
            proc_macro2::TokenTree::Ident(id) => {
                let s = id.to_string();
                if s == "U" {
                    let qualified = i >= 2
                        && matches!(&toks[i - 1], proc_macro2::TokenTree::Punct(p) if p.as_char() == ':')
                        && matches!(&toks[i - 2], proc_macro2::TokenTree::Punct(p) if p.as_char() == ':');
                    if let (Some(proc_macro2::TokenTree::Punct(p)), Some(proc_macro2::TokenTree::Group(g))) =
                        (toks.get(i + 1), toks.get(i + 2))
                    {
                        if p.as_char() == '<' && g.delimiter() == proc_macro2::Delimiter::Brace && !qualified {
                            *needs_u = true;
                        }
                    }
                }
                out.insert(s);
            }
            proc_macro2::TokenTree::Literal(l) => {
                // Only `bound = "..."` strings name types; doc comments and
                // other literals must not count as uses.
                let is_bound = i >= 2
                    && matches!(&toks[i - 1], proc_macro2::TokenTree::Punct(p) if p.as_char() == '=')
                    && matches!(&toks[i - 2], proc_macro2::TokenTree::Ident(id) if id == "bound");
                let t = l.to_string();
                if is_bound && t.starts_with('"') {
                    for w in t.split(|c: char| !(c.is_alphanumeric() || c == '_')) {
                        if !w.is_empty() {
                            out.insert(w.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

fn build_scope(items: &[Item], default_insert: usize) -> Scope {
    let mut scope = Scope {
        uses: Vec::new(),
        own_idents: HashSet::new(),
        needs_u: false,
        children: Vec::new(),
        insert_at: default_insert,
    };
    let mut first_use: Option<usize> = None;
    let mut first_item: Option<usize> = None;
    for item in items {
        let r = range_of(item);
        if first_item.is_none() {
            first_item = Some(r.start);
        }
        match item {
            Item::Use(u) => {
                if first_use.is_none() {
                    first_use = Some(r.start);
                }
                scope.uses.push(build_use(u));
            }
            Item::Mod(m) => {
                collect_idents(m.attrs.iter().map(|a| a.to_token_stream()).collect(), &mut scope.own_idents, &mut scope.needs_u);
                if let Some((brace, content)) = &m.content {
                    let inner_start = crate::spans::span_range(brace.span.open()).end;
                    scope.children.push(build_scope(content, inner_start));
                }
            }
            other => collect_idents(other.to_token_stream(), &mut scope.own_idents, &mut scope.needs_u),
        }
    }
    scope.insert_at = first_use.or(first_item).unwrap_or(default_insert);
    scope
}

fn build_use(u: &ItemUse) -> Use {
    let mut globs = Vec::new();
    glob_prefix(&u.tree, &mut Vec::new(), &mut globs);
    Use {
        range: range_of(u),
        node: build_node(&u.tree),
        globs,
    }
}

impl Scope {
    fn has_super_glob(&self) -> bool {
        self.uses.iter().any(|u| u.globs.iter().any(|g| g == "super"))
    }

    /// Identifiers visible as "used" in this scope: its own plus those of
    /// descendants that pull this scope's imports in via `use super::*`.
    fn used(&self, name: &str) -> bool {
        self.own_idents.contains(name)
            || self.children.iter().any(|c| c.has_super_glob() && c.used(name))
    }

    fn needs_u(&self) -> bool {
        self.needs_u || self.children.iter().any(|c| c.has_super_glob() && c.needs_u())
    }

    fn has_leaf(&self, name: &str) -> bool {
        self.uses.iter().any(|u| {
            let mut v = Vec::new();
            leaves(&u.node, &mut v);
            v.iter().any(|l| l.name == name && !l.removed)
        })
    }
}

fn leaves<'a>(node: &'a Node, out: &mut Vec<&'a Leaf>) {
    match node {
        Node::Leaf(l) => out.push(l),
        Node::Path(c) => leaves(c, out),
        Node::Group { elems, .. } => {
            for (n, _) in elems {
                leaves(n, out);
            }
        }
        Node::Other => {}
    }
}

fn indent_at(source: &str, pos: usize) -> String {
    let line_start = source[..pos].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let ws: String = source[line_start..pos]
        .chars()
        .take_while(|c| c.is_whitespace())
        .collect();
    ws
}

pub fn import_edits(source: &str, ast: &syn::File, in_types_crate: bool) -> Vec<Edit> {
    let default_insert = ast.items.first().map(|i| range_of(i).start).unwrap_or(source.len());
    let mut root = build_scope(&ast.items, default_insert);
    let mut edits = Vec::new();
    process_scope(&mut root, source, in_types_crate, &mut edits, /*parent_provides_spec=*/ false, false);
    edits
}

fn process_scope(
    scope: &mut Scope,
    source: &str,
    in_types_crate: bool,
    edits: &mut Vec<Edit>,
    parent_spec: bool,
    parent_u: bool,
) {
    // 1. Rename EthSpecId leaves.
    for u in &mut scope.uses {
        let mut v = Vec::new();
        leaves_mut(&mut u.node, &mut v);
        for l in v {
            if l.name == "EthSpecId" {
                edits.push(Edit::replace(R_IMPORTS, l.ident_range.clone(), "SpecId"));
                l.name = "SpecId".to_string();
            }
        }
    }

    let super_glob = scope.has_super_glob();
    let glob_provides = |scope: &Scope, prefixes: &[&str]| {
        scope
            .uses
            .iter()
            .any(|u| u.globs.iter().any(|g| prefixes.iter().any(|p| g == p)))
    };

    // 2. Spec.
    let needs_spec = scope.used("Spec");
    let mut spec_provided = scope.has_leaf("Spec")
        || glob_provides(scope, &["types"])
        || (in_types_crate && glob_provides(scope, &["crate", "crate::core"]))
        || (super_glob && parent_spec);
    if needs_spec && !spec_provided {
        // Prefer renaming an existing EthSpec leaf.
        let mut renamed = false;
        'outer: for u in &mut scope.uses {
            let mut v = Vec::new();
            leaves_mut(&mut u.node, &mut v);
            for l in v {
                if l.name == "EthSpec" {
                    edits.push(Edit::replace(R_IMPORTS, l.ident_range.clone(), "Spec"));
                    l.name = "Spec".to_string();
                    renamed = true;
                    break 'outer;
                }
            }
        }
        if !renamed {
            let line = if in_types_crate {
                "use crate::core::Spec;"
            } else {
                "use types::Spec;"
            };
            let at = scope.insert_at;
            let indent = indent_at(source, at);
            edits.push(Edit::insert(R_IMPORTS, at, format!("{line}\n{indent}")));
        }
        spec_provided = true;
    }

    // 3. typenum::U.
    let needs_u = scope.needs_u();
    let mut u_provided = scope.has_leaf("U") || glob_provides(scope, &["typenum"]) || (super_glob && parent_u);
    if needs_u && !u_provided {
        let at = scope.insert_at;
        let indent = indent_at(source, at);
        edits.push(Edit::insert(R_IMPORTS, at, format!("use typenum::U;\n{indent}")));
        u_provided = true;
    }

    // 4. Remove unused leaves.
    let unused: Vec<String> = CLEANUP_NAMES
        .iter()
        .filter(|n| !scope.used(n))
        .map(|n| n.to_string())
        .collect();
    for u in &mut scope.uses {
        let mut v = Vec::new();
        leaves_mut(&mut u.node, &mut v);
        for l in v {
            if unused.contains(&l.name) {
                l.removed = true;
            }
        }
    }
    for u in &scope.uses {
        if emit(&u.node, edits) {
            edits.push(Edit::delete(R_IMPORTS, extend_to_full_lines(source, u.range.clone())));
        }
    }

    for child in &mut scope.children {
        process_scope(child, source, in_types_crate, edits, spec_provided, u_provided);
    }
}
