//! Attribute handling: `#[serde(bound = "E: EthSpec, ...")]`,
//! `#[educe(Hash(bound(E: EthSpec)))]`, `#[arbitrary(bound = "...")]`, and the
//! same forms nested inside `#[superstruct(variant_attributes(...))]` and
//! `#[cfg_attr(...)]`.
//!
//! The attribute token stream is walked as a comma-separated list of items,
//! recursively. Each `bound` entry is parsed as where-predicates and rewritten
//! with the ordinary predicate rules. Entries that end up empty are removed,
//! and emptiness cascades outwards (an empty `serde(...)` is removed; an empty
//! `Hash(bound(...))` becomes `Hash`; an attribute that ends up with nothing
//! left is removed entirely).

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use std::ops::Range;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::WherePredicate;

use crate::db::ScopeStack;
use crate::edit::{extend_to_full_lines, remove_list_elems, remove_list_elems_opts, Edit, ListElem};
use crate::rewrite::{list_elems, predicate_removed, Rewriter, R_BOUNDS, R_CLEANUP};
use crate::spans::span_range;

pub fn process_attribute(
    attr: &syn::Attribute,
    stack: &ScopeStack<'_>,
    source: &str,
    edits: &mut Vec<Edit>,
    warnings: &mut Vec<String>,
) {
    let syn::Meta::List(list) = &attr.meta else { return };
    let name = list.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
    if name == "derive" || name == "cfg" || name == "doc" {
        return;
    }
    let mut ctx = Ctx {
        stack,
        source,
        edits,
        warnings,
    };
    let all_removed = ctx.process_list(list.tokens.clone(), &name);
    if all_removed {
        let r = extend_to_full_lines(source, span_range(attr.span_full()));
        ctx.edits.push(Edit::delete(R_CLEANUP, r));
    }
}

trait AttrSpan {
    fn span_full(&self) -> proc_macro2::Span;
}
impl AttrSpan for syn::Attribute {
    fn span_full(&self) -> proc_macro2::Span {
        use syn::spanned::Spanned;
        self.span()
    }
}

struct Ctx<'a, 'db> {
    stack: &'a ScopeStack<'db>,
    source: &'a str,
    edits: &'a mut Vec<Edit>,
    warnings: &'a mut Vec<String>,
}

struct Item {
    toks: Vec<TokenTree>,
    elem: ListElem,
}

fn split_items(tokens: TokenStream) -> Vec<Item> {
    let mut items = Vec::new();
    let mut cur: Vec<TokenTree> = Vec::new();
    for tt in tokens {
        match &tt {
            TokenTree::Punct(p) if p.as_char() == ',' => {
                if !cur.is_empty() {
                    let range = span_range(cur[0].span()).start..span_range(cur.last().unwrap().span()).end;
                    items.push(Item {
                        toks: std::mem::take(&mut cur),
                        elem: ListElem {
                            range,
                            comma: Some(span_range(p.span())),
                        },
                    });
                }
            }
            _ => cur.push(tt),
        }
    }
    if !cur.is_empty() {
        let range = span_range(cur[0].span()).start..span_range(cur.last().unwrap().span()).end;
        items.push(Item {
            toks: cur,
            elem: ListElem { range, comma: None },
        });
    }
    items
}

fn ident_is(tt: &TokenTree, s: &str) -> bool {
    matches!(tt, TokenTree::Ident(i) if i == s)
}

impl<'a, 'db> Ctx<'a, 'db> {
    /// Returns true if every item of the list was removed (caller removes the
    /// enclosing group / attribute).
    fn process_list(&mut self, tokens: TokenStream, owner: &str) -> bool {
        let items = split_items(tokens);
        if items.is_empty() {
            return false;
        }
        let mut removed = vec![false; items.len()];
        for (i, item) in items.iter().enumerate() {
            let t = &item.toks;
            match t.as_slice() {
                [a, TokenTree::Punct(eq), TokenTree::Literal(lit)] if ident_is(a, "bound") && eq.as_char() == '=' => {
                    removed[i] = self.process_bound_string(lit);
                }
                [a, TokenTree::Group(g)] if ident_is(a, "bound") && g.delimiter() == Delimiter::Parenthesis => {
                    removed[i] = self.process_bound_tokens(g.stream());
                }
                [TokenTree::Ident(id), TokenTree::Group(g)]
                    if matches!(g.delimiter(), Delimiter::Parenthesis | Delimiter::Bracket) =>
                {
                    let name = id.to_string();
                    let inner_all = self.process_list(g.stream(), &name);
                    if inner_all {
                        if name.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                            // `Hash(bound(...))` -> `Hash`
                            self.edits.push(Edit::delete(R_CLEANUP, span_range(g.span())));
                        } else {
                            removed[i] = true;
                        }
                    }
                }
                _ => {}
            }
        }
        let all = if owner == "cfg_attr" {
            removed.len() > 1 && removed[1..].iter().all(|r| *r)
        } else {
            removed.iter().all(|r| *r)
        };
        if all {
            return true;
        }
        // `derive(..., Educe, ...)` + `educe(A, B)` with no bounds left:
        // fold the educe derives into `derive` (as the PR does).
        self.merge_educe_into_derive(&items, &mut removed);
        let elems: Vec<ListElem> = items.iter().map(|i| i.elem.clone()).collect();
        self.edits
            .extend(remove_list_elems_opts(R_CLEANUP, &elems, &removed, 0..0, true));
        false
    }

    /// If the list has both a `derive(.. Educe ..)` item and an `educe(...)`
    /// item whose entries all become bare derive names, replace `Educe` with
    /// those names and drop the `educe` item.
    fn merge_educe_into_derive(&mut self, items: &[Item], removed: &mut [bool]) {
        let mut derive_educe_ident: Option<Range<usize>> = None;
        let mut educe_idx: Option<usize> = None;
        for (i, item) in items.iter().enumerate() {
            if let [TokenTree::Ident(id), TokenTree::Group(g)] = item.toks.as_slice() {
                if id == "derive" {
                    for t in g.stream() {
                        if let TokenTree::Ident(x) = &t {
                            if x == "Educe" {
                                derive_educe_ident = Some(span_range(x.span()));
                            }
                        }
                    }
                } else if id == "educe" && !removed[i] {
                    educe_idx = Some(i);
                }
            }
        }
        let (Some(educe_range), Some(ei)) = (derive_educe_ident, educe_idx) else {
            return;
        };
        let TokenTree::Group(g) = &items[ei].toks[1] else { return };
        let Some(names) = educe_bare_items(g.stream(), self.stack) else {
            return;
        };
        self.edits
            .push(Edit::replace(R_CLEANUP, educe_range, names.join(", ")));
        removed[ei] = true;
    }

    /// `bound = "E: EthSpec, Payload: AbstractExecPayload<E>"`.
    fn process_bound_string(&mut self, lit: &proc_macro2::Literal) -> bool {
        let text = lit.to_string();
        if !text.starts_with('"') || !text.ends_with('"') || text.contains('\\') {
            self.warnings.push(format!("unsupported bound literal: {text}"));
            return false;
        }
        let inner = &text[1..text.len() - 1];
        let base = span_range(lit.span()).start + 1;
        let parser = Punctuated::<WherePredicate, syn::Token![,]>::parse_terminated;
        let preds = match syn::parse::Parser::parse_str(parser, inner) {
            Ok(p) => p,
            Err(_) => {
                self.warnings.push(format!("unparseable bound string: {text}"));
                return false;
            }
        };
        self.rewrite_predicates(&preds, inner, base)
    }

    /// educe form: `bound(E: EthSpec, Payload: AbstractExecPayload<E>)`.
    fn process_bound_tokens(&mut self, tokens: TokenStream) -> bool {
        let parser = Punctuated::<WherePredicate, syn::Token![,]>::parse_terminated;
        let preds = match syn::parse::Parser::parse2(parser, tokens) {
            Ok(p) => p,
            Err(_) => {
                self.warnings.push("unparseable educe bound".to_string());
                return false;
            }
        };
        self.rewrite_predicates(&preds, self.source, 0)
    }

    fn rewrite_predicates(
        &mut self,
        preds: &Punctuated<WherePredicate, syn::Token![,]>,
        sub_source: &str,
        base: usize,
    ) -> bool {
        let removed: Vec<bool> = preds.iter().map(|p| predicate_removed(self.stack, p)).collect();
        if !preds.is_empty() && removed.iter().all(|r| *r) {
            return true;
        }
        let elems = list_elems(preds);
        let mut local = remove_list_elems(R_BOUNDS, &elems, &removed, 0..0);
        // Rewrite the kept predicates with a sub-rewriter over the sub-source.
        let empty = Default::default();
        let mut sub = Rewriter::new(self.stack.clone(), &empty, sub_source);
        for (p, r) in preds.iter().zip(removed.iter()) {
            if !r {
                sub.visit_where_predicate(p);
            }
        }
        local.extend(sub.edits);
        self.warnings.extend(sub.warnings);
        for mut e in local {
            e.range = shift(e.range, base);
            self.edits.push(e);
        }
        false
    }
}

fn shift(r: Range<usize>, base: usize) -> Range<usize> {
    r.start + base..r.end + base
}

/// For `educe(PartialEq, Hash(bound(E: EthSpec)))`: if at least one `bound`
/// is present and every entry becomes a bare identifier once the spec bounds
/// are dropped, return the identifiers.
pub fn educe_bare_items(tokens: TokenStream, stack: &ScopeStack<'_>) -> Option<Vec<String>> {
    let items = split_items(tokens);
    let mut names = Vec::new();
    let mut had_bound = false;
    for item in &items {
        match item.toks.as_slice() {
            [TokenTree::Ident(id)] => names.push(id.to_string()),
            [TokenTree::Ident(id), TokenTree::Group(g)] => {
                // Must be exactly `bound(preds)` or `bound = "preds"` with all
                // preds removable.
                let inner = split_items(g.stream());
                if inner.len() != 1 {
                    return None;
                }
                let parser = Punctuated::<WherePredicate, syn::Token![,]>::parse_terminated;
                let preds = match inner[0].toks.as_slice() {
                    [b, TokenTree::Group(bg)] if ident_is(b, "bound") => {
                        syn::parse::Parser::parse2(parser, bg.stream()).ok()?
                    }
                    [b, TokenTree::Punct(eq), TokenTree::Literal(lit)] if ident_is(b, "bound") && eq.as_char() == '=' => {
                        let text = lit.to_string();
                        if !text.starts_with('"') || text.contains('\\') {
                            return None;
                        }
                        syn::parse::Parser::parse_str(parser, &text[1..text.len() - 1]).ok()?
                    }
                    _ => return None,
                };
                had_bound = true;
                if preds.is_empty() || !preds.iter().all(|p| predicate_removed(stack, p)) {
                    return None;
                }
                names.push(id.to_string());
            }
            _ => return None,
        }
    }
    if had_bound && !names.is_empty() {
        Some(names)
    } else {
        None
    }
}

/// Item-level form: `#[derive(.., Educe, ..)]` + `#[educe(...)]` as separate
/// attributes. Returns the byte offset of the educe attribute if merged (so the
/// caller skips its normal processing).
pub fn merge_item_educe(
    attrs: &[syn::Attribute],
    stack: &ScopeStack<'_>,
    source: &str,
    edits: &mut Vec<Edit>,
) -> Option<usize> {
    let mut educe_ident: Option<Range<usize>> = None;
    let mut educe_attr: Option<&syn::Attribute> = None;
    for attr in attrs {
        let syn::Meta::List(list) = &attr.meta else { continue };
        if list.path.is_ident("derive") {
            for t in list.tokens.clone() {
                if let TokenTree::Ident(x) = &t {
                    if x == "Educe" {
                        educe_ident = Some(span_range(x.span()));
                    }
                }
            }
        } else if list.path.is_ident("educe") {
            educe_attr = Some(attr);
        }
    }
    let (Some(range), Some(attr)) = (educe_ident, educe_attr) else {
        return None;
    };
    let syn::Meta::List(list) = &attr.meta else { return None };
    let names = educe_bare_items(list.tokens.clone(), stack)?;
    edits.push(Edit::replace(R_CLEANUP, range, names.join(", ")));
    let r = extend_to_full_lines(source, span_range(attr.span_full()));
    let key = r.start;
    edits.push(Edit::delete(R_CLEANUP, r));
    Some(key)
}
