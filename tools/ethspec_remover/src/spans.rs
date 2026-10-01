//! Helpers to turn `syn` spans into byte ranges of the source text.

use proc_macro2::Span;
use std::ops::Range;
use syn::spanned::Spanned;

pub fn span_range(span: Span) -> Range<usize> {
    span.byte_range()
}

pub fn range_of<T: Spanned>(node: &T) -> Range<usize> {
    span_range(node.span())
}

/// Range covering from the start of `a` to the end of `b`.
pub fn join<A: Spanned, B: Spanned>(a: &A, b: &B) -> Range<usize> {
    let ra = range_of(a);
    let rb = range_of(b);
    ra.start..rb.end
}
