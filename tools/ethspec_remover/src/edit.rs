//! Span-based text editing.
//!
//! All transformations are expressed as byte-range edits against the original
//! source text. Edits are tagged with the rule that produced them so that a
//! subset of rules can be applied and reported on independently.

use std::ops::Range;

#[derive(Debug, Clone)]
pub struct Edit {
    pub rule: &'static str,
    pub range: Range<usize>,
    pub replacement: String,
}

impl Edit {
    pub fn delete(rule: &'static str, range: Range<usize>) -> Self {
        Edit {
            rule,
            range,
            replacement: String::new(),
        }
    }

    pub fn replace(rule: &'static str, range: Range<usize>, replacement: impl Into<String>) -> Self {
        Edit {
            rule,
            range,
            replacement: replacement.into(),
        }
    }

    pub fn insert(rule: &'static str, at: usize, text: impl Into<String>) -> Self {
        Edit {
            rule,
            range: at..at,
            replacement: text.into(),
        }
    }
}

/// Outcome of applying a set of edits to a source string.
pub struct Applied {
    pub text: String,
    pub applied: Vec<Edit>,
    /// Edits that were dropped because they were nested inside another edit.
    pub dropped: Vec<Edit>,
    /// Edits that partially overlapped another edit and could not be applied.
    pub conflicts: Vec<(Edit, Edit)>,
}

/// Apply edits to `source`.
///
/// Edits are sorted by start offset. An edit that lies entirely inside a
/// previous edit's range is dropped (the outer edit wins, which is what we want
/// when e.g. a whole field is deleted and there were also edits inside its
/// type). Partial overlaps are reported as conflicts and skipped.
pub fn apply_edits(source: &str, mut edits: Vec<Edit>) -> Applied {
    // Sort by start; for equal starts, longer ranges first so outer edits win.
    edits.sort_by(|a, b| {
        a.range
            .start
            .cmp(&b.range.start)
            .then(b.range.end.cmp(&a.range.end))
    });

    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize;
    let mut applied: Vec<Edit> = Vec::new();
    let mut dropped = Vec::new();
    let mut conflicts = Vec::new();

    for edit in edits {
        if let Some(prev) = applied.last() {
            if edit.range.start < prev.range.end {
                if edit.range.end <= prev.range.end {
                    dropped.push(edit);
                } else {
                    conflicts.push((prev.clone(), edit));
                }
                continue;
            }
            // Pure insertions at the same point as a previous zero-width edit are
            // fine; identical deletions are de-duplicated.
            if edit.range == prev.range && edit.replacement == prev.replacement {
                continue;
            }
        }
        out.push_str(&source[cursor..edit.range.start]);
        out.push_str(&edit.replacement);
        cursor = edit.range.end;
        applied.push(edit);
    }
    out.push_str(&source[cursor..]);

    Applied {
        text: out,
        applied,
        dropped,
        conflicts,
    }
}

/// Extend `range` so that it covers whole lines when the text on the same
/// line(s) outside the range is only whitespace. Used when deleting entire
/// items so that no blank line with trailing indentation is left behind.
pub fn extend_to_full_lines(source: &str, range: Range<usize>) -> Range<usize> {
    let bytes = source.as_bytes();
    let mut start = range.start;
    let mut end = range.end;

    // Walk back to line start if only whitespace precedes on this line.
    let mut s = start;
    while s > 0 && bytes[s - 1] != b'\n' {
        if !(bytes[s - 1] as char).is_whitespace() {
            return range; // Not alone on the line: leave as-is.
        }
        s -= 1;
    }
    // Walk forward to end of line if only whitespace follows.
    let mut e = end;
    while e < bytes.len() && bytes[e] != b'\n' {
        if !(bytes[e] as char).is_whitespace() {
            return range;
        }
        e += 1;
    }
    if e < bytes.len() {
        e += 1; // include the newline
    }
    start = s;
    end = e;
    start..end
}

/// Description of one element of a comma-separated list.
#[derive(Debug, Clone)]
pub struct ListElem {
    pub range: Range<usize>,
    /// Range of the trailing comma, if present.
    pub comma: Option<Range<usize>>,
}

/// Produce deletion edits removing the elements at `remove` from a
/// comma-separated list. If every element is removed, `whole` (the range of the
/// entire list including delimiters) is deleted instead.
pub fn remove_list_elems(
    rule: &'static str,
    elems: &[ListElem],
    remove: &[bool],
    whole: Range<usize>,
) -> Vec<Edit> {
    remove_list_elems_opts(rule, elems, remove, whole, false)
}

/// As `remove_list_elems`; with `keep_trailing_comma` a trailing run of
/// removed elements leaves the comma after the last kept element in place
/// (attribute lists tolerate trailing commas and this matches the PR).
pub fn remove_list_elems_opts(
    rule: &'static str,
    elems: &[ListElem],
    remove: &[bool],
    whole: Range<usize>,
    keep_trailing_comma: bool,
) -> Vec<Edit> {
    debug_assert_eq!(elems.len(), remove.len());
    let mut edits = Vec::new();
    if elems.is_empty() {
        return edits;
    }
    if remove.iter().all(|r| *r) {
        edits.push(Edit::delete(rule, whole));
        return edits;
    }
    let last_kept = remove.iter().rposition(|r| !*r).expect("some kept");
    let mut i = 0;
    while i < elems.len() {
        if !remove[i] {
            i += 1;
            continue;
        }
        if i < last_kept {
            // Delete from this element's start to the next element's start.
            let next_start = elems[i + 1].range.start;
            edits.push(Edit::delete(rule, elems[i].range.start..next_start));
            i += 1;
        } else {
            // Trailing run: delete from end of last kept element to end of the
            // final element (including its trailing comma, if any).
            let last = elems.last().unwrap();
            let end = last
                .comma
                .as_ref()
                .map(|c| c.end)
                .unwrap_or(last.range.end);
            let start = if keep_trailing_comma {
                elems[last_kept]
                    .comma
                    .as_ref()
                    .map(|c| c.end)
                    .unwrap_or(elems[last_kept].range.end)
            } else {
                elems[last_kept].range.end
            };
            edits.push(Edit::delete(rule, start..end));
            break;
        }
    }
    edits
}
