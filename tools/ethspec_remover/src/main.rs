//! `ethspec_remover` — reproduce the mechanical part of Lighthouse PR 9229
//! ("Remove EthSpec") as a re-runnable, reviewable transformation.
//!
//! See `README.md` in this directory for the rules and the residual.

mod analysis;
mod attrs;
mod db;
mod edit;
mod imports;
mod rewrite;
mod spans;
mod specmap;
mod walk;

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use syn::visit::Visit;

use analysis::FileInput;
use db::ScopeStack;
use edit::{apply_edits, Edit};

/// Files that are handled by hand (part of the residual).
const EXCLUDED_FILES: &[&str] = &[
    "consensus/types/src/core/eth_spec.rs",
    "consensus/types/src/core/spec.rs",
];

#[derive(Parser)]
#[command(name = "ethspec_remover", about = "Transform the EthSpec trait into the compile-time Spec type")]
struct Cli {
    /// Repository root (default: current directory).
    #[arg(long, default_value = ".")]
    repo: PathBuf,

    /// Only apply these rules (comma separated). See --list-rules.
    #[arg(long, value_delimiter = ',')]
    only: Vec<String>,

    /// Only process files whose path contains one of these substrings.
    #[arg(long)]
    file: Vec<String>,

    /// Report what would change without writing anything.
    #[arg(long)]
    dry_run: bool,

    /// Skip `cargo fmt --all` after writing.
    #[arg(long)]
    no_fmt: bool,

    /// Write warnings and per-file details to this report file.
    #[arg(long)]
    report: Option<PathBuf>,

    /// Print the rule names and exit.
    #[arg(long)]
    list_rules: bool,

    /// Debug: print the analysed generic parameters of items whose name
    /// contains this string, then exit.
    #[arg(long)]
    dump_scopes: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.list_rules {
        for r in rewrite::ALL_RULES {
            println!("{r}");
        }
        return Ok(());
    }
    let repo = cli.repo.canonicalize().context("repo path")?;
    let paths = list_rust_files(&repo)?;
    eprintln!("loading {} files", paths.len());

    let mut files = Vec::new();
    let mut unparsed = Vec::new();
    for rel in &paths {
        let abs = repo.join(rel);
        let source = std::fs::read_to_string(&abs).with_context(|| abs.display().to_string())?;
        match syn::parse_file(&source) {
            Ok(ast) => {
                let aliases = analysis::collect_file_aliases(&ast);
                let macros = walk::preparse_macros(&ast);
                files.push(FileInput {
                    path: rel.clone(),
                    source,
                    ast,
                    aliases,
                    macros,
                });
            }
            Err(e) => unparsed.push(format!("{}: {e}", rel.display())),
        }
    }
    for u in &unparsed {
        eprintln!("skip (parse error): {u}");
    }

    let db = analysis::analyze(&files);
    let (mut n_spec, mut n_orphan) = (0, 0);
    for f in &db.files {
        for s in &f.scopes {
            for p in &s.params {
                if p.flags.spec_like {
                    n_spec += 1;
                } else if p.flags.orphan {
                    n_orphan += 1;
                }
            }
        }
    }
    eprintln!("analysis: {n_spec} spec-like params, {n_orphan} orphaned params");

    if let Some(needle) = &cli.dump_scopes {
        for (fi, f) in db.files.iter().enumerate() {
            for (oi, s) in f.scopes.iter().enumerate() {
                let name = s.name.clone().unwrap_or_else(|| "<impl>".to_string());
                if !name.contains(needle.as_str()) || s.params.is_empty() {
                    continue;
                }
                println!("{}#{oi} {:?} {name}", files[fi].path.display(), s.kind);
                for p in &s.params {
                    println!(
                        "    {:<12} spec_like={} orphan={} method_uses={:?} phantom_any={} spec_bound={} vetoed={}  [{}]",
                        p.name, p.flags.spec_like, p.flags.orphan, p.flags.method_uses, p.flags.phantom_any, p.flags.spec_bound, p.flags.vetoed, p.decl_text
                    );
                }
            }
        }
        return Ok(());
    }

    let only: HashSet<&str> = cli.only.iter().map(|s| s.as_str()).collect();
    let mut rule_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut report = String::new();
    let mut changed_files = 0;
    let mut u_files: Vec<PathBuf> = Vec::new();

    for (fi, f) in files.iter().enumerate() {
        if !cli.file.is_empty() && !cli.file.iter().any(|s| f.path.to_string_lossy().contains(s)) {
            continue;
        }
        let stack = ScopeStack::new(&db, fi, f.aliases.clone());
        let mut rw = rewrite::Rewriter::new(stack, &f.macros, &f.source);
        rw.visit_file(&f.ast);
        if rw.ordinal_count() != db.files[fi].scopes.len() {
            anyhow::bail!(
                "internal error: scope count mismatch in {} ({} vs {})",
                f.path.display(),
                rw.ordinal_count(),
                db.files[fi].scopes.len()
            );
        }
        for w in &rw.warnings {
            report.push_str(&format!("{}: warning: {w}\n", f.path.display()));
        }
        let mut edits: Vec<Edit> = rw
            .edits
            .into_iter()
            .filter(|e| only.is_empty() || only.contains(e.rule))
            .collect();
        // typenum style: files with only a couple of `U<{ .. }>` uses spell
        // them `typenum::U<{ .. }>` inline instead of importing `U` (matches
        // the PR in the large majority of files).
        let n_typenum = edits.iter().filter(|e| e.rule == rewrite::R_TYPENUM).count();
        if n_typenum <= 2 {
            for e in edits.iter_mut().filter(|e| e.rule == rewrite::R_TYPENUM) {
                e.replacement = e.replacement.replace("U<{", "typenum::U<{");
            }
        }
        let pass1 = apply_edits(&f.source, edits);
        for (a, b) in &pass1.conflicts {
            report.push_str(&format!(
                "{}: conflict: {:?}@{:?} vs {:?}@{:?}\n",
                f.path.display(),
                a.rule,
                a.range,
                b.rule,
                b.range
            ));
        }
        for e in &pass1.applied {
            *rule_counts.entry(e.rule).or_default() += 1;
        }

        // Import fix-up on the rewritten text.
        let mut text = pass1.text;
        if only.is_empty() || only.contains(rewrite::R_IMPORTS) {
            match syn::parse_file(&text) {
                Ok(ast2) => {
                    let in_types = f.path.starts_with("consensus/types/src");
                    let iedits = imports::import_edits(&text, &ast2, in_types);
                    if iedits.iter().any(|e| e.replacement.contains("typenum::U")) {
                        u_files.push(f.path.clone());
                    }
                    let pass2 = apply_edits(&text, iedits);
                    for e in &pass2.applied {
                        *rule_counts.entry(e.rule).or_default() += 1;
                    }
                    text = pass2.text;
                }
                Err(e) => {
                    report.push_str(&format!(
                        "{}: rewritten file does not parse: {e}\n",
                        f.path.display()
                    ));
                }
            }
        }

        if text != f.source {
            changed_files += 1;
            if !cli.dry_run {
                std::fs::write(repo.join(&f.path), &text)?;
            }
        }
    }

    // Root Cargo.toml: typenum const-generics feature.
    let root_toml = repo.join("Cargo.toml");
    if let Ok(toml) = std::fs::read_to_string(&root_toml) {
        let old = "typenum = \"1\"";
        if toml.contains(old) {
            let new = toml.replace(old, "typenum = { version = \"1\", features = [\"const-generics\"] }");
            *rule_counts.entry("cargo-toml").or_default() += 1;
            if !cli.dry_run {
                std::fs::write(&root_toml, new)?;
            }
        }
    }

    eprintln!("changed files: {changed_files}");
    for (rule, n) in &rule_counts {
        eprintln!("  {rule:<14} {n}");
    }
    if !u_files.is_empty() {
        report.push_str(&format!("files given `use typenum::U`: {}\n", u_files.len()));
    }
    if let Some(p) = &cli.report {
        std::fs::write(p, &report)?;
        eprintln!("report written to {}", p.display());
    } else if !report.is_empty() {
        let lines: Vec<&str> = report.lines().collect();
        for l in lines.iter().take(40) {
            eprintln!("{l}");
        }
        if lines.len() > 40 {
            eprintln!("... {} more report lines (use --report)", lines.len() - 40);
        }
    }

    if !cli.dry_run && !cli.no_fmt {
        eprintln!("running cargo fmt --all");
        let status = std::process::Command::new("cargo")
            .args(["fmt", "--all"])
            .current_dir(&repo)
            .status()?;
        if !status.success() {
            eprintln!("cargo fmt failed (some files may not parse)");
        }
    }
    Ok(())
}

/// Tracked `.rs` files, excluding the residual-only files and this tool.
fn list_rust_files(repo: &Path) -> Result<Vec<PathBuf>> {
    let out = std::process::Command::new("git")
        .args(["ls-files", "--", "*.rs"])
        .current_dir(repo)
        .output()
        .context("git ls-files")?;
    if !out.status.success() {
        anyhow::bail!("git ls-files failed");
    }
    let mut paths = Vec::new();
    for line in String::from_utf8(out.stdout)?.lines() {
        if line.starts_with("tools/") || EXCLUDED_FILES.contains(&line) {
            continue;
        }
        paths.push(PathBuf::from(line));
    }
    Ok(paths)
}
