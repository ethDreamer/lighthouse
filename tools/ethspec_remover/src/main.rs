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

    /// Apply the rules one at a time, committing after each (git). The final
    /// tree is byte-identical to a normal run; the intermediate commits are
    /// review slices and are not expected to compile.
    #[arg(long)]
    stack: bool,

    /// Extra trailer line(s) appended to every commit message in --stack mode.
    #[arg(long)]
    trailer: Vec<String>,
}

/// Stage order for --stack. Each entry is (rule, one-line description).
const STAGES: &[(&str, &str)] = &[
    (rewrite::R_TEST, "MinimalEthSpec/MainnetEthSpec -> Spec in tests; `type E = ..` aliases dropped"),
    (rewrite::R_CONST, "E::Assoc::to_usize() / E::method() -> Spec::CONST / Spec::method()"),
    (rewrite::R_TYPENUM, "E::Assoc in type position -> U<{ Spec::CONST }>"),
    (rewrite::R_BOUNDS, "drop `where E: EthSpec` and predicates on removed parameters"),
    (rewrite::R_TYPEPARAMS, "remove spec-like and orphaned type parameters at declarations and use sites"),
    (rewrite::R_PROJ, "bare T::EthSpec in type position -> Spec"),
    (rewrite::R_CLEANUP, "attribute bounds, PhantomData fields, assoc types, redundant closures"),
];

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

    // Per-file rewrite edits (the full set, computed once).
    let mut file_edits: Vec<Vec<Edit>> = Vec::with_capacity(files.len());
    for (fi, f) in files.iter().enumerate() {
        if !cli.file.is_empty() && !cli.file.iter().any(|s| f.path.to_string_lossy().contains(s)) {
            file_edits.push(Vec::new());
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
        file_edits.push(edits);
    }

    // Apply `rules` to every file, starting from the original text. With
    // `finalize`, also run the import post-pass and count edits.
    let apply_rules = |rules: &HashSet<&str>,
                       finalize: bool,
                       rule_counts: &mut BTreeMap<&'static str, usize>,
                       report: &mut String,
                       u_files: &mut Vec<PathBuf>|
     -> Vec<String> {
        let mut out = Vec::with_capacity(files.len());
        for (fi, f) in files.iter().enumerate() {
            let edits: Vec<Edit> = file_edits[fi]
                .iter()
                .filter(|e| rules.contains(e.rule))
                .cloned()
                .collect();
            let pass1 = apply_edits(&f.source, edits);
            if finalize {
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
            }
            let mut text = pass1.text;
            if finalize && (only.is_empty() || only.contains(rewrite::R_IMPORTS)) {
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
            out.push(text);
        }
        out
    };

    let write_texts = |texts: &[String], changed: &mut usize| -> Result<()> {
        for (f, text) in files.iter().zip(texts.iter()) {
            let abs = repo.join(&f.path);
            let current = std::fs::read_to_string(&abs)?;
            if *text != current {
                *changed += 1;
                if !cli.dry_run {
                    std::fs::write(&abs, text)?;
                }
            }
        }
        Ok(())
    };

    if cli.stack {
        if cli.dry_run {
            anyhow::bail!("--stack cannot be combined with --dry-run");
        }
        let n = STAGES.len() + 3;
        let mut active: HashSet<&str> = HashSet::new();
        let mut dummy_counts = BTreeMap::new();
        for (i, (rule, desc)) in STAGES.iter().enumerate() {
            active.insert(rule);
            let texts = apply_rules(&active, false, &mut dummy_counts, &mut String::new(), &mut Vec::new());
            let mut changed = 0;
            write_texts(&texts, &mut changed)?;
            eprintln!("stage {}/{n} {rule}: {changed} files", i + 1);
            git_commit(&repo, &format!("ethspec_remover [{}/{n}] {rule}: {desc}", i + 1), &cli.trailer)?;
        }
        let texts = apply_rules(&active, true, &mut rule_counts, &mut report, &mut u_files);
        write_texts(&texts, &mut changed_files)?;
        eprintln!("stage {}/{n} imports", STAGES.len() + 1);
        git_commit(
            &repo,
            &format!(
                "ethspec_remover [{}/{n}] imports: EthSpec -> Spec / SpecId imports, drop unused, add `use types::Spec` / `use typenum::U`",
                STAGES.len() + 1
            ),
            &cli.trailer,
        )?;
        if patch_root_cargo_toml(&repo, false)? {
            *rule_counts.entry("cargo-toml").or_default() += 1;
        }
        git_commit(
            &repo,
            &format!("ethspec_remover [{}/{n}] cargo-toml: enable typenum const-generics", STAGES.len() + 2),
            &cli.trailer,
        )?;
        run_cargo_fmt(&repo);
        git_commit(
            &repo,
            &format!("ethspec_remover [{}/{n}] cargo fmt --all", STAGES.len() + 3),
            &cli.trailer,
        )?;
    } else {
        let all: HashSet<&str> = rewrite::ALL_RULES.iter().copied().collect();
        let texts = apply_rules(&all, true, &mut rule_counts, &mut report, &mut u_files);
        write_texts(&texts, &mut changed_files)?;
    }

    if !cli.stack {
        if patch_root_cargo_toml(&repo, cli.dry_run)? {
            *rule_counts.entry("cargo-toml").or_default() += 1;
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

    if !cli.stack && !cli.dry_run && !cli.no_fmt {
        run_cargo_fmt(&repo);
    }
    Ok(())
}

/// Root `Cargo.toml`: `typenum = "1"` -> with the `const-generics` feature.
fn patch_root_cargo_toml(repo: &Path, dry_run: bool) -> Result<bool> {
    let root_toml = repo.join("Cargo.toml");
    let Ok(toml) = std::fs::read_to_string(&root_toml) else { return Ok(false) };
    let old = "typenum = \"1\"";
    if !toml.contains(old) {
        return Ok(false);
    }
    if !dry_run {
        let new = toml.replace(old, "typenum = { version = \"1\", features = [\"const-generics\"] }");
        std::fs::write(&root_toml, new)?;
    }
    Ok(true)
}

fn run_cargo_fmt(repo: &Path) {
    eprintln!("running cargo fmt --all");
    let status = std::process::Command::new("cargo")
        .args(["fmt", "--all"])
        .current_dir(repo)
        .status();
    match status {
        Ok(s) if s.success() => {}
        _ => eprintln!("cargo fmt failed (some files may not parse)"),
    }
}

/// `git add -u && git commit` (skipped when nothing changed).
fn git_commit(repo: &Path, message: &str, trailers: &[String]) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(["add", "-u"])
        .current_dir(repo)
        .status()?;
    if !status.success() {
        anyhow::bail!("git add failed");
    }
    let staged = std::process::Command::new("git")
        .args(["diff", "--cached", "--quiet"])
        .current_dir(repo)
        .status()?;
    if staged.success() {
        eprintln!("  (nothing to commit)");
        return Ok(());
    }
    let mut msg = message.to_string();
    if !trailers.is_empty() {
        msg.push_str("\n\n");
        msg.push_str(&trailers.join("\n"));
    }
    let status = std::process::Command::new("git")
        .args(["commit", "-q", "-m", &msg])
        .current_dir(repo)
        .status()?;
    if !status.success() {
        anyhow::bail!("git commit failed");
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
