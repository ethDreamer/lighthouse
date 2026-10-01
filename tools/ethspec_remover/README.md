# ethspec_remover

A re-runnable, reviewable reproduction of the mechanical part of
[PR 9229 "Remove EthSpec"](https://github.com/sigp/lighthouse/pull/9229).

That PR replaces the runtime `EthSpec` trait with a compile-time `Spec` type
alias selected by cargo feature. It is one commit touching 571 files
(+14.5k / −16.4k), which nobody can review with confidence. This tool applies
the mechanical transformation rules to a checkout, so that reviewers can read
the **rules** and a small **residual** (what was done by hand) instead of the
whole diff.

Measured against the PR's own merge base (`44ad49ed85`), the tool reproduces
all but ~2.5k lines each way of the ~31k-line diff. See [Residual](#residual).

## Usage

```bash
# Build (standalone crate; not part of the Lighthouse workspace)
cd tools/ethspec_remover && cargo build --release

# From the repository root, on a clean checkout of the base commit:
./tools/ethspec_remover/target/release/ethspec_remover            # rewrite in place + cargo fmt
./tools/ethspec_remover/target/release/ethspec_remover --dry-run  # only report
./tools/ethspec_remover/target/release/ethspec_remover --only type-params,const-access
./tools/ethspec_remover/target/release/ethspec_remover --file beacon_chain/src/beacon_chain.rs
./tools/ethspec_remover/target/release/ethspec_remover --dump-scopes BeaconBlock  # debug the analysis
```

Options: `--repo <path>`, `--only <rules>` (comma separated, see
`--list-rules`), `--file <substring>` (repeatable), `--dry-run`, `--no-fmt`,
`--report <file>` (warnings and conflicts), `--dump-scopes <substring>`.

`--stack` applies the rules one at a time and makes a git commit after each
(`--trailer` adds commit-message trailers). The edit set is computed once and
applied cumulatively, so the final tree is byte-identical to a plain run; the
intermediate commits are review slices and are not expected to compile.

## The stacked PR

The PR built with this tool has three layers:

1. **Additive** (compiles, no behaviour change): `spec.rs`, its exports, the
   cargo features, and an equivalence test asserting the new constant tables
   match the old `EthSpec` trait for every preset.
2. **Mechanical + residual** (compiles at the top): the tool itself, then the
   `--stack` commits (`ethspec_remover [k/N] ...`), then the hand-written
   residual split by cause (compile fixes, `eth_spec.rs` removal, test
   gating, CLI/network-selection logic, ...). CI can re-run the tool on the
   commit before the first `[1/N]` commit and diff against the last `[N/N]`
   commit; an empty diff means the mechanical commits need no line-by-line
   review.
3. **Remove the tool.**

To compare against the PR:

```bash
git checkout 44ad49ed85 -b script-output
./tools/ethspec_remover/target/release/ethspec_remover
git add -u && git commit -m "Apply ethspec_remover"
git diff script-output pr-9229 --stat        # the residual
```

## How it works

Nothing is re-printed. The tool parses every tracked `.rs` file with `syn`
(with span locations), decides what to change, and applies **byte-range edits**
to the original text. Formatting and comments survive; `cargo fmt --all` runs
at the end so the output is rustfmt-clean.

### 1. Global analysis (a fixpoint over all files)

Every generic-bearing item (struct, enum, trait, type alias, fn, impl block,
method) is a *scope* with an ordered list of type parameters. Each parameter
gets two flags:

* **spec-like** — the parameter *is* the spec. Seeded by `E: EthSpec` bounds
  and by the Lighthouse convention that an unbounded `E` is the spec, then
  propagated: a parameter passed into a spec-like position of another item
  (`type Foo<E> = Bar<E>`) or used as `E::SlotsPerEpoch` / `E::slots_per_epoch()`
  becomes spec-like too.
* **orphan** — not the spec, but no longer used once spec-like references are
  rewritten. The classic case is `T: BeaconChainTypes` on a struct that only
  used `T::EthSpec`. Orphans are removed from the declaration. For impl blocks
  and traits, an orphaned parameter that methods still use is re-declared on
  those methods (`impl<T: BeaconChainTypes> GossipVerifiedBlock<T>` becomes
  `impl GossipVerifiedBlock` with `fn new<T: BeaconChainTypes>(..)`), which is
  exactly what the PR does. Trait impls follow the trait definition.

  A parameter bounded by a spec-bearing trait (`T: BeaconChainTypes`) whose
  only remaining use is a `PhantomData<T>` field is dropped together with the
  field, unless a trait impl needs it (`ValidatorPubkeyCache`,
  `ProvenancedBlock`). Parameters of unrelated types (`Pub`, `Sig` in `bls`)
  are never touched.

The analysis also records which positional arguments each named type or fn
loses, so use sites in other files can be fixed (`ValidatorPubkeyCache<T>` →
`ValidatorPubkeyCache`). superstruct variants (`BeaconBlockBase`,
`BeaconBlockRef`, …) share the base struct's generics.

### 2. Rewrite rules

Every edit is tagged with the rule that produced it (`--only` selects rules,
the summary counts them). Counts are from the run against `44ad49ed85`.

| Rule | Edits | What it does |
|---|---|---|
| `type-params` | 10993 | Remove spec-like and orphaned parameters from `<..>` declarations and from every use site (`BeaconState<E>` → `BeaconState`, `Foo<'a, E, P>` → `Foo<'a, P>`, turbofish `::<E>` removed; a turbofish whose remaining parameters are defaulted is spelled out: `BeaconBlock::<E>::empty` → `BeaconBlock::<FullPayload>::empty`). Bare spec-typed fn parameters and struct fields are removed. `PhantomData<(T, E)>` → `PhantomData<T>`. |
| `const-access` | 1780 | `E::SlotsPerEpoch::to_usize()` → `Spec::SLOTS_PER_EPOCH`; `::to_u64()` → `Spec::slots_per_epoch()` when the new spec has a `u64` getter, else `Spec::CONST as u64`. `E::method()` → `Spec::method()` when the new spec keeps the method with the same return type, otherwise the constant (`E::max_withdrawals_per_payload()` → `Spec::MAX_WITHDRAWALS_PER_PAYLOAD`). Casts are folded (`E::slots_per_epoch() as usize` → `Spec::SLOTS_PER_EPOCH`, `E::number_of_columns() as u64` → `Spec::number_of_columns()`). `E::genesis_epoch()` → `Epoch::new(Spec::genesis_epoch())`. `E::spec_name()` → `Spec::SPEC_ID`, `E::name()` → `Spec::PRESET_BASE`, `get_committee_count_per_slot(n, spec)` → `(n, spec.max_committees_per_slot, spec.target_committee_size)`. |
| `imports` | 710 | `use types::EthSpec` → `use types::Spec` when the file needs `Spec`, else removed; `EthSpecId` → `SpecId`; unused `Unsigned`, `PhantomData`, `MainnetEthSpec`, `Educe` … leaves removed; `use types::Spec;` / `use typenum::U;` inserted where nothing else provides them (per module, honouring `use super::*`). |
| `cleanup` | 425 | `#[serde(bound = "E: EthSpec, P: Trait<E>")]` → `#[serde(bound = "P: Trait")]` (and the same inside `educe(..)`, `arbitrary(..)`, `cfg_attr(..)` and superstruct `variant_attributes(..)`); attributes that become empty are removed; `#[derive(Educe)] #[educe(Debug(bound(T: ..)))]` folds back into `#[derive(Debug)]`. `PhantomData<E>` fields, their struct-literal initialisers and tuple-variant positions are removed; `type EthSpec = E;` items and the `BeaconChainTypes::EthSpec` associated type vanish; `\|x\| Foo::<E>(x)` → `Foo`. |
| `test-spec` | 213 | `MinimalEthSpec` / `MainnetEthSpec` as a type → `Spec`; as a value argument (`BeaconChainHarness::builder(MinimalEthSpec)`) → removed; `type E = MainnetEthSpec;` aliases deleted. |
| `typenum` | 175 | `BitVector<E::MaxCommitteesPerSlot>` → `BitVector<U<{ Spec::MAX_COMMITTEES_PER_SLOT }>>`. Files with more than two such uses import `typenum::U`; others spell `typenum::U<..>` inline (the PR's majority style). |
| `trait-bounds` | 80 | `where E: EthSpec` predicates (and predicates on removed parameters) dropped; empty `where` clauses removed. |
| `projections` | 3 | A bare `T::EthSpec` / `<T as BeaconChainTypes>::EthSpec` in type position → `Spec` (almost all such projections are generic *arguments* and are counted under `type-params`). |
| `cargo-toml` | 1 | Root `Cargo.toml`: `typenum = { version = "1", features = ["const-generics"] }`. |

Macro invocations (`assert_eq!`, `vec!`, `impl_from!(..)` …) are handled: bodies
that parse as expression lists go through the normal rules, everything else
through a token-level fallback that removes `<E>` / `E,` / `E: EthSpec` and
rewrites `E::foo()` paths. `macro_rules!` **definitions** are not touched.

## Residual

On the PR's merge base the residual (`git diff <script output> pr-9229`, all
files) was measured as below. Everything in it was written by hand in the PR
or is a judgement call the tool does not make; in the stacked PR it appears
as the hand-written commits after the last `[N/N]` commit.

| | files | lines |
|---|---|---|
| PR 9229 | 571 | +14502 / −16423 |
| script output | 513 | +12659 / −14767 |
| residual (`.rs` only) | 249 | +2510 / −2454 |
| residual (all files) | 287 | +2543 / −2730 |

Where the residual comes from (`+` lines are the script's version, `−` the PR's):

| lines (+ / −) | files | cause |
|---|---|---|
| 820 / 413 | 2 | `consensus/types/src/core/eth_spec.rs` deleted, `core/spec.rs` added. Excluded from the tool on purpose: this *is* the new type system. |
| 33 / 276 | 38 | Non-Rust: `spec-minimal` / `spec-gnosis` / `spec-non-mainnet` cargo features, new `[[test]]` targets, Makefile targets, CI jobs, book help text. |
| 591 / 424 | 38 | Test reorganisation: `tests/spec_minimal.rs` entry points, `#[cfg(all(test, feature = "spec-minimal"))]` and `#[cfg(not(feature = "spec-non-mainnet"))]` gates. The gates are a per-test policy decision, not derivable from the code. |
| 0 / 288 | 2 | `sync/block_lookups/common.rs` and `requests/blobs_by_root.rs`: new files in the PR unrelated to `EthSpec`. |
| 203 / 174 | 4 | `macro_rules!` bodies (`ef_tests/type_name.rs`, `execution/payload.rs`, `dumb_macros.rs`, `naive_aggregation_pool.rs`). |
| 23 / 84 | 10 | `ef_tests` handler logic (`is_known_missing_vector_dir`, per-preset handlers). |
| 128 / 315 | 11 | New `ChainSpec::preset_base` field, hardcoded-network selection by preset (`eth2_network_config`, `clap_utils`, `lcli`, `environment`), CLI help. |
| 73 / 72 | 4 | `SyncingChain` / `SingleBlockLookup` (and their holders): the PR kept `T` by *adding* a `PhantomData<T>` field, where the tool moves `T` onto the methods like everywhere else. Both compile; it is a taste choice. |
| 672 / 684 | 178 | Everything else: mostly type-driven choices between `Spec::foo()` (u64) and `Spec::FOO` (usize) that the tool cannot see, inconsistent `Educe`/`typenum::U` styling in the PR, comments, and small hand edits. |

The script output alone does not compile until the residual is applied on
top. To bring the transformation to a newer base, run the tool there and
re-apply the residual by hand, using the residual commits as the checklist.

## Layout

```
src/main.rs       CLI, file loading, orchestration, report
src/db.rs         scope / parameter database and name resolution (ScopeStack)
src/analysis.rs   scope collection, veto scan, classification fixpoint
src/rewrite.rs    per-file edit generation (the rules)
src/attrs.rs      bound strings inside attributes
src/imports.rs    post-pass fixing `use` items
src/edit.rs       span-based edit application, list-element removal
src/specmap.rs    old EthSpec vocabulary -> new Spec constants/methods
src/walk.rs       shared scope-tracking visitor plumbing
```

The three visitors (collector, analyzer, rewriter) must traverse the AST in the
same order; `check_scope` panics if they ever diverge.
