# EIP-8453 local devnet

Prototype tooling for the EIP-8453 ("decoupled consensus") devnet milestone: `N` nodes, each a
(mock-EL + beacon node + validator client) triple, on an EIP-8453-at-genesis testnet with the
minimal preset (6s slots, 4-slot rounds), running as local processes built from this branch.

Why not kurtosis: the public genesis tooling (ethereum-genesis-generator / eth-beacon-genesis)
cannot produce an EIP-8453 genesis state — only this codebase knows the fork's state shape — so
genesis comes from the new `lcli eip8453-testnet` subcommand. With genesis in-repo anyway, local
processes beat containers for iteration: no image builds, direct logs, instant restarts. The
execution layer is `lcli mock-el` (all payloads valid; its deterministic genesis block is baked
into the genesis state's bid). The EIP-8453 fork does not change the engine API, so a mock EL
with every timestamp fork active from genesis suffices.

## Usage

```bash
./devnet.sh build       # cargo release builds (lighthouse needs --features spec-minimal)
./devnet.sh start       # genesis (+30s) + keystores + N nodes, fresh data dir
./devnet.sh watch       # per-node head slot / justified+finalized (synthesized epochs) / peers
```

Tunables via env: `NODE_COUNT=4 VALIDATOR_COUNT=64 GENESIS_DELAY=30 DEBUG_LEVEL=info`.
Logs land in `data/logs/node_<i>.{el,bn,vc}.log`; data in `data/` (gitignored).

The reported finality is the epoch-synthesized compat view (`finalized_slot`'s epoch); on the
minimal preset an epoch is two rounds, so a healthy devnet's finalized epoch tracks ~1 behind the
head's epoch.

## Drills (the milestone gate)

```bash
./devnet.sh partition   # kill the BN+VC of the last ceil(N/3) nodes: 1/3 stake offline
./devnet.sh heal        # restart them: cold fg store (fork.md boundary semantics), resync, rejoin
./devnet.sh restart 2   # kill/restart one node
./devnet.sh stop
```

Gate: the devnet finalizes within the pending-finality window, keeps advancing (and recovers
finality) across the partition/heal cycle, and nodes survive restarts (the fg store is not
persisted; a restarted node re-anchors at its stored finalized block and rebuilds from gossip).
