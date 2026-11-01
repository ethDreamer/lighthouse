#!/usr/bin/env bash
#
# EIP-8453 local devnet (prototype tooling).
#
# Runs N nodes, each a (mock-EL + beacon node + validator client) triple, at an
# EIP-8453-at-genesis testnet on the minimal preset. Everything runs as local
# processes from this repo's release binaries: no public genesis generator knows
# the EIP-8453 state shape, so genesis comes from `lcli eip8453-testnet` and the
# execution layer is `lcli mock-el` (deterministic genesis block matching the
# state; all payloads valid).
#
# Commands:
#   ./devnet.sh build              # release binaries (lighthouse needs spec-minimal)
#   ./devnet.sh start              # fresh devnet: genesis + keystores + all nodes
#   ./devnet.sh status             # per-node head slot / finalized (synthesized) epoch
#   ./devnet.sh watch              # status every few seconds
#   ./devnet.sh partition          # kill the BN+VC of the last ceil(N/3) nodes (1/3 stake offline)
#   ./devnet.sh heal               # restart the partitioned nodes (cold fg store)
#   ./devnet.sh restart <i>        # kill and restart one node's BN+VC
#   ./devnet.sh stop               # kill everything
#
# Tunables (env): NODE_COUNT=4 VALIDATOR_COUNT=64 GENESIS_DELAY=30 DEBUG_LEVEL=info

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="${DATA_DIR:-$SCRIPT_DIR/data}"
NODE_COUNT="${NODE_COUNT:-4}"
VALIDATOR_COUNT="${VALIDATOR_COUNT:-64}"
GENESIS_DELAY="${GENESIS_DELAY:-30}"
DEBUG_LEVEL="${DEBUG_LEVEL:-info}"
MNEMONIC="giant issue aisle success illegal bike spike question tent bar rely arctic volcano long crawl hungry vocal artwork sniff fantasy very lucky have athlete"
FEE_RECIPIENT="0x690b9a9e9aa1c9db991c7721a92d351db4fac990"

LIGHTHOUSE="$ROOT/target/release/lighthouse"
LCLI="$ROOT/target/release/lcli"
TESTNET_DIR="$DATA_DIR/testnet"

el_port() { echo $((5000 + $1)); }
bn_http_port() { echo $((8000 + $1)); }
bn_p2p_port() { echo $((9000 + 10 * $1)); }

pid_file() { echo "$DATA_DIR/pids/node_$1.$2.pid"; }
log_file() { echo "$DATA_DIR/logs/node_$1.$2.log"; }

is_running() {
    local file
    file=$(pid_file "$1" "$2")
    [[ -f "$file" ]] && kill -0 "$(cat "$file")" 2>/dev/null
}

kill_role() {
    local file
    file=$(pid_file "$1" "$2")
    if [[ -f "$file" ]]; then
        kill "$(cat "$file")" 2>/dev/null
        rm -f "$file"
    fi
}

start_el() {
    local i=$1
    mkdir -p "$DATA_DIR/node_$i" "$DATA_DIR/logs" "$DATA_DIR/pids"
    # Every fork-by-timestamp is enabled from the first payload: the chain is Heze+ at genesis
    # on the engine API (EIP-8453 does not change it).
    "$LCLI" --spec minimal mock-el \
        --jwt-output-path "$DATA_DIR/node_$i/jwt.hex" \
        --listen-port "$(el_port "$i")" \
        --all-payloads-valid true \
        --shanghai-time 0 --cancun-time 0 --prague-time 0 \
        --osaka-time 0 --amsterdam-time 0 --heze-time 0 \
        &> "$(log_file "$i" el)" &
    echo $! > "$(pid_file "$i" el)"
}

start_bn() {
    local i=$1
    local boot_nodes=$2
    local boot_flag=()
    if [[ -n "$boot_nodes" ]]; then
        boot_flag=(--boot-nodes "$boot_nodes")
    fi
    "$LIGHTHOUSE" --testnet-dir "$TESTNET_DIR" bn \
        --datadir "$DATA_DIR/node_$i/beacon" \
        --execution-endpoint "http://127.0.0.1:$(el_port "$i")" \
        --execution-jwt "$DATA_DIR/node_$i/jwt.hex" \
        --http --http-port "$(bn_http_port "$i")" \
        --port "$(bn_p2p_port "$i")" \
        --enr-address 127.0.0.1 \
        --enr-udp-port "$(bn_p2p_port "$i")" \
        --enr-tcp-port "$(bn_p2p_port "$i")" \
        --disable-packet-filter \
        --disable-upnp \
        --target-peers $((NODE_COUNT - 1)) \
        --suggested-fee-recipient "$FEE_RECIPIENT" \
        --debug-level "$DEBUG_LEVEL" \
        ${boot_flag[@]+"${boot_flag[@]}"} \
        &> "$(log_file "$i" bn)" &
    echo $! > "$(pid_file "$i" bn)"
}

start_vc() {
    local i=$1
    "$LIGHTHOUSE" --testnet-dir "$TESTNET_DIR" vc \
        --validators-dir "$DATA_DIR/validators/node_$i/validators" \
        --secrets-dir "$DATA_DIR/validators/node_$i/secrets" \
        --beacon-nodes "http://127.0.0.1:$(bn_http_port "$i")" \
        --init-slashing-protection \
        --suggested-fee-recipient "$FEE_RECIPIENT" \
        --debug-level "$DEBUG_LEVEL" \
        &> "$(log_file "$i" vc)" &
    echo $! > "$(pid_file "$i" vc)"
}

wait_for_bn() {
    local i=$1
    for _ in $(seq 1 60); do
        if curl -sf "http://127.0.0.1:$(bn_http_port "$i")/eth/v1/node/identity" > /dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "node $i beacon API did not come up" >&2
    return 1
}

node_enr() {
    curl -sf "http://127.0.0.1:$(bn_http_port "$1")/eth/v1/node/identity" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["enr"])'
}

start_node() {
    local i=$1
    local boot_nodes=$2
    if ! is_running "$i" el; then
        start_el "$i"
    fi
    start_bn "$i" "$boot_nodes"
    wait_for_bn "$i"
    start_vc "$i"
}

boot_enr() {
    cat "$DATA_DIR/boot_enr.txt" 2>/dev/null || true
}

partition_set() {
    # The last ceil(N/3) nodes.
    local n=$(((NODE_COUNT + 2) / 3))
    seq $((NODE_COUNT - n + 1)) "$NODE_COUNT"
}

cmd_build() {
    cd "$ROOT"
    cargo build --release --bin lcli
    cargo build --release --features spec-minimal --bin lighthouse
}

cmd_start() {
    [[ -x "$LIGHTHOUSE" && -x "$LCLI" ]] || { echo "run './devnet.sh build' first" >&2; exit 1; }
    cmd_stop 2>/dev/null
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR/pids" "$DATA_DIR/logs"

    echo "Generating the EIP-8453 genesis ($VALIDATOR_COUNT validators, +${GENESIS_DELAY}s)..."
    "$LCLI" --spec minimal eip8453-testnet \
        --testnet-dir "$TESTNET_DIR" \
        --validator-count "$VALIDATOR_COUNT" \
        --genesis-delay "$GENESIS_DELAY" \
        --mnemonic-phrase "$MNEMONIC"

    echo "Generating keystores..."
    "$LCLI" mnemonic-validators \
        --count "$VALIDATOR_COUNT" \
        --node-count "$NODE_COUNT" \
        --base-dir "$DATA_DIR/validators" \
        --mnemonic-phrase "$MNEMONIC" > /dev/null

    echo "Starting node 1..."
    start_node 1 ""
    node_enr 1 > "$DATA_DIR/boot_enr.txt"

    for i in $(seq 2 "$NODE_COUNT"); do
        echo "Starting node $i..."
        start_node "$i" "$(boot_enr)"
    done
    echo "Started $NODE_COUNT nodes. Logs in $DATA_DIR/logs."
}

cmd_stop() {
    for i in $(seq 1 "${NODE_COUNT}"); do
        kill_role "$i" vc
        kill_role "$i" bn
        kill_role "$i" el
    done
}

cmd_status() {
    for i in $(seq 1 "$NODE_COUNT"); do
        local http="http://127.0.0.1:$(bn_http_port "$i")"
        local head finality peers
        head=$(curl -sf "$http/eth/v1/beacon/headers/head" \
            | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["header"]["message"]["slot"])' 2>/dev/null)
        finality=$(curl -sf "$http/eth/v1/beacon/states/head/finality_checkpoints" \
            | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print("just=" + d["current_justified"]["epoch"] + " fin=" + d["finalized"]["epoch"])' 2>/dev/null)
        peers=$(curl -sf "$http/eth/v1/node/peer_count" \
            | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["connected"])' 2>/dev/null)
        if [[ -n "$head" ]]; then
            echo "node $i: head slot $head, $finality (synthesized epochs), peers $peers"
        else
            echo "node $i: down"
        fi
    done
}

cmd_watch() {
    while true; do
        date
        cmd_status
        echo
        sleep 6
    done
}

cmd_partition() {
    for i in $(partition_set); do
        echo "Killing node $i (BN+VC)..."
        kill_role "$i" vc
        kill_role "$i" bn
    done
    echo "Partitioned: $(partition_set | tr '\n' ' ') offline."
}

cmd_heal() {
    for i in $(partition_set); do
        echo "Restarting node $i..."
        start_node "$i" "$(boot_enr)"
    done
}

cmd_restart() {
    local i=$1
    kill_role "$i" vc
    kill_role "$i" bn
    sleep 1
    start_node "$i" "$(boot_enr)"
}

case "${1:-}" in
    build) cmd_build ;;
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    watch) cmd_watch ;;
    partition) cmd_partition ;;
    heal) cmd_heal ;;
    restart) cmd_restart "${2:?usage: devnet.sh restart <node>}" ;;
    *) echo "usage: devnet.sh {build|start|stop|status|watch|partition|heal|restart <i>}" >&2; exit 1 ;;
esac
