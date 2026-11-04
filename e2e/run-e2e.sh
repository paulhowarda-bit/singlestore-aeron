#!/usr/bin/env bash
# End-to-end test of the connector inside SingleStore, reading a three-member open-source Aeron Cluster that
# runs in the same container. Run from Git Bash or any POSIX shell with Docker; needs Java 21 and Maven to
# build.
#
#   e2e/run-e2e.sh setup                  build, start the container and cluster, create schema and pipeline
#   e2e/run-e2e.sh kill-test <count> <rate>
#                                         send <count> messages at <rate>/s while killing the leader, the
#                                         extractor, the member being read from and another follower; then
#                                         verify
#   e2e/run-e2e.sh verify                 compare SingleStore with the cluster's oracle and check the chain
#   e2e/run-e2e.sh gap-test               show the procedure stops the pipeline on a gap, with nothing loaded
#   e2e/run-e2e.sh rebuild-from <position>
#                                         delete rows after <position> and reload them from the cluster's log
#   e2e/run-e2e.sh reset                  new empty cluster, empty tables and a new pipeline (offsets from 0)
#   e2e/run-e2e.sh teardown               remove the container
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

C=${CONTAINER:-aeron-s2-test}
IMAGE=${IMAGE:-ghcr.io/singlestore-labs/singlestoredb-dev:0.2.88}
PW_FILE=e2e/local/singlestore-root-password
WRAPPER=/opt/e2e/java-with-aeron-opens.sh

sql() { e2e/s2sql.sh -D sequencer "$@" 2> >(grep -v getopt >&2); }
sqlv() { sql -N -e "$1"; }
cluster() { docker exec "$C" /opt/e2e/cluster.sh "$@"; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

# Runs SQL directly on one node (3306 aggregator, 3307 leaf); some settings are per node.
node_sql() {
    docker exec -e MYSQL_PWD="$(cat "$PW_FILE")" "$C" singlestore -uroot -h127.0.0.1 -P"$1" -N -e "$2" \
        2> >(grep -v getopt >&2)
}

wait_for() { # wait_for <seconds> <description> <command...>
    local limit=$1 what=$2; shift 2
    for _ in $(seq 1 "$limit"); do
        if "$@"; then return 0; fi
        sleep 1
    done
    log "timed out waiting for $what"; return 1
}

has_leader() { [ -n "$(cluster leader)" ]; }
loaded_rows() { sqlv "SELECT COUNT(*) FROM sequenced_log"; }
watermark() { sqlv "SELECT position FROM sequencer_watermark WHERE sequencer = 'oss-dev'"; }
pipeline_state() { sqlv "SELECT STATE FROM information_schema.PIPELINES WHERE PIPELINE_NAME = 'sequenced_log_ingest'"; }
source_offset() { sqlv "SELECT JSON_EXTRACT_BIGINT(VALUE, 'position') FROM information_schema.PIPELINES_SOURCE_OFFSETS WHERE PIPELINE_NAME = 'sequenced_log_ingest'"; }

# Caught up: the pipeline's stored offset equals the watermark, and neither moved in the last 5 seconds.
# (The watermark alone stands still while the pipeline skips entries it already holds.)
wait_caught_up() {
    local last="" now
    while true; do
        now="$(source_offset) $(watermark)"
        if [ "$now" = "$last" ] && [ "${now% *}" = "${now#* }" ]; then return 0; fi
        last=$now
        sleep 5
    done
}

setup() {
    mvn -q -B package -DskipTests
    mkdir -p e2e/local
    [ -s "$PW_FILE" ] || python -c "import secrets; print(secrets.token_urlsafe(24))" > "$PW_FILE"

    if ! docker inspect "$C" > /dev/null 2>&1; then
        docker run -d --name "$C" --cpus=8 -e ROOT_PASSWORD="$(cat "$PW_FILE")" "$IMAGE" > /dev/null
    fi
    wait_for 180 "container health" sh -c "docker inspect -f '{{.State.Health.Status}}' $C | grep -q healthy"

    local ext
    ext=$(docker exec "$C" sh -c 'ls -d /server/singlestoredb-server-*/extractors/lib-extractor-connect')
    docker exec -u root "$C" mkdir -p /opt/e2e
    docker cp e2e/target/e2e-cluster.jar "$C:/opt/e2e/"
    docker cp e2e/container/cluster.sh "$C:/opt/e2e/"
    docker cp e2e/container/java-with-aeron-opens.sh "$C:/opt/e2e/"
    docker cp e2e/oracle-check.sql "$C:/opt/e2e/"
    docker cp connector/target/aeron-singlestore-connector-*.jar "$C:$ext/"
    docker exec -u root "$C" sh -c "chown -R singlestore /opt/e2e $ext/aeron-singlestore-connector-*.jar; chmod 755 /opt/e2e/*.sh"

    # java_path is per node, and SET GLOBAL on the aggregator does not reach the leaf.
    for port in 3306 3307; do node_sql "$port" "SET GLOBAL java_path = '$WRAPPER'"; done

    for i in 0 1 2; do cluster start "$i"; done
    wait_for 60 "a leader" has_leader
    cluster status

    e2e/s2sql.sh -e "CREATE DATABASE IF NOT EXISTS sequencer" 2> >(grep -v getopt >&2)
    for f in schema apply_sequenced_entries pipeline; do sql < "singlestore/$f.sql"; done
    sql -e "START PIPELINE sequenced_log_ingest"
    log "setup done: $(pipeline_state)"
}

has_batches() { [ "$(sqlv "SELECT COUNT(*) FROM information_schema.PIPELINES_BATCHES_SUMMARY WHERE PIPELINE_NAME = 'sequenced_log_ingest'")" -gt 0 ]; }
one_reader() { [ "$(cluster reader | wc -w)" = 1 ]; }
member_in_office() { [ "$(cluster status | grep -c -E "member $1: (LEADER|FOLLOWER)")" = 1 ]; }
all_members_up() { [ "$(cluster status | grep -c -E 'DEAD|STARTING|FAILED')" = 0 ]; }

kill_member_and_restart() { # kill_member_and_restart <id> <label>
    log "kill -9 $2 (member $1)"
    cluster kill "$1"
    wait_for 60 "a leader" has_leader
    # A restart within a few seconds of kill -9 fails: Aeron's archive sees its own mark file as still
    # in use ("active mark file detected") and exits.
    sleep 15
    cluster start "$1"
    wait_for 120 "member $1 to rejoin" member_in_office "$1"
}

wait_rows() { # wait_rows <rows>
    wait_for 900 "$1 rows" sh -c "[ \"\$(e2e/s2sql.sh -N -D sequencer -e 'SELECT COUNT(*) FROM sequenced_log' 2>/dev/null)\" -ge $1 ]"
}

kill_test() {
    local count=$1 rate=$2 base prefix
    base=$(loaded_rows)
    prefix="kt$(date +%H%M%S)-"
    log "rows before: $base; sending $count messages at $rate/s as $prefix"
    cluster load "$count" "$rate" "$prefix"
    local started; started=$(date +%s)

    wait_rows $((base + count * 2 / 10))
    kill_member_and_restart "$(cluster leader)" leader

    wait_rows $((base + count * 4 / 10))
    log "kill -9 the SingleStore extractor"
    docker exec "$C" pkill -9 -f ExtractorDriver || log "no extractor process was running at that moment"

    wait_rows $((base + count * 6 / 10))
    wait_for 60 "the connector to be reading from exactly one member" one_reader
    local reader; reader=$(cluster reader)
    kill_member_and_restart "$reader" "the member being read from, a $(cluster status | grep "member $reader:" | cut -d' ' -f3)"

    wait_rows $((base + count * 8 / 10))
    kill_member_and_restart "$(cluster followers | grep -v -x "$(cluster reader)" | head -1)" "a follower not being read from"

    wait_for 900 "load generator to finish" sh -c "docker exec $C grep -q '^done' /opt/e2e/run/load-$prefix.log"
    wait_for 120 "all members up" all_members_up
    log "load: $(docker exec "$C" tail -n 2 "/opt/e2e/run/load-$prefix.log" | head -1)"

    wait_caught_up
    local elapsed=$(( $(date +%s) - started ))
    log "pipeline caught up at watermark $(watermark); $(( $(loaded_rows) - base )) rows in ${elapsed}s, $(( ($(loaded_rows) - base) / elapsed )) rows/s overall"
    verify
}

verify() {
    cluster oracle
    log "oracle check (expect oracle_messages = log_messages, both mismatch counts 0)"
    sql --table < e2e/oracle-check.sql
    log "chain and watermark (expect chain_breaks 0, first_previous_position 0, watermark_matches 1)"
    sql --table < singlestore/verify.sql
    log "pipeline: $(pipeline_state)"
}

# Rebuilds sequenced_log from the cluster's log after <position>: deletes the rows after it, moves the
# watermark back to it, and recreates the pipeline so it reads the log from the start again. The procedure
# skips everything up to the watermark. (ALTER PIPELINE ... SET OFFSETS is not available for custom
# connectors in SingleStore 10.9, and CREATE OR REPLACE PIPELINE keeps the stored offsets.)
rebuild_from() {
    local p=$1 t
    t=$(sqlv "SELECT term FROM sequenced_log WHERE sequencer = 'oss-dev' AND position = $p")
    sql -e "STOP PIPELINE sequenced_log_ingest" 2>/dev/null || true
    sql -e "DROP PIPELINE IF EXISTS sequenced_log_ingest"
    sqlv "DELETE FROM sequenced_log WHERE sequencer = 'oss-dev' AND position > $p"
    sqlv "UPDATE sequencer_watermark SET position = $p, term = $t WHERE sequencer = 'oss-dev'"
    sed -n '/^CREATE PIPELINE/,$p' singlestore/pipeline.sql | sql
    sql -e "START PIPELINE sequenced_log_ingest"
}

gap_test() {
    local before wm earlier
    sql -e "STOP PIPELINE sequenced_log_ingest"
    wm=$(watermark)
    earlier=$(sqlv "SELECT position FROM sequenced_log WHERE sequencer = 'oss-dev' ORDER BY position DESC LIMIT 1 OFFSET 100")
    log "1) delete the last 100 rows and move the watermark back to $earlier; the pipeline's offset stays at $wm"
    sqlv "DELETE FROM sequenced_log WHERE sequencer = 'oss-dev' AND position > $earlier"
    sqlv "UPDATE sequencer_watermark SET position = $earlier WHERE sequencer = 'oss-dev'"
    before=$(loaded_rows)
    cluster load 10 100 "gap$(date +%H%M%S)-"
    sql -e "START PIPELINE sequenced_log_ingest"
    wait_for 120 "the pipeline to stop in Error" sh -c "e2e/s2sql.sh -N -D sequencer -e \"SELECT STATE FROM information_schema.PIPELINES WHERE PIPELINE_NAME = 'sequenced_log_ingest'\" 2>/dev/null | grep -q -i error"
    log "pipeline: $(pipeline_state); rows before $before, after $(loaded_rows); watermark $(watermark)"
    sqlv "SELECT ERROR_MESSAGE FROM information_schema.PIPELINES_ERRORS WHERE PIPELINE_NAME = 'sequenced_log_ingest' AND ERROR_MESSAGE LIKE '%apply_sequenced_entries%' ORDER BY ERROR_UNIX_TIMESTAMP DESC LIMIT 1"

    log "repair: rebuild from $earlier by reading the log again from the start"
    rebuild_from "$earlier"
    wait_caught_up
    log "pipeline: $(pipeline_state); rows now $(loaded_rows); watermark $(watermark)"
    verify

    log "2) the procedure called directly with a batch that skips an entry"
    wm=$(watermark); before=$(loaded_rows)
    sql -e "STOP PIPELINE sequenced_log_ingest"
    sql < e2e/gap-batch.sql
    sql -e "CALL e2e_apply_gapped_batch($wm)" && log "UNEXPECTED: the gapped batch was accepted" || true
    log "rows before $before, after $(loaded_rows); watermark before $wm, after $(watermark)"
    sql -e "START PIPELINE sequenced_log_ingest"
}

reset() {
    sql -e "STOP PIPELINE sequenced_log_ingest" 2>/dev/null || true
    sql -e "DROP PIPELINE IF EXISTS sequenced_log_ingest"
    cluster wipe
    # An extractor can outlive its dropped pipeline (see VERIFICATION.md); remove those, and the reader
    # directories that killed extractors leave behind.
    docker exec "$C" sh -c 'pkill -9 -f ExtractorDriver; rm -rf /tmp/aeron-gateway-*' || true
    for i in 0 1 2; do cluster start "$i"; done
    wait_for 60 "a leader" has_leader
    sqlv "TRUNCATE TABLE sequenced_log"
    sqlv "DELETE FROM sequencer_watermark WHERE sequencer = 'oss-dev'"
    sql < singlestore/pipeline.sql
    sql -e "START PIPELINE sequenced_log_ingest"
    wait_for 60 "the pipeline's first batch" has_batches
    log "reset done: $(cluster status | tr '
' ' ')"
}

# Everything runs from main, which bash parses whole before running, so editing this file during a run
# cannot change what the run does.
main() {
    case "${1:-}" in
        setup) setup ;;
        reset) reset ;;
        kill-test) kill_test "${2:-1000000}" "${3:-10000}" ;;
        verify) verify ;;
        gap-test) gap_test ;;
        rebuild-from) rebuild_from "$2"; wait_caught_up; verify ;;
        teardown) docker rm -f "$C" ;;
        *) sed -n '2,14p' "$0"; exit 2 ;;
    esac
}
main "$@"; exit
