#!/bin/sh
# Controls the three-member test cluster and its load generator inside the SingleStore test container.
#
#   cluster.sh start <id>                    start member <id> (0, 1 or 2); its state survives a restart
#   cluster.sh kill <id>                     kill -9 member <id>
#   cluster.sh leader                        print the id of the live leader
#   cluster.sh followers                     print the ids of the live followers
#   cluster.sh status                        one line per member: LEADER, FOLLOWER, STARTING, FAILED or DEAD
#   cluster.sh load <count> <rate> <prefix>  run the load generator in the background (log: load-<prefix>.log)
#   cluster.sh reader                        print the id of the member the connector is replaying from
#   cluster.sh oracle                        compare the members' oracle files; copy the leader's to $RUN/oracle.tsv
#   cluster.sh wipe                          kill everything and delete all cluster state
set -eu

JAVA=${JAVA:-/usr/local/jdk-21/bin/java}
JAR=${JAR:-/opt/e2e/e2e-cluster.jar}
RUN=${RUN:-/opt/e2e/run}
OPTS="-Xmx256m -XX:+UseSerialGC
 --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
 --add-opens java.base/sun.nio.ch=ALL-UNNAMED
 --add-opens java.base/java.nio=ALL-UNNAMED"

alive() {
    [ -f "$RUN/member-$1.pid" ] && kill -0 "$(cat "$RUN/member-$1.pid")" 2>/dev/null
}

# After a fatal error a member's agents stop but its JVM stays up; its consensus thread is then gone.
consensus_running() {
    grep -q consensus-modul /proc/"$(cat "$RUN/member-$1.pid")"/task/*/comm 2>/dev/null
}

role() {
    if ! alive "$1"; then echo DEAD
    elif [ -f "$RUN/member-$1/role" ] && ! consensus_running "$1"; then echo FAILED
    elif [ -f "$RUN/member-$1/role" ]; then cat "$RUN/member-$1/role"
    else echo STARTING; fi
}

mkdir -p "$RUN"
cmd=${1:-status}
case "$cmd" in
    start)
        if alive "$2"; then echo "member $2 already running"; exit 0; fi
        mkdir -p "$RUN/member-$2"
        rm -f "$RUN/member-$2/role"
        # shellcheck disable=SC2086
        setsid nohup "$JAVA" $OPTS -cp "$JAR" com.paulhoward.sequencer.e2e.E2eMember "$2" "$RUN" \
            >> "$RUN/member-$2.log" 2>&1 < /dev/null &
        echo $! > "$RUN/member-$2.pid"
        echo "member $2 started, pid $!"
        ;;
    kill)
        if alive "$2"; then kill -9 "$(cat "$RUN/member-$2.pid")"; echo "member $2 killed"; else echo "member $2 not running"; fi
        ;;
    leader)
        for i in 0 1 2; do [ "$(role $i)" = LEADER ] && echo $i; done; true
        ;;
    followers)
        for i in 0 1 2; do [ "$(role $i)" = FOLLOWER ] && echo $i; done; true
        ;;
    status)
        for i in 0 1 2; do echo "member $i: $(role $i)"; done
        ;;
    load)
        # shellcheck disable=SC2086
        setsid nohup "$JAVA" $OPTS -cp "$JAR" com.paulhoward.sequencer.e2e.E2eLoad "$2" "$3" "$4" \
            > "$RUN/load-$4.log" 2>&1 < /dev/null &
        echo $! > "$RUN/load-$4.pid"
        echo "load $4 started, pid $!"
        ;;
    reader)
        # The connector holds an archive control connection to the member it replays from; find which
        # member's archive port (offset 1) one of the extractor's UDP sockets is connected to.
        for x in $(pgrep -f ExtractorDriver); do
            for inode in $(ls -l /proc/"$x"/fd 2>/dev/null | grep -o 'socket:\[[0-9]*\]' | tr -dc '0-9\n'); do
                awk -v i="$inode" '$10 == i { print $3 }' /proc/net/udp
            done
        done | while read -r remote; do
            port=$(printf '%d' "0x${remote#*:}")
            for i in 0 1 2; do [ "$port" = $((9001 + i * 100)) ] && echo "$i"; done
        done | sort -u
        ;;
    oracle)
        # Every member records the same committed messages; show that, then publish the leader's copy.
        for i in 0 1 2; do
            echo "member $i: $(wc -l < "$RUN/member-$i/oracle.tsv") lines, $(sha256sum < "$RUN/member-$i/oracle.tsv" | cut -c1-16)"
        done
        cp "$RUN/member-$("$0" leader | head -1)/oracle.tsv" "$RUN/oracle.tsv"
        ;;
    wipe)
        pkill -9 -f com.paulhoward.sequencer.e2e. || true
        rm -rf "$RUN"
        echo wiped
        ;;
    *)
        echo "unknown command $cmd" >&2
        exit 2
        ;;
esac
