# Verification: phases 1–4

Author: Paul Howard · 2026-11-03, Phase 4 on 2026-11-04 · Aeron 1.54.0 · Kafka Connect 3.9.1 · SingleStore 10.9.0 · built and tested on JDK 21 (bytecode for Java 17)

Every claim below comes from a run on this machine. Phases 1–3 run without SingleStore. [Phase 4](#phase-4-the-connector-inside-singlestore) runs the connector inside SingleStore 10.9.

## Build

```bash
mvn -B clean verify
```

All five modules build, and 40 tests pass. The run takes about 3.5 minutes, mostly the in-process clusters.

| Module | Tests | What they cover |
|---|---|---|
| `gateway-aeron-cluster` | 6 contract (TCK) | order and chaining (500 messages), resume, committed-only, leader failover, losing each member in turn, refusing a resume past the end of the log |
| | 3 `AeronClusterLogTest` | found by name; refuses misaligned resume points; refuses a resume point inside an entry |
| | 9 `ClusterLogDecoderTest`, 6 `PositionChainTest` | decoding, reassembly, unknown schema/template, term order, gaps, end-of-term padding |
| `connector` | 10 `SequencerSourceTaskTest`, 5 `SequencerSourceConnectorTest` | offset round trip, `poll()` bound, record contents, error mapping, one task whatever `tasks.max` says, config validation |
| | 1 `SequencerConnectorIntegrationTest` | a real Kafka Connect 3.9.1 worker and broker (`EmbeddedConnectCluster`) reading a real three-member Aeron Cluster |

The integration test delivered 402 entries in order as one unbroken chain. It then stopped the connector, read the stored offset back through the Connect REST API, committed 200 more messages and resumed the connector. The first record after the resume chained exactly onto the stored offset, and 0 entries were redelivered.

## Each integrity check fails its test when removed

Each check was broken on purpose, the named test was run, and the code was restored (file hashes compared before and after).

| Check removed | Test that failed |
|---|---|
| Replay bounded by the commit-position counter (replaced by an unbounded replay) | `neverDeliversAnEntryThatIsLaterRetracted`: the leader held 14656 bytes of log against 9856 committed; the reader delivered the uncommitted tail |
| Leader-behind check (resume point past the end of the leader's log) | `refusesToResumeBeyondTheEndOfTheLog`: nothing was thrown |
| `INVALID_POSITION` from the archive treated as an integrity failure | `refusesAResumePositionInsideAnEntry`: nothing was thrown |
| Frame contiguity check | 4 `PositionChainTest` tests and `ClusterLogDecoderTest.refusesAGapBetweenEntries` |
| Padding rule widened to accept any forward jump | 3 `PositionChainTest` tests and `refusesAGapBetweenEntries` |
| Leadership term must not go backwards | `refusesALeadershipTermThatGoesBackwards` |
| `poll()` bound (`batch.max.entries`) | `neverReturnsMoreThanTheBatchLimitFromOnePoll`: 2500 returned, expected 1000 |
| Stored offset used on start | `resumesWithTheEntryAfterTheStoredOffset`, `refusesAStoredOffsetWithoutAPosition`, and the integration test (resumed from 0 instead of 19456) |

The committed-only test needed one fix before it could fail. The first version stopped the followers and expected the leader to append entries it could not commit. Instrumenting showed the leader appended nothing: Aeron only lets a leader's log publication run without receivers in a one-member cluster (`ssc` is set only when `activeMembers.length == 1`). The test now uses `SequencerFixture.cutOffFromLog`, which drops inbound log frames on the followers while they keep running. With it, the leader holds a real uncommitted tail.

## The deployable JAR

`connector/target/aeron-singlestore-connector-0.1.0-SNAPSHOT.jar` (3.1 MB):
- It holds only our classes, Aeron and Agrona. It has no `org/apache/kafka`, `org/slf4j`, JUnit, TCK or test-cluster classes.
- The service files list `SequencerSourceConnector` and `AeronClusterLogProvider`.
- `version.properties` says `0.1.0-SNAPSHOT`.

The JAR was loaded with nothing beside it but `connect-api`, `kafka-clients` and `slf4j-api`, as SingleStore's extractor would load it:
- The connector was found through `ServiceLoader`.
- It started with `sequencer.gateway=aeron-cluster`.
- It returned one task config for `tasks.max=4`.
- With `sequencer.gateway=aeron-sequencer` it refused with `no sequencer gateway named 'aeron-sequencer' on the classpath; available: [aeron-cluster]`.

This check did not catch the JVM flag Aeron needs inside SingleStore. Phase 4 found it.

## Differences from the plan

- `SequencedLogProvider` gained `startOfLog()`. The connector needs a place to start when SingleStore holds no offset yet, and the provider is the only thing that knows it.
- `SequencerFixture` gained `cutOffFromLog(memberId)`, for the reason above. The runbook explains how to implement it for the Aeron Sequencer and how to bite-test it.
- When nothing is ready, `poll()` waits 5 ms and returns an empty list, so a Kafka Connect worker does not spin a core. It never waits for data. It used to return `null`. The SingleStore 10.9 smoke test in [PLAN.md](PLAN.md) showed that SingleStore fails a batch when `poll()` returns `null`, although Kafka Connect accepts it. `returnsAnEmptyListNotNullWhenNothingNewIsCommitted` now guards this.
- TCK volume is 500 messages from one client, not 100k from three. Volume is left to Phase 4's 1M-entry run.
- Two checks exist without a test:
  - A resume point before the start of a purged log. Open-source Aeron never purges, so it cannot be produced here.
  - A foreign-schema message inside a live cluster's log. It is covered at decoder level only.

## Java 25

The build and its tests run on Java 21, the work JDK. Java 25 matters only inside SingleStore: its extractor runs connectors on its own JRE 25 (`java_path` in `memsql.cnf`). The connector is Java 17 bytecode, so the same JAR runs on both.

As a check, the same 40 tests also pass on Amazon Corretto 25.0.5 (each surefire report records `java.version` 25.0.5), and so does the uber-JAR load check. With Corretto 25 installed but not on `PATH`:

```bash
JAVA_HOME=<path to corretto-25.0.5> mvn -B clean verify
```

`mvn clean` works on both JDKs in this OneDrive folder. OneDrive marks a new folder read-only within about 20 seconds, and from JDK 25 Windows file deletion refuses read-only entries. The root POM therefore pins `maven-clean-plugin` 3.5.0 with `force` on. With 3.5.0 and `force` off, `clean` on JDK 25 failed on `gateway-api\target`. With `force` on, `mvn clean verify` passed on 25.0.5 and on 21.0.9 with the read-only folders present.

## Phase 4: the connector inside SingleStore

The connector ran inside SingleStore's own extractor and loaded a three-member open-source Aeron Cluster's log exactly once and in order. It did so through kills of the leader, the extractor, the member being read from and another follower. The procedure refused every gap it was given, and with its check removed it let each gap through.

### How to run it

Needs Docker, Maven, Java 21 and a POSIX shell (Git Bash on Windows).

```bash
e2e/run-e2e.sh setup
```

```bash
e2e/run-e2e.sh kill-test 1000000 10000
```

```bash
e2e/run-e2e.sh gap-test
```

```bash
e2e/run-e2e.sh teardown
```

- `setup` builds everything, starts the container and the cluster, and creates the schema, procedure and pipeline.
- `kill-test <count> <rate>` sends the messages while making the four kills below, then runs `verify`.
- `gap-test` runs both gap methods and repairs after the first.
- `reset` gives a new empty cluster, empty tables and a new pipeline. `rebuild-from <position>` reloads everything after a position.
- `setup` generates a root password into `e2e/local/`. `e2e/s2sql.sh` runs SQL in the container with it.

### Setup

- Container `aeron-s2-test` from `ghcr.io/singlestore-labs/singlestoredb-dev:0.2.88`. It runs SingleStore 10.9.0 with one aggregator (port 3306) and one leaf (3307), and its extractors run on the image's JDK 25.0.2. The container is limited to 8 CPUs.
- The cluster runs in the same container: three `E2eMember` JVMs on Java 21, talking UDP over localhost. Each keeps its archive and cluster directories across restarts, so a killed member rejoins. The clustered service is the oracle: it writes every committed message to `oracle.tsv` and echoes it to the sender.
- `E2eLoad` sends numbered 100-byte messages at a set rate. It resends anything not echoed within 10 s, inside the same rate. A failover can therefore put duplicates in the log, never a loss. Duplicates are legitimate log entries; the test is that SingleStore holds exactly what the cluster committed.
- The pipeline is `singlestore/pipeline.sql`: one task, `batch.max.entries` 10000, `BATCH_INTERVAL` 100, into `apply_sequenced_entries`.

### Blocker found: Aeron needs a JVM flag that SingleStore does not pass

The first pipeline failed on its first batch:

```
java.lang.IllegalAccessError: class org.agrona.UnsafeApi (in unnamed module) cannot access class
jdk.internal.misc.Unsafe (in module java.base) because module java.base does not export jdk.internal.misc
```

Agrona 2.x, which Aeron 1.54 uses, needs `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`. SingleStore starts the extractor with only the options below (read from `/proc/<pid>/cmdline`). It passes no module flags and sets no `JAVA_*` environment variables:

```
<java_path> -Xmx128M -XX:+IgnoreUnrecognizedVMOptions -XX:-UsePerfData -cp extractor-connect.jar:lib-extractor-connect/* com.memsql.extractor.impl.ExtractorDriver ...
```

Phases 1–3 never saw this, because surefire's `argLine` adds the flag.

The only way found to add the flag is `java_path`. Pointing it at this wrapper (`e2e/container/java-with-aeron-opens.sh`) works:

```sh
#!/bin/sh
exec "${AERON_REAL_JAVA:-/usr/local/jdk-25/bin/java}" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED "$@"
```

- `java_path` is per node. `SET GLOBAL java_path` on the aggregator did not reach the leaf, and extractors run on the leaf. The test sets it on each node. A real install should also put it in each node's `memsql.cnf` so it survives a restart.
- The wrapper applies to every Java extractor on the node. The flag only widens what code inside it may access.
- Expect the same at work if the Aeron Sequencer SDK uses Agrona 2.x. Plan the `java_path` change with whoever runs SingleStore there.

### Kill test

`e2e/run-e2e.sh reset`, then `e2e/run-e2e.sh kill-test 1000000 10000`:

| When (share of rows loaded) | Killed with `kill -9` | Restarted |
|---|---|---|
| 20% | the leader (member 1) | 15 s after a new leader was elected |
| 40% | the SingleStore extractor, mid-batch | by SingleStore |
| 60% | the member the connector was replaying from (member 1, a follower) | 15 s later |
| 80% | a follower not being read from (member 1) | 15 s later |

| Check | Result |
|---|---|
| Messages acknowledged by the cluster | 1,000,000 of 1,000,000 (1,002,152 offers) |
| Committed messages in the oracle | 1,000,076 on each member; the three files are identical |
| `SESSION_MESSAGE` rows in SingleStore | 1,000,076; none missing and none extra, joined on position, kind, session id and payload |
| All entries | 1,000,082 |
| `chain_breaks` | 0; first `previous_position` is 0 |
| Watermark | 192026880, the last row's position |

The member being read from was found from the extractor's UDP sockets (`cluster.sh reader`), not assumed.

Earlier runs passed the same checks:
- a run of 1,009,305 entries with the same four kills;
- two longer runs, of 4.9 and 5.9 million entries, in which unplanned double failures cost the cluster its quorum, once for about 3 minutes and once for about 17.

### Gap test

The procedure must stop the pipeline on a gap with nothing loaded. Each refusal must also be shown to come from the check.

| Method | With the check | Check removed (bite) |
|---|---|---|
| 1. Through the pipeline. Stop it, delete the last 100 rows, move the watermark back to the row before them, send 10 messages, start it. The pipeline's stored offset is still past the deleted rows, so its next batch does not continue from the watermark. | The pipeline went to **Error**. Rows were 1,000,019 before and after. The error read `batch does not continue the log from watermark 192014976 (new rows 12, linked 11, distinct positions 12, distinct previous positions 12)`. | The pipeline stayed Running and loaded 12 rows across the gap. `verify.sql` then reported `chain_breaks = 1`. |
| 2. Directly. `CALL e2e_apply_gapped_batch(<watermark>)` (`e2e/gap-batch.sql`) hands the procedure two rows with an entry missing between them. | Refused with `ER_USER_RAISE (2242)`. Rows (1,000,131) and watermark (192035680) were unchanged. | Accepted: 2 rows written, and the watermark moved across the gap. |

After each bite, the real procedure was reinstalled from `singlestore/apply_sequenced_entries.sql`, whose sha256 matched the copy taken before. The gap was then refused again.

**Repair** (`e2e/run-e2e.sh rebuild-from <position>`):
1. Delete the rows after `<position>` and set the watermark to it.
2. Drop and recreate the pipeline, so it reads the log from the start.
3. Let the procedure skip what it already has.

After method 1, the repair gave 1,000,116 oracle messages equal to 1,000,116 log messages, 0 mismatches, `chain_breaks` 0, and a matching watermark. Dropping the pipeline is the only rewind: `ALTER PIPELINE … SET OFFSETS` is unsupported for custom connectors, and `CREATE OR REPLACE PIPELINE` keeps the stored offsets.

### Throughput

This is a functional rig, not a benchmark. SingleStore, three cluster members and the load generator share 8 CPUs, and the container ran at its limit (about 880%) while the load ran.

| Condition | Rows per second into SingleStore |
|---|---|
| Catching up a backlog, no new load, `batch.max.entries` 1000 | 9,000–10,000 (each 1,000-row batch about 0.12 s) |
| Catching up a backlog, no new load, `batch.max.entries` 10000 | 11,000–13,000 (average batch 0.19 s) |
| While the cluster takes 10,000 messages/s | 1,500–5,000 |
| Kill tests end to end, including elections and 15 s restart waits | 3,937 and 6,819 |

### Phase 0 questions, answered

| Question | Answer |
|---|---|
| Does the extractor JVM persist across batches? | Yes. It runs with `--max-batches 2147483647`, and one process served the pipeline across batches. |
| After a failed batch, is the task restarted from the stored offset? | Yes. After the extractor was killed mid-batch, the batch was retried and the chain continued with no gap and no duplicate. |
| One `poll()` per batch? | Yes. Every full batch held exactly `batch.max.entries` rows. |
| How does the record render in `FORMAT JSON`? | The struct's fields are under `value`. BYTES fields arrive as base64, which `FROM_BASE64` restores. The mapping is `position <- value::position`, and `timestamp` needs backquotes. See `singlestore/pipeline.sql`. |
| Does Agrona run on JRE 25 in the extractor, and can JVM flags be passed? | Only with `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`, passed through the `java_path` wrapper above. |
| Can the extractor reach the cluster over UDP? | On one host, yes. Across hosts is untested. |

### Other findings

- **SingleStore leaves the extractor JVM running after `DROP PIPELINE` and `CREATE OR REPLACE PIPELINE`.**
  - The leaf logs `GC-ing Managed extractor`, but the process stays up, its main thread waiting on its stdin pipe from SingleStore.
  - Our connector inside it keeps its replay open on a cluster member and uses 20–40% of a core.
  - Once, after the laptop slept, a leftover extractor blocked a re-created pipeline of the same name. The pipeline showed Running, with no batches and no errors, until the old process was killed.
  - After dropping or replacing a pipeline, check for leftover `ExtractorDriver` processes.
- **Each killed extractor leaves a `/tmp/aeron-gateway-<uuid>` directory of about 50 MB.** The connector deletes it only on a clean close.
- **The connector's embedded media driver uses 20–40% of a core**, even with nothing new to read.
- **A laptop sleep stops every Aeron process on wake.** The media driver sees its keepalive as over an hour old. Recover by restarting the members. This is a property of the test machine, not of the connector.
- Some test-harness problems are fixed in `run-e2e.sh` and `cluster.sh`:
  - Aeron refuses to restart an archive within a few seconds of `kill -9` ("active mark file detected"), so restarts wait 15 s.
  - A member whose agents stopped after a fatal error keeps its JVM alive, so `cluster.sh status` reports it as FAILED.

## Not yet verified

- The extractor and the cluster on different hosts (UDP across a network, with `aeron.gateway.host` set to a routable address).
- A SingleStore cluster with more than one leaf.
- The `aeron-sequencer` build profile. It needs the commercial SDK.
