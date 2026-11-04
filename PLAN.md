# Aeron sequencer gateway into SingleStore: build plan

Author: Paul Howard · Drafted 2026-11-03 · Status: **phases 1–4 built and verified** (see [VERIFICATION.md](VERIFICATION.md)). Phase 0's questions were answered inside SingleStore 10.9 during Phase 4. Phase 5 was dropped: the project goes to work as source, and [SWITCH-TO-AERON-SEQUENCER.md](SWITCH-TO-AERON-SEQUENCER.md) covers the Sequencer gateway and the install.

## Goal

A Kafka Connect **source connector** that acts as a **sequencer gateway**. It reads the committed, totally ordered log of an Aeron sequencer and delivers it into SingleStore through SingleStore Connector Pipelines, with no Kafka brokers involved. SingleStore must hold exactly the sequencer's committed log: same order, no gaps, no duplicates, and a watermark that applications can wait on.

- **Here:** built and tested against **open-source Aeron Cluster 1.54.0**.
- **At work:** switched to the commercial **Aeron Sequencer** by following [SWITCH-TO-AERON-SEQUENCER.md](SWITCH-TO-AERON-SEQUENCER.md).
- The connector stays a standard Kafka Connect plugin, so the same JAR also runs in a normal Kafka Connect worker.

## What the research established

| Fact | Consequence for the design | Source |
|---|---|---|
| SingleStore calls the feature **Connector Pipelines** (`CREATE PIPELINE … AS LOAD DATA CONNECTOR`). It is a **Preview** feature, switched on with `SET GLOBAL experimental_features_config = "connector_pipelines_enabled=true"`. | Expect changes between SingleStore releases. Phase 0 tests our assumptions before we build on them. | SingleStore v10 docs, *Connector Pipelines* |
| **Custom connectors run only on self-managed SingleStore 10**, not on Helios. | Work must run self-managed v10. | same |
| SingleStore provides **Kafka Connect API 3.9.1** and requires **JRE 25+** (`java_path` in `memsql.cnf`). The connector runs in an **extractor process on leaf nodes**. | Compile against `connect-api` 3.9.1 (`provided` scope) and target Java 17 bytecode. Test on Java 25 before deploying. | *Create a Custom Connector Pipeline* |
| The JAR is an **uber-JAR** copied to `<install-dir>/extractors/lib-extractor-connect/` on every aggregator and leaf, and registered through `META-INF/services/org.apache.kafka.connect.source.SourceConnector`. | Build it with the Maven Shade plugin. Confirmed on 10.9.0: a JAR copied into that folder is used without a restart, and the short class name resolves through the manifest. The folder is one classpath shared by every connector. It already holds Kafka Connect 3.9.1, kafka-clients 3.9.2, Jackson 2.22.3, Debezium and more, but no Aeron or Agrona, so our JAR clashes with nothing today. Recheck after each SingleStore upgrade. | same, plus the smoke test |
| SingleStore calls `poll()` on every task, waits for all of them, writes the batch **in one transaction**, then updates offsets. **`poll()` must not block.** Offsets are stored in `information_schema.PIPELINES_SOURCE_OFFSETS`. | `poll()` returns at most one bounded batch and returns immediately. **It must return an empty list, never `null`.** On 10.9.0 a `null` fails the batch (see the smoke test). `SequencerSourceTask.poll()` was changed to return an empty list. | same, plus the smoke test |
| Delivery is **at-least-once**. SingleStore's own guidance for exactly-once results is a unique key on the target table. | We go further: a stored procedure checks the chain of positions and skips anything already applied (see *Consistency*). | *Connector Pipelines*, "Stores offset information…" |
| Aeron Cluster's log is one recording per member, extended at every leadership term. **The log position is one increasing number, identical on every member.** | The log position is the resume offset. Never store a recording id, because each member numbers its recordings differently. | Aeron 1.54.0 source: `ConsensusModuleAgent`, `RecordingLog` |
| Members record entries **before** they are committed, and an election can **truncate** those entries. | Never read a member's log without limiting the read to what is committed. | `Election.onTruncateLogEntry`, `ConsensusModuleAgent.truncateLogEntry` |
| Open-source `ClusterBackup` reads only committed entries. It sends a `BackupQuery` to a member, which replies with its `logRecordingId` and `commitPositionCounterId`. It then replays that member's archive with `boundedReplay(…, commitPositionCounterId, …)`. | The gateway uses the same two-step mechanism directly. | `ClusterBackupAgent.java:837`, `aeron-cluster-codecs.xml` (BackupQuery id 77, BackupResponse id 78) |
| Open-source Aeron **never purges** the cluster log. | A stored offset older than the start of the log can only happen after a manual purge. The gateway treats it as a hard error. | source search, ClusterTool command list |
| **The Aeron Sequencer API is not public.** It has no Maven Central artifacts, no SDK documentation, and "gateway" does not appear in its public material. It is "built directly on top of Aeron Cluster". | The gateway code sits behind our own interface. The switch runbook says what to collect from Adaptive and does not guess the SDK's classes. | aeron.io, Adaptive blog posts, datasheet (Dec 2025) |

**Provenance:**
- **Read verbatim:**
  - SingleStore v10 *Create a Custom Connector Pipeline* and *Connector Pipelines* pages, from their raw `.md`.
  - The Aeron 1.54.0 source excerpts cited above.
- **Read by a research agent, not re-checked by me:**
  - the remaining SingleStore reference pages;
  - the Aeron Sequencer material, all of it public marketing, blog posts and a datasheet.
- **Run on this machine:** the SingleStore 10.9 smoke test below.

## SingleStore 10.9 smoke test (2026-11-03)

This tested the platform before our connector goes in. It used Kafka's own `FileStreamSourceConnector` (`connect-file-3.9.1.jar`, checksum matched against Maven Central), reading lines from a file inside the container.

**Setup:**
- Image `ghcr.io/singlestore-labs/singlestoredb-dev:0.2.88`: SingleStore 10.9.0, one aggregator and one leaf in one container, no licence. The image sets `java_path` to its own JDK 25.0.2 on both nodes.
- Start it with:
  ```bash
  docker run -d --name singlestoredb-dev --cpus=8 -e ROOT_PASSWORD="…" -p 3306:3306 -p 8080:8080 -p 9000:9000 ghcr.io/singlestore-labs/singlestoredb-dev:0.2.88
  ```
  `--cpus=8` is required: without a licence the image allows at most 8 cores, and this laptop has 16.
- The connector folder is `/server/singlestoredb-server-10.9.0-d16d12a6ad/extractors/lib-extractor-connect/`. Copy the JAR in with `docker cp`.
- Limits of the image: two partitions per database, and not supported for production or benchmarks.

| Checked | Observed |
|---|---|
| Preview flag without a licence | `connector_pipelines_enabled=true` was accepted and connector pipelines ran. |
| Custom JAR found by short name | `LOAD DATA CONNECTOR 'FileStreamSourceConnector'` resolved through the JAR's `META-INF/services` entry. |
| Order and offsets | 3 lines loaded in order. `PIPELINES_SOURCE_OFFSETS` held one row per task, `KEY` `{"filename":"/tmp/s2test/events.json"}` and `VALUE` `{"position":74}` (the end of the file), updated with each batch. |
| New data | A 4th line appended later loaded after the pipeline restarted. The offset moved to 98. |
| `FORMAT JSON` with every `output.*` option on | Each record arrived as `{"key":null,"sourceOffset":{…},"sourcePartition":{…},"timestamp":…,"topic":"events","value":"<line>"}`. A string value arrives as a JSON string. BYTES has not been tested. |
| `poll()` returning `null` | The file connector returns `null` when it has nothing new. Each such batch was recorded as **Failed** in `PIPELINES_BATCHES_SUMMARY` (7 of 10 batches), with two `PIPELINES_ERRORS` rows (one from the leaf, one from the aggregator): `Cannot invoke "java.util.List.isEmpty()" because "batch" is null`. The pipeline stayed Running for the two minutes observed. A Kafka Connect worker accepts `null`, so the Phase 3 integration test cannot catch this. |
| `ALTER PIPELINE … SET OFFSETS` | Rejected in all three forms (`EARLIEST`, `LATEST`, and the documented JSON array) with `ERROR 1706: Feature 'Offset-modifying ALTER PIPELINE with CDC pipelines' is not supported by SingleStore`. This happened whether the pipeline was made with `CREATE PIPELINE` or `CREATE INFERRED PIPELINE`. The 10.x release notes say `SET OFFSETS` works on Connector pipelines. It may work only for the built-in Kinesis and Pub/Sub connectors, which cannot be tested here. |
| The docs' example `CREATE INFERRED PIPELINE … INTO TABLE …` | Syntax error. `CREATE PIPELINE … INTO TABLE …` works. `CREATE INFERRED PIPELINE … FORMAT JSON` with no `INTO` creates its own table. |

**Not answered yet:** Phase 0 questions 1–3, 5 and 6, and BYTES rendering in question 4. They need our own connector inside the extractor.

## Architecture

```
Open-source Aeron Cluster (3 members)                SingleStore 10, self-managed
┌──────────────────────────────┐                     ┌──────────────────────────────────────────┐
│ member: consensus module      │  1. BackupQuery    │ extractor JVM on a leaf node              │
│   commit-pos counter          │◄───────────────────│  SequencerSourceTask  (the gateway)       │
│ member: archive               │  2. replay, bounded│   └ SequencedLog "aeron-cluster"          │
│   cluster log recording ──────┼────────────────────►│       decode, check chain, emit entries  │
└──────────────────────────────┘   by commit-pos     │  poll() → SourceRecords                   │
                                                      │  one transaction per batch:               │
                                                      │   apply_sequenced_entries(batch)          │
                                                      │    · check chain against watermark        │
                                                      │    · insert into sequenced_log            │
                                                      │    · advance sequencer_watermark          │
                                                      │  offsets → PIPELINES_SOURCE_OFFSETS       │
                                                      └──────────────────────────────────────────┘
```

**The connector is the gateway.** It runs outside the cluster, the way Aeron describes off-cluster gateways.

**It keeps no state of its own.** The resume point lives in SingleStore twice:
- in SingleStore's pipeline offsets;
- in `sequencer_watermark`, written in the same transaction as the data.

**It reads from a follower by default**, so the leader's hot path is untouched.

**Alternative, only if work networking forbids the direct path:** run Aeron's own `ClusterBackup` as a sidecar beside SingleStore and point the reader at the sidecar's archive. The guarantees are the same; it costs one more process to operate.

## Consistency: each guarantee, how it is enforced, and the test that proves it

| Guarantee | Enforced by | Proven by |
|---|---|---|
| Only committed entries leave the cluster | The archive replay is limited by the member's commit-position counter, the mechanism `ClusterBackup` uses. | TCK `neverDeliversAnEntryThatIsLaterRetracted`: the followers are cut off from the log, so the leader appends entries it cannot commit. They are then stopped, so the leader is the only member left to read from. Nothing past the commit position is delivered. After a new leader moves on without that tail, everything delivered equals a fresh read. (Stopping the followers alone is not enough: the leader then appends nothing.) |
| Total order, no gaps leaving the gateway | One task and one partition. Every entry must start where the previous one ended. The one exception is Aeron's end-of-term padding: the gap must end exactly on a term-buffer boundary. Any other break throws `SequenceIntegrityException`. | TCK: 500 messages compared entry by entry with the oracle (what a clustered service actually processed), across many 64 KB term boundaries. `PositionChainTest` covers gaps and padding. Volume is Phase 4's 1M-entry run. |
| No gaps in SingleStore | Each row carries `previous_position`. The procedure refuses a batch unless the new rows form one chain starting at the current watermark, and raises an error so the pipeline stops. | E2E: create a gap; the pipeline must go to Error with nothing loaded. **This test must bite.** `ALTER PIPELINE … SET OFFSETS` cannot create the gap, because SingleStore 10.9.0 rejects it for custom connectors (see the smoke test). Instead, with the pipeline stopped, move `sequencer_watermark.position` back to an earlier entry. The next batch then starts after the stored offset, not at the watermark. Separately, call `apply_sequenced_entries` directly with a batch that skips an entry. Both methods were run. Each refused the gap with nothing loaded, and with the check removed each let the gap through (VERIFICATION.md, Phase 4). |
| No duplicates from at-least-once replays | Rows with `position <= watermark` are skipped, backed by primary key `(sequencer, position)`. | E2E: kill the extractor mid-batch and fail a batch on purpose. Row count and positions stay exact. |
| Data and watermark always agree | Both are written by the procedure inside the batch transaction. | E2E reconciliation query. |
| Survives leader failover and member loss | Positions are the same on every member. On replay loss the gateway asks another member and resumes from its last position. | TCK: kill the leader mid-stream; kill the member being read from. |
| A resume never silently skips | The resume position must be 32-byte aligned, at or after the start of the log, and at or before the commit position. The leadership term must never go backwards. | TCK `refusesToResumeBeyondTheEndOfTheLog` (wrong cluster, or past the end). `AeronClusterLogTest`: misaligned, and inside an entry. Purged: the check exists but has no test, because open-source Aeron never purges its log. |
| Unknown log content is never dropped | Every log entry is emitted: session messages, timers, session open/close, cluster actions, new-term events. Any other message type, schema id or template id throws. | `ClusterLogDecoderTest`: unknown schema and unknown template are refused. |
| Applications can read their own writes | An application waits until `sequencer_watermark.position` ≥ the log position of its command. The clustered service already knows that position (`cluster.logPosition()`) and can return it on egress. | E2E query, documented in the README. |

## Modules and key types

Maven multi-module project. Group `com.paulhoward`, Java `--release 17`, Aeron `1.54.0` (overridable with `-Daeron.version`), `connect-api` `3.9.1` (provided).

| Module | Contents |
|---|---|
| `gateway-api` | The gateway interface, with no Aeron or Kafka dependencies. **This is the switch point.** |
| `gateway-tck` | `SequencedLogContractTest`: abstract JUnit 5 contract tests that every gateway implementation must pass. A `SequencerFixture` interface covers start, submit, kill leader, kill member and lose quorum. The Aeron Sequencer adapter reuses this at work. |
| `gateway-aeron-cluster` | Package `com.paulhoward.sequencer.gateway.aeroncluster`. The open-source implementation, plus an in-process 3-member test cluster that records an oracle of what the service processed. Also a small `LogTail` main that prints committed entries from a position, for diagnosis at work. |
| `connector` | `SequencerSourceConnector`, `SequencerSourceTask` and `SequencerConnectorConfig`. Packaged as an uber-JAR: `aeron-singlestore-connector-<ver>.jar`. |
| root `pom.xml`, profile `aeron-sequencer` | Adds the `gateway-aeron-sequencer` module (written at work) and builds the JAR with classifier `aeron-sequencer` instead of bundling the open-source reader. The default build never activates it. |
| `singlestore/` (SQL, not Maven) | `schema.sql`, `apply_sequenced_entries.sql`, `pipeline.sql`, `verify.sql`. |

**`gateway-api`, package `com.paulhoward.sequencer.gateway`:**

```java
public interface SequencedLogProvider {            // found with ServiceLoader
    String name();                                 // "aeron-cluster" or, at work, "aeron-sequencer"
    long startOfLog();                             // resume point meaning "nothing applied yet"
    SequencedLog open(Map<String, String> config, long resumeAfter);
}

public interface SequencedLog extends AutoCloseable {
    /** Non-blocking. Delivers at most maxEntries committed entries, in order,
     *  each chained to the one before. Throws SequenceIntegrityException rather than skip. */
    int poll(Consumer<SequencedEntry> handler, int maxEntries);
    @Override void close();
}

public record SequencedEntry(
    long position,          // resume token for this entry (Aeron Cluster: log position at its end)
    long previousPosition,  // position of the entry before it; forms the chain
    long term,              // leadership term id
    long timestamp,         // sequencer timestamp
    String kind,            // SESSION_MESSAGE, TIMER, SESSION_OPEN, SESSION_CLOSE, CLUSTER_ACTION, NEW_TERM
    long sourceId,          // cluster session id (negative = service-originated)
    byte[] payload) {}      // application bytes, unchanged; empty for non-message kinds

public final class SequenceIntegrityException extends RuntimeException { … }
```

**`gateway-aeron-cluster`:**

| Class | What it does |
|---|---|
| `AeronClusterLogProvider` | Named `aeron-cluster`. |
| `CommittedLogLocator` | Sends a `BackupQuery` on the consensus channel and receives the `BackupResponse`, following `ClusterBackupAgent`. Prefers a follower. |
| `AeronClusterLog` | Embedded media driver plus an `AeronArchive` client. Runs `startBoundedReplay(recordingId, resumeAfter, NULL_LENGTH, commitPositionCounterId, …)` against the chosen member. |
| `ClusterLogDecoder` | SBE schema 111: SessionMessageHeader, TimerEvent, SessionOpen/Close, ClusterActionRequest, NewLeadershipTermEvent. Throws on anything else. |
| `PositionChain` | The contiguity check, including the term-padding rule. |

## Record format and offsets

| Part | Content |
|---|---|
| `sourcePartition` | `{"sequencer": "<sequencer.name>"}`. One partition, so `taskConfigs()` returns exactly one task whatever `tasks.max` says. |
| `sourceOffset` | `{"position": <long>, "term": <long>}` |
| value (Connect `Struct`) | `sequencer`, `position`, `previous_position`, `term`, `timestamp`, `kind`, `source_id`, `payload` (BYTES) |
| topic | `sequencer.name`. Kafka Connect requires a topic; SingleStore exposes it as a column. Records go to Kafka partition 0, so order also holds in a normal Kafka Connect deployment whatever the topic's partition count. |

## Connector configuration

| Key | Meaning |
|---|---|
| `sequencer.gateway` | Required. `aeron-cluster`, or `aeron-sequencer` at work. Selects the `SequencedLogProvider`; startup fails if no provider of that name is on the classpath. |
| `sequencer.name` | Required. The logical sequencer id, used for the partition, the DB `sequencer` column and the watermark row. |
| `batch.max.entries` | Default 1000. Upper bound on what one `poll()` returns. |
| `aeron.cluster.consensus.endpoints` | `host:port,…` for the members' consensus endpoints. |
| `aeron.gateway.host` | Address of this host that cluster members can reach, for the replay and response streams. |

## SingleStore side (sketch; final SQL in `singlestore/`)

```sql
CREATE TABLE sequenced_log (
  sequencer VARCHAR(64) NOT NULL, position BIGINT NOT NULL, previous_position BIGINT NOT NULL,
  term BIGINT NOT NULL, ts BIGINT NOT NULL, kind VARCHAR(24) NOT NULL, source_id BIGINT NOT NULL,
  payload LONGBLOB, loaded_at DATETIME(6) DEFAULT NOW(6),
  PRIMARY KEY (sequencer, position), SHARD KEY (position));

CREATE ROWSTORE TABLE sequencer_watermark (
  sequencer VARCHAR(64) PRIMARY KEY, position BIGINT NOT NULL, term BIGINT NOT NULL, applied_at DATETIME(6));

-- apply_sequenced_entries(batch QUERY(...)):
--   wm = watermark for this sequencer (the seed row is created before the pipeline starts)
--   drop rows with position <= wm                      (replays)
--   require: the remaining rows form one chain whose first previous_position = wm, else RAISE
--   INSERT INTO sequenced_log ...; UPDATE sequencer_watermark SET position = max(position) ...

CREATE PIPELINE sequenced_log_ingest AS LOAD DATA CONNECTOR 'SequencerSourceConnector'
  CONFIG '{"tasks.max":"1","sequencer.gateway":"aeron-cluster","sequencer.name":"oss-dev", ...}'
  DISABLE OUT_OF_ORDER OPTIMIZATION
  INTO PROCEDURE apply_sequenced_entries FORMAT JSON (...field mapping decided in Phase 0...);
```

## Build phases and exit criteria

Each phase is done only when its exit criteria have been **run**, not reviewed.

**Phase 0: SingleStore spike.** Can run alongside phases 1–3. Needs SingleStore 10 with JRE 25.
- Build a throwaway connector that emits a synthetic chained sequence. Deploy it and answer the questions the docs leave open:
  1. Does the extractor JVM persist across batches, or start fresh each batch?
  2. After a failed batch, is the task restarted with the stored offset, or must it rewind itself?
  3. Is there one `poll()` per batch per task?
  4. How does a `Struct` with BYTES render in `FORMAT JSON`, and which `output.*` mode gives clean columns?
  5. Does Agrona/Aeron run on JRE 25 inside the extractor? Does it need `--sun-misc-unsafe-memory-access=allow`, and can we pass JVM flags at all?
  6. Can the extractor host open UDP to and from the cluster?
- **Exit:** each question answered with the observed output saved under `spike/`. Answers that change the design are written back into this plan.
- **Done (2026-11-04)** with the real connector instead of a throwaway one. The answers are in VERIFICATION.md, under *Phase 0 questions, answered*, rather than in `spike/`. Question 5 changed the install: Aeron needs `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`, which only a `java_path` wrapper can supply.

**Phase 1: skeleton.**
- Maven multi-module build, `gateway-api`, `gateway-tck`, and the in-process 3-member test cluster with its oracle service.
- **Exit:** `mvn verify` green. The test cluster elects a leader, survives a leader kill, and the oracle records `(position, sessionId, payload)` for each message.

**Phase 2: open-source gateway reader** (`gateway-aeron-cluster`).
- `CommittedLogLocator`, `AeronClusterLog`, `ClusterLogDecoder`, `PositionChain`, `LogTail`.
- **Exit:** the full TCK passes: order, committed-only (quorum loss), leader failover, read-member loss, resume, bad offsets, foreign schema. Each integrity test is shown to fail when its check is removed.

**Phase 3: Kafka Connect connector.**
- Connector, task and config; uber-JAR with `META-INF/services`.
- **Exit:**
  - Unit tests for offset round trip and the `poll()` bound.
  - An integration test in a **real Kafka Connect 3.9.1 worker** (`EmbeddedConnectCluster`, in-process, no Docker) shows records in order and resume after a connector restart.
  - `jar tf` shows `connect-api` is not bundled.

**Phase 4: SingleStore end to end.**
- `schema.sql`, the procedure, `pipeline.sql`, `verify.sql`.
- **Exit:**
  - 1M entries loaded while killing the leader, the read member and the extractor.
  - `verify.sql` shows row count = oracle count, an unbroken chain, and watermark = last position.
  - The gap test (method in the *Consistency* table, since `SET OFFSETS` is unavailable) puts the pipeline in Error with zero rows loaded.
  - Throughput measured and recorded.

**Phase 5: carry it to work.** Dropped on 2026-11-04. The project goes to work as source. The verified install is Step 8 of SWITCH-TO-AERON-SEQUENCER.md.
- A release zip containing:
  - the connector uber-JAR;
  - `singlestore/*.sql`;
  - `INSTALL.md`;
  - `SWITCH-TO-AERON-SEQUENCER.md`;
  - the `gateway-api` and `gateway-tck` JARs plus sources, so the Sequencer adapter can be built at work against an internal Maven mirror.
- **Exit:** `INSTALL.md` followed step by step on a clean SingleStore instance by someone other than its author (a fresh agent), with the open-source cluster as the source.

## Inputs needed and risks

1. **A SingleStore 10 instance with JRE 25 for Phases 0 and 4.** Resolved: the `singlestoredb-dev` image 0.2.88 runs SingleStore 10.9.0 with JDK 25 here (see the smoke test).
2. **JDK 25 locally.** Resolved: Corretto 25.0.5 is installed (see [VERIFICATION.md](VERIFICATION.md)).
3. **Network at work.** Every SingleStore leaf must reach the cluster members' consensus and archive-control endpoints over UDP, and receive the replay stream. If that is refused, use the `ClusterBackup` sidecar alternative.
4. **Aeron version.** The gateway's Aeron version must match the cluster's. `BackupQuery` carries the consensus protocol version.
5. **Preview feature.** SingleStore may change Connector Pipelines. Phase 0 and the E2E suite are the guard; rerun them on every SingleStore upgrade.
6. **Throughput.** A single total order means one task, and `INTO PROCEDURE` is slower than `INTO TABLE`. That is the price of a chain check inside the database. Phase 4 measures it. The dev image is not supported for benchmarks, so a number measured on it is indicative only.
7. **The sibling `../cluster-sequencer` cannot be a resumable source as it stands.** `ClusterNode` deletes its archive and cluster dirs on every start (`deleteArchiveOnStart(true)`, `deleteDirOnStart(true)`, `dirDeleteOnStart(true)`), so positions restart at 0. The gateway would correctly refuse to resume. The tests here use their own embedded cluster, and this plan does not change that project.
