# Switching the gateway to the commercial Aeron Sequencer

Use this when installing at work, where the source is Adaptive's **Aeron Sequencer** rather than open-source Aeron Cluster.

## What changes and what does not

- **Changes:**
  - **The gateway reader:** you add one module, `gateway-aeron-sequencer`, with two classes and one service file.
  - **Two connector settings:** `sequencer.gateway` and the connection keys.
- **Does not change:**
  - the connector (`SequencerSourceConnector` / `SequencerSourceTask`);
  - the record format;
  - the SingleStore tables, `apply_sequenced_entries` procedure and chain/watermark checks;
  - the TCK.

The consistency guarantees in PLAN.md hold only if the new reader passes the same TCK.

## Why this cannot be pre-built here

Aeron Sequencer has no public API, SDK documentation or Maven artifacts (checked 2026-11-03). Its classes cannot be written or compiled outside your employer's Adaptive account. The steps below say exactly what to collect and where it plugs in. They do **not** guess the SDK's class names.

---

## Step 1: Collect from Adaptive (blocking: do not start coding without all five)

From your employer's Adaptive account or support contact, and the Sequencer SDK reference guide:

1. **Artifacts.** The Maven `groupId:artifactId:version` of the Sequencer client SDK, and the repository it comes from. Adaptive ships premium components from `weareadaptive.jfrog.io`, repo `aeron-premium`; at work this is usually mirrored into the internal Artifactory.
2. **How an off-sequencer process reads the sequenced log.** The public material names a *Replay Service* and a *log replay client*. You need the SDK calls to:
   - start reading at a given point;
   - keep following live;
   - poll **without blocking**;
   - stop.
3. **The resume point.** Public material describes a monotonically increasing **sequence index** on every message. Confirm two things:
   - it increases by exactly 1 per message;
   - it is the value you resume from, and whether the read starts *at* or *after* the given index.
4. **Committed-only delivery.** Confirm the reader only ever sees messages accepted by a majority. If the SDK can expose uncommitted messages, find the call that limits reading to the committed point. Open-source Aeron 1.49+ defines a Sequencer counter for the "greatest committed message id", which suggests one exists.
5. **The message envelope and the Aeron version:**
   - What identifies a message's type, its publishing service and its sequencer timestamp.
   - The SBE schema id(s).
   - Which open-source Aeron version the SDK depends on: `mvn dependency:tree` on a project using the SDK.

## Step 2 (optional, 30 minutes): check whether the open-source reader already works

Aeron Sequencer is built on Aeron Cluster, so the existing reader *might* read its log unchanged. **This is unverified.** Run it against a **non-production** Sequencer environment:

```bash
java -cp aeron-singlestore-connector-<ver>.jar com.paulhoward.sequencer.gateway.aeroncluster.LogTail --consensus-endpoints <host:port,...> --gateway-host <this-host> --from 0 --count 100
```

| Result | What it means |
|---|---|
| Entries print and the `kind` values make sense | The Sequencer exposes a standard cluster log. Rebuild the default (open-source) JAR with `-Daeron.version=<Sequencer's Aeron version>`, because `BackupQuery` carries the consensus protocol version. Then run the open-source reader's TCK against the Sequencer environment. If it passes, skip Steps 3–5 and use `sequencer.gateway=aeron-cluster`. |
| `SequenceIntegrityException: unknown schema id <N>` | The log holds Sequencer-format messages. Continue with Step 3. Do **not** widen the decoder to let them through; that would bypass the Sequencer's own read path. |
| No `BackupResponse` / timeout | The Sequencer does not answer cluster backup queries on that endpoint. Continue with Step 3. |

## Step 3: Make the build reach the SDK

1. In `~/.m2/settings.xml` on the work machine, add the internal Artifactory as a mirror. It must hold Maven Central and Adaptive's `aeron-premium`:

   ```xml
   <mirrors>
     <mirror>
       <id>internal</id>
       <mirrorOf>*</mirrorOf>
       <url>https://<your-artifactory>/artifactory/<virtual-repo></url>
     </mirror>
   </mirrors>
   ```

   Put credentials in a matching `<server><id>internal</id>…</server>` entry, never in the POM.
2. Set the Aeron version to the one the SDK depends on (Step 1.5). Every build command below passes `-Daeron.version=<that version>`.

## Step 4: Create the `gateway-aeron-sequencer` module

The root `pom.xml` already has a Maven profile named `aeron-sequencer` that adds this module. The default build ignores it.

1. Create `gateway-aeron-sequencer/pom.xml` with three dependencies:
   - `com.paulhoward:gateway-api`;
   - the SDK artifact from Step 1.1;
   - test scope: `com.paulhoward:gateway-tck`.
2. Create two classes in package `com.paulhoward.sequencer.gateway.aeronsequencer`:

   **`AeronSequencerLogProvider implements SequencedLogProvider`**
   - `name()` returns `"aeron-sequencer"`.
   - `startOfLog()` returns the resume point meaning "nothing applied yet". It is the value before the first sequence index (Step 1.3), so the first entry delivered is the first in the log. If indexes start at 1, return 0. The connector uses it when SingleStore holds no offset yet.
   - `open(config, resumeAfter)` reads its settings from keys prefixed `aeron.sequencer.` and returns a new `AeronSequencerLog`.

   **`AeronSequencerLog implements SequencedLog`**: map each part of the contract to what Step 1 gave you:

   | Our contract | Implement with |
   |---|---|
   | `open(…, resumeAfter)` | Start the SDK's log read so the **first entry delivered is the one after `resumeAfter`**. If that point is no longer available (purged or retained elsewhere), throw `SequenceIntegrityException`. **Never start later.** |
   | `poll(handler, maxEntries)` | Poll the SDK reader **without blocking**. Deliver at most `maxEntries`. Return the count, 0 if nothing is ready. |
   | `SequencedEntry.position` | The sequence index (Step 1.3). |
   | `SequencedEntry.previousPosition` | The index of the entry delivered before it. If `position != previousPosition + 1`, throw `SequenceIntegrityException`. |
   | `SequencedEntry.term` | The leadership term id if the SDK exposes it, otherwise `0`. |
   | `SequencedEntry.timestamp` | The sequencer timestamp. |
   | `SequencedEntry.kind` | A fixed string per Sequencer message type, listed in the module README. **An unknown type throws; never skip it.** |
   | `SequencedEntry.sourceId` | The publishing service or session id. |
   | `SequencedEntry.payload` | The message body bytes, unchanged. |
   | Committed-only | Read only majority-accepted messages (Step 1.4). |
   | `close()` | Release every SDK resource. |

3. Register the provider: create `src/main/resources/META-INF/services/com.paulhoward.sequencer.gateway.SequencedLogProvider` containing one line:

   ```
   com.paulhoward.sequencer.gateway.aeronsequencer.AeronSequencerLogProvider
   ```

## Step 5: Implement the TCK fixture

In `gateway-aeron-sequencer/src/test`, implement `SequencerFixture` against a **non-production** Sequencer test environment, or an in-process one if the SDK provides it. The open-source version to copy from is `gateway-aeron-cluster/src/test/.../AeronClusterFixture.java`.

| Method | What it must do |
|---|---|
| `memberCount()`, `gatewayConfig()`, `startOfLog()` | Describe the test sequencer and how the reader connects to it. `startOfLog()` must equal your provider's. |
| `offer(payload, timeout)` | Send one message. Acceptance only means it was sent; the tests confirm through `committed()` and resend. |
| `committed()` | The oracle: what the sequencer's own application has processed, with each message's position. It must come from the application side, not from the reader under test. |
| `awaitLeader`, `stopMember`, `startMember` | Find the leader; stop a member; start it again with its existing log. |
| `cutOffFromLog(memberId)` | Stop a running follower from receiving new log entries while it stays in contact with the others, as a one-way network fault would. |

`cutOffFromLog` is the hard one, and it matters. It is how the committed-only test gives the leader entries it can never commit. The open-source fixture installs a loss generator on each member's media driver that drops inbound frames on the log stream. If the Sequencer runs on an Aeron media driver you configure, do the same; otherwise use the test environment's fault-injection tools.

Simply stopping the followers does **not** work. On open-source Aeron Cluster the leader then appends nothing, and the test passes without proving anything.

Then create `AeronSequencerLogTest extends SequencedLogContractTest`, returning your fixture and provider.

**Bite-test the committed-only check once.** Temporarily make `AeronSequencerLog` read past the commit point (for example, ask the SDK for all received messages rather than majority-accepted ones). Confirm that `neverDeliversAnEntryThatIsLaterRetracted` fails, then restore the code. If the test still passes, the fixture is not creating an uncommitted tail, and a green run proves nothing about committed-only reads.

## Step 6: Prove it before deploying

```bash
mvn -P aeron-sequencer -Daeron.version=<ver> -pl gateway-aeron-sequencer -am verify
```

**Every TCK test must pass.** Each test names the guarantee it checks, for example `neverDeliversAnEntryThatIsLaterRetracted` or `survivesLeaderFailoverWithoutGapsOrDuplicates`. A failure means that guarantee does not hold, and the reader is not deployable until it does. Do not disable tests to get a green build.

## Step 7: Build the deployable JAR

```bash
mvn -P aeron-sequencer -Daeron.version=<ver> clean package
```

Check that the provider is inside and Kafka Connect is not:

```bash
jar tf connector/target/aeron-singlestore-connector-*-aeron-sequencer.jar | grep -E "SequencedLogProvider|AeronSequencerLogProvider|org/apache/kafka/connect/source/SourceTask"
```

You should see the service file and `AeronSequencerLogProvider`, and **no** `org/apache/kafka/connect/...` classes.

## Step 8: Install into SingleStore

These steps were run end to end with the open-source cluster on SingleStore 10.9.0 (see Phase 4 in [VERIFICATION.md](VERIFICATION.md)). For the Sequencer, only the JAR and the pipeline CONFIG differ.

1. Use **self-managed SingleStore 10**. Custom connectors are not supported on Helios.
2. **Give the extractors the JVM flag Aeron needs. This requires a SingleStore admin.**
   - SingleStore starts extractors without module flags. Aeron's Agrona 2.x then fails on the first batch with `IllegalAccessError: class org.agrona.UnsafeApi … cannot access class jdk.internal.misc.Unsafe`.
   - On **every aggregator and leaf**, install a wrapper script, readable and executable by the SingleStore user:

     ```sh
     #!/bin/sh
     exec /path/to/jre-25/bin/java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED "$@"
     ```

   - Point `java_path` at the wrapper in each node's `memsql.cnf`. To apply it without a restart, also run `SET GLOBAL java_path = '<wrapper path>';` connected to each node directly. Running it on the aggregator alone did not reach the leaf, and the leaves run the extractors.
   - The wrapper serves every Java extractor on the node. The flag only widens what code may access.
   - If the Sequencer SDK does not use Agrona 2.x, this step may not be needed. Test without the wrapper first if you want to find out.
3. Copy the JAR to `<install-dir>/extractors/lib-extractor-connect/` on **every aggregator and leaf node**.
4. Enable the feature:

   ```sql
   SET GLOBAL experimental_features_config = 'connector_pipelines_enabled=true';
   ```
5. Run `singlestore/schema.sql` and `singlestore/apply_sequenced_entries.sql`.
6. Seed the watermark with the provider's `startOfLog()`, the position just before the first entry. For open-source Aeron Cluster it is 0.

   ```sql
   INSERT IGNORE INTO sequencer_watermark (sequencer, position, term, applied_at)
   VALUES ('<prod-sequencer-name>', <startOfLog()>, 0, NULL);
   ```

   - The procedure only accepts a batch whose first new entry's `previous_position` equals the watermark. A wrong seed therefore stops the pipeline on its first batch, with nothing loaded.
   - To skip history, seed it with the position of an entry boundary instead. The connector still reads from `startOfLog()`, and the procedure ignores everything up to the watermark.
7. Create the pipeline. Compared with `singlestore/pipeline.sql`, only the CONFIG and CREDENTIALS change:

   ```sql
   CREATE PIPELINE sequenced_log_ingest
   AS LOAD DATA CONNECTOR 'com.paulhoward.sequencer.gateway.connect.SequencerSourceConnector'
   CONFIG '{
       "tasks.max": "1",
       "batch.max.entries": "10000",
       "sequencer.gateway": "aeron-sequencer",
       "sequencer.name": "<prod-sequencer-name>",
       "aeron.sequencer.<key from Step 1.2>": "<value>"
   }'
   CREDENTIALS '{"aeron.sequencer.<secret key>": "<secret>"}'
   BATCH_INTERVAL 100
   INTO PROCEDURE apply_sequenced_entries
   FORMAT JSON (
       sequencer <- value::sequencer,
       position <- value::position,
       previous_position <- value::previous_position,
       term <- value::term,
       ts <- value::`timestamp`,
       kind <- value::kind,
       source_id <- value::source_id,
       @payload <- value::payload)
   SET payload = FROM_BASE64(@payload);
   ```

   - Use the connector's full class name. That is the form that was tested.
   - Remove every `aeron.cluster.*` key. Keep `aeron.gateway.host` only if the Sequencer provider uses it.
   - Put secrets in CREDENTIALS, not in CONFIG; `SHOW CREATE PIPELINE` redacts CREDENTIALS. Only an empty CREDENTIALS was tested here, so confirm the provider receives these values.
   - Use a new `sequencer.name`. Offsets are keyed by it, so open-source and Sequencer positions can never be mixed.
   - The payload is BYTES, which arrive as base64; `FROM_BASE64` restores them.
8. Start the pipeline with `START PIPELINE sequenced_log_ingest;`

### Operating notes from testing on 10.9.0

- **After `DROP PIPELINE` or `CREATE OR REPLACE PIPELINE`, kill the old extractor.** SingleStore leaves its JVM running, and with it a replay open on the source. Once, a leftover extractor kept a re-created pipeline of the same name at Running with no batches and no errors. On each leaf, find leftovers with `ps -ef | grep ExtractorDriver`.
- **A killed extractor running the open-source gateway leaves a `/tmp/aeron-gateway-<uuid>` directory of about 50 MB.** Delete these when no extractor is running. Check whether the Sequencer provider leaves anything similar.
- **`CREATE OR REPLACE PIPELINE` keeps the stored offsets, and `ALTER PIPELINE … SET OFFSETS` is unsupported for custom connectors.** The only way to reload part of the log is to rebuild from a position `P` that is a loaded entry. `e2e/run-e2e.sh rebuild-from` does this:

  ```sql
  STOP PIPELINE sequenced_log_ingest;
  DROP PIPELINE sequenced_log_ingest;
  DELETE FROM sequenced_log WHERE sequencer = '<name>' AND position > <P>;
  UPDATE sequencer_watermark SET position = <P>, term = <term of the row at P> WHERE sequencer = '<name>';
  -- CREATE PIPELINE exactly as in step 7, then:
  START PIPELINE sequenced_log_ingest;
  ```

  The new pipeline reads from `startOfLog()`, and the procedure skips everything up to `P`. With the open-source cluster, it reloaded the rows after `P` and `verify.sql` came back clean.

## Step 9: Verify in SingleStore

```sql
SELECT * FROM information_schema.PIPELINES_ERRORS WHERE PIPELINE_NAME = 'sequenced_log_ingest';
SELECT * FROM sequencer_watermark;  -- position must keep advancing
SOURCE singlestore/verify.sql;      -- chain unbroken, count matches, watermark = max(position)
```

**Healthy:**
- no rows in `PIPELINES_ERRORS`;
- the watermark advancing;
- `verify.sql` reporting `chain_breaks = 0`.

**A chain-break error from `apply_sequenced_entries` is a stop condition.** It means the reader handed SingleStore a gap. Investigate the reader; never reset offsets to get past it.

## Rollback

1. `STOP PIPELINE sequenced_log_ingest;`
2. Put the previous JAR back on every node.
3. On each leaf, kill any `ExtractorDriver` process still running the old JAR.
4. `START PIPELINE sequenced_log_ingest;`

Loaded rows and the watermark stay valid, because the chain check guarantees they are a correct prefix of the log.
