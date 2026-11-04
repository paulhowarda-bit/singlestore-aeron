-- Seeds the watermark and creates the pipeline. Run in the target database after schema.sql and
-- apply_sequenced_entries.sql. Values to change for another sequencer are marked EDIT.
--
-- Before this: the connector JAR is in <singlestore>/extractors/lib-extractor-connect/ on every host, and
-- java_path on every node runs Java 25 with --add-opens java.base/jdk.internal.misc=ALL-UNNAMED (see
-- SWITCH-TO-AERON-SEQUENCER.md, Step 8, and VERIFICATION.md, "Blocker found").

SET GLOBAL experimental_features_config = 'connector_pipelines_enabled=true';

-- The watermark starts at the start of the log: 0 for open-source Aeron Cluster. EDIT the name, and the
-- start position for another sequencer (its provider's startOfLog()).
INSERT IGNORE INTO sequencer_watermark (sequencer, position, term, applied_at) VALUES ('oss-dev', 0, 0, NULL);

CREATE PIPELINE sequenced_log_ingest
AS LOAD DATA CONNECTOR 'com.paulhoward.sequencer.gateway.connect.SequencerSourceConnector'
CONFIG '{
    "tasks.max": "1",
    "batch.max.entries": "10000",
    "sequencer.gateway": "aeron-cluster",
    "sequencer.name": "oss-dev",
    "aeron.cluster.consensus.endpoints": "localhost:9003,localhost:9103,localhost:9203",
    "aeron.gateway.host": "localhost"
}'
CREDENTIALS '{}'
BATCH_INTERVAL 100
INTO PROCEDURE apply_sequenced_entries
FORMAT JSON (
    sequencer         <- value::sequencer,
    position          <- value::position,
    previous_position <- value::previous_position,
    term              <- value::term,
    ts                <- value::`timestamp`,
    kind              <- value::kind,
    source_id         <- value::source_id,
    @payload          <- value::payload
)
SET payload = FROM_BASE64(@payload);
