-- Compares sequenced_log with the test cluster's oracle: the file in which the cluster's own service recorded
-- every committed message as it processed it. Run in the target database inside the test container, after
-- the load has finished and the pipeline has caught up. The oracle file path is set below.

CREATE TABLE IF NOT EXISTS e2e_oracle (
    position   BIGINT NOT NULL,
    session_id BIGINT NOT NULL,
    payload    LONGBLOB NOT NULL,
    SHARD KEY (position)
);
TRUNCATE TABLE e2e_oracle;
LOAD DATA INFILE '/opt/e2e/run/oracle.tsv' INTO TABLE e2e_oracle
    FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (position, session_id, payload);

-- oracle_messages = log_messages, and both mismatch counts = 0.
SELECT
    (SELECT COUNT(*) FROM e2e_oracle) AS oracle_messages,
    (SELECT COUNT(*) FROM sequenced_log WHERE kind = 'SESSION_MESSAGE') AS log_messages,
    (SELECT COUNT(*) FROM e2e_oracle o
        LEFT JOIN sequenced_log l
               ON l.position = o.position AND l.kind = 'SESSION_MESSAGE'
              AND l.source_id = o.session_id AND l.payload = o.payload
        WHERE l.position IS NULL) AS oracle_rows_missing_from_log,
    (SELECT COUNT(*) FROM sequenced_log l
        LEFT JOIN e2e_oracle o ON o.position = l.position
        WHERE l.kind = 'SESSION_MESSAGE' AND o.position IS NULL) AS log_rows_not_in_oracle;
