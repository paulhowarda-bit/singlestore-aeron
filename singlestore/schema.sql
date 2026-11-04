-- Tables that hold a sequencer's committed log in SingleStore. Run in the target database.
--
-- sequenced_log        one row per log entry, exactly the sequencer's committed log: same order, no gaps,
--                      no duplicates. previous_position links each row to the one before it.
-- sequencer_watermark  one row per sequencer: the position of the last entry applied. It is written in the
--                      same transaction as the rows, so an application that sees watermark >= P can read
--                      every entry up to P.

CREATE TABLE IF NOT EXISTS sequenced_log (
    sequencer         VARCHAR(64) NOT NULL,
    position          BIGINT NOT NULL,       -- end of this entry in the sequencer's log; strictly increasing
    previous_position BIGINT NOT NULL,       -- end of the entry before; the first entry has the start of the log
    term              BIGINT NOT NULL,       -- leadership term the entry was appended in
    ts                BIGINT NOT NULL,       -- the sequencer's timestamp for the entry
    kind              VARCHAR(24) NOT NULL,  -- SESSION_MESSAGE, SESSION_OPEN, SESSION_CLOSE, TIMER, NEW_TERM, ...
    source_id         BIGINT NOT NULL,       -- client session id for session entries, else 0
    payload           LONGBLOB,
    loaded_at         DATETIME(6) DEFAULT NOW(6),
    PRIMARY KEY (sequencer, position),
    SHARD KEY (position)
);

CREATE ROWSTORE TABLE IF NOT EXISTS sequencer_watermark (
    sequencer  VARCHAR(64) NOT NULL PRIMARY KEY,
    position   BIGINT NOT NULL,
    term       BIGINT NOT NULL,
    applied_at DATETIME(6)
);
