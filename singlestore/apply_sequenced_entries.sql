-- The pipeline's procedure. It applies one batch of log entries, and refuses any batch that would leave a
-- gap or a fork in sequenced_log.
--
--  1. Rows at or below the watermark were applied before; the connector delivers at least once, so these are
--     replays and are skipped.
--  2. The remaining rows must form one chain that starts at the watermark: distinct positions, distinct
--     previous positions, each previous position is the watermark or another new row's position, and each
--     position is past its previous position. Together these allow only a single unbroken chain.
--  3. Otherwise the procedure raises an error. The batch then fails, nothing in it is written, its offsets
--     are not committed, and after the retries the pipeline stops in Error.
--  4. The rows and the new watermark are written in the batch's transaction.

DELIMITER //
CREATE OR REPLACE PROCEDURE apply_sequenced_entries(
    batch QUERY(
        sequencer         VARCHAR(64) NOT NULL,
        position          BIGINT NOT NULL,
        previous_position BIGINT NOT NULL,
        term              BIGINT NOT NULL,
        ts                BIGINT NOT NULL,
        kind              VARCHAR(24) NOT NULL,
        source_id         BIGINT NOT NULL,
        payload           LONGBLOB))
AS
DECLARE
    sequencers        BIGINT;
    seq               VARCHAR(64);
    seeded            BIGINT;
    watermark         BIGINT;
    new_rows          BIGINT;
    distinct_position BIGINT;
    distinct_previous BIGINT;
    linked            BIGINT;
    last_position     BIGINT;
    last_term         BIGINT;
BEGIN
    SELECT COUNT(DISTINCT sequencer), MIN(sequencer) INTO sequencers, seq FROM batch;
    IF sequencers = 0 THEN
        RETURN;
    END IF;
    IF sequencers > 1 THEN
        RAISE user_exception('apply_sequenced_entries: a batch holds more than one sequencer');
    END IF;

    SELECT COUNT(*), MAX(position) INTO seeded, watermark FROM sequencer_watermark WHERE sequencer = seq;
    IF seeded = 0 THEN
        RAISE user_exception(CONCAT('apply_sequenced_entries: no sequencer_watermark row for ', seq,
            '; insert one at the start of the log before starting the pipeline'));
    END IF;

    SELECT COUNT(*), COUNT(DISTINCT position), COUNT(DISTINCT previous_position), MAX(position)
        INTO new_rows, distinct_position, distinct_previous, last_position
        FROM batch WHERE position > watermark;
    IF new_rows = 0 THEN
        RETURN;
    END IF;

    SELECT COUNT(*) INTO linked
        FROM batch b
        WHERE b.position > watermark
          AND b.position > b.previous_position
          AND (b.previous_position = watermark
               OR b.previous_position IN (SELECT n.position FROM batch n WHERE n.position > watermark));

    IF distinct_position <> new_rows OR distinct_previous <> new_rows OR linked <> new_rows THEN
        RAISE user_exception(CONCAT('apply_sequenced_entries: sequencer ', seq,
            ' batch does not continue the log from watermark ', watermark,
            ' (new rows ', new_rows, ', linked ', linked, ', distinct positions ', distinct_position,
            ', distinct previous positions ', distinct_previous, ')'));
    END IF;

    INSERT INTO sequenced_log (sequencer, position, previous_position, term, ts, kind, source_id, payload)
        SELECT sequencer, position, previous_position, term, ts, kind, source_id, payload
        FROM batch WHERE position > watermark;

    SELECT MAX(term) INTO last_term FROM batch WHERE position = last_position;
    UPDATE sequencer_watermark
        SET position = last_position, term = last_term, applied_at = NOW(6)
        WHERE sequencer = seq;
END //
DELIMITER ;
