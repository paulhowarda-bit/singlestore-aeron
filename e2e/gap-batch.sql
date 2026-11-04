-- Test helper: hands apply_sequenced_entries a batch with a gap, as a pipeline would. The batch continues
-- from the given watermark, then skips the entry between position wm + 100 and wm + 200.
-- (A QUERY argument cannot be built in a plain CALL: SingleStore rejects TO_QUERY there.)

DELIMITER //
CREATE OR REPLACE PROCEDURE e2e_apply_gapped_batch(wm BIGINT) AS
DECLARE
    gapped QUERY(
        sequencer         VARCHAR(64) NOT NULL,
        position          BIGINT NOT NULL,
        previous_position BIGINT NOT NULL,
        term              BIGINT NOT NULL,
        ts                BIGINT NOT NULL,
        kind              VARCHAR(24) NOT NULL,
        source_id         BIGINT NOT NULL,
        payload           LONGBLOB) =
        SELECT 'oss-dev', wm + 100, wm, 0, 0, 'SESSION_MESSAGE', 0, 'a'
        UNION ALL
        SELECT 'oss-dev', wm + 300, wm + 200, 0, 0, 'SESSION_MESSAGE', 0, 'c';
BEGIN
    CALL apply_sequenced_entries(gapped);
END //
DELIMITER ;
