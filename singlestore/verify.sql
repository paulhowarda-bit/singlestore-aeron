-- Checks that sequenced_log is one unbroken chain per sequencer and that the watermark agrees with it.
-- Run in the target database. Every check prints one row per sequencer; the expected values are in the
-- column names.

-- 1. The chain: each row's previous_position is the position of the row before it.
--    chain_breaks = 0. first_previous_position = the start of the log (0 for open-source Aeron Cluster).
SELECT sequencer,
       COUNT(*)                                    AS entries,
       SUM(previous_position <> prior_position)    AS chain_breaks,
       MIN(CASE WHEN prior_position IS NULL THEN previous_position END) AS first_previous_position,
       MAX(position)                               AS last_position
FROM (
    SELECT sequencer, position, previous_position,
           LAG(position) OVER (PARTITION BY sequencer ORDER BY position) AS prior_position
    FROM sequenced_log
) chained
GROUP BY sequencer;

-- 2. The watermark is the last row's position and term. watermark_matches = 1.
SELECT w.sequencer,
       w.position                                         AS watermark,
       l.position                                         AS last_position,
       (w.position = l.position AND w.term = l.term)      AS watermark_matches
FROM sequencer_watermark w
LEFT JOIN (SELECT sequencer, MAX(position) AS position FROM sequenced_log GROUP BY sequencer) last
       ON last.sequencer = w.sequencer
LEFT JOIN sequenced_log l
       ON l.sequencer = last.sequencer AND l.position = last.position;

-- 3. Entries by kind.
SELECT sequencer, kind, COUNT(*) AS entries
FROM sequenced_log
GROUP BY sequencer, kind
ORDER BY sequencer, kind;
