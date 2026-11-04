package com.paulhoward.sequencer.gateway;

import java.util.Map;

/**
 * Creates {@link SequencedLog} readers for one kind of sequencer. Implementations are found with
 * {@link java.util.ServiceLoader}, registered in
 * {@code META-INF/services/com.paulhoward.sequencer.gateway.SequencedLogProvider}.
 */
public interface SequencedLogProvider
{
    /**
     * @return the name selected by the connector setting {@code sequencer.gateway},
     * for example {@code aeron-cluster}.
     */
    String name();

    /**
     * @return the resume position that means "read from the beginning of the log", used when nothing has
     * been applied downstream yet.
     */
    long startOfLog();

    /**
     * Open a reader that delivers the committed entries after {@code resumeAfter}.
     *
     * @param config      connector configuration; each implementation reads its own keys.
     * @param resumeAfter position of the last entry already applied downstream, or the implementation's
     *                    start-of-log position to read from the beginning.
     * @return a reader; the caller closes it.
     * @throws SequenceIntegrityException if {@code resumeAfter} can never be a valid resume point.
     */
    SequencedLog open(Map<String, String> config, long resumeAfter);
}
