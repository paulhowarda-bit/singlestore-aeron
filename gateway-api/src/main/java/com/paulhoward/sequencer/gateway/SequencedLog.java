package com.paulhoward.sequencer.gateway;

import java.util.function.Consumer;

/**
 * A reader positioned on a sequencer's committed log.
 * <p>
 * Contract every implementation must keep (checked by the gateway TCK):
 * <ul>
 *     <li>Only committed entries are delivered: an entry, once delivered, is never retracted.</li>
 *     <li>Entries arrive in sequence order, each chained to the one before it. Nothing is skipped.</li>
 *     <li>If continuity cannot be guaranteed, {@link SequenceIntegrityException} is thrown instead.</li>
 * </ul>
 */
public interface SequencedLog extends AutoCloseable
{
    /**
     * Deliver the committed entries that are ready, without blocking.
     *
     * @param handler    receives each entry, in order.
     * @param maxEntries upper bound on the entries delivered by this call.
     * @return the number of entries delivered, 0 when nothing is ready.
     * @throws SequenceIntegrityException if the log cannot be continued without a gap.
     */
    int poll(Consumer<SequencedEntry> handler, int maxEntries);

    /**
     * Release every resource. Safe to call more than once and from another thread than {@link #poll}.
     */
    @Override
    void close();
}
