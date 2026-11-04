package com.paulhoward.sequencer.gateway;

import java.util.Arrays;

/**
 * One committed entry of a sequencer's log, as delivered by a {@link SequencedLog}.
 *
 * @param position         resume token for this entry. Storing it and later passing it to
 *                         {@link SequencedLogProvider#open} resumes with the entry after this one.
 *                         Strictly increasing.
 * @param previousPosition position of the entry delivered before this one (or the resume point for the
 *                         first entry after open). Consecutive entries always chain:
 *                         {@code next.previousPosition() == this.position()}.
 * @param term             leadership term id the entry was sequenced in. Never decreases.
 * @param timestamp        sequencer timestamp of the entry.
 * @param kind             entry type, a fixed vocabulary per implementation (see each reader's docs).
 * @param sourceId         the session or service that caused the entry, or 0 when there is none.
 * @param payload          the entry's bytes, unchanged.
 */
public record SequencedEntry(
    long position,
    long previousPosition,
    long term,
    long timestamp,
    String kind,
    long sourceId,
    byte[] payload)
{
    @Override
    public boolean equals(final Object o)
    {
        return o instanceof SequencedEntry that &&
            position == that.position &&
            previousPosition == that.previousPosition &&
            term == that.term &&
            timestamp == that.timestamp &&
            sourceId == that.sourceId &&
            kind.equals(that.kind) &&
            Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(position) * 31 + Arrays.hashCode(payload);
    }

    @Override
    public String toString()
    {
        return "SequencedEntry{position=" + position + ", previousPosition=" + previousPosition +
            ", term=" + term + ", timestamp=" + timestamp + ", kind=" + kind + ", sourceId=" + sourceId +
            ", payloadLength=" + payload.length + '}';
    }
}
