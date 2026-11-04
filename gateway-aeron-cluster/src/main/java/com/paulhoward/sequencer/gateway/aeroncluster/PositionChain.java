package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;

/**
 * Checks that every frame read from the log starts exactly where the previous one ended.
 * <p>
 * The one permitted gap is Aeron's end-of-term padding: padding frames are never delivered to a reader, so
 * when a message does not fit in the rest of a term buffer the next frame starts at the next term boundary.
 * Any other jump, backwards or forwards, is a break in the sequence.
 */
final class PositionChain
{
    private long expected;

    void reset(final long position)
    {
        expected = position;
    }

    long expected()
    {
        return expected;
    }

    void frame(final long start, final long end, final int termLength)
    {
        if (start != expected && !isPaddingToNextTerm(expected, start, termLength))
        {
            throw new SequenceIntegrityException(
                "gap in the cluster log: expected the next frame at position " + expected +
                " but received a frame at position " + start);
        }

        expected = end;
    }

    private static boolean isPaddingToNextTerm(final long expected, final long start, final int termLength)
    {
        final long offsetInTerm = expected & (termLength - 1);
        return 0 != offsetInTerm && start == expected - offsetInTerm + termLength;
    }
}
