package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PositionChainTest
{
    private static final int TERM = 64 * 1024;

    private final PositionChain chain = new PositionChain();

    @Test
    void acceptsFramesThatFollowEachOther()
    {
        chain.reset(0);
        chain.frame(0, 96, TERM);
        chain.frame(96, 160, TERM);
        assertEquals(160, chain.expected());
    }

    @Test
    void refusesAGapInsideATerm()
    {
        chain.reset(96);
        assertThrows(SequenceIntegrityException.class, () -> chain.frame(128, 192, TERM));
    }

    @Test
    void refusesAFrameThatGoesBackwards()
    {
        chain.reset(160);
        assertThrows(SequenceIntegrityException.class, () -> chain.frame(96, 160, TERM));
    }

    @Test
    void acceptsEndOfTermPaddingUpToTheNextTermBoundary()
    {
        chain.reset(TERM - 96);
        assertDoesNotThrow(() -> chain.frame(TERM, TERM + 64, TERM));
    }

    @Test
    void refusesAJumpPastTheNextTermBoundary()
    {
        chain.reset(TERM - 96);
        assertThrows(SequenceIntegrityException.class, () -> chain.frame(2L * TERM, 2L * TERM + 64, TERM));
    }

    @Test
    void refusesAWholeMissingTermWhenAlreadyOnABoundary()
    {
        chain.reset(TERM);
        assertThrows(SequenceIntegrityException.class, () -> chain.frame(2L * TERM, 2L * TERM + 64, TERM));
    }
}
