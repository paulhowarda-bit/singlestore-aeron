package com.paulhoward.sequencer.gateway;

/**
 * Thrown when a reader cannot continue the sequence without a gap, a duplicate or an unknown entry.
 * It is never safe to catch this and carry on: the downstream copy would stop matching the sequencer.
 */
public final class SequenceIntegrityException extends RuntimeException
{
    public SequenceIntegrityException(final String message)
    {
        super(message);
    }

    public SequenceIntegrityException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
