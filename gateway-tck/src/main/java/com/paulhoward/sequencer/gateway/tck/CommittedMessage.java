package com.paulhoward.sequencer.gateway.tck;

/**
 * A message as the sequencer's application processed it.
 *
 * @param position the log position the application saw for it; the reader must deliver an entry with this
 *                 position.
 * @param sourceId the session that sent it.
 * @param payload  the message bytes.
 */
public record CommittedMessage(long position, long sourceId, byte[] payload)
{
}
