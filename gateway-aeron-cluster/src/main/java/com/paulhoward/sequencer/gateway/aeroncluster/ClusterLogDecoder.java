package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import io.aeron.BufferBuilder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.ClusterActionRequestDecoder;
import io.aeron.cluster.codecs.MembershipChangeEventDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventDecoder;
import io.aeron.cluster.codecs.SessionCloseEventDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionOpenEventDecoder;
import io.aeron.cluster.codecs.TimerEventDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.function.Consumer;

/**
 * Turns raw fragments of a replayed cluster log into {@link SequencedEntry} values.
 * <p>
 * Every log entry becomes one {@code SequencedEntry}; nothing is skipped. The {@code kind} values are:
 * <ul>
 *     <li>{@code SESSION_MESSAGE}: a client or service message. {@code sourceId} is the cluster session id
 *     (negative for messages a clustered service sent itself); {@code payload} is the application bytes.</li>
 *     <li>{@code SESSION_OPEN}, {@code SESSION_CLOSE}: {@code sourceId} is the cluster session id.</li>
 *     <li>{@code TIMER}, {@code CLUSTER_ACTION}, {@code NEW_TERM}, {@code MEMBERSHIP_CHANGE}: {@code sourceId}
 *     is 0.</li>
 * </ul>
 * For every kind except {@code SESSION_MESSAGE} the payload is the complete SBE-encoded log event, so no
 * field is lost. A schema or template this decoder does not know is a {@link SequenceIntegrityException}.
 */
final class ClusterLogDecoder implements FragmentHandler
{
    static final String SESSION_MESSAGE = "SESSION_MESSAGE";
    static final String SESSION_OPEN = "SESSION_OPEN";
    static final String SESSION_CLOSE = "SESSION_CLOSE";
    static final String TIMER = "TIMER";
    static final String CLUSTER_ACTION = "CLUSTER_ACTION";
    static final String NEW_TERM = "NEW_TERM";
    static final String MEMBERSHIP_CHANGE = "MEMBERSHIP_CHANGE";

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessageDecoder = new SessionMessageHeaderDecoder();
    private final TimerEventDecoder timerDecoder = new TimerEventDecoder();
    private final SessionOpenEventDecoder openDecoder = new SessionOpenEventDecoder();
    private final SessionCloseEventDecoder closeDecoder = new SessionCloseEventDecoder();
    private final ClusterActionRequestDecoder actionDecoder = new ClusterActionRequestDecoder();
    private final NewLeadershipTermEventDecoder newTermDecoder = new NewLeadershipTermEventDecoder();
    private final MembershipChangeEventDecoder membershipDecoder = new MembershipChangeEventDecoder();

    private final PositionChain chain = new PositionChain();
    private final BufferBuilder builder = new BufferBuilder();
    private final Consumer<SequencedEntry> sink;

    private long lastEntryPosition;
    private long lastTerm = Long.MIN_VALUE;

    ClusterLogDecoder(final Consumer<SequencedEntry> sink)
    {
        this.sink = sink;
    }

    /**
     * Prepare to read the log from {@code position}, the end of the last entry already delivered.
     *
     * @param position resume point.
     */
    void resumeFrom(final long position)
    {
        chain.reset(position);
        builder.reset();
        lastEntryPosition = position;
    }

    long lastEntryPosition()
    {
        return lastEntryPosition;
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long end = header.position();
        final long start = end - BitUtil.align(header.frameLength(), FrameDescriptor.FRAME_ALIGNMENT);
        chain.frame(start, end, 1 << header.positionBitsToShift());

        final byte flags = header.flags();
        if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED)
        {
            if (builder.limit() != 0)
            {
                throw new SequenceIntegrityException("message at position " + start +
                    " started before the previous fragmented message ended");
            }
            decode(buffer, offset, length, end);
        }
        else if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) == FrameDescriptor.BEGIN_FRAG_FLAG)
        {
            if (builder.limit() != 0)
            {
                throw new SequenceIntegrityException("fragmented message at position " + start +
                    " started before the previous fragmented message ended");
            }
            builder.append(buffer, offset, length);
        }
        else
        {
            if (builder.limit() == 0)
            {
                throw new SequenceIntegrityException("continuation fragment at position " + start +
                    " without the start of its message");
            }
            builder.append(buffer, offset, length);
            if ((flags & FrameDescriptor.END_FRAG_FLAG) == FrameDescriptor.END_FRAG_FLAG)
            {
                decode(builder.buffer(), 0, builder.limit(), end);
                builder.reset();
            }
        }
    }

    private void decode(final DirectBuffer buffer, final int offset, final int length, final long position)
    {
        headerDecoder.wrap(buffer, offset);
        final int schemaId = headerDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new SequenceIntegrityException("unknown schema id " + schemaId + " (template " +
                headerDecoder.templateId() + ") at log position " + position);
        }

        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = headerDecoder.blockLength();
        final int version = headerDecoder.version();
        final int templateId = headerDecoder.templateId();

        final String kind;
        final long term;
        final long timestamp;
        final long sourceId;
        final byte[] payload;

        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID ->
            {
                sessionMessageDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = SESSION_MESSAGE;
                term = sessionMessageDecoder.leadershipTermId();
                timestamp = sessionMessageDecoder.timestamp();
                sourceId = sessionMessageDecoder.clusterSessionId();
                payload = new byte[length - AeronCluster.SESSION_HEADER_LENGTH];
                buffer.getBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, payload);
            }
            case TimerEventDecoder.TEMPLATE_ID ->
            {
                timerDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = TIMER;
                term = timerDecoder.leadershipTermId();
                timestamp = timerDecoder.timestamp();
                sourceId = 0;
                payload = copy(buffer, offset, length);
            }
            case SessionOpenEventDecoder.TEMPLATE_ID ->
            {
                openDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = SESSION_OPEN;
                term = openDecoder.leadershipTermId();
                timestamp = openDecoder.timestamp();
                sourceId = openDecoder.clusterSessionId();
                payload = copy(buffer, offset, length);
            }
            case SessionCloseEventDecoder.TEMPLATE_ID ->
            {
                closeDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = SESSION_CLOSE;
                term = closeDecoder.leadershipTermId();
                timestamp = closeDecoder.timestamp();
                sourceId = closeDecoder.clusterSessionId();
                payload = copy(buffer, offset, length);
            }
            case ClusterActionRequestDecoder.TEMPLATE_ID ->
            {
                actionDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = CLUSTER_ACTION;
                term = actionDecoder.leadershipTermId();
                timestamp = actionDecoder.timestamp();
                sourceId = 0;
                payload = copy(buffer, offset, length);
            }
            case NewLeadershipTermEventDecoder.TEMPLATE_ID ->
            {
                newTermDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = NEW_TERM;
                term = newTermDecoder.leadershipTermId();
                timestamp = newTermDecoder.timestamp();
                sourceId = 0;
                payload = copy(buffer, offset, length);
            }
            case MembershipChangeEventDecoder.TEMPLATE_ID ->
            {
                membershipDecoder.wrap(buffer, bodyOffset, blockLength, version);
                kind = MEMBERSHIP_CHANGE;
                term = membershipDecoder.leadershipTermId();
                timestamp = membershipDecoder.timestamp();
                sourceId = 0;
                payload = copy(buffer, offset, length);
            }
            default -> throw new SequenceIntegrityException(
                "unknown cluster log template id " + templateId + " at log position " + position);
        }

        if (term < lastTerm)
        {
            throw new SequenceIntegrityException("leadership term went backwards from " + lastTerm + " to " + term +
                " at log position " + position);
        }

        final SequencedEntry entry = new SequencedEntry(
            position, lastEntryPosition, term, timestamp, kind, sourceId, payload);
        lastTerm = term;
        lastEntryPosition = position;
        sink.accept(entry);
    }

    private static byte[] copy(final DirectBuffer buffer, final int offset, final int length)
    {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }
}
