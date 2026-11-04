package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.codecs.TimerEventEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterLogDecoderTest
{
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int POSITION_BITS = Integer.numberOfTrailingZeros(TERM_LENGTH);

    private final List<SequencedEntry> entries = new ArrayList<>();
    private final ClusterLogDecoder decoder = new ClusterLogDecoder(entries::add);
    private final Header header = new Header(0, POSITION_BITS);

    @Test
    void decodesASessionMessageAsPositionedPayload()
    {
        decoder.resumeFrom(0);
        final long end = frame(0, FrameDescriptor.UNFRAGMENTED, sessionMessage(3, 42, 1_000, "hello"));

        assertEquals(1, entries.size());
        final SequencedEntry entry = entries.get(0);
        assertEquals(end, entry.position());
        assertEquals(0, entry.previousPosition());
        assertEquals(3, entry.term());
        assertEquals(1_000, entry.timestamp());
        assertEquals(ClusterLogDecoder.SESSION_MESSAGE, entry.kind());
        assertEquals(42, entry.sourceId());
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), entry.payload());
    }

    @Test
    void chainsEachEntryToTheOneBefore()
    {
        decoder.resumeFrom(0);
        final long first = frame(0, FrameDescriptor.UNFRAGMENTED, sessionMessage(1, 7, 1, "a"));
        final long second = frame(first, FrameDescriptor.UNFRAGMENTED, sessionMessage(1, 7, 2, "b"));

        assertEquals(first, entries.get(1).previousPosition());
        assertEquals(second, entries.get(1).position());
        assertEquals(second, decoder.lastEntryPosition());
    }

    @Test
    void reassemblesAFragmentedMessage()
    {
        final byte[] message = sessionMessage(1, 7, 1, "x".repeat(200));
        final byte[] firstPart = Arrays.copyOfRange(message, 0, 100);
        final byte[] secondPart = Arrays.copyOfRange(message, 100, message.length);

        decoder.resumeFrom(0);
        final long mid = frame(0, FrameDescriptor.BEGIN_FRAG_FLAG, firstPart);
        assertEquals(0, entries.size());
        final long end = frame(mid, FrameDescriptor.END_FRAG_FLAG, secondPart);

        assertEquals(1, entries.size());
        assertEquals(end, entries.get(0).position());
        assertArrayEquals("x".repeat(200).getBytes(StandardCharsets.UTF_8), entries.get(0).payload());
    }

    @Test
    void keepsTheWholeEventAsPayloadForNonMessageEntries()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        new TimerEventEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .leadershipTermId(2).correlationId(99).timestamp(5_000);
        final byte[] event = Arrays.copyOf(buffer.byteArray(),
            MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH);

        decoder.resumeFrom(0);
        frame(0, FrameDescriptor.UNFRAGMENTED, event);

        final SequencedEntry entry = entries.get(0);
        assertEquals(ClusterLogDecoder.TIMER, entry.kind());
        assertEquals(2, entry.term());
        assertEquals(5_000, entry.timestamp());
        assertEquals(0, entry.sourceId());
        assertArrayEquals(event, entry.payload());
    }

    @Test
    void refusesAnUnknownSchema()
    {
        final byte[] message = sessionMessage(1, 7, 1, "a");
        new UnsafeBuffer(message).putShort(4, (short)999);

        decoder.resumeFrom(0);
        assertThrows(SequenceIntegrityException.class, () -> frame(0, FrameDescriptor.UNFRAGMENTED, message));
    }

    @Test
    void refusesAnUnknownTemplate()
    {
        final byte[] message = sessionMessage(1, 7, 1, "a");
        new UnsafeBuffer(message).putShort(2, (short)9_999);

        decoder.resumeFrom(0);
        assertThrows(SequenceIntegrityException.class, () -> frame(0, FrameDescriptor.UNFRAGMENTED, message));
    }

    @Test
    void refusesALeadershipTermThatGoesBackwards()
    {
        decoder.resumeFrom(0);
        final long first = frame(0, FrameDescriptor.UNFRAGMENTED, sessionMessage(5, 7, 1, "a"));
        assertThrows(SequenceIntegrityException.class,
            () -> frame(first, FrameDescriptor.UNFRAGMENTED, sessionMessage(4, 7, 2, "b")));
    }

    @Test
    void refusesAGapBetweenEntries()
    {
        decoder.resumeFrom(0);
        final long first = frame(0, FrameDescriptor.UNFRAGMENTED, sessionMessage(1, 7, 1, "a"));
        assertThrows(SequenceIntegrityException.class,
            () -> frame(first + FrameDescriptor.FRAME_ALIGNMENT, FrameDescriptor.UNFRAGMENTED,
                sessionMessage(1, 7, 2, "b")));
    }

    @Test
    void refusesAContinuationWithoutItsStart()
    {
        decoder.resumeFrom(0);
        assertThrows(SequenceIntegrityException.class,
            () -> frame(0, FrameDescriptor.END_FRAG_FLAG, sessionMessage(1, 7, 1, "a")));
    }

    private static byte[] sessionMessage(final long term, final long sessionId, final long timestamp,
        final String text)
    {
        final byte[] payload = text.getBytes(StandardCharsets.UTF_8);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32 + payload.length]);
        new SessionMessageHeaderEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .leadershipTermId(term).clusterSessionId(sessionId).timestamp(timestamp);
        buffer.putBytes(32, payload);
        return buffer.byteArray();
    }

    /**
     * Deliver one frame starting at {@code position}, the way a replay image would.
     *
     * @return the position at the end of the frame.
     */
    private long frame(final long position, final byte flags, final byte[] message)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[DataHeaderFlyweight.HEADER_LENGTH + message.length]);
        final DataHeaderFlyweight frame = new DataHeaderFlyweight(buffer);
        frame.frameLength(DataHeaderFlyweight.HEADER_LENGTH + message.length);
        frame.version(HeaderFlyweight.CURRENT_VERSION);
        frame.flags((short)(flags & 0xFF));
        frame.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        frame.termOffset((int)(position & (TERM_LENGTH - 1)));
        frame.sessionId(1);
        frame.streamId(100);
        frame.termId((int)(position >> POSITION_BITS));
        buffer.putBytes(DataHeaderFlyweight.HEADER_LENGTH, message);

        header.buffer(buffer);
        header.offset(0);
        decoder.onFragment(buffer, DataHeaderFlyweight.HEADER_LENGTH, message.length, header);
        return position + BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH + message.length,
            FrameDescriptor.FRAME_ALIGNMENT);
    }
}
