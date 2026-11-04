package com.paulhoward.sequencer.gateway.tck;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import com.paulhoward.sequencer.gateway.SequencedLog;
import com.paulhoward.sequencer.gateway.SequencedLogProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The guarantees every {@link SequencedLog} implementation must keep. A reader implementation runs this
 * suite by extending it and supplying a fixture for its sequencer.
 * <p>
 * Each test name states the guarantee it checks. A failure means that guarantee does not hold and the
 * reader must not be deployed.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public abstract class SequencedLogContractTest
{
    protected static final Duration WAIT = Duration.ofSeconds(60);
    private static final Duration RESEND_AFTER = Duration.ofSeconds(5);

    protected SequencerFixture fixture;

    /**
     * @return a fresh, running sequencer with an empty log.
     */
    protected abstract SequencerFixture createFixture();

    /**
     * @return the provider under test.
     */
    protected abstract SequencedLogProvider provider();

    @BeforeEach
    void startSequencer()
    {
        fixture = createFixture();
        assertEquals(fixture.startOfLog(), provider().startOfLog(),
            "the provider's start of log is where the connector reads from on its first start");
        fixture.awaitLeader(WAIT);
    }

    @AfterEach
    void stopSequencer()
    {
        if (null != fixture)
        {
            fixture.close();
        }
    }

    @Test
    void deliversEveryCommittedMessageInOrderEachChainedToTheOneBefore()
    {
        submit("msg-", 0, 500);

        try (Reader reader = open(fixture.startOfLog()))
        {
            reader.readUntilCovered(fixture.committed());

            assertChained(reader.entries, fixture.startOfLog());
            assertCovers(reader.entries, fixture.committed());
        }
    }

    @Test
    void resumesWithTheEntryAfterTheGivenPosition()
    {
        submit("msg-", 0, 300);

        final List<SequencedEntry> all;
        try (Reader reader = open(fixture.startOfLog()))
        {
            reader.readUntilCovered(fixture.committed());
            all = List.copyOf(reader.entries);
        }

        final SequencedEntry last = all.get(all.size() - 1);
        for (final int k : new int[]{ 0, all.size() / 2, all.size() - 2 })
        {
            final long resumeAfter = all.get(k).position();
            try (Reader reader = open(resumeAfter))
            {
                reader.readUntil(() -> !reader.entries.isEmpty() &&
                    reader.entries.get(reader.entries.size() - 1).position() >= last.position());

                assertEquals(all.subList(k + 1, all.size()), reader.entries,
                    "resuming after position " + resumeAfter + " must deliver exactly the entries that follow it");
            }
        }
    }

    @Test
    void neverDeliversAnEntryThatIsLaterRetracted()
    {
        submit("before-", 0, 100);

        try (Reader reader = open(fixture.startOfLog()))
        {
            reader.readUntilCovered(fixture.committed());

            // Cut enough followers off the log that the leader has no majority, hand it messages it appends
            // but can never commit, then stop those followers so that the leader is the only member to read.
            final int leader = fixture.awaitLeader(WAIT);
            final List<Integer> others = otherMembers(leader);
            final int majority = fixture.memberCount() / 2 + 1;
            final List<Integer> stopped = others.subList(0, fixture.memberCount() - majority + 1);
            for (final int member : stopped)
            {
                fixture.cutOffFromLog(member);
            }

            int accepted = 0;
            for (int i = 0; i < 50; i++)
            {
                if (fixture.offer(payload("uncommitted-", i), Duration.ofMillis(200)))
                {
                    accepted++;
                }
            }
            assertTrue(accepted > 0, "the leader accepted no messages after losing its majority, so this test " +
                "proved nothing");
            sleep(500);
            for (final int member : stopped)
            {
                fixture.stopMember(member);
            }

            // A reader opened now can only read from the leader, whose log holds the uncommitted tail.
            final List<SequencedEntry> readFromLeaderAlone;
            try (Reader duringQuorumLoss = open(fixture.startOfLog()))
            {
                duringQuorumLoss.pump(Duration.ofSeconds(4));
                readFromLeaderAlone = List.copyOf(duringQuorumLoss.entries);
            }
            reader.pump(Duration.ofMillis(500));

            // The old leader leaves; the majority returns, elects a new leader and moves on without its tail.
            fixture.stopMember(leader);
            for (final int member : stopped)
            {
                fixture.startMember(member);
            }
            fixture.awaitLeader(WAIT);
            submit("after-", 0, 100);
            fixture.startMember(leader);

            reader.readUntilCovered(fixture.committed());

            assertChained(reader.entries, fixture.startOfLog());
            assertCovers(reader.entries, fixture.committed());
            assertSameAsFreshRead(reader.entries);
            assertFalse(readFromLeaderAlone.isEmpty(), "nothing was read from the leader while it had no majority");
            assertSameAsFreshRead(readFromLeaderAlone);
            for (final List<SequencedEntry> delivered : List.of(reader.entries, readFromLeaderAlone))
            {
                for (final SequencedEntry entry : delivered)
                {
                    assertFalse(text(entry.payload()).startsWith("uncommitted-") && !isCommitted(entry),
                        "delivered an entry that was never committed: " + entry);
                }
            }
        }
    }

    @Test
    void survivesLeaderFailoverWithoutGapsOrDuplicates()
    {
        try (Reader reader = open(fixture.startOfLog()))
        {
            submit("first-", 0, 200);
            reader.pump(Duration.ofMillis(500));

            final int leader = fixture.awaitLeader(WAIT);
            fixture.stopMember(leader);
            fixture.awaitLeader(WAIT);
            submit("second-", 0, 200);
            fixture.startMember(leader);

            reader.readUntilCovered(fixture.committed());

            assertChained(reader.entries, fixture.startOfLog());
            assertCovers(reader.entries, fixture.committed());
            assertSameAsFreshRead(reader.entries);
        }
    }

    @Test
    void survivesTheLossOfEachMemberInTurn()
    {
        try (Reader reader = open(fixture.startOfLog()))
        {
            for (int member = 0; member < fixture.memberCount(); member++)
            {
                submit("pre-" + member + "-", 0, 100);
                reader.pump(Duration.ofMillis(300));
                fixture.stopMember(member);
                fixture.awaitLeader(WAIT);
                submit("post-" + member + "-", 0, 100);
                reader.pump(Duration.ofMillis(300));
                fixture.startMember(member);
            }

            reader.readUntilCovered(fixture.committed());

            assertChained(reader.entries, fixture.startOfLog());
            assertCovers(reader.entries, fixture.committed());
            assertSameAsFreshRead(reader.entries);
        }
    }

    @Test
    void refusesToResumeBeyondTheEndOfTheLog()
    {
        submit("msg-", 0, 50);

        final List<CommittedMessage> committed = fixture.committed();
        final long beyond = committed.get(committed.size() - 1).position() + (1 << 20);

        final List<SequencedEntry> delivered = new ArrayList<>();
        assertThrows(SequenceIntegrityException.class, () ->
        {
            try (SequencedLog log = provider().open(fixture.gatewayConfig(), beyond))
            {
                final long deadline = System.nanoTime() + WAIT.toNanos();
                while (System.nanoTime() < deadline)
                {
                    if (0 == log.poll(delivered::add, 100))
                    {
                        Thread.sleep(1);
                    }
                }
            }
        }, "a resume point past the end of the committed log must be refused, not waited on");
        assertTrue(delivered.isEmpty(), "delivered entries before refusing: " + delivered);
    }

    // ---------------------------------------------------------------------------------------------------------

    protected final Reader open(final long resumeAfter)
    {
        return new Reader(provider().open(fixture.gatewayConfig(), resumeAfter));
    }

    /**
     * Send messages and return once every one of them is committed. A message offered to a leader that
     * fails before sequencing it is lost (normal sequencer client behaviour), so missing messages are sent
     * again; a resend that turns out to be a duplicate is harmless because the tests compare positions.
     */
    protected final void submit(final String prefix, final int from, final int count)
    {
        final List<String> missing = new ArrayList<>();
        for (int i = from; i < from + count; i++)
        {
            missing.add(prefix + i);
        }

        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (!missing.isEmpty())
        {
            for (final String text : missing)
            {
                assertTrue(fixture.offer(text.getBytes(StandardCharsets.UTF_8), WAIT),
                    "sequencer did not accept " + text);
            }

            final long resendAt = System.nanoTime() + RESEND_AFTER.toNanos();
            while (!missing.isEmpty() && System.nanoTime() < resendAt)
            {
                final Set<String> committed = new HashSet<>();
                for (final CommittedMessage message : fixture.committed())
                {
                    committed.add(text(message.payload()));
                }
                missing.removeIf(committed::contains);
                sleep(5);
            }

            if (System.nanoTime() > deadline)
            {
                fail("the sequencer did not commit " + missing.size() + " messages, first: " + missing.get(0));
            }
        }
    }

    protected final void assertSameAsFreshRead(final List<SequencedEntry> delivered)
    {
        try (Reader fresh = open(fixture.startOfLog()))
        {
            final SequencedEntry last = delivered.get(delivered.size() - 1);
            fresh.readUntil(() -> !fresh.entries.isEmpty() &&
                fresh.entries.get(fresh.entries.size() - 1).position() >= last.position());

            assertEquals(fresh.entries.subList(0, delivered.size()), delivered,
                "entries delivered while the cluster was failing must equal the committed log read afterwards");
        }
    }

    protected static void assertChained(final List<SequencedEntry> entries, final long resumeAfter)
    {
        long previous = resumeAfter;
        long term = Long.MIN_VALUE;
        for (final SequencedEntry entry : entries)
        {
            assertEquals(previous, entry.previousPosition(), "chain broken at " + entry);
            assertTrue(entry.position() > previous, "position did not advance at " + entry);
            assertTrue(entry.term() >= term, "term went backwards at " + entry);
            previous = entry.position();
            term = entry.term();
        }
    }

    protected static void assertCovers(final List<SequencedEntry> entries, final List<CommittedMessage> committed)
    {
        final Map<Long, SequencedEntry> byPosition = new HashMap<>();
        for (final SequencedEntry entry : entries)
        {
            byPosition.put(entry.position(), entry);
        }

        for (final CommittedMessage message : committed)
        {
            final SequencedEntry entry = byPosition.get(message.position());
            assertNotNull(entry, "no entry delivered at committed position " + message.position() +
                " ('" + text(message.payload()) + "')");
            assertArrayEquals(message.payload(), entry.payload(), "payload differs at " + message.position());
            assertEquals(message.sourceId(), entry.sourceId(), "source differs at " + message.position());
        }
    }

    private boolean isCommitted(final SequencedEntry entry)
    {
        return fixture.committed().stream()
            .anyMatch(m -> m.position() == entry.position() && Arrays.equals(m.payload(), entry.payload()));
    }

    private List<Integer> otherMembers(final int leader)
    {
        final List<Integer> others = new ArrayList<>();
        for (int i = 0; i < fixture.memberCount(); i++)
        {
            if (i != leader)
            {
                others.add(i);
            }
        }
        return others;
    }

    protected static byte[] payload(final String prefix, final int i)
    {
        return (prefix + i).getBytes(StandardCharsets.UTF_8);
    }

    protected static String text(final byte[] bytes)
    {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    protected static void awaitCondition(final BooleanSupplier condition, final String what)
    {
        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
            {
                fail("timed out waiting for " + what);
            }
            sleep(5);
        }
    }

    private static void sleep(final long ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Wraps a reader under test and keeps everything it delivers.
     */
    protected final class Reader implements AutoCloseable
    {
        final SequencedLog log;
        final List<SequencedEntry> entries = new ArrayList<>();

        Reader(final SequencedLog log)
        {
            this.log = log;
        }

        void pump(final Duration duration)
        {
            final long deadline = System.nanoTime() + duration.toNanos();
            while (System.nanoTime() < deadline)
            {
                if (0 == log.poll(entries::add, 1000))
                {
                    sleep(1);
                }
            }
        }

        void readUntil(final BooleanSupplier done)
        {
            final long deadline = System.nanoTime() + WAIT.toNanos();
            while (!done.getAsBoolean())
            {
                if (System.nanoTime() > deadline)
                {
                    fail("reader stalled: delivered " + entries.size() + " entries, last=" +
                        (entries.isEmpty() ? "none" : entries.get(entries.size() - 1)));
                }
                if (0 == log.poll(entries::add, 1000))
                {
                    sleep(1);
                }
            }
        }

        void readUntilCovered(final List<CommittedMessage> committed)
        {
            final long target = committed.get(committed.size() - 1).position();
            readUntil(() -> !entries.isEmpty() && entries.get(entries.size() - 1).position() >= target);
        }

        @Override
        public void close()
        {
            log.close();
        }
    }
}
