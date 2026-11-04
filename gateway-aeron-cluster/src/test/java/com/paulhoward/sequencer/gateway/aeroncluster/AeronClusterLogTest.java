package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import com.paulhoward.sequencer.gateway.SequencedLog;
import com.paulhoward.sequencer.gateway.SequencedLogProviders;
import com.paulhoward.sequencer.gateway.tck.CommittedMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks specific to cluster log positions, beyond the generic contract.
 */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class AeronClusterLogTest
{
    private final AeronClusterLogProvider provider = new AeronClusterLogProvider();

    @Test
    void isFoundByName()
    {
        assertInstanceOf(AeronClusterLogProvider.class, SequencedLogProviders.find("aeron-cluster"));
    }

    @Test
    void refusesAResumePositionThatIsNotFrameAligned()
    {
        assertThrows(SequenceIntegrityException.class, () -> provider.open(Map.of(), 7));
        assertThrows(SequenceIntegrityException.class, () -> provider.open(Map.of(), -32));
    }

    @Test
    void refusesAResumePositionInsideAnEntry()
    {
        try (AeronClusterFixture fixture = new AeronClusterFixture(22_000))
        {
            fixture.awaitLeader(Duration.ofSeconds(60));
            for (int i = 0; i < 20; i++)
            {
                assertTrue(fixture.offer(("long-message-" + "x".repeat(200) + i).getBytes(StandardCharsets.UTF_8),
                    Duration.ofSeconds(30)));
            }
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (fixture.committed().size() < 20 && System.nanoTime() < deadline)
            {
                Thread.yield();
            }

            // The entry after the 10th starts at its position; 32 bytes further on is inside that entry.
            final List<CommittedMessage> committed = fixture.committed();
            final long insideAnEntry = committed.get(9).position() + 32;

            final List<SequencedEntry> delivered = new ArrayList<>();
            assertThrows(SequenceIntegrityException.class, () ->
            {
                try (SequencedLog log = provider.open(fixture.gatewayConfig(), insideAnEntry))
                {
                    final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    while (System.nanoTime() < until)
                    {
                        if (0 == log.poll(delivered::add, 100))
                        {
                            Thread.sleep(1);
                        }
                    }
                }
            });
            assertTrue(delivered.isEmpty(), "delivered entries from a position inside an entry: " + delivered);
        }
    }
}
