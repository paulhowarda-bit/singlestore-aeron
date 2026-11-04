package com.paulhoward.sequencer.gateway.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paulhoward.sequencer.gateway.aeroncluster.AeronClusterFixture;
import com.paulhoward.sequencer.gateway.tck.CommittedMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The connector in a real Kafka Connect 3.9.1 worker, reading a real three-member Aeron Cluster, all in
 * this JVM. Shows the committed log arriving in order as one unbroken chain, and a stopped connector
 * resuming from its stored offset without a gap.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class SequencerConnectorIntegrationTest
{
    private static final String CONNECTOR = "sequencer-gateway";
    private static final String SEQUENCER = "it-seq";
    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final ObjectMapper JSON = new ObjectMapper();

    private AeronClusterFixture cluster;
    private EmbeddedConnectCluster connect;

    @BeforeEach
    void start()
    {
        cluster = new AeronClusterFixture(24_000);
        cluster.awaitLeader(WAIT);

        connect = new EmbeddedConnectCluster.Builder()
            .name("sequencer-gateway-it")
            .numWorkers(1)
            .numBrokers(1)
            .workerProps(new HashMap<>(Map.of("offset.flush.interval.ms", "500")))
            .build();
        connect.start();
        connect.kafka().createTopic(SEQUENCER, 1);
    }

    @AfterEach
    void stop()
    {
        if (null != connect)
        {
            connect.stop();
        }
        if (null != cluster)
        {
            cluster.close();
        }
    }

    @Test
    void deliversTheCommittedLogInOrderAndResumesFromTheStoredOffset() throws InterruptedException
    {
        commit("first-", 200);
        connect.configureConnector(CONNECTOR, connectorConfig());
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1,
            "the connector must run exactly one task even though tasks.max is 4");

        final List<Row> firstRun = readUntilCovered(cluster.committed());
        assertChain(firstRun, 0);
        assertCovers(firstRun, cluster.committed());

        connect.stopConnector(CONNECTOR);
        connect.assertions().assertConnectorIsStopped(CONNECTOR, "the connector did not stop");
        final long stored = storedPosition();
        final List<Row> beforeResume = readToEnd();
        assertTrue(beforeResume.stream().anyMatch(row -> row.position() == stored),
            "the stored offset " + stored + " is not a position the connector delivered");

        commit("second-", 200);
        connect.resumeConnector(CONNECTOR);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR, 1, "the connector did not resume");

        final List<Row> all = readUntilCovered(cluster.committed());
        final List<Row> afterResume = all.subList(beforeResume.size(), all.size());
        assertEquals(stored, afterResume.get(0).previousPosition(),
            "the resumed connector must continue with the entry after its stored offset");

        // At-least-once: anything delivered twice must be an exact copy; the rest must extend the chain.
        final long lastBefore = beforeResume.get(beforeResume.size() - 1).position();
        final List<Row> log = new ArrayList<>(beforeResume);
        for (final Row row : afterResume)
        {
            if (row.position() <= lastBefore)
            {
                assertTrue(beforeResume.stream().anyMatch(row::sameEntryAs), "a redelivered entry differs: " + row);
            }
            else
            {
                log.add(row);
            }
        }
        assertChain(log, 0);
        assertCovers(log, cluster.committed());
        System.out.printf("delivered %d entries; %d redelivered after the resume%n",
            log.size(), afterResume.size() - (log.size() - beforeResume.size()));
    }

    private Map<String, String> connectorConfig()
    {
        final Map<String, String> config = new HashMap<>(cluster.gatewayConfig());
        config.put("connector.class", SequencerSourceConnector.class.getName());
        config.put("tasks.max", "4");
        config.put(SequencerConnectorConfig.NAME, SEQUENCER);
        config.put("value.converter", JsonConverter.class.getName());
        config.put("value.converter.schemas.enable", "false");
        return config;
    }

    private long storedPosition()
    {
        final List<ConnectorOffset> offsets = connect.connectorOffsets(CONNECTOR).offsets();
        assertEquals(1, offsets.size(), "expected one source partition: " + offsets);
        assertEquals(Map.of(SequencerSourceTask.PARTITION_KEY, SEQUENCER), offsets.get(0).partition());
        return ((Number)offsets.get(0).offset().get(SequencerSourceTask.POSITION)).longValue();
    }

    /**
     * Offer each message and wait until the cluster's service has processed it, resending any that a
     * leader change lost.
     */
    private void commit(final String prefix, final int count)
    {
        final Set<String> missing = new HashSet<>();
        for (int i = 0; i < count; i++)
        {
            missing.add(prefix + i);
        }

        final long deadline = System.nanoTime() + WAIT.toNanos();
        while (!missing.isEmpty())
        {
            for (final String text : missing)
            {
                cluster.offer(text.getBytes(StandardCharsets.UTF_8), WAIT);
            }

            final long resendAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!missing.isEmpty() && System.nanoTime() < resendAt)
            {
                for (final CommittedMessage message : cluster.committed())
                {
                    missing.remove(new String(message.payload(), StandardCharsets.UTF_8));
                }
                sleep(20);
            }
            if (System.nanoTime() > deadline)
            {
                fail("the cluster did not commit " + missing.size() + " messages, e.g. " + missing.iterator().next());
            }
        }
    }

    private List<Row> readUntilCovered(final List<CommittedMessage> committed)
    {
        final long deadline = System.nanoTime() + WAIT.toNanos();
        List<Row> rows;
        while (!covers(rows = readToEnd(), committed))
        {
            if (System.nanoTime() > deadline)
            {
                fail("the connector delivered " + rows.size() + " records, not the whole committed log");
            }
            sleep(200);
        }
        return rows;
    }

    private List<Row> readToEnd()
    {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final TopicPartition partition = new TopicPartition(SEQUENCER, 0);
        final List<Row> rows = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props))
        {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            final long end = consumer.endOffsets(List.of(partition)).get(partition);
            while (consumer.position(partition) < end)
            {
                for (final ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200)))
                {
                    rows.add(Row.parse(record.value()));
                }
            }
        }
        return rows;
    }

    private static boolean covers(final List<Row> rows, final List<CommittedMessage> committed)
    {
        final Set<Long> positions = new HashSet<>();
        rows.forEach(row -> positions.add(row.position()));
        return committed.stream().allMatch(message -> positions.contains(message.position()));
    }

    private static void assertChain(final List<Row> rows, final long startOfLog)
    {
        long previous = startOfLog;
        for (final Row row : rows)
        {
            assertEquals(SEQUENCER, row.sequencer());
            assertEquals(previous, row.previousPosition(), "break in the chain at " + row);
            assertTrue(row.position() > row.previousPosition(), "position did not advance at " + row);
            previous = row.position();
        }
    }

    private static void assertCovers(final List<Row> rows, final List<CommittedMessage> committed)
    {
        final Map<Long, Row> byPosition = new HashMap<>();
        rows.forEach(row -> byPosition.put(row.position(), row));
        for (final CommittedMessage message : committed)
        {
            final Row row = byPosition.get(message.position());
            assertTrue(null != row, "no record at committed position " + message.position());
            assertEquals("SESSION_MESSAGE", row.kind());
            assertEquals(message.sourceId(), row.sourceId());
            assertTrue(Arrays.equals(message.payload(), row.payload()), "payload differs at " + row);
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

    private record Row(
        String sequencer, long position, long previousPosition, long term, String kind, long sourceId, byte[] payload)
    {
        static Row parse(final byte[] json)
        {
            try
            {
                final JsonNode node = JSON.readTree(json);
                return new Row(
                    node.get("sequencer").asText(),
                    node.get("position").asLong(),
                    node.get("previous_position").asLong(),
                    node.get("term").asLong(),
                    node.get("kind").asText(),
                    node.get("source_id").asLong(),
                    node.get("payload").binaryValue());
            }
            catch (final IOException ex)
            {
                throw new UncheckedIOException(ex);
            }
        }

        boolean sameEntryAs(final Row other)
        {
            return position == other.position && previousPosition == other.previousPosition &&
                term == other.term && kind.equals(other.kind) && sourceId == other.sourceId &&
                sequencer.equals(other.sequencer) && Arrays.equals(payload, other.payload);
        }

        @Override
        public String toString()
        {
            return "Row{position=" + position + ", previousPosition=" + previousPosition + ", term=" + term +
                ", kind=" + kind + ", sourceId=" + sourceId + ", payload=" +
                new String(payload, StandardCharsets.UTF_8) + "}";
        }
    }
}
