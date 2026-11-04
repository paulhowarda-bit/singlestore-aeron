package com.paulhoward.sequencer.gateway.connect;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencerSourceTaskTest
{
    private static final String SEQUENCER = "unit-seq";

    private final List<SequencerSourceTask> tasks = new ArrayList<>();

    @BeforeEach
    void resetSequencer()
    {
        FakeSequencerProvider.reset();
    }

    @AfterEach
    void stopTasks()
    {
        tasks.forEach(SequencerSourceTask::stop);
    }

    @Test
    void readsFromTheStartOfTheLogWhenNoOffsetIsStored()
    {
        FakeSequencerProvider.append(3);

        final List<SourceRecord> records = start(null, Map.of()).poll();

        assertEquals(FakeSequencerProvider.START, FakeSequencerProvider.openedAfter);
        assertEquals(3, records.size());
        assertEquals(FakeSequencerProvider.START, value(records.get(0)).getInt64("previous_position"));
    }

    @Test
    void resumesWithTheEntryAfterTheStoredOffset()
    {
        FakeSequencerProvider.append(10);
        final SourceRecord last = drain(start(null, Map.of())).get(4);

        final List<SourceRecord> resumed = drain(start(last.sourceOffset(), Map.of()));

        assertEquals(last.sourceOffset().get(SequencerSourceTask.POSITION), FakeSequencerProvider.openedAfter);
        assertEquals(5, resumed.size());
        assertEquals(value(last).getInt64("position"), value(resumed.get(0)).getInt64("previous_position"));
    }

    @Test
    void neverReturnsMoreThanTheBatchLimitFromOnePoll()
    {
        FakeSequencerProvider.append(2_500);
        final SequencerSourceTask task = start(null, Map.of(SequencerConnectorConfig.BATCH_MAX_ENTRIES, "1000"));

        assertEquals(1_000, task.poll().size());
        assertEquals(1_000, task.poll().size());
        assertEquals(500, task.poll().size());
        assertEquals(List.of(), task.poll());
    }

    @Test
    void returnsAnEmptyListNotNullWhenNothingNewIsCommitted()
    {
        assertEquals(List.of(), start(null, Map.of()).poll());
    }

    @Test
    void eachRecordCarriesItsEntryAndItsResumePoint()
    {
        FakeSequencerProvider.append(1);
        final SequencedEntry entry = FakeSequencerProvider.LOG.get(0);

        final SourceRecord record = start(null, Map.of()).poll().get(0);
        final Struct value = value(record);

        value.validate();
        assertEquals(SEQUENCER, record.topic());
        assertEquals(0, record.kafkaPartition());
        assertNull(record.key());
        assertEquals(Map.of(SequencerSourceTask.PARTITION_KEY, SEQUENCER), record.sourcePartition());
        assertEquals(Map.of(SequencerSourceTask.POSITION, entry.position(), SequencerSourceTask.TERM, entry.term()),
            record.sourceOffset());
        assertEquals(SEQUENCER, value.getString("sequencer"));
        assertEquals(entry.position(), value.getInt64("position"));
        assertEquals(entry.previousPosition(), value.getInt64("previous_position"));
        assertEquals(entry.term(), value.getInt64("term"));
        assertEquals(entry.timestamp(), value.getInt64("timestamp"));
        assertEquals(entry.kind(), value.getString("kind"));
        assertEquals(entry.sourceId(), value.getInt64("source_id"));
        assertArrayEquals(entry.payload(), value.getBytes("payload"));
    }

    @Test
    void passesItsOtherSettingsToTheReader()
    {
        start(null, Map.of("fake.endpoint", "somewhere:1234"));

        assertEquals("somewhere:1234", FakeSequencerProvider.openedWith.get("fake.endpoint"));
    }

    @Test
    void stopsWithAConnectErrorWhenTheSequenceBreaks()
    {
        FakeSequencerProvider.append(1);
        final SequencerSourceTask task = start(null, Map.of());
        FakeSequencerProvider.pollFailure = new SequenceIntegrityException("gap in the cluster log");

        final ConnectException ex = assertThrows(ConnectException.class, task::poll);

        assertTrue(ex.getMessage().contains("gap in the cluster log"), ex.getMessage());
    }

    @Test
    void refusesAStoredOffsetWithoutAPosition()
    {
        assertThrows(ConnectException.class, () -> start(Map.of(SequencerSourceTask.TERM, 3L), Map.of()));
    }

    @Test
    void refusesAnUnknownGateway()
    {
        final ConnectException ex = assertThrows(ConnectException.class,
            () -> start(null, Map.of(SequencerConnectorConfig.GATEWAY, "no-such-sequencer")));

        assertTrue(ex.getMessage().contains(FakeSequencerProvider.NAME), ex.getMessage());
    }

    @Test
    void stopClosesTheReader()
    {
        final SequencerSourceTask task = start(null, Map.of());

        task.stop();

        assertTrue(FakeSequencerProvider.closed);
    }

    private SequencerSourceTask start(final Map<String, ?> storedOffset, final Map<String, String> overrides)
    {
        final Map<String, String> props = new HashMap<>();
        props.put(SequencerConnectorConfig.GATEWAY, FakeSequencerProvider.NAME);
        props.put(SequencerConnectorConfig.NAME, SEQUENCER);
        props.putAll(overrides);

        final SequencerSourceTask task = new SequencerSourceTask();
        task.initialize(new StoredOffsetContext(storedOffset));
        task.start(props);
        tasks.add(task);
        return task;
    }

    private static List<SourceRecord> drain(final SequencerSourceTask task)
    {
        final List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> batch;
        while (!(batch = task.poll()).isEmpty())
        {
            records.addAll(batch);
        }
        return records;
    }

    private static Struct value(final SourceRecord record)
    {
        return (Struct)record.value();
    }

    private record StoredOffsetContext(Map<String, ?> storedOffset) implements SourceTaskContext
    {
        @Override
        public Map<String, String> configs()
        {
            return Map.of();
        }

        @Override
        public OffsetStorageReader offsetStorageReader()
        {
            return new OffsetStorageReader()
            {
                @Override
                @SuppressWarnings("unchecked")
                public <T> Map<String, Object> offset(final Map<String, T> partition)
                {
                    assertEquals(Map.of(SequencerSourceTask.PARTITION_KEY, SEQUENCER), partition);
                    return (Map<String, Object>)storedOffset;
                }

                @Override
                public <T> Map<Map<String, T>, Map<String, Object>> offsets(final Collection<Map<String, T>> partitions)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
