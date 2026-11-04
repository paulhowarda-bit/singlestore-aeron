package com.paulhoward.sequencer.gateway.connect;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequencerSourceConnectorTest
{
    private static final Map<String, String> PROPS = Map.of(
        SequencerConnectorConfig.GATEWAY, FakeSequencerProvider.NAME,
        SequencerConnectorConfig.NAME, "unit-seq",
        "tasks.max", "4");

    @Test
    void runsExactlyOneTaskWhateverTasksMaxSays()
    {
        final SequencerSourceConnector connector = new SequencerSourceConnector();
        connector.start(PROPS);

        final List<Map<String, String>> configs = connector.taskConfigs(4);

        assertEquals(List.of(PROPS), configs);
        assertEquals(SequencerSourceTask.class, connector.taskClass());
    }

    @Test
    void refusesToStartWithoutASequencerName()
    {
        assertThrows(ConfigException.class, () -> new SequencerSourceConnector().start(
            Map.of(SequencerConnectorConfig.GATEWAY, FakeSequencerProvider.NAME)));
    }

    @Test
    void refusesToStartWithAnUnknownGateway()
    {
        final ConnectException ex = assertThrows(ConnectException.class, () -> new SequencerSourceConnector().start(
            Map.of(SequencerConnectorConfig.GATEWAY, "no-such-sequencer", SequencerConnectorConfig.NAME, "x")));

        assertTrue(ex.getMessage().contains("no-such-sequencer"), ex.getMessage());
    }

    @Test
    void refusesABatchLimitBelowOne()
    {
        assertThrows(ConfigException.class, () -> new SequencerSourceConnector().start(Map.of(
            SequencerConnectorConfig.GATEWAY, FakeSequencerProvider.NAME,
            SequencerConnectorConfig.NAME, "x",
            SequencerConnectorConfig.BATCH_MAX_ENTRIES, "0")));
    }

    @Test
    void reportsTheBuildVersion()
    {
        final String version = new SequencerSourceConnector().version();

        assertTrue(version.matches("\\d+\\.\\d+\\.\\d+.*"), version);
    }
}
