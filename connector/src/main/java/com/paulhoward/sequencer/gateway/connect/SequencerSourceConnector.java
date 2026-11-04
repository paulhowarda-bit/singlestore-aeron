package com.paulhoward.sequencer.gateway.connect;

import com.paulhoward.sequencer.gateway.SequencedLogProviders;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sequencer gateway: a Kafka Connect source connector that delivers a sequencer's committed log, in order,
 * with no gaps. It runs unchanged in SingleStore Connector Pipelines and in any Kafka Connect worker.
 */
public final class SequencerSourceConnector extends SourceConnector
{
    private Map<String, String> props;

    @Override
    public String version()
    {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props)
    {
        final SequencerConnectorConfig config = new SequencerConnectorConfig(props);
        try
        {
            SequencedLogProviders.find(config.gateway());
        }
        catch (final IllegalArgumentException ex)
        {
            throw new ConnectException(ex.getMessage(), ex);
        }
        this.props = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass()
    {
        return SequencerSourceTask.class;
    }

    /**
     * Always one task, whatever {@code tasks.max} says: the log is one totally ordered sequence, and splitting
     * it between tasks would lose that order.
     */
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks)
    {
        return List.of(props);
    }

    @Override
    public void stop()
    {
        // Nothing to release: the task owns the reader.
    }

    @Override
    public ConfigDef config()
    {
        return SequencerConnectorConfig.DEFINITION;
    }
}
