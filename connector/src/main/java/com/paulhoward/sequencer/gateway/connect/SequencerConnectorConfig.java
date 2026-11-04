package com.paulhoward.sequencer.gateway.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * The connector's own settings. Every other key is passed through untouched to the selected
 * {@link com.paulhoward.sequencer.gateway.SequencedLogProvider}, which reads its own.
 */
public final class SequencerConnectorConfig extends AbstractConfig
{
    public static final String GATEWAY = "sequencer.gateway";
    public static final String NAME = "sequencer.name";
    public static final String BATCH_MAX_ENTRIES = "batch.max.entries";

    static final ConfigDef DEFINITION = new ConfigDef()
        .define(GATEWAY, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH,
            "Which sequencer to read: aeron-cluster for open-source Aeron Cluster, aeron-sequencer for the " +
            "Aeron Sequencer. Selects the reader of that name; startup fails if it is not in the plugin.")
        .define(NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), Importance.HIGH,
            "Logical id of the sequencer. Used as the source partition, the topic and the sequencer column, " +
            "so it must stay the same for as long as the stored offsets are to be resumed from.")
        .define(BATCH_MAX_ENTRIES, Type.INT, 1000, ConfigDef.Range.atLeast(1), Importance.MEDIUM,
            "Most entries a single poll() returns.");

    public SequencerConnectorConfig(final Map<String, String> props)
    {
        super(DEFINITION, props);
    }

    String gateway()
    {
        return getString(GATEWAY);
    }

    String sequencerName()
    {
        return getString(NAME);
    }

    int batchMaxEntries()
    {
        return getInt(BATCH_MAX_ENTRIES);
    }

    Map<String, String> providerConfig()
    {
        return originalsStrings();
    }
}
