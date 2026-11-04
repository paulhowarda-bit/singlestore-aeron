package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedLog;
import com.paulhoward.sequencer.gateway.SequencedLogProvider;
import io.aeron.logbuffer.FrameDescriptor;

import java.util.Map;

/**
 * {@code sequencer.gateway=aeron-cluster}: reads the committed log of an open-source Aeron Cluster.
 * Positions are cluster log positions; 0 is the start of the log.
 */
public final class AeronClusterLogProvider implements SequencedLogProvider
{
    public static final String NAME = "aeron-cluster";

    @Override
    public String name()
    {
        return NAME;
    }

    /**
     * Cluster log positions start at zero and the open-source cluster never purges its log.
     */
    @Override
    public long startOfLog()
    {
        return 0;
    }

    @Override
    public SequencedLog open(final Map<String, String> config, final long resumeAfter)
    {
        if (resumeAfter < 0 || 0 != (resumeAfter & (FrameDescriptor.FRAME_ALIGNMENT - 1)))
        {
            throw new SequenceIntegrityException("resume position " + resumeAfter + " is not a cluster log " +
                "position: it must be zero or a positive multiple of " + FrameDescriptor.FRAME_ALIGNMENT);
        }

        return new AeronClusterLog(AeronClusterSettings.from(config), resumeAfter);
    }
}
