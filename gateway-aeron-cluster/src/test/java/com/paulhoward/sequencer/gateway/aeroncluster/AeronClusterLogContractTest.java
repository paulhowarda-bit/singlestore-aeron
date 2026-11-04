package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequencedLogProvider;
import com.paulhoward.sequencer.gateway.tck.SequencedLogContractTest;
import com.paulhoward.sequencer.gateway.tck.SequencerFixture;

/**
 * Runs the gateway contract against an in-process three-member open-source Aeron Cluster.
 */
class AeronClusterLogContractTest extends SequencedLogContractTest
{
    @Override
    protected SequencerFixture createFixture()
    {
        return new AeronClusterFixture(21_000);
    }

    @Override
    protected SequencedLogProvider provider()
    {
        return new AeronClusterLogProvider();
    }
}
