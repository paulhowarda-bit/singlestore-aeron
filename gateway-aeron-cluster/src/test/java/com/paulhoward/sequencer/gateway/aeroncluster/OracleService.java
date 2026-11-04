package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.tck.CommittedMessage;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Test clustered service that records every message it processes with the log position the cluster gave
 * it. Clustered services only ever process committed entries, so this list is the oracle for what the
 * cluster has committed.
 */
public final class OracleService implements ClusteredService
{
    private final List<CommittedMessage> processed = new ArrayList<>();
    private volatile Cluster.Role role = Cluster.Role.FOLLOWER;
    private Cluster cluster;

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
    }

    @Override
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final byte[] payload = new byte[length];
        buffer.getBytes(offset, payload);
        synchronized (processed)
        {
            processed.add(new CommittedMessage(cluster.logPosition(), session.id(), payload));
        }
    }

    public List<CommittedMessage> processed()
    {
        synchronized (processed)
        {
            return List.copyOf(processed);
        }
    }

    public int processedCount()
    {
        synchronized (processed)
        {
            return processed.size();
        }
    }

    public Cluster.Role role()
    {
        return role;
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        role = newRole;
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
    }
}
