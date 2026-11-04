package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.tck.CommittedMessage;
import com.paulhoward.sequencer.gateway.tck.SequencerFixture;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A three-member open-source Aeron Cluster running in this JVM, for the contract tests and the connector's
 * integration tests. Each member has its own driver, archive and directories under {@code target/}.
 */
public final class AeronClusterFixture implements SequencerFixture
{
    private static final int MEMBER_COUNT = 3;

    private final int basePort;
    private final File baseDir;
    private final String clusterMembers;
    private final TestClusterMember[] members = new TestClusterMember[MEMBER_COUNT];
    private final MediaDriver clientDriver;
    private final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[64 * 1024]);
    private AeronCluster client;

    public AeronClusterFixture(final int basePort)
    {
        this.basePort = basePort;
        this.baseDir = new File("target/test-cluster/" + System.nanoTime());
        this.clusterMembers = TestClusterMember.clusterMembers(basePort, MEMBER_COUNT);

        clientDriver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(new File(baseDir, "client-driver").getPath())
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            startMember(i);
        }
    }

    @Override
    public int memberCount()
    {
        return MEMBER_COUNT;
    }

    @Override
    public Map<String, String> gatewayConfig()
    {
        final StringBuilder endpoints = new StringBuilder();
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            endpoints.append(i == 0 ? "" : ",").append("localhost:")
                .append(TestClusterMember.port(basePort, i, TestClusterMember.CONSENSUS_PORT_OFFSET));
        }

        return Map.of(
            "sequencer.gateway", AeronClusterLogProvider.NAME,
            AeronClusterSettings.CONSENSUS_ENDPOINTS, endpoints.toString(),
            AeronClusterSettings.GATEWAY_HOST, "localhost",
            AeronClusterSettings.STALL_TIMEOUT_MS, "3000");
    }

    @Override
    public long startOfLog()
    {
        return 0;
    }

    @Override
    public synchronized boolean offer(final byte[] payload, final Duration timeout)
    {
        offerBuffer.putBytes(0, payload);
        final long deadline = System.nanoTime() + timeout.toNanos();
        long failingSinceNs = 0;
        while (System.nanoTime() < deadline)
        {
            if (null == client || client.isClosed())
            {
                connectClient();
                if (null == client)
                {
                    sleep(50);
                    continue;
                }
            }

            final long result = client.offer(offerBuffer, 0, payload.length);
            if (result > 0)
            {
                return true;
            }

            client.pollEgress();
            if (Publication.BACK_PRESSURED != result && Publication.ADMIN_ACTION != result)
            {
                final long nowNs = System.nanoTime();
                failingSinceNs = 0 == failingSinceNs ? nowNs : failingSinceNs;
                if (nowNs - failingSinceNs > TimeUnit.SECONDS.toNanos(1))
                {
                    CloseHelper.quietClose(client);
                    client = null;
                    failingSinceNs = 0;
                }
            }
            sleep(1);
        }
        return false;
    }

    @Override
    public List<CommittedMessage> committed()
    {
        List<CommittedMessage> longest = List.of();
        for (final TestClusterMember member : members)
        {
            if (null != member)
            {
                final List<CommittedMessage> processed = member.service().processed();
                if (processed.size() > longest.size())
                {
                    longest = processed;
                }
            }
        }
        return longest;
    }

    @Override
    public int awaitLeader(final Duration timeout)
    {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline)
        {
            for (final TestClusterMember member : members)
            {
                if (null != member && Cluster.Role.LEADER == member.service().role())
                {
                    return member.memberId();
                }
            }
            sleep(10);
        }
        throw new IllegalStateException("no leader elected within " + timeout);
    }

    @Override
    public void stopMember(final int memberId)
    {
        CloseHelper.close(members[memberId]);
        members[memberId] = null;
    }

    @Override
    public void cutOffFromLog(final int memberId)
    {
        members[memberId].cutOffFromLog();
    }

    @Override
    public void startMember(final int memberId)
    {
        members[memberId] = new TestClusterMember(memberId, baseDir, basePort, clusterMembers);
    }

    @Override
    public synchronized void close()
    {
        CloseHelper.quietClose(client);
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            CloseHelper.quietClose(members[i]);
            members[i] = null;
        }
        CloseHelper.quietClose(clientDriver);
        IoUtil.delete(baseDir, true);
    }

    private void connectClient()
    {
        final StringBuilder ingress = new StringBuilder();
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            ingress.append(i == 0 ? "" : ",").append(i).append("=localhost:")
                .append(TestClusterMember.port(basePort, i, TestClusterMember.INGRESS_PORT_OFFSET));
        }

        try
        {
            client = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(clientDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ingress.toString())
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(3))
                .egressListener((clusterSessionId, timestamp, buffer, offset, length, header) -> {}));
        }
        catch (final RuntimeException ex)
        {
            client = null;
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
}
