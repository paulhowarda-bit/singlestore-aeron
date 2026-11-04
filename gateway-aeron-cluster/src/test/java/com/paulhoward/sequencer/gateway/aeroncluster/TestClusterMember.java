package com.paulhoward.sequencer.gateway.aeroncluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * One member of the in-process test cluster: media driver, archive, consensus module and an
 * {@link OracleService}. Stopping and starting a member keeps its archive and cluster directories, so a
 * restarted member recovers its log and rejoins exactly as a real member would.
 */
public final class TestClusterMember implements AutoCloseable
{
    static final int ARCHIVE_PORT_OFFSET = 1;
    static final int INGRESS_PORT_OFFSET = 2;
    static final int CONSENSUS_PORT_OFFSET = 3;
    static final int LOG_PORT_OFFSET = 4;
    static final int CATCHUP_PORT_OFFSET = 5;
    static final int PORTS_PER_MEMBER = 100;

    /** Small term so that tests cross many term boundaries and exercise end-of-term padding. */
    static final String LOG_TERM_LENGTH = "64k";

    private final int memberId;
    private final File memberDir;
    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final OracleService service = new OracleService();
    private volatile boolean cutOffFromLog;

    TestClusterMember(final int memberId, final File baseDir, final int basePort, final String clusterMembers)
    {
        this.memberId = memberId;
        this.memberDir = new File(baseDir, "member-" + memberId);
        final String aeronDir = new File(memberDir, "driver").getPath();

        final int logStreamId = new ConsensusModule.Context().logStreamId();
        final LossGenerator dropLogWhenCutOff = new LossGenerator()
        {
            public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
            {
                return false;
            }

            public boolean shouldDropFrame(
                final InetSocketAddress address,
                final UnsafeBuffer buffer,
                final int streamId,
                final int sessionId,
                final int termId,
                final int termOffset,
                final int length)
            {
                return cutOffFromLog && logStreamId == streamId;
            }
        };
        final LossGenerator dropNothing = (address, buffer, length) -> false;

        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .receiveChannelEndpointSupplier((udpChannel, dispatcher, statusIndicator, context) ->
                new DebugReceiveChannelEndpoint(
                    udpChannel, dispatcher, statusIndicator, context, dropLogWhenCutOff, dropNothing))
            .errorHandler(TestClusterMember::ignore);

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(memberDir, "archive"))
            .archiveId(memberId)
            .controlChannel("aeron:udp?endpoint=localhost:" + port(basePort, memberId, ARCHIVE_PORT_OFFSET))
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel("aeron:udp?endpoint=localhost:0"))
            .localControlChannel("aeron:ipc?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsEnabled(false)
            .segmentFileLength(1024 * 1024)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(false)
            .errorHandler(TestClusterMember::ignore);

        final AeronArchive.Context localArchiveClient = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc?term-length=64k")
            .controlResponseChannel("aeron:ipc?term-length=64k")
            .aeronDirectoryName(aeronDir);

        final ConsensusModule.Context consensusContext = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .clusterDir(new File(memberDir, "cluster"))
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel("aeron:udp?term-length=" + LOG_TERM_LENGTH)
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .archiveContext(localArchiveClient.clone())
            .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(20))
            .deleteDirOnStart(false)
            .errorHandler(TestClusterMember::ignore);

        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .archiveContext(localArchiveClient.clone())
            .clusterDir(new File(memberDir, "cluster"))
            .clusteredService(service)
            .errorHandler(TestClusterMember::ignore);

        clusteredMediaDriver = ClusteredMediaDriver.launch(driverContext, archiveContext, consensusContext);
        container = ClusteredServiceContainer.launch(serviceContext);
    }

    static int port(final int basePort, final int memberId, final int offset)
    {
        return basePort + memberId * PORTS_PER_MEMBER + offset;
    }

    static String clusterMembers(final int basePort, final int memberCount)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < memberCount; i++)
        {
            sb.append(i)
                .append(",localhost:").append(port(basePort, i, INGRESS_PORT_OFFSET))
                .append(",localhost:").append(port(basePort, i, CONSENSUS_PORT_OFFSET))
                .append(",localhost:").append(port(basePort, i, LOG_PORT_OFFSET))
                .append(",localhost:").append(port(basePort, i, CATCHUP_PORT_OFFSET))
                .append(",localhost:").append(port(basePort, i, ARCHIVE_PORT_OFFSET))
                .append('|');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    int memberId()
    {
        return memberId;
    }

    OracleService service()
    {
        return service;
    }

    /**
     * Drop every log frame arriving at this member from now on. Everything else, including the consensus
     * heartbeats that keep the leader in office, still gets through.
     */
    void cutOffFromLog()
    {
        cutOffFromLog = true;
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(container, clusteredMediaDriver);
    }

    private static void ignore(final Throwable throwable)
    {
        // Members are stopped abruptly on purpose; the errors that follows are expected noise.
    }
}
