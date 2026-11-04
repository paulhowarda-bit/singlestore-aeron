package com.paulhoward.sequencer.e2e;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

/**
 * One member of a three-member open-source Aeron Cluster on localhost, run as its own process so it can be
 * killed and restarted like a real member. Its archive and cluster directories survive a restart, so the
 * member recovers its log and rejoins.
 * <p>
 * Usage: {@code E2eMember <memberId> <baseDir>}
 */
public final class E2eMember
{
    static final int MEMBER_COUNT = 3;
    static final int BASE_PORT = 9000;
    static final int ARCHIVE_PORT_OFFSET = 1;
    static final int INGRESS_PORT_OFFSET = 2;
    static final int CONSENSUS_PORT_OFFSET = 3;
    static final int LOG_PORT_OFFSET = 4;
    static final int CATCHUP_PORT_OFFSET = 5;

    private E2eMember()
    {
    }

    public static void main(final String[] args)
    {
        final int memberId = Integer.parseInt(args[0]);
        final File memberDir = new File(args[1], "member-" + memberId);
        final String aeronDir = new File(memberDir, "driver").getPath();

        final AeronArchive.Context localArchive = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc")
            .aeronDirectoryName(aeronDir);

        final ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true),
            new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDir(new File(memberDir, "archive"))
                .archiveId(memberId)
                .controlChannel("aeron:udp?endpoint=localhost:" + port(memberId, ARCHIVE_PORT_OFFSET))
                .localControlChannel("aeron:ipc")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .segmentFileLength(16 * 1024 * 1024)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false),
            new ConsensusModule.Context()
                .clusterMemberId(memberId)
                .clusterMembers(clusterMembers())
                .clusterDir(new File(memberDir, "cluster"))
                .ingressChannel("aeron:udp")
                .logChannel("aeron:udp?term-length=1m")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .archiveContext(localArchive.clone())
                .deleteDirOnStart(false));

        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(localArchive.clone())
                .clusterDir(new File(memberDir, "cluster"))
                .clusteredService(new OracleService(memberDir)));

        System.out.println("member " + memberId + " started");
        new ShutdownSignalBarrier().await();
        CloseHelper.closeAll(container, driver);
    }

    static int port(final int memberId, final int offset)
    {
        return BASE_PORT + memberId * 100 + offset;
    }

    static String clusterMembers()
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            sb.append(i)
                .append(",localhost:").append(port(i, INGRESS_PORT_OFFSET))
                .append(",localhost:").append(port(i, CONSENSUS_PORT_OFFSET))
                .append(",localhost:").append(port(i, LOG_PORT_OFFSET))
                .append(",localhost:").append(port(i, CATCHUP_PORT_OFFSET))
                .append(",localhost:").append(port(i, ARCHIVE_PORT_OFFSET))
                .append('|');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
