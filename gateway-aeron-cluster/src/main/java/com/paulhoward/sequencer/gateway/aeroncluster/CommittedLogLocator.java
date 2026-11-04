package com.paulhoward.sequencer.gateway.aeroncluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.BackupQueryEncoder;
import io.aeron.cluster.codecs.BackupResponseDecoder;
import io.aeron.cluster.codecs.ChallengeDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionEventDecoder;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds where each reachable cluster member keeps its log and which counter holds its commit position.
 * <p>
 * This is the same {@code BackupQuery} / {@code BackupResponse} exchange that Aeron's own {@code ClusterBackup}
 * uses: a member answers with its log recording id and the id of its commit-position counter, and replaying
 * that recording bounded by that counter yields committed entries only. Members do not answer during an
 * election, so an empty result means "ask again shortly".
 */
final class CommittedLogLocator implements AutoCloseable
{
    private static final int RESPONSE_STREAM_ID = 108;
    private static final Duration CONNECT_WINDOW = Duration.ofMillis(500);

    /**
     * What one member reported.
     *
     * @param memberId                id of the member that answered.
     * @param leaderMemberId          id of the leader as that member sees it.
     * @param recordingId             the member's log recording id (local to that member's archive).
     * @param commitPositionCounterId the member's commit-position counter id (local to that member's driver).
     * @param archiveEndpoint         the member's archive control endpoint.
     * @param archiveResponseEndpoint the member's archive response endpoint, or null if not configured.
     */
    record LogSource(
        int memberId,
        int leaderMemberId,
        long recordingId,
        int commitPositionCounterId,
        String archiveEndpoint,
        String archiveResponseEndpoint)
    {
        boolean isLeader()
        {
            return memberId == leaderMemberId;
        }
    }

    private final Aeron aeron;
    private final AeronClusterSettings settings;
    private final Subscription responseSubscription;
    private final Map<String, ExclusivePublication> publications = new HashMap<>();
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(512);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final BackupQueryEncoder queryEncoder = new BackupQueryEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final BackupResponseDecoder responseDecoder = new BackupResponseDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final FragmentAssembler assembler = new FragmentAssembler(this::onFragment);

    private final Map<Long, LogSource> responses = new HashMap<>();
    private final Map<Long, String> outstanding = new HashMap<>();

    CommittedLogLocator(final Aeron aeron, final AeronClusterSettings settings)
    {
        this.aeron = aeron;
        this.settings = settings;
        this.responseSubscription = aeron.addSubscription(
            "aeron:udp?endpoint=" + settings.gatewayHost() + ":0", RESPONSE_STREAM_ID);
    }

    /**
     * Ask every configured member, and collect the answers that arrive within {@code timeout}.
     *
     * @param timeout how long to wait for answers.
     * @return the answers, possibly empty.
     */
    List<LogSource> query(final Duration timeout) throws InterruptedException
    {
        final long deadline = System.nanoTime() + timeout.toNanos();
        final String responseChannel = awaitResponseChannel(deadline);
        if (null == responseChannel)
        {
            return List.of();
        }

        responses.clear();
        outstanding.clear();

        for (final String endpoint : settings.consensusEndpoints())
        {
            publications.computeIfAbsent(endpoint, e -> aeron.addExclusivePublication(
                "aeron:udp?term-length=64k|endpoint=" + e, settings.consensusStreamId()));
        }

        // A stopped member never connects; stop waiting for it after a short window so one dead member does
        // not slow every query down to the full timeout. Publications are kept, so live members stay connected.
        final long connectDeadline = Math.min(deadline, System.nanoTime() + CONNECT_WINDOW.toNanos());
        final List<String> unsent = new ArrayList<>(settings.consensusEndpoints());
        while (System.nanoTime() < deadline && (!unsent.isEmpty() || !outstanding.isEmpty()))
        {
            unsent.removeIf(endpoint -> sendQuery(endpoint, responseChannel));
            if (System.nanoTime() > connectDeadline)
            {
                unsent.clear();
            }
            if (0 == responseSubscription.poll(assembler, 10))
            {
                Thread.sleep(1);
            }
        }

        return new ArrayList<>(responses.values());
    }

    private boolean sendQuery(final String endpoint, final String responseChannel)
    {
        final ExclusivePublication publication = publications.get(endpoint);
        if (!publication.isConnected())
        {
            return false;
        }

        final long correlationId = aeron.nextCorrelationId();
        queryEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
            .correlationId(correlationId)
            .responseStreamId(RESPONSE_STREAM_ID)
            .version(AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION)
            .logPosition(BackupQueryEncoder.logPositionNullValue())
            .responseChannel(responseChannel)
            .putEncodedCredentials(new byte[0], 0, 0);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + queryEncoder.encodedLength();
        if (publication.offer(buffer, 0, length) > 0)
        {
            outstanding.put(correlationId, endpoint);
            return true;
        }
        return false;
    }

    private String awaitResponseChannel(final long deadline) throws InterruptedException
    {
        String resolved;
        while (null == (resolved = responseSubscription.resolvedEndpoint()))
        {
            if (System.nanoTime() > deadline)
            {
                return null;
            }
            Thread.sleep(1);
        }
        return "aeron:udp?endpoint=" + settings.gatewayHost() + ":" + resolved.substring(resolved.lastIndexOf(':') + 1);
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID)
        {
            return;
        }

        switch (headerDecoder.templateId())
        {
            case BackupResponseDecoder.TEMPLATE_ID ->
            {
                responseDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                final long correlationId = responseDecoder.correlationId();
                if (null == outstanding.remove(correlationId))
                {
                    return;
                }

                final long recordingId = responseDecoder.logRecordingId();
                final int commitPositionCounterId = responseDecoder.commitPositionCounterId();
                final int leaderMemberId = responseDecoder.leaderMemberId();
                final int memberId = responseDecoder.memberId();
                for (final BackupResponseDecoder.SnapshotsDecoder ignored : responseDecoder.snapshots())
                {
                    // The snapshots group must be consumed before the members string can be read.
                }
                final ClusterMember member = ClusterMember.findMember(
                    ClusterMember.parse(responseDecoder.clusterMembers()), memberId);
                if (null != member && BackupResponseDecoder.memberIdNullValue() != memberId)
                {
                    responses.put(correlationId, new LogSource(
                        memberId,
                        leaderMemberId,
                        recordingId,
                        commitPositionCounterId,
                        member.archiveEndpoint(),
                        member.archiveResponseEndpoint()));
                }
            }
            case SessionEventDecoder.TEMPLATE_ID ->
            {
                sessionEventDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                final String endpoint = outstanding.remove(sessionEventDecoder.correlationId());
                if (null != endpoint)
                {
                    throw new IllegalStateException("cluster member at " + endpoint + " refused the log query: " +
                        sessionEventDecoder.code() + " " + sessionEventDecoder.detail());
                }
            }
            case ChallengeDecoder.TEMPLATE_ID -> throw new IllegalStateException(
                "the cluster requires authentication for log queries; this reader sends no credentials");
            default ->
            {
            }
        }
    }

    @Override
    public void close()
    {
        publications.values().forEach(CloseHelper::quietClose);
        publications.clear();
        CloseHelper.quietClose(responseSubscription);
    }
}
