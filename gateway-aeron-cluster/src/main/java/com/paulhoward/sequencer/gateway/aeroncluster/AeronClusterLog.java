package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequenceIntegrityException;
import com.paulhoward.sequencer.gateway.SequencedEntry;
import com.paulhoward.sequencer.gateway.SequencedLog;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Reads the committed log of an open-source Aeron Cluster from outside the cluster.
 * <p>
 * A background thread asks the members where their logs are ({@link CommittedLogLocator}), connects to one
 * member's archive (a follower by preference, to keep load off the leader) and replays its log recording
 * <b>bounded by that member's commit-position counter</b>, so only committed entries are ever read. Decoded
 * entries are queued; {@link #poll} only drains the queue, so it never blocks. If the member stops, stalls
 * or loses its place, the thread asks again and continues from the last complete entry on another member;
 * positions are identical on every member, so the sequence continues without a gap.
 */
final class AeronClusterLog implements SequencedLog
{
    private static final System.Logger LOG = System.getLogger(AeronClusterLog.class.getName());
    private static final int REPLAY_STREAM_ID = 1_001;
    private static final int QUEUE_CAPACITY = 16_384;
    private static final int FRAGMENT_LIMIT = 256;
    private static final Duration QUERY_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration IMAGE_TIMEOUT = Duration.ofSeconds(10);
    private static final long ARCHIVE_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    private enum Outcome
    {
        /** The member's log does not reach the resume position yet. */
        BEHIND,
        /** The member could not be read from right now. */
        UNAVAILABLE,
        /** The replay ended, normally because the member stopped. */
        ENDED,
        /** The member delivered nothing for the stall timeout. */
        STALLED
    }

    private final AeronClusterSettings settings;
    private final MediaDriver driver;
    private final Aeron aeron;
    private final ArrayBlockingQueue<SequencedEntry> ready = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final ClusterLogDecoder decoder = new ClusterLogDecoder(this::enqueue);
    private final Thread reader;

    private volatile boolean closed;
    private volatile RuntimeException failure;

    // Reader thread only.
    private long position;
    private int avoidMemberId = Aeron.NULL_VALUE;
    private long leaderBehindSinceNs;

    AeronClusterLog(final AeronClusterSettings settings, final long resumeAfter)
    {
        this.settings = settings;
        this.position = resumeAfter;
        this.driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(settings.aeronDir())
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        reader = new Thread(this::run, "aeron-cluster-gateway");
        reader.setDaemon(true);
        reader.start();
    }

    @Override
    public int poll(final Consumer<SequencedEntry> handler, final int maxEntries)
    {
        int delivered = 0;
        SequencedEntry entry;
        while (delivered < maxEntries && null != (entry = ready.poll()))
        {
            handler.accept(entry);
            delivered++;
        }

        final RuntimeException failure = this.failure;
        if (0 == delivered && null != failure && ready.isEmpty())
        {
            throw failure;
        }

        return delivered;
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;

        try
        {
            reader.join(TimeUnit.SECONDS.toMillis(20));
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
        CloseHelper.quietCloseAll(aeron, driver);
    }

    private void run()
    {
        try (CommittedLogLocator locator = new CommittedLogLocator(aeron, settings))
        {
            while (!closed)
            {
                final List<CommittedLogLocator.LogSource> sources = locator.query(QUERY_TIMEOUT);
                if (sources.isEmpty())
                {
                    Thread.sleep(100);
                    continue;
                }

                boolean leaderBehind = false;
                boolean read = false;
                for (final CommittedLogLocator.LogSource source : preferFollowers(sources))
                {
                    final Outcome outcome = readFrom(source);
                    if (Outcome.BEHIND == outcome)
                    {
                        leaderBehind |= source.isLeader();
                        continue;
                    }
                    if (Outcome.UNAVAILABLE == outcome)
                    {
                        continue;
                    }

                    read = true;
                    leaderBehindSinceNs = 0;
                    avoidMemberId = Outcome.STALLED == outcome ? source.memberId() : Aeron.NULL_VALUE;
                    break;
                }

                if (leaderBehind)
                {
                    checkLeaderBehind();
                }
                if (!read)
                {
                    Thread.sleep(100);
                }
            }
        }
        catch (final SequenceIntegrityException ex)
        {
            failure = ex;
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
        catch (final RuntimeException ex)
        {
            if (!closed)
            {
                failure = ex;
            }
        }
    }

    /**
     * The leader always holds every committed entry, so a leader whose log ends before the resume position
     * means the resume position does not belong to this cluster's log (the log was reset, or the stored
     * offset came from another cluster). Waiting would eventually resume mid-stream in a different log.
     * A deposed leader can briefly still claim leadership, so the condition must hold for the stall timeout.
     */
    private void checkLeaderBehind()
    {
        final long nowNs = System.nanoTime();
        if (0 == leaderBehindSinceNs)
        {
            leaderBehindSinceNs = nowNs;
        }
        else if (nowNs - leaderBehindSinceNs > settings.stallTimeout().toNanos())
        {
            throw new SequenceIntegrityException("resume position " + position + " is beyond the end of the " +
                "cluster's log as recorded by its leader; the cluster's log has been reset or the stored offset " +
                "belongs to a different cluster");
        }
    }

    private List<CommittedLogLocator.LogSource> preferFollowers(final List<CommittedLogLocator.LogSource> sources)
    {
        final List<CommittedLogLocator.LogSource> ordered = new ArrayList<>(sources);
        ordered.sort(Comparator
            .comparing((CommittedLogLocator.LogSource s) -> s.memberId() == avoidMemberId)
            .thenComparing(CommittedLogLocator.LogSource::isLeader)
            .thenComparingInt(CommittedLogLocator.LogSource::memberId));
        return ordered;
    }

    private Outcome readFrom(final CommittedLogLocator.LogSource source) throws InterruptedException
    {
        final AeronArchive.Context context = new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .messageTimeoutNs(ARCHIVE_MESSAGE_TIMEOUT_NS)
            .controlRequestChannel("aeron:udp?endpoint=" + source.archiveEndpoint())
            .controlResponseChannel(null == source.archiveResponseEndpoint() ?
                "aeron:udp?endpoint=" + settings.gatewayHost() + ":0" :
                "aeron:udp?control-mode=response|control=" + source.archiveResponseEndpoint());

        try (AeronArchive archive = AeronArchive.connect(context))
        {
            final long recordingId = source.recordingId();
            final long start = archive.getStartPosition(recordingId);
            if (position < start)
            {
                throw new SequenceIntegrityException("resume position " + position + " is before the start of the " +
                    "log retained by member " + source.memberId() + " (" + start + "); the entries in between " +
                    "have been purged and cannot be delivered");
            }

            final long active = archive.getRecordingPosition(recordingId);
            final long recorded = NULL_POSITION != active ? active : archive.getStopPosition(recordingId);
            if (position > recorded)
            {
                return Outcome.BEHIND;
            }
            if (NULL_POSITION == active && position == recorded)
            {
                return Outcome.UNAVAILABLE;
            }

            return replay(archive, source);
        }
        catch (final SequenceIntegrityException ex)
        {
            throw ex;
        }
        catch (final RuntimeException ex)
        {
            if (!closed)
            {
                LOG.log(System.Logger.Level.INFO, "cannot read the log from member " + source.memberId() +
                    " (" + source.archiveEndpoint() + "): " + ex);
            }
            return Outcome.UNAVAILABLE;
        }
    }

    private Outcome replay(final AeronArchive archive, final CommittedLogLocator.LogSource source)
        throws InterruptedException
    {
        try (Subscription subscription = aeron.addSubscription(
            "aeron:udp?endpoint=" + settings.gatewayHost() + ":0", REPLAY_STREAM_ID))
        {
            String resolved;
            while (null == (resolved = subscription.resolvedEndpoint()))
            {
                Thread.sleep(1);
            }
            final String replayChannel = "aeron:udp?endpoint=" + settings.gatewayHost() + ":" +
                resolved.substring(resolved.lastIndexOf(':') + 1);

            final long replaySessionId;
            try
            {
                replaySessionId = archive.startBoundedReplay(
                    source.recordingId(),
                    position,
                    NULL_LENGTH,
                    source.commitPositionCounterId(),
                    replayChannel,
                    REPLAY_STREAM_ID);
            }
            catch (final ArchiveException ex)
            {
                if (ArchiveException.INVALID_POSITION == ex.errorCode())
                {
                    throw new SequenceIntegrityException("member " + source.memberId() + " cannot replay its log " +
                        "from resume position " + position + ": " + ex.getMessage(), ex);
                }
                throw ex;
            }

            try
            {
                return stream(subscription, (int)replaySessionId, source);
            }
            finally
            {
                try
                {
                    archive.stopReplay(replaySessionId);
                }
                catch (final RuntimeException ignore)
                {
                    // The replay has normally ended already because the member went away.
                }
            }
        }
    }

    private Outcome stream(final Subscription subscription, final int sessionId,
        final CommittedLogLocator.LogSource source) throws InterruptedException
    {
        final long imageDeadline = System.nanoTime() + IMAGE_TIMEOUT.toNanos();
        Image image;
        while (null == (image = subscription.imageBySessionId(sessionId)))
        {
            if (closed || System.nanoTime() > imageDeadline)
            {
                return Outcome.UNAVAILABLE;
            }
            Thread.sleep(1);
        }

        decoder.resumeFrom(position);
        long lastProgressNs = System.nanoTime();
        while (!closed)
        {
            final int fragments = image.poll(decoder, FRAGMENT_LIMIT);
            position = decoder.lastEntryPosition();
            if (fragments > 0)
            {
                lastProgressNs = System.nanoTime();
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                return Outcome.ENDED;
            }
            else if (System.nanoTime() - lastProgressNs > settings.stallTimeout().toNanos())
            {
                LOG.log(System.Logger.Level.DEBUG, "no log progress from member " + source.memberId() +
                    " for " + settings.stallTimeout() + ", asking the cluster again");
                return Outcome.STALLED;
            }
            else
            {
                Thread.sleep(1);
            }
        }
        return Outcome.ENDED;
    }

    private void enqueue(final SequencedEntry entry)
    {
        try
        {
            while (!closed && !ready.offer(entry, 100, TimeUnit.MILLISECONDS))
            {
                // Back pressure: the consumer is behind. The replay waits with it.
            }
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }
}
