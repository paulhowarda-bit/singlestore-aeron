package com.paulhoward.sequencer.e2e;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/**
 * The cluster's application, and the test's oracle. Every message it processes is appended to
 * {@code oracle.tsv} as {@code position<TAB>sessionId<TAB>payload}, and echoed back to the sender so the
 * load generator knows it was committed. Applications only ever process committed messages, so the file
 * is an independent record of the committed log.
 * <p>
 * The member takes no snapshots, so after a restart it replays the whole log from the start. The file is
 * therefore rewritten from scratch on every start and ends up complete again once replay catches up.
 */
final class OracleService implements ClusteredService
{
    private static final long FLUSH_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(200);

    private final File memberDir;
    private Cluster cluster;
    private BufferedWriter oracle;
    private boolean unflushed;
    private long lastFlushNs;

    OracleService(final File memberDir)
    {
        this.memberDir = memberDir;
    }

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        try
        {
            oracle = new BufferedWriter(new FileWriter(new File(memberDir, "oracle.tsv"), StandardCharsets.UTF_8,
                false), 1 << 16);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
        // onRoleChange is only called on a change, and every member starts as a follower.
        onRoleChange(cluster.role());
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
        try
        {
            oracle.write(Long.toString(cluster.logPosition()));
            oracle.write('\t');
            oracle.write(Long.toString(session.id()));
            oracle.write('\t');
            oracle.write(buffer.getStringWithoutLengthAscii(offset, length));
            oracle.write('\n');
            unflushed = true;
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }

        // Echo for the load generator's acknowledgement. While a restarted member replays its log this is
        // a no-op, and if it fails the sender simply resends.
        session.offer(buffer, offset, length);
    }

    @Override
    public int doBackgroundWork(final long nowNs)
    {
        if (unflushed && nowNs - lastFlushNs > FLUSH_INTERVAL_NS)
        {
            flush();
            lastFlushNs = nowNs;
            return 1;
        }
        return 0;
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        try
        {
            Files.writeString(new File(memberDir, "role").toPath(), newRole.name());
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
        flush();
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

    private void flush()
    {
        try
        {
            oracle.flush();
            unflushed = false;
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }
}
