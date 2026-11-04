package com.paulhoward.sequencer.e2e;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

/**
 * Sends {@code count} numbered messages to the cluster at about {@code ratePerSecond}, and keeps going until
 * every one has been committed (echoed back by the cluster's service). Anything not echoed within 10
 * seconds is sent again, within the same rate, so a leader failover can cause duplicates in the log but
 * never a loss.
 * <p>
 * Usage: {@code E2eLoad <count> <ratePerSecond> <prefix>}. Prints {@code acked=<n>} once a second and
 * {@code done} at the end.
 */
public final class E2eLoad
{
    private static final int PAYLOAD_LENGTH = 100;
    private static final long RESEND_AFTER_NS = TimeUnit.SECONDS.toNanos(10);

    private final int count;
    private final String prefix;
    private final BitSet acked;
    private final long[] sentAtNs;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[PAYLOAD_LENGTH]);
    private final MediaDriver driver;
    private AeronCluster client;

    private E2eLoad(final int count, final String prefix)
    {
        this.count = count;
        this.prefix = prefix;
        this.acked = new BitSet(count);
        this.sentAtNs = new long[count];
        this.driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(new File(System.getProperty("java.io.tmpdir"), "aeron-e2e-load").getPath())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
    }

    public static void main(final String[] args)
    {
        final E2eLoad load = new E2eLoad(Integer.parseInt(args[0]), args[2]);
        try
        {
            load.run(Integer.parseInt(args[1]));
        }
        finally
        {
            CloseHelper.quietCloseAll(load.client, load.driver);
        }
    }

    private void run(final int ratePerSecond)
    {
        final long startNs = System.nanoTime();
        long nextReportNs = startNs;
        long sends = 0;
        int nextNew = 0;

        while (acked.cardinality() < count)
        {
            if (null == client || client.isClosed())
            {
                connect();
                continue;
            }

            // New messages and resends share one budget, so a backlog of resends after an outage cannot
            // exceed the requested rate. Resends go first.
            final long nowNs = System.nanoTime();
            final long budget = (nowNs - startNs) * ratePerSecond / 1_000_000_000L + 1;
            for (int i = acked.nextClearBit(0); i < nextNew && sends < budget && null != client;
                i = acked.nextClearBit(i + 1))
            {
                if (nowNs - sentAtNs[i] > RESEND_AFTER_NS && send(i, nowNs))
                {
                    sends++;
                }
            }
            while (nextNew < count && sends < budget && null != client && send(nextNew, nowNs))
            {
                nextNew++;
                sends++;
            }

            // A send that finds the session closed drops the client; reconnect on the next pass.
            if (null == client)
            {
                continue;
            }
            client.pollEgress();

            if (nowNs >= nextReportNs)
            {
                System.out.println("acked=" + acked.cardinality() + " sent=" + nextNew + " offers=" + sends);
                nextReportNs = nowNs + TimeUnit.SECONDS.toNanos(1);
            }
        }
        System.out.println("acked=" + acked.cardinality() + " sent=" + nextNew + " offers=" + sends);
        System.out.println("done");
    }

    private boolean send(final int n, final long nowNs)
    {
        final String text = prefix + n + "|";
        buffer.setMemory(0, PAYLOAD_LENGTH, (byte)'.');
        buffer.putStringWithoutLengthAscii(0, text);

        final long result = client.offer(buffer, 0, PAYLOAD_LENGTH);
        if (result > 0)
        {
            sentAtNs[n] = nowNs;
            return true;
        }
        if (Publication.CLOSED == result || Publication.MAX_POSITION_EXCEEDED == result)
        {
            CloseHelper.quietClose(client);
            client = null;
        }
        return false;
    }

    private void onEcho(final org.agrona.DirectBuffer echo, final int offset, final int length)
    {
        final String text = echo.getStringWithoutLengthAscii(offset, length);
        if (text.startsWith(prefix))
        {
            acked.set(Integer.parseInt(text.substring(prefix.length(), text.indexOf('|'))));
        }
    }

    private void connect()
    {
        final StringBuilder ingress = new StringBuilder();
        for (int i = 0; i < E2eMember.MEMBER_COUNT; i++)
        {
            ingress.append(i == 0 ? "" : ",").append(i).append("=localhost:")
                .append(E2eMember.port(i, E2eMember.INGRESS_PORT_OFFSET));
        }

        try
        {
            client = AeronCluster.connect(new AeronCluster.Context()
                .aeronDirectoryName(driver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ingress.toString())
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .egressListener((sessionId, timestamp, echo, offset, length, header) ->
                    onEcho(echo, offset, length)));
            System.out.println("connected to leader " + client.leaderMemberId());
        }
        catch (final RuntimeException ex)
        {
            System.out.println("cannot connect yet: " + ex.getMessage());
            client = null;
        }
    }
}
