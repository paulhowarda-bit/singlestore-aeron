package com.paulhoward.sequencer.gateway.connect;

import com.paulhoward.sequencer.gateway.SequencedEntry;
import com.paulhoward.sequencer.gateway.SequencedLog;
import com.paulhoward.sequencer.gateway.SequencedLogProvider;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * An in-memory sequencer for the connector's unit tests. {@link java.util.ServiceLoader} creates the
 * instance, so the tests reach its log and what it was opened with through static state.
 */
public final class FakeSequencerProvider implements SequencedLogProvider
{
    static final String NAME = "fake";
    static final long START = 64;
    static final int ENTRY_LENGTH = 96;

    static final List<SequencedEntry> LOG = new CopyOnWriteArrayList<>();
    static volatile long openedAfter;
    static volatile Map<String, String> openedWith;
    static volatile RuntimeException pollFailure;
    static volatile boolean closed;

    static void reset()
    {
        LOG.clear();
        openedAfter = Long.MIN_VALUE;
        openedWith = null;
        pollFailure = null;
        closed = false;
    }

    static void append(final int count)
    {
        for (int i = 0; i < count; i++)
        {
            final long previous = LOG.isEmpty() ? START : LOG.get(LOG.size() - 1).position();
            final int n = LOG.size();
            LOG.add(new SequencedEntry(previous + ENTRY_LENGTH, previous, 1 + n / 100, 1_000_000L + n,
                "SESSION_MESSAGE", 7, ("entry-" + n).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public long startOfLog()
    {
        return START;
    }

    @Override
    public SequencedLog open(final Map<String, String> config, final long resumeAfter)
    {
        openedAfter = resumeAfter;
        openedWith = Map.copyOf(config);
        return new SequencedLog()
        {
            private long position = resumeAfter;

            @Override
            public int poll(final Consumer<SequencedEntry> handler, final int maxEntries)
            {
                if (null != pollFailure)
                {
                    throw pollFailure;
                }

                int delivered = 0;
                for (final SequencedEntry entry : LOG)
                {
                    if (delivered == maxEntries)
                    {
                        break;
                    }
                    if (entry.position() > position)
                    {
                        handler.accept(entry);
                        position = entry.position();
                        delivered++;
                    }
                }
                return delivered;
            }

            @Override
            public void close()
            {
                closed = true;
            }
        };
    }
}
