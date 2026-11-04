package com.paulhoward.sequencer.gateway.aeroncluster;

import com.paulhoward.sequencer.gateway.SequencedEntry;
import com.paulhoward.sequencer.gateway.SequencedLog;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/**
 * Diagnostic: print committed log entries from a position, using exactly the reader the connector uses.
 * <pre>
 * java -cp aeron-singlestore-connector.jar com.paulhoward.sequencer.gateway.aeroncluster.LogTail \
 *     --consensus-endpoints host:port,host:port,host:port --gateway-host this-host --from 0 --count 100
 * </pre>
 */
public final class LogTail
{
    private LogTail()
    {
    }

    public static void main(final String[] args) throws InterruptedException
    {
        final Map<String, String> config = new HashMap<>();
        long from = 0;
        long count = 100;
        for (int i = 0; i + 1 < args.length; i += 2)
        {
            switch (args[i])
            {
                case "--consensus-endpoints" -> config.put(AeronClusterSettings.CONSENSUS_ENDPOINTS, args[i + 1]);
                case "--gateway-host" -> config.put(AeronClusterSettings.GATEWAY_HOST, args[i + 1]);
                case "--consensus-stream-id" -> config.put(AeronClusterSettings.CONSENSUS_STREAM_ID, args[i + 1]);
                case "--from" -> from = Long.parseLong(args[i + 1]);
                case "--count" -> count = Long.parseLong(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }

        final long[] printed = { 0 };
        try (SequencedLog log = new AeronClusterLogProvider().open(config, from))
        {
            while (printed[0] < count)
            {
                if (0 == log.poll(entry -> print(entry, printed), (int)Math.min(1000, count - printed[0])))
                {
                    Thread.sleep(10);
                }
            }
        }
    }

    private static void print(final SequencedEntry entry, final long[] printed)
    {
        System.out.println(entry.position() + "\tprev=" + entry.previousPosition() + "\tterm=" + entry.term() +
            "\t" + entry.kind() + "\tsource=" + entry.sourceId() + "\t" + render(entry.payload()));
        printed[0]++;
    }

    private static String render(final byte[] payload)
    {
        final String text = new String(payload, StandardCharsets.UTF_8);
        final boolean printable = text.chars().allMatch(c -> c >= 0x20 && c < 0x7f);
        final String shown = printable ? text : HexFormat.of().formatHex(payload);
        return shown.length() > 120 ? shown.substring(0, 120) + "..." : shown;
    }
}
