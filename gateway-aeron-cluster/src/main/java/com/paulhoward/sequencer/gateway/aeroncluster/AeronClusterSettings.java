package com.paulhoward.sequencer.gateway.aeroncluster;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Connector settings read by the open-source Aeron Cluster reader.
 *
 * @param consensusEndpoints {@code aeron.cluster.consensus.endpoints}: the members' consensus endpoints,
 *                           {@code host:port,host:port,...}. Required.
 * @param consensusStreamId  {@code aeron.cluster.consensus.stream.id}: the cluster's consensus stream id.
 *                           Default 108, the Aeron default.
 * @param gatewayHost        {@code aeron.gateway.host}: an address of this host that the cluster members can
 *                           reach; the query response, archive control responses and the replay are sent
 *                           here. Default {@code localhost}.
 * @param aeronDir           {@code aeron.dir}: directory for this reader's embedded media driver. Default: a
 *                           new directory under {@code java.io.tmpdir}, deleted on close.
 * @param stallTimeout       {@code aeron.gateway.stall.timeout.ms}: if the member being read from delivers
 *                           nothing for this long, ask the cluster again, preferring another member.
 *                           Default 10000.
 */
record AeronClusterSettings(
    List<String> consensusEndpoints,
    int consensusStreamId,
    String gatewayHost,
    String aeronDir,
    Duration stallTimeout)
{
    static final String CONSENSUS_ENDPOINTS = "aeron.cluster.consensus.endpoints";
    static final String CONSENSUS_STREAM_ID = "aeron.cluster.consensus.stream.id";
    static final String GATEWAY_HOST = "aeron.gateway.host";
    static final String AERON_DIR = "aeron.dir";
    static final String STALL_TIMEOUT_MS = "aeron.gateway.stall.timeout.ms";

    static AeronClusterSettings from(final Map<String, String> config)
    {
        final String endpoints = config.get(CONSENSUS_ENDPOINTS);
        if (null == endpoints || endpoints.isBlank())
        {
            throw new IllegalArgumentException(CONSENSUS_ENDPOINTS + " is required (host:port,host:port,...)");
        }

        return new AeronClusterSettings(
            List.of(endpoints.trim().split("\\s*,\\s*")),
            Integer.parseInt(config.getOrDefault(CONSENSUS_STREAM_ID, "108")),
            config.getOrDefault(GATEWAY_HOST, "localhost"),
            config.getOrDefault(AERON_DIR,
                new File(System.getProperty("java.io.tmpdir"), "aeron-gateway-" + UUID.randomUUID()).getPath()),
            Duration.ofMillis(Long.parseLong(config.getOrDefault(STALL_TIMEOUT_MS, "10000"))));
    }
}
