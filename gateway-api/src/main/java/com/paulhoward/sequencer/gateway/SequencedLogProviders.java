package com.paulhoward.sequencer.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Finds the {@link SequencedLogProvider} selected by name.
 */
public final class SequencedLogProviders
{
    private SequencedLogProviders()
    {
    }

    /**
     * Look up a provider by name. Uses this library's own class loader, not the thread context loader,
     * so it works inside Kafka Connect's per-plugin class loader isolation.
     *
     * @param name the {@code sequencer.gateway} setting.
     * @return the provider.
     * @throws IllegalArgumentException naming the providers that are available if none matches.
     */
    public static SequencedLogProvider find(final String name)
    {
        final List<String> available = new ArrayList<>();
        for (final SequencedLogProvider provider :
            ServiceLoader.load(SequencedLogProvider.class, SequencedLogProvider.class.getClassLoader()))
        {
            if (provider.name().equals(name))
            {
                return provider;
            }
            available.add(provider.name());
        }

        throw new IllegalArgumentException(
            "no sequencer gateway named '" + name + "' on the classpath; available: " + available);
    }
}
