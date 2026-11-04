package com.paulhoward.sequencer.gateway.connect;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * The connector's version, written into {@code version.properties} by the build.
 */
final class Version
{
    static final String VERSION = load();

    private Version()
    {
    }

    private static String load()
    {
        try (InputStream in = Version.class.getResourceAsStream("version.properties"))
        {
            final Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version");
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }
}
