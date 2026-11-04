#!/bin/sh
# SingleStore's java_path points here so its extractor JVMs can load Aeron. Aeron's Agrona library uses
# jdk.internal.misc.Unsafe, which the JVM only allows with this flag; SingleStore passes no module flags.
exec "${AERON_REAL_JAVA:-/usr/local/jdk-25/bin/java}" --add-opens java.base/jdk.internal.misc=ALL-UNNAMED "$@"
