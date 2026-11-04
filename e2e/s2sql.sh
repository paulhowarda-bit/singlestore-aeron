#!/bin/sh
# Runs the SingleStore client inside the test container; arguments go to the client.
#   e2e/s2sql.sh -e "SHOW DATABASES"
#   e2e/s2sql.sh -D sequencer < singlestore/schema.sql
# The root password is read from e2e/local/singlestore-root-password and never printed.
here=$(dirname "$0")
MSYS_NO_PATHCONV=1 exec docker exec -i -e MYSQL_PWD="$(cat "$here/local/singlestore-root-password")" \
    "${CONTAINER:-aeron-s2-test}" singlestore -uroot "$@"
