package com.paulhoward.sequencer.gateway.tck;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A running sequencer that the contract tests can drive and break. Each reader implementation supplies one.
 */
public interface SequencerFixture extends AutoCloseable
{
    /**
     * @return the number of members; a majority must be running for anything to commit.
     */
    int memberCount();

    /**
     * @return reader configuration that points a {@code SequencedLog} at this sequencer.
     */
    Map<String, String> gatewayConfig();

    /**
     * @return the resume point that means "from the beginning of the log".
     */
    long startOfLog();

    /**
     * Offer one message for sequencing. Acceptance means the message was sent towards the leader; it can
     * still be lost if the leader fails before sequencing it, and it is not committed until a majority holds
     * it. The tests confirm commitment through {@link #committed()} and resend what is missing.
     *
     * @param payload message bytes.
     * @param timeout how long to keep retrying through back pressure or a leader change.
     * @return true if accepted, false if not accepted within the timeout.
     */
    boolean offer(byte[] payload, Duration timeout);

    /**
     * @return the messages the sequencer's application has processed so far, in order. Applications
     * only ever process committed messages, so this is the oracle for what is committed.
     */
    List<CommittedMessage> committed();

    /**
     * @param timeout how long to wait for an election.
     * @return the member id of the current leader.
     */
    int awaitLeader(Duration timeout);

    /**
     * Stop a member. The remaining members carry on if they still hold a majority.
     *
     * @param memberId member to stop.
     */
    void stopMember(int memberId);

    /**
     * Cut a running member off from the leader's log, as a one-way network fault would: it stops receiving
     * new entries, so it stops acknowledging them, but it keeps running and stays in contact with the other
     * members. A leader whose followers are all cut off keeps appending entries it can never commit, which
     * is how the tests put an uncommitted tail on the leader. Stopping and starting the member undoes it.
     *
     * @param memberId member to cut off; never the leader.
     */
    void cutOffFromLog(int memberId);

    /**
     * Start a stopped member again with its existing log, so it rejoins and catches up.
     *
     * @param memberId member to start.
     */
    void startMember(int memberId);

    @Override
    void close();
}
