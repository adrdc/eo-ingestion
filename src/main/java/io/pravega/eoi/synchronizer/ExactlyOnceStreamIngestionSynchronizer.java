package io.pravega.eoi.synchronizer;

import io.pravega.avro.StreamPosition;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

@RequiredArgsConstructor
public class ExactlyOnceStreamIngestionSynchronizer {
    static Logger log = LoggerFactory.getLogger(ExactlyOnceFileIngestionSynchronizer.class);

    private String scopedName;
    private Revision revision;
    private int sequence;
    private UUID txnId;

    /**
     * A private data class defining the revisioned state object that this synchronizer uses.
     */
    @Data
    private static class UpdatablePosition implements Revisioned {
        private final String streamName;
        private final Revision revision;
        private final StreamPosition position;


        @Override
        public String getScopedStreamName() {
            return streamName;
        }

        @Override
        public Revision getRevision() {
            return revision;
        }
    }

    @ToString
    @RequiredArgsConstructor
    static class StreamPositionUpdate implements Update<UpdatablePosition>, Serializable {
        private final StreamPosition position;

        ByteBuffer getStreamPositionBytes() throws IOException {
            return position.toByteBuffer();
        }

        @Override
        public UpdatablePosition applyTo(UpdatablePosition oldState, Revision newRevision) {
            log.debug("Applying update {} to {} ", this, oldState);

            /*
             * The new revision is given by the state synchronizer, and it is expected
             * to uniquely identify a revision of the state object for this stream. Under
             * the hood, this is the offset of the segment holding the data.
             */
            return new UpdatablePosition(oldState.streamName, newRevision, position);
        }

    }

    /**
     * Defines the initial status to be a file id of -1 and an arbitrary UUID.
     */
    @ToString
    static class StreamPositionInit implements InitialUpdate<UpdatablePosition>, Serializable {
        private final StreamPosition position = StreamPosition.newBuilder()
                .setCycle(-1)
                .setCheckpoint(ByteBuffer.allocate(0))
                .setTxnId(UUID.randomUUID().toString())
                .build();

        ByteBuffer getStreamPositionBytes() throws IOException {
            return position.toByteBuffer();
        }

        @Override
        public UpdatablePosition create(String streamName, Revision revision) {
            return new UpdatablePosition(streamName, revision, position);
        }
    }


    /**
     * Private attribute holding a reference to the underlying state synchronizer
     * that this class builds upon.
     */
    private final StateSynchronizer<UpdatablePosition> stateSynchronizer;

    /**
     * Private constructor used by the static method
     * {@link this#createNewSynchronizer(String, SynchronizerClientFactory)}
     *
     * @param streamName
     * @param synchronizer
     */
    private ExactlyOnceStreamIngestionSynchronizer(String streamName,
                                                   StateSynchronizer<UpdatablePosition> synchronizer) {
        this.stateSynchronizer = synchronizer;
        synchronizer.initialize(new StreamPositionInit());
    }

    /**
     * Updates the state of this synchronizer by fetching the latest updates if any.
     */
    public void update() {
        stateSynchronizer.fetchUpdates();
    }

    /**
     * Returns the current status object.
     *
     * @return
     */
    public StreamPosition getStreamPosition() { return stateSynchronizer.getState().position; }


    /**
     * Updates the status using the current expected one to spot concurrent access.
     *
     * @param newPosition
     * @param currentPosition
     * @return
     */
    public boolean updateStatus(StreamPosition newPosition, StreamPosition currentPosition) {

        return stateSynchronizer.updateState((current, updates)-> {
            /*
             * Validates that the current status is the expected one, no
             * concurrent updates.
             */

            if (currentPosition.getTxnId().equals(current.position.getTxnId())) {
                /*
                 * updates contains the list of updates to be applied; in this case,
                 * there is only one.
                 */
                updates.add(new StreamPositionUpdate(newPosition));

                return true;
            } else {

                return false;
            }
        });
    }

    /**
     * Creates an instance of this status synchronizer.
     *
     * @param streamName Name of the stream to hold the data of this synchronizer
     * @param factory A factory for clients of the status synchronizer
     * @return A new instance of #ExactlyOnceFileIngestionSynchronizer
     */

    public static ExactlyOnceStreamIngestionSynchronizer createNewSynchronizer(String streamName, SynchronizerClientFactory factory) {
        return new ExactlyOnceStreamIngestionSynchronizer(streamName,
                factory.createStateSynchronizer(streamName,
                        new StreamPositionUpdateSerializer(),
                        new StreamPositionInitSerializer(),
                        SynchronizerConfig.builder().build()));
    }


}
