package io.pravega.eoi;

import io.pravega.avro.Status;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.*;
import io.pravega.client.stream.impl.JavaSerializer;
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
public class ExactlyOnceIngestionSynchronizer {
    static Logger log = LoggerFactory.getLogger(ExactlyOnceIngestionSynchronizer.class);

    private String scopedName;
    private Revision revision;
    private int sequence;
    private UUID txnId;

    @Data
    private static class UpdatableStatus implements Revisioned {
        private final String streamName;
        private final Revision revision;
        private final Status status;


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
    static class StatusUpdate implements Update<UpdatableStatus>, Serializable {
        private final Status status;

        ByteBuffer getStatusBytes() throws IOException {
            return status.toByteBuffer();
        }

        @Override
        public UpdatableStatus applyTo(UpdatableStatus oldState, Revision newRevision) {
            log.trace("Applying update {} to {} ", this, oldState);
            return new UpdatableStatus(oldState.streamName, newRevision, status);
        }

    }

    @ToString
    static class StatusInit implements InitialUpdate<UpdatableStatus>, Serializable {
        private final Status status = Status.newBuilder()
                                            .setFileId(-1)
                                            .setTxnId(UUID.randomUUID().toString())
                                            .build();

        ByteBuffer getStatusBytes() throws IOException {
            return status.toByteBuffer();
        }

        @Override
        public UpdatableStatus create(String streamName, Revision revision) {
            return new UpdatableStatus(streamName, revision, status);
        }
    }

    private final StateSynchronizer<UpdatableStatus> stateSynchronizer;

    private ExactlyOnceIngestionSynchronizer(String streamName,
                                             StateSynchronizer<UpdatableStatus> synchronizer) {
        this.stateSynchronizer = synchronizer;
        synchronizer.initialize(new StatusInit());
    }

    public void update() {
        stateSynchronizer.fetchUpdates();
    }

    public Status getStatus() { return stateSynchronizer.getState().status; }

    public boolean updateStatus(Status newStatus, Status currentStatus) {
        return stateSynchronizer.updateState((current, updates)-> {
            if (currentStatus.getFileId() == current.status.getFileId() &&
                    currentStatus.getTxnId().equals(current.status.getTxnId())) {
                updates.add(new StatusUpdate(newStatus));

                return true;
            } else {

                return false;
            }
        });
    }

    public static <T extends Serializable> ExactlyOnceIngestionSynchronizer createNewSynchronizer(String streamName, SynchronizerClientFactory factory) {
        return new ExactlyOnceIngestionSynchronizer(streamName,
                factory.createStateSynchronizer(streamName,
                        new StatusUpdateSerializer(),
                        new StatusInitSerializer(),
                        SynchronizerConfig.builder().build()));
    }

}
