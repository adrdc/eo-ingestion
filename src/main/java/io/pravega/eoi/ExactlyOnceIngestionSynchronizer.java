package io.pravega.eoi;

import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
    private static class StatusUpdate<T> implements Update<UpdatableStatus>, Serializable {
        private final Status status;

        @Override
        public UpdatableStatus applyTo(UpdatableStatus oldState, Revision newRevision) {
            log.trace("Applying update {} to {} ", this, oldState);
            return new UpdatableStatus(oldState.streamName, newRevision, status);
        }

    }

    @ToString
    private static class StatusInit implements InitialUpdate<UpdatableStatus>, Serializable {

        @Override
        public UpdatableStatus create(String streamName, Revision revision) {
            return new UpdatableStatus(streamName, revision, new Status(-1, UUID.randomUUID()));
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

    public void updateStatus(Status status) {
        stateSynchronizer.updateState((current, updates)-> {
            updates.add(new StatusUpdate(status));
        });
    }

    public static <T extends Serializable> ExactlyOnceIngestionSynchronizer createNewSynchronizer(String streamName, SynchronizerClientFactory factory) {
        return new ExactlyOnceIngestionSynchronizer(streamName,
                factory.createStateSynchronizer(streamName,
                        new JavaSerializer<StatusUpdate>(),
                        new JavaSerializer<StatusInit>(),
                        SynchronizerConfig.builder().build()));
    }

}
